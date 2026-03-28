# Distributed Data Systems — Group 24

Aditya Patil · Danil Vorotilov · Pedro Gomes Moreira · Ruben Van Seventer · Veselin Mitev

Three microservices (order, stock, payment) coordinate checkouts so that stock is never oversold and users are never double-charged. Two protocols are implemented: **2PC** for synchronous strongly-consistent commits, and **SAGA** for asynchronous event-driven compensation. Both expose the same HTTP API. The active protocol is selected at deploy time.

---

## Architecture

### Services

| Service | Responsibility |
|---|---|
| `nginx` (port 8000) | Public entry point; routes requests to services or gateway |
| `api-gateway` (port 8001) | SAGA mode only — assigns `correlation_id`, bridges HTTP to streams |
| `order-service` | Manages orders; drives checkout as 2PC coordinator or SAGA orchestrator |
| `stock-service` | Manages inventory; 2PC/SAGA participant |
| `payment-service` | Manages user credit; 2PC/SAGA participant |

### Storage

Each service has a dedicated Redis instance with AOF persistence (`appendfsync everysec`):

- `redis-order` — orders, TPC transaction log (`txn:*`), SAGA state (`saga:*`)
- `redis-stock` — item hashes, prepared stock reservations
- `redis-payment` — user hashes, prepared payment reservations
- `redis-bus` — shared message bus; kept separate so crashing a service's Redis does not drop in-flight stream messages

### Inter-service Communication

All inter-service calls go through **Redis Streams**, not HTTP. This gives at-least-once delivery — consumers re-read unACKed (pending) messages on startup before processing new ones.

```
TPC mode:   tpc.stock / tpc.payment → tpc.responses
SAGA mode:  gateway.{orders,stock,payment} → gateway.responses
            internal.{stock,payment} → internal.responses
```

### 2PC Protocol

1. **Prepare** — coordinator publishes to `tpc.stock` and `tpc.payment`. Each participant runs a Lua script that checks availability, deducts optimistically, and writes a `prepared:{service}:{txn_id}` reservation key (600s TTL).
2. **Commit** — both vote YES → coordinator fires commit to both (fire-and-forget; decision is final once written to AOF). Participants drop the reservation key.
3. **Abort** — either votes NO → coordinator aborts both; participants restore deducted amounts from the reservation key.

A checkout lock (`checkout-lock:{order_id}`) prevents concurrent duplicate checkouts. `recovery_tpc()` runs at startup: `committing` → re-commit, anything else → abort.

### SAGA Protocol

Orchestrator state machine: `PENDING → STOCK_REQUESTED → PAYMENT_REQUESTED → COMPLETED` (or compensation path → `FAILED`).

1. Publishes `subtract_batch` to `internal.stock`. On success, publishes `pay` to `internal.payment`.
2. If payment fails, publishes `add_batch` (compensating transaction) to `internal.stock`.
3. State persisted in `saga:{saga_id}`; `recovery_saga()` resumes or compensates after a coordinator crash.

### Atomicity

All read-modify-write operations are implemented as **Lua scripts** registered at startup (SHA1-cached). This includes stock reservation, payment reservation, commit, abort, and compensation. Scripts return sentinel strings (`INSUFFICIENT`, `NOT_FOUND`) on failure so the caller can act without a second round trip.

---

## How To Run

### Requirements

- Docker + Docker Compose
- Python 3.11+ (test suite only)

### Start

```bash
# 2PC mode (default)
docker compose up -d --build

# SAGA mode
TRANSACTION_MODE=SAGA NGINX_CONF=gateway_nginx_saga.conf docker compose up -d --build
```

### Stop

```bash
docker compose down -v
```

### Configuration

These can be passed as env vars before `docker compose up` or written to a `.env` file:

| Variable | Default | Effect |
|---|---|---|
| `TRANSACTION_MODE` | `TPC` | Protocol used by all three services (`TPC` or `SAGA`) |
| `NGINX_CONF` | `gateway_nginx.conf` | Nginx routing config (use `gateway_nginx_saga.conf` for SAGA) |
| `REDIS_MAX_CONNECTIONS` | `6000` | Connection pool size per Redis pool per service; sized for 5000+ concurrent users |
| `STREAM_BATCH_SIZE` | `500` | Messages pulled per `XREADGROUP` call; processed concurrently via gevent |

Example `.env` for a smaller local machine:

```env
REDIS_MAX_CONNECTIONS=1200
STREAM_BATCH_SIZE=100
```

---

## Test Suite

Located in `test/custom/`. Standalone CLI — no external test framework. Starts the stack, runs all cases, reports results.

### Run

```bash
cd test/custom
pip install -r ../../requirements.txt

python run.py                                    # TPC, full build + restart
python run.py --mode SAGA                        # SAGA mode
python run.py --skip-build --no-restart          # reuse already-running stack
BASE_URL=http://192.168.1.10:8000 python run.py  # custom gateway address
```

### Structure

| File | Cases | Covers |
|---|---|---|
| `test_common.py` | 29 | Checkout correctness, concurrency, boundary conditions, idempotency, fault injection |
| `test_tpc.py` | 12 | 2PC prepare/commit/abort, locking, coordinator and participant crash recovery |
| `test_sagas.py` | 8 | Compensation, double-checkout prevention, stream re-delivery, coordinator recovery |

Each test prefixes its output with a **blue container label** indicating which service to inspect on failure:

```
[1/29] [order-service] Multi-Item Checkout With Per-Item Stock Verification
  [PASS] Checkout Succeeds For An Order Containing Three Different Items
  [PASS] Item Stock Decreased By The Correct Quantity

[2/29] [redis-bus] Malformed Stream Message: Consumer Survives And Continues
  [PASS] Consumer Processes Next Valid Message After Malformed Entry
```

### What Each Suite Proves

**Common (29)** — checkout never oversells or double-charges across normal, boundary, and concurrent cases; idempotency keys prevent duplicate mutations; AOF durability survives Redis restarts; consumers survive malformed messages and service crashes.

**2PC (12)** — prepare/commit/abort are individually correct and idempotent; competing prepares on the same resource correctly yield one YES and one NO; the coordinator correctly aborts all participants when any one votes NO; crash recovery deterministically resolves all in-doubt transactions.

**SAGA (8)** — compensating transactions fully restore state when payment fails; correlation IDs correctly isolate concurrent checkouts; pending stream messages survive service restarts and are re-delivered; stuck sagas are resolved by recovery on restart.
