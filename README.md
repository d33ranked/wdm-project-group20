# Distributed Data Systems — Group 24

Aditya Patil · Danil Vorotilov · Pedro Gomes Moreira · Ruben Van Seventer · Veselin Mitev

Three microservices — order, stock, payment — coordinate so that checkout never oversells stock or double-charges users. Two transaction protocols are implemented: **2PC (Two-Phase Commit)** for synchronous strongly-consistent commits, and **SAGA** for asynchronous event-driven compensation. Both expose the same HTTP API and pass the same test suite; the active protocol is chosen at deploy time with no code changes.

---

## Architecture

### Services

| Service | Port | Responsibility |
|---|---|---|
| `api-gateway` | 8001 | HTTP entry point; assigns `correlation_id`, routes to streams, blocks on response event |
| `nginx` | 8000 | Load-balances across gateway replicas |
| `order-service` | internal | Manages orders; drives checkout as 2PC coordinator or SAGA orchestrator |
| `stock-service` | internal | Manages item inventory; 2PC participant or SAGA participant |
| `payment-service` | internal | Manages user credit; 2PC participant or SAGA participant |

### Storage

Each service has a **dedicated Redis instance** (AOF persistence, `appendfsync everysec`):

- `redis-order` — order hashes, TPC transaction log (`txn:*`), SAGA state (`saga:*`)
- `redis-stock` — item hashes (`item:{id}`), prepared reservations (`prepared:stock:{txn_id}`)
- `redis-payment` — user hashes (`user:{id}`), prepared reservations (`prepared:payment:{txn_id}`)
- `redis-bus` — **shared message bus** for all Redis Streams; kept separate so killing a service's data store does not drop in-flight messages

### Communication

All inter-service communication uses **Redis Streams** (not HTTP between services). The gateway publishes requests to streams and waits on a `threading.Event` keyed by `correlation_id`. Services consume, process, and publish responses back. This gives at-least-once delivery: each consumer group re-reads pending (unACKed) messages on startup before processing new ones.

Stream layout:

```
gateway.orders     → order-service  → gateway.responses
gateway.stock      → stock-service  → gateway.responses
gateway.payment    → payment-service→ gateway.responses
internal.stock     → stock-service  → internal.responses
internal.payment   → payment-service→ internal.responses
tpc.stock          → stock-service  → tpc.responses
tpc.payment        → payment-service→ tpc.responses
```

### 2PC Protocol (TPC mode)

The order service acts as coordinator:

1. **Prepare** — publishes to `tpc.stock` and `tpc.payment`; each participant runs an atomic Lua script that checks availability, deducts optimistically, and writes a `prepared:{service}:{txn_id}` reservation key with a 600s TTL.
2. **Commit** — if both vote YES, coordinator publishes commit to both; participants drop the reservation key.
3. **Abort** — if either votes NO, coordinator publishes abort to both; participants restore the deducted amount from the reservation key.

A checkout lock (`lock:checkout:{order_id}`) prevents concurrent double-checkout. On startup, `recovery_tpc()` scans all `txn:*` keys and resolves any in-doubt transactions: `committing` → commit, anything else → abort.

### SAGA Protocol (SAGA mode)

The order service acts as orchestrator, driving a state machine: `PENDING → STOCK_REQUESTED → PAYMENT_REQUESTED → COMPLETED` (or compensation path back to `FAILED`).

1. Publishes `subtract_batch` to `internal.stock`. On success, publishes `pay` to `internal.payment`.
2. If payment fails, publishes `add_batch` to `internal.stock` as compensation.
3. State is persisted in `saga:{saga_id}` so `recovery_saga()` can resume or compensate after a coordinator crash.

### Idempotency

All state-mutating endpoints accept an optional `Idempotency-Key` header. Results are stored in Redis under `idem:{key}` (hash with `status_code` + `body`) with a 1-hour TTL. Duplicate requests within the TTL return the cached response without re-executing.

### Lua Scripts

All atomic operations are implemented as Lua scripts registered once at startup (SHA1-cached):

- `deduct_stock_batch` — checks all items have sufficient stock before deducting any; returns `INSUFFICIENT` or `NOT_FOUND` sentinels on failure
- `restore_stock_batch` — unconditionally restores stock
- `prepare_stock_batch` — deducts + writes reservation key
- `commit_stock` / `abort_stock` — resolves reservation
- `prepare_payment` / `commit_payment` / `abort_payment` — same pattern for credit

---

## How To Run

### Prerequisites

- Docker and Docker Compose
- Python 3.11+

### Start The Stack

```bash
# TPC mode (default)
docker compose up -d

# SAGA mode
TRANSACTION_MODE=SAGA NGINX_CONF=gateway_nginx_saga.conf docker compose up -d
```

The `TRANSACTION_MODE` env var is read by order, stock, and payment services at startup. The `NGINX_CONF` var selects the nginx routing config (TPC routes go to `/stock/prepare`, `/stock/commit`, etc.; SAGA mode omits those).

### Stop

```bash
docker compose down -v
```

---

## Test Suite

The test suite lives in `test/custom/`. It is a standalone CLI that starts the stack, runs all test cases, and reports results. No external test framework is required.

### Run

```bash
cd test/custom
pip install -r ../../requirements.txt

# TPC (default)
python run.py

# SAGA
python run.py --mode SAGA

# Skip rebuild (stack already running with correct mode)
python run.py --mode TPC --skip-build --no-restart

# Custom gateway address
BASE_URL=http://192.168.1.10:8000 python run.py --mode TPC
```

Flags:

| Flag | Effect |
|---|---|
| `--mode TPC\|SAGA` | Protocol to test (default: TPC) |
| `--skip-build` | Skip `docker compose build` |
| `--no-restart` | Skip `docker compose down/up`; reuse running stack |

### Structure

Three test files, no external framework:

| File | Cases | What It Tests |
|---|---|---|
| `test_common.py` | 29 | Mode-agnostic correctness, idempotency, storage, fault injection |
| `test_tpc.py` | 12 | 2PC-specific: prepare/commit/abort, locking, coordinator crash recovery |
| `test_sagas.py` | 8 | SAGA-specific: compensation, coordinator crash, stream re-delivery |

Each test case name is prefixed in the output with a **blue container label** indicating which service or Redis instance to inspect if the test fails:

```
  [1/29] [order-service] Multi-Item Checkout With Per-Item Stock Verification
    [PASS] Checkout Succeeds For An Order Containing Three Different Items
    [PASS] Item 1 (Price=10, Qty=2) Stock Decreased From 20 To 18
    ...

  [2/29] [stock-service] External Stock Change Between Order And Checkout
    [PASS] Checkout Rejected — Order Needs 8 Units But Only 5 Remain
    ...
```

### Common Tests (29 cases)

**Checkout correctness** — multi-item checkout with per-item stock verification; double checkout rejected with no double charge; items added to paid order do not re-trigger charges; empty order checkout is a no-op.

**Concurrency** — 10 users race for 1 unit: exactly 1 wins, stock never goes negative; 5 independent users checkout in parallel with no cross-contamination; 20 concurrent users compete for 3 units with no oversell.

**Boundary conditions** — checkout succeeds when balance exactly equals cost; checkout succeeds when stock exactly equals quantity; one credit short is correctly rejected; one unit short is correctly rejected.

**Storage / idempotency** — batch init creates integer-keyed items and users; add-stock idempotency key prevents double-add; pay idempotency key prevents double-deduct; addItem with same key merges quantity instead of duplicating; zero/negative amounts are rejected with 4xx.

**Fault injection** — malformed stream message: consumer survives and continues processing; payment Redis restart: AOF ensures all written credit survives; stock Redis restart: AOF ensures all written stock survives; stock service crash mid-batch: winners equal charged users, no units lost; Redis bus restart: stream consumer groups re-establish and checkouts succeed.

### 2PC Tests (12 cases)

**Protocol correctness** — PREPARE reserves units and reduces visible stock; COMMIT makes the deduction permanent; ABORT fully restores the reserved amount; services vote NO when resources are insufficient with no state change.

**Locking** — PREPARE locks units from concurrent regular operations; ABORT releases locks for subsequent use; two competing PREPAREs on the same stock: second correctly votes NO.

**Idempotency** — double COMMIT is a no-op with no double deduction; ABORT on non-existent or already-committed transaction returns 200 safely.

**End-to-end** — checkout where stock votes NO: coordinator aborts all, payment untouched; checkout where payment votes NO: coordinator aborts, stock reservation reversed.

**Recovery** — participant crash: coordinator retries via TPC stream pending re-delivery; coordinator crash: injected stuck `preparing_payment` transaction is deterministically aborted on restart, stock restored.

### SAGA Tests (8 cases)

**Compensation** — payment failure after stock reservation: compensating `add_batch` restores all stock; stock failure: payment is never attempted; multi-item compensation: all 3 items fully restored when payment fails.

**Double-checkout prevention** — second checkout of a COMPLETED saga returns 4xx; stock and credit remain at post-first-checkout values.

**Stream semantics** — 20 simultaneous checkouts each get their own correct response (correlation isolation); stock service crash with pending add-stock message: on restart the pending entry is re-delivered and stock reaches the expected value.

**Coordinator recovery** — stuck `STOCK_REQUESTED` saga injected directly into Redis: on order service restart, recovery resolves the saga either by completing or compensating, leaving a fully consistent state.

---

## Parameter Tuning

| Parameter | Location | Default | Effect |
|---|---|---|---|
| `TRANSACTION_MODE` | `docker-compose.yml` env | `TPC` | Protocol used by all three services |
| `NGINX_CONF` | `docker-compose.yml` volumes | `gateway_nginx.conf` | Nginx routing config |
| `REQUEST_TIMEOUT_MS` | `docker-compose.yml` env | `30000` | Gateway response timeout |
| `TPC_TIMEOUT_S` | `order/tpc.py` | `15` | Max wait per TPC stream response |
| Gunicorn workers | `docker-compose.yml` command | `1` | `-w N` per service |
| AOF sync | `redis-*.command` | `everysec` | Durability vs latency tradeoff |
