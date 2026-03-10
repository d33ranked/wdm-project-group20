# PROJECT TRACKER

Central reference for implementation details and pending work.
Content here feeds directly into the final README.

---

## Two-Phase Commit

### Coordinator (Order Service)

The order service acts as the 2PC coordinator. `checkout_tpc()` drives the protocol.

- **State machine** — `started → preparing_stock → preparing_payment → committing → committed` on the happy path; any vote-NO or failure branches to `aborting → aborted`. Every state transition is `conn.commit()`'d to PostgreSQL *before* the next external call. This is the durable coordinator log — if the process crashes at any point, the last persisted state tells recovery exactly where to resume.
- **Progressive prepare persistence** — `prepared_stock` (JSONB) is appended after each successful per-item prepare and flushed to disk. On crash mid-prepare, recovery knows exactly which participants were prepared and need an abort.
- **Row-level locking** — `SELECT ... FOR UPDATE` on the order row serialises concurrent checkouts of the same order. Two threads hitting `/checkout/<same_order>` will not interleave; the second blocks until the first completes.
- **Recovery** — `recovery_tpc()` runs at startup before the app accepts requests. Scans `transaction_log` for non-terminal states. Anything in `{started, preparing_stock, preparing_payment, aborting}` gets aborted. Anything in `{committing}` gets committed. This guarantees that no in-doubt transaction survives a restart.

### Participants (Stock & Payment Services)

Both follow the same pessimistic reservation pattern.

- **PREPARE** — immediately deducts the resource (stock or credit) and writes a `prepared_transactions` record. The deduction is real at prepare time; the record exists solely to enable rollback.
- **COMMIT** — deletes the `prepared_transactions` record. The deduction is already in place, so commit is just cleanup. Idempotent by nature: committing a non-existent record is a no-op DELETE.
- **ABORT** — reads the `prepared_transactions` record, restores the resource, deletes the record. Also idempotent: aborting a non-existent or already-aborted record changes nothing.
- **Stock compound PK** — `(txn_id, item_id)` because one checkout can reserve multiple items. Payment uses `txn_id` alone (one charge per transaction).
- **Participant timeout** — on startup, both services auto-abort any `prepared_transactions` older than 5 minutes. Safety net for the case where the coordinator died and never sent commit/abort.

### Idempotency

- **Key construction** — every inter-service call carries an `Idempotency-Key` header in the form `{txn_id}:{service}:{operation}:{resource_id}`. Deterministic and unique per side-effect.
- **Advisory lock** — `pg_advisory_xact_lock(md5(key) % 2^31)` serialises concurrent requests with the same key at the database level, scoped to the current transaction. Prevents the race between "check if key exists" and "insert new key".
- **Cached response** — `idempotency_keys` table stores `(key, status_code, body)`. Duplicate requests return the cached response without re-executing the operation.

### Retry & Backoff

- `send_post_request()` retries up to 3 times with exponential backoff: 100ms, 200ms, 400ms.
- 2xx and 4xx are terminal (business success or business failure — no retry). 5xx triggers retry.
- Per-request timeout of 5 seconds.

---

## SAGA (Kafka Orchestration)

### API Gateway

HTTP-to-Kafka bridge. Accepts REST requests from clients, assigns a correlation ID (UUID), publishes the request envelope to the service's topic (`gateway.orders`, `gateway.stock`, `gateway.payment`), and blocks until a correlated response arrives on `gateway.responses`. A single shared `KafkaConsumer` on `gateway.responses` handles all in-flight requests — each request registers a `threading.Event` keyed by correlation ID; the consumer sets the matching event when the response arrives. Timeout is 30 seconds; unmatched or late responses are silently dropped. This avoids the O(N) anti-pattern of one consumer per request.

### Dual Kafka Cluster

Two separate KRaft-mode brokers isolate traffic domains:

- **kafka-external** — carries gateway-to-service messages (`gateway.`* topics). The API gateway publishes here; each service's gateway consumer reads its own topic.
- **kafka-internal** — carries inter-service saga messages (`internal.stock`, `internal.payment`, `internal.responses`). The order service orchestrates the saga by publishing commands here; stock and payment publish responses back.

Separation prevents gateway traffic from interfering with internal saga round-trips and vice versa. Both brokers run 3 partitions per topic, replication factor 1.

### Nginx Config Swap

In TPC mode, Nginx routes `/orders/`, `/stock/`, `/payment/` directly to each service's HTTP endpoint (`gateway_nginx.conf`). In SAGA mode, `run.py` patches `docker-compose.yml` to mount `gateway_nginx_saga.conf`, which proxies all traffic to the API gateway (`api-gateway:8000`). The swap is transparent — `BASE_URL=http://localhost:8000` works identically for both modes.

### Orchestrator (Order Service)

The order service is the saga orchestrator. `handle_checkout_saga()` initiates the flow when a checkout message arrives from the gateway consumer.

- **State machine** — `STOCK_REQUESTED → PAYMENT_REQUESTED → COMPLETED` on the happy path. Payment failure branches to `ROLLBACK_REQUESTED → ROLLED_BACK`. Stock failure goes directly to `STOCK_FAILED`. Each state transition is persisted to the `sagas` table (`conn.commit()`) before the next Kafka publish. The persisted state is the durable saga log.
- **Correlation-based routing** — each sub-step uses a deterministic correlation ID: `{saga_id}:stock:subtract_batch`, `{saga_id}:payment:pay`, `{saga_id}:stock:rollback`. The internal response consumer matches these suffixes to dispatch to the correct handler (`_on_stock_response`, `_on_payment_response`, `_on_rollback_response`).
- **State guards** — before advancing, the handler acquires a `FOR UPDATE` lock on the saga row and verifies the current state matches the expected state. Duplicate or out-of-order responses are silently skipped. This is the second layer of protection against Kafka redelivery, complementing idempotency.
- **Compensating transaction** — on payment failure, `_fire_rollback()` publishes `add_batch` to stock, reversing the earlier subtraction. On rollback completion, the saga transitions to `ROLLED_BACK` and the gateway receives a 400.

### Participants (Stock & Payment — Kafka Mode)

Both services run a Kafka consumer on their `gateway.`* topic (direct CRUD from the API gateway) and on `internal.`* topics (saga commands from the order service).

- **Kafka routing** — `route_kafka_message()` dispatches by HTTP method and path segments, mirroring the Flask routing logic. Returns `(status_code, body)`.
- **Idempotency** — `check_idempotency_kafka()` / `save_idempotency_kafka()` use the same `idempotency_keys` table as TPC mode but without advisory locks (single-threaded consumer poll loop removes the concurrent-request race).
- **Manual offset commit** — `enable_auto_commit=False`. Offset committed only after the handler returns and the DB transaction succeeds. On crash between DB commit and offset commit, the message is redelivered — idempotency prevents double-processing.
- **Response publishing** — after processing a saga command, the participant publishes the result to `internal.responses` with the same correlation ID. The order service's internal consumer picks it up and advances the saga.

### Idempotency in SAGA Mode

- **Key construction** — saga sub-steps use `{saga_id}:{service}:{operation}` (e.g. `abc-123:stock:subtract_batch`, `abc-123:payment:pay`). Deterministic and unique per side-effect.
- **Cached response** — same `idempotency_keys` table as TPC. Duplicate messages return the cached result without re-executing.
- **Gateway strips client keys** — the API gateway removes any client-supplied `Idempotency-Key` header. Idempotency is an internal concern; keys are generated by the order service per saga step, not by external clients.

### Recovery

`recovery_saga()` runs on order service startup, after Kafka producers are built but before consumers start. Scans the `sagas` table for non-terminal states and re-publishes the pending Kafka message:

- `STOCK_REQUESTED` → re-publishes `subtract_batch` to `internal.stock`
- `PAYMENT_REQUESTED` → re-publishes `pay` to `internal.payment`
- `ROLLBACK_REQUESTED` → re-publishes `add_batch` to `internal.stock`

Re-publishing is safe because all downstream consumers are idempotent (same key = cached result). The internal consumer starts after recovery, picks up responses, and the saga completes normally. No in-flight saga survives a restart.

---

## Infrastructure

- **Gunicorn + gevent** — cooperative multitasking via greenlets. Each worker handles many concurrent I/O-bound requests without OS thread overhead. Monkey-patching (`gevent.monkey.patch_all()`) at import time makes stdlib blocking calls non-blocking.
- **Connection pooling** — `psycopg2.pool.ThreadedConnectionPool(minconn=10, maxconn=100)`. Connection acquired in `before_request`, returned in `teardown_request`. Auto-commit on success, rollback on exception.
- **Nginx gateway** — reverse proxy on port 8000. In TPC mode: path-based routing directly to services. In SAGA mode: all traffic proxied to the API gateway, which routes through Kafka.
- **Dual Kafka (KRaft)** — two KRaft-mode brokers (`kafka-external`, `kafka-internal`) with 3 partitions per topic, replication factor 1. Auto-topic creation enabled.
- **PostgreSQL per service** — data sovereignty. Each service owns its schema. No cross-service database access. `init.sql` creates tables on first container start.
- **Feature flag** — `TRANSACTION_MODE` environment variable (`TPC` or `SAGA`) in docker-compose. Order service reads it at startup and routes `/checkout` to the corresponding implementation.
- **Restart policy** — `restart: on-failure` on all application containers. Services auto-restart on crash; Kafka consumers reconnect in a `while True` loop with 3-second backoff.
- **Health endpoints** — `/health` on all services and the API gateway. Used by `wait_for_services()` in the test runner to confirm readiness before test execution.

---

## Test Suite

### Runner (`test/custom/run.py`)

Interactive runner: prompts for mode → patches `docker-compose.yml` (swaps `TRANSACTION_MODE` + nginx config) → `docker compose down -v` → build → up → polls stock, payment, and order endpoints until all respond with 2xx → runs the selected suites with Enter-to-proceed between test cases. Final summary: pass/fail counts per suite and overall.

### Common Suite — 14 Tests (mode-agnostic)

Correctness, consistency, concurrency, boundaries, edge cases. Validates: multi-item checkout math, double-checkout prevention, post-checkout tampering, empty orders, 10-way contention for 1 unit, sequential stock drain, parallel isolation, stale-snapshot rejection, late funding, exact-boundary success, off-by-one rejection, non-existent resource handling.

### 2PC Suite — 12 Tests

Protocol-level validation: prepare→commit permanence, prepare→abort full restoration, vote-NO on insufficient resources, prepare locks resources from regular operations, abort frees locked resources, competing prepares, idempotent commit (no double deduction), abort safety (non-existent and already-committed), coordinator-driven checkout with stock/payment vote-NO, participant crash recovery (stop stock → restart → checkout retries and succeeds), coordinator crash recovery (kill order service mid-checkout → restart → state is fully committed or fully rolled back, never half-and-half).

### SAGA Suite — 4 Tests

Compensating transaction: payment fails (insufficient credit) → saga fires `add_batch` rollback → stock restored, credit unchanged, order not paid. Stock reservation fails: requested quantity exceeds available stock → saga never reaches payment → everything unchanged. Participant crash: stock service killed mid-saga → message persists in Kafka → stock restarts, consumer reconnects, processes message → saga completes with correct deductions. Coordinator crash: stuck saga injected directly into order-db (state `STOCK_REQUESTED`, stock already subtracted, idempotency key cached in stock-db) → order service restarted → `recovery_saga()` re-publishes to stock (idempotent cached return) → saga advances through payment → fully committed. Without recovery, state is inconsistent (stock subtracted but payment never charged) — neither committed nor rolled back.

---

## Pending

### Scalability

- Service replication (`deploy.replicas`) behind Nginx
- Nginx load balancing across replicas
- Kafka consumer groups: partition-aware across replicas
- Connection pool tuning under replication
- Docker resource limits (`cpus`, `mem_limit`) within 20 CPU cap

### Benchmarking

- Strong Benchmarking Suite Cases

- Parameter Tuning + Increasing RPS
- Structured Locust scenarios (create → add items → checkout)
- Latency (p50/p95/p99) and throughput (req/s) for TPC
- Latency and throughput for SAGA
- TPC vs SAGA comparison at 10, 50, 100, 200 concurrent users
- Consistency stress test: N concurrent checkouts on limited stock
- Fault tolerance stress test: kill container mid-load, verify recovery
- Results documented with tables/charts

### Additional Fault Tolerance

- Docker healthcheck directives in `docker-compose.yml`
- Stale saga cleanup (stuck sagas compensated after timeout threshold)
- Idempotency key TTL / periodic pruning

### Bonus

- Kafka dead-letter topic for poison messages
- Structured logging with correlation/saga IDs across services

