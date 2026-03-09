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

## Infrastructure

- **Gunicorn + gevent** — cooperative multitasking via greenlets. Each of the 2 workers per service can handle many concurrent I/O-bound requests without OS thread overhead. Monkey-patching (`gevent.monkey.patch_all()`) at import time makes stdlib blocking calls non-blocking.
- **Connection pooling** — `psycopg2.pool.ThreadedConnectionPool(minconn=10, maxconn=100)`. Connection acquired in `before_request`, returned in `teardown_request`. Auto-commit on success, rollback on exception.
- **Nginx gateway** — reverse proxy on port 8000, 2048 worker connections. Path-based routing (`/orders/`, `/payment/`, `/stock/`) with trailing-slash `proxy_pass` to strip the prefix.
- **PostgreSQL per service** — data sovereignty. Each service owns its schema. No cross-service database access. `init.sql` creates tables on first container start.
- **Feature flag** — `TRANSACTION_MODE` environment variable (`TPC` or `SAGA`) in docker-compose. Order service reads it at startup and routes `/checkout` to the corresponding implementation.

---

## Test Suite

### Runner (`test/custom/run.py`)

Interactive runner: prompts for mode → patches `docker-compose.yml` → `docker compose down -v` → build → up → polls until services respond → runs the selected suites with Enter-to-proceed between test cases. Final summary: pass/fail counts per suite and overall.

### Common Suite — 14 Tests (mode-agnostic)

Correctness, consistency, concurrency, boundaries, edge cases. Validates: multi-item checkout math, double-checkout prevention, post-checkout tampering, empty orders, 10-way contention for 1 unit, sequential stock drain, parallel isolation, stale-snapshot rejection, late funding, exact-boundary success, off-by-one rejection, non-existent resource handling.

### 2PC Suite — 12 Tests

Protocol-level validation: prepare→commit permanence, prepare→abort full restoration, vote-NO on insufficient resources, prepare locks resources from regular operations, abort frees locked resources, competing prepares, idempotent commit (no double deduction), abort safety (non-existent and already-committed), coordinator-driven checkout with stock/payment vote-NO, participant crash recovery (stop stock → restart → checkout retries and succeeds), coordinator crash recovery (kill order service mid-checkout → restart → state is fully committed or fully rolled back, never half-and-half).

---

## Pending

### 1. SAGA via Kafka (Core Requirement)

- [ ] Kafka producer in Order service (publish saga commands on checkout)
- [ ] Kafka consumer in Stock service (reserve/rollback on command, publish result)
- [ ] Kafka consumer in Payment service (pay/refund on command, publish result)
- [ ] Saga state machine in Order DB (`saga_log` table: saga_id, step, status, timestamps)
- [ ] Orchestration flow: `StockReserve → StockReserved/StockFailed → PaymentDeduct → PaymentSuccess/PaymentFailed → compensating commands on failure`
- [ ] Idempotent consumers (dedup by saga ID, exactly-once processing semantics)
- [ ] Manual offset commit only after DB write succeeds
- [ ] Saga timeout: if no reply within threshold, trigger compensation
- [ ] Recovery on Order service restart: scan `saga_log` for incomplete sagas, resume or compensate
- [ ] Replace synchronous `checkout_saga()` with Kafka-driven async path
- [ ] Wire `TRANSACTION_MODE=SAGA` end-to-end

### 2. SAGA Test Suite

- [ ] Compensating transaction: stock reserved but payment fails → stock rolled back
- [ ] Compensating transaction: payment fails → verify no money lost
- [ ] Eventual consistency: checkout returns quickly, final state converges
- [ ] Concurrent SAGA checkouts on limited stock — no oversell
- [ ] Double checkout prevention under SAGA mode
- [ ] Consumer crash mid-saga → restart → saga completes or compensates
- [ ] Order service crash mid-saga → restart → saga resolves consistently
- [ ] Kafka broker restart → consumers reconnect and resume

### 3. Fault Tolerance

- [ ] `/health` endpoint on all services
- [ ] Docker healthcheck directives in `docker-compose.yml`
- [ ] Kafka consumer reconnect on broker unavailability
- [ ] Stale saga cleanup (stuck sagas get compensated after timeout)
- [ ] Idempotency key TTL / periodic pruning

### 4. Scalability

- [ ] Service replication (`deploy.replicas`) behind Nginx
- [ ] Nginx load balancing across replicas
- [ ] Kafka consumer groups: partition-aware across replicas
- [ ] Connection pool tuning under replication
- [ ] Docker resource limits (`cpus`, `mem_limit`) within 20 CPU cap

### 5. Benchmarking

- [ ] Structured Locust scenarios (create → add items → checkout)
- [ ] Latency (p50/p95/p99) and throughput (req/s) for TPC
- [ ] Latency and throughput for SAGA
- [ ] TPC vs SAGA comparison at 10, 50, 100, 200 concurrent users
- [ ] Consistency stress test: N concurrent checkouts on limited stock
- [ ] Fault tolerance stress test: kill container mid-load, verify recovery
- [ ] Results documented with tables/charts

### 6. Bonus

- [ ] Event-driven architecture (covered by SAGA/Kafka)
- [ ] Custom benchmark suite beyond provided tests
- [ ] Kafka dead-letter topic for poison messages
- [ ] Structured logging with correlation/saga IDs across services
