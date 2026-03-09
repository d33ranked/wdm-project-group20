# CHECKLIST

## Two Phase Commit (2PC Implementation)

- [ ] Add `/prepare` Endpoint to Stock Service (Locks Item Stock)
- [ ] Add `/prepare` Endpoint to Payment Service (Locks User Credit)
- [ ] Add `/commit` & `/abort` Endpoints to both Stock and Payment Service
- [ ] Implement the Coordinator Logic in Order Service's `checkout`:
  - Send PREPARE to all Participants
  - If ALL say YES → Send COMMIT to ALL
  - If ANY says NO or Time Out → Send ABORT to ALL
- [ ] Handle Timeouts (Participant doesn't respond to PREPARE)
- [ ] Handle Coordinator Crash (Participants stuck in PREPARE Phase — Add timeout-based ABORT or Recovery Log)
- [ ] Use Redis Transactions (`WATCH`/`MULTI`/`EXEC` or Lua scripts) for Atomic PREPARE/COMMIT/ABORT
- [ ] Add `TRANSACTION_MODE` Feature Flag so Checkout Service switches between 2PC and SAGA
- [ ] Test: Two Concurrent Checkouts on the SAME item never Oversell

## SAGAs + Kafka (Event-Driven Architecture)

- [ ] Set up Kafka Producer in Order Service (Publishes Events on Checkout)
- [ ] Set up Kafka Consumers in Stock and Payment Services (React to Events)
- [ ] Define Event Schema (Topic Names, Message Format, Idempotency Keys)
- [ ] Implement the Orchestration Saga Flow for Checkout:
  - Order publishes `StockReserveRequested` per Item
  - Stock consumes, reserves Stock, publishes `StockReserved` or `StockReserveFailed`
  - Order collects Responses; if ALL reserved → publishes `PaymentRequested`
  - Payment consumes, deducts Credit, publishes `PaymentSuccess` or `PaymentFailed`
  - On ANY Failure → Order publishes Compensating Events (`StockRollbackRequested`)
- [ ] Make ALL Kafka Consumers Idempotent (processing SAME Event twice has no extra effect)
- [ ] Ensure Consumer Offset Commits happen only after DB Write succeeds
- [ ] Handle Event Ordering and Deduplication
- [ ] Run the Existing Consistency Test Suite and pass ALL Checks
- [ ] Integrate with the `TRANSACTION_MODE` Feature Flag (Saga Mode uses Kafka Path)

## Benchmarking Process

- [ ] Set up Locust Stress Tests beyond the provided `locustfile.py`
- [ ] Measure Baseline Latency and Throughput (current Synchronous Implementation)
- [ ] Measure SAGA (Kafka) Latency and Throughput
- [ ] Measure 2PC Latency and Throughput
- [ ] Compare the Three under Varying Concurrency Levels (10, 50, 100, 200 Users)
- [ ] Write a Consistency Benchmark: Concurrent Checkouts on Limited Stock, Verify no Oversell
- [ ] Write a Fault-Tolerance Benchmark: Kill a Service Mid-Checkout, Verify Recovery
- [ ] Document Results (Latency p50/p95/p99, Throughput req/s, Consistency Pass/Fail)
- [ ] Verify System stays Stable under 20 CPUs Max

## Fault Tolerance & Handling

- [ ] Make Stock and Payment Operations Idempotent (use Unique Transaction IDs to prevent Double-Processing)
- [ ] Implement Redis Lua Scripts or `WATCH`/`MULTI` for Atomic Read-Modify-Write (no Race Conditions)
- [ ] Add Health Check Endpoints (`/health`) to ALL Services for Docker/K8s Restarts
- [ ] Handle Kafka Consumer Crash Recovery (Consumer resumes from last committed Offset on Restart)
- [ ] Handle Mid-Transaction Service Crash:
  - Stock dies after reserving Stock but before Ack → on Restart, Pending Reservations are recovered or timed out
  - Payment dies after deducting Credit but before Ack → on Restart, Idempotency Key prevents Double Deduction
  - Order Coordinator dies Mid-Saga → on Restart, reads Saga State from DB and resumes/compensates
- [ ] Persist Saga/Transaction State in Redis so Order can recover Incomplete Checkouts on Restart
- [ ] Add Retry Logic with Exponential Backoff for Inter-Service Calls
- [ ] Test: Kill each Service Container during Checkout, Verify no Money/Stock Leaks after Recovery
- [ ] Test: Kill and Restart Kafka, Verify Consumers Reconnect and Resume
