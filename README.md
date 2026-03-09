# Web-Scale Data Management — Group 20

## How To Test

```bash
cd test/custom
pip install requests
python run.py
```

The runner prompts for a transaction mode (`TPC` or `SAGA`), then automatically tears down Docker, rebuilds all containers, waits for services to become healthy, and runs the test suites. Press **Enter** between test cases to proceed.

When run with **TPC**, the runner executes the Common Suite followed by the 2PC Suite.
When run with **SAGA**, the runner executes the Common Suite followed by the SAGA Suite *(SAGA suite to be added)*.

---

## Common Suite (14 Test Cases — Both Modes)

These tests validate correctness that must hold regardless of the underlying transaction protocol.

| # | Test Case | What It Verifies |
|---|-----------|------------------|
| 1 | Multi-Item Checkout With Per-Item Stock Verification | An order with three items at different prices checks out; each item's stock decreases by its quantity and the total charge is the correct sum |
| 2 | Double Checkout On A Paid Order | Checking out the same order twice does not double-deduct stock or double-charge the user |
| 3 | Add Items To A Paid Order And Re-Checkout | Adding items to an already-paid order and re-checking out causes no additional financial impact |
| 4 | Checkout An Empty Order (No Items) | An order with zero items does not crash the server |
| 5 | 10 Concurrent Checkouts For 1 Unit Of Stock | 10 users race for a single unit; exactly 1 wins, 9 are rejected, no oversell, only the winner is charged |
| 6 | Sequential Checkouts Until Stock Exhausted | Three orders of 2 units each against stock of 5; first two succeed, third is rejected at the boundary |
| 7 | 5 Independent Checkouts In Parallel | Five users with separate items check out simultaneously; all succeed without cross-contaminating each other's data |
| 8 | External Stock Change Between Order And Checkout | Stock is reduced externally after the order is created; checkout uses the current stock, not a stale snapshot |
| 9 | Fund User After Order Creation, Then Checkout | User has 0 credit when the order is created, funds are added afterwards; checkout uses the current balance |
| 10 | Boundary: Balance Exactly Equals Order Total | Checkout succeeds and balance drops to exactly 0 |
| 11 | Boundary: Stock Exactly Equals Order Quantity | Checkout succeeds and stock drops to exactly 0 |
| 12 | Boundary: One Credit Short Of Order Total | Order costs 100, user has 99; checkout is rejected, nothing changes |
| 13 | Boundary: One Stock Unit Short Of Order Quantity | Order needs 5, stock has 4; checkout is rejected, nothing changes |
| 14 | GET On Non-Existent Stock, User, And Order IDs | Querying random UUIDs returns a client error, not a server crash |

---

## 2PC Suite (12 Test Cases — TPC Mode Only)

These tests target the Two-Phase Commit protocol: prepare/commit/abort correctness, resource locking, competing transactions, idempotency, mixed-vote scenarios, and failure recovery.

| # | Test Case | What It Verifies |
|---|-----------|------------------|
| 1 | PREPARE → COMMIT On Stock And Payment | Prepare reserves resources; commit makes the deduction permanent on both services |
| 2 | PREPARE → ABORT On Stock And Payment | Prepare reserves resources; abort fully restores them on both services |
| 3 | Vote NO When Resources Are Insufficient | Requesting more than available stock or credit is rejected; nothing is reserved |
| 4 | PREPARE Locks Resources From Regular Operations | After prepare holds 8 of 10 stock, a regular subtract of 5 fails because only 2 are free |
| 5 | ABORT Releases Locked Resources For Reuse | After prepare and abort, the full stock capacity is available again for regular operations |
| 6 | Two Competing PREPAREs On The Same Stock | Two transactions each request 7 from stock of 10; first succeeds, second votes NO |
| 7 | Idempotent COMMIT — No Double Deduction | Committing the same transaction twice does not deduct stock or credit a second time |
| 8 | ABORT On Non-Existent And Already-Committed Transactions | Aborting a non-existent or already-committed transaction returns 200 and changes nothing |
| 9 | 2PC Checkout: Stock Votes NO, Coordinator Aborts All | Order requests more stock than available; checkout fails, payment is never charged |
| 10 | 2PC Checkout: Payment Votes NO, Stock Reservation Rolled Back | Stock votes YES but payment votes NO; the coordinator aborts and stock reservation is fully reversed |
| 11 | Participant Crash: Stock Dies Mid-Checkout And Recovers | Stock service is stopped during checkout and restarted after 3 seconds; the order service retries and the checkout completes successfully |
| 12 | Coordinator Crash: Order Service Killed After PREPARE, Recovers Consistently | The order service is killed mid-checkout after participants have prepared; on restart it resolves the in-doubt transaction so the final state is either fully committed or fully rolled back — never half-and-half |
