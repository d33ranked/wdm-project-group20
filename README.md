# Distributed Data Management — Group 20

Aditya Patil · Danil Vorotilov · Pedro Gomes Moreira · Ruben Van Seventer · Veselin Mitev

## Deployment

### Requirements

- Docker v25+
- Docker Compose v5+
- Docker Buildx v33+
- Python v3.9+

Verify if you have the required versions with:
```bash
docker --version
docker compose version
docker buildx version
python --version
```

### Start

```bash
python start.py
```

The Launcher Asks Five Short Questions and Handles Everything Else.

| Prompt | Options | What It Does |
| --- | --- | ---- |
| **Mode?** | TPC / SAGA | Selects the Transaction Protocol |
| **Action?** | Start Stack / Run Tests | Builds and starts the system  or runs our [full test suite (additioanl requirements apply)](#test-suite). |
| **Replicas?** | Defaults / Optimized 50 CPUs / Custom | How many replicas of the different scalable services should we have? The first options are presets, while the last allows you to tune the individually tune the number of replicas for each service. |
| **Resource Limits?** | No Limits / Shared Core / One Core Per Container | Limits for the CPU resources available to the docker containers. Shared core means that all containers will share the same dedicated core. One per container means that each container will have its own dedicated core; this requires that the host has enough cores. |
| **Pool And Stream Batch?** | Defaults / Custom | Tunes the Redis Connection Pool Size and Stream Batch Size |
| **Skip Build?** *(Run Tests Only)* | Yes / No | Skips `docker compose build` if Images Are Already Up to Date |

### Stop

After the benchmark finishes the stack is torn down automatically. On startup, `start.py` will stop the currently running instance of this compose stack, if any. In `Start Stack` mode, the the `start.py` script will exit once the stack is fully started, thus, the stack will continue running.  You can stop it with:

```bash
docker compose down -v --remove-orphans
```

### Manual Deployment
If you'd like to start the system manually, you must first ensure that you have the [required environment variables set](#configuration). Then, you can build and start the compose stack with:

```bash
docker compose up --build -d --wait
```

#### Configuration

`start.py` Sets All Variables Interactively. For Headless or CI Runs, Export Them as Environment Variables or Place a `.env` File at the Project Root — Docker Compose Picks It Up Automatically.


| Variable | Default | Description |
| --- | --- | --- |
| `TRANSACTION_MODE` | `SAGA` | Protocol to Use. `TPC` for Two-Phase Commit, `SAGA` for Event-Driven Compensation. |
| `NGINX_CONF` | `gateway_nginx_saga.conf` | Nginx Config File. Must Match the Selected `TRANSACTION_MODE`. |
| `GATEWAY_REPLICAS` | `1` | Number of API Gateway Containers. |
| `ORDER_REPLICAS` | `2` | Number of Order Service Containers. |
| `STOCK_REPLICAS` | `2` | Number of Stock Service Containers. |
| `PAYMENT_REPLICAS` | `2` | Number of Payment Service Containers. |
| `REDIS_MAX_CONNECTIONS` | `6000` | Connection Pool Size Per Redis Pool Per Service Worker. |
| `STREAM_BATCH_SIZE` | `100` | Messages Fetched Per `XREADGROUP` Call. All Messages in a Batch Are Processed Concurrently via `gevent`. |
| `DDM_SKIP_BUILD` | *(unset)* | Set to `1` to Skip `docker compose build` When Running the Test Suite Headlessly. |
| `DDM_NO_RESTART` | *(unset)* | Set to `1` to Skip `docker compose down/up` and Reuse the Running Stack. |
| `CPU_LIMIT` | `0` | When set to `0`, the containers won't be limited by the CPU time that they use. When set to `1`, each container will be limited to using no more than 1 (potentially virtual) CPU core worth of CPU time. Corresponds to the `services/cpus` docker compose attribute. |
| `*_CPUSET` (In particular, `*` can be replaced by: `GATEWAY`, `ORDER`, `STOCK`, `PAYMENT`, `NGINX`, `REDIS_ORDER`, `REDIS_STOCK`, `REDIS_PAYMENT`, `REDIS_BUS`, `REDIS_ORDER_REPLICA`, `REDIS_STOCK_REPLICA`, `REDIS_PAYMENT_REPLICA`, `REDIS_BUS_REPLICA`, `SENTINEL_1`, `SENTINEL_2`, `SENTINEL_3`) | *(unset)* |  These are the (potentially virtual) CPU cores that each of the containers will be used. For containers that are replicated via Docker's `deploy` attribute (`GATEWAY`, `ORDER`, `STOCK`, `PAYMENT`), these cores will be used for all replicas cumulatively. Leaving it unset, means the container won't be limited to any specific cores. Otherwise you can give a list of 1 or more cores (`1`, `2,3`) or an inclusive range (`2-5`). |


## Benchmarking
Use the [provided benchmarking repo](https://github.com/delftdata/wdm-project-benchmark/tree/master). There are good instructions there, but in short:

```sh
git clone https://github.com/delftdata/wdm-project-benchmark.git
cd wdm-project-benchmark

python -m venv venv

# Activate with
source venv/bin/activate 
# Or on Windows:
.\venv\Scripts\Activate.ps1 

pip install -r requirements.txt
```

You need to then have the system up and running on the same host.

Check for consistency correctness with:
```sh
cd consistency-test
python run_consistency_test.py
```

Stress test with:
```
cd stress-test
python init_orders.py
locust -f locustfile.py --host="localhost" --users=1000 --spawn-rate=100 --autostart --processes=2
```
Adjust the locust parameters as needed (you may also exclude `--processes` as applicable). You can monitor the benchmarking from the [Locust web UI](http://localhost:8089/?tab=charts).

Use `docker stats --no-stream` to monitor the resource usage of the system if you need that.

## Design
### The Orchestrator

Both Protocols Are Driven by a Single Generic Workflow Engine in `common/orchestrator.py`. It Knows Nothing About Orders or Payments — It Only Knows How to Run a List of Python Functions in Order, Write Its Position to Redis Before Each Step, and Run Them in Reverse If Something Goes Wrong.

**Write Before You Run.** Before Executing Step N, the Engine Writes "Currently at Step N" to Redis. If the Process Crashes Mid-Step, the Position Is Already on Disk. On Restart, It Reads the Position and Re-Runs From There. Steps Are Idempotent, so Re-Running Is Always Safe.

**Steps and Compensations.** A Workflow Is a List of Forward Steps and a Matching List of Undo Steps. If a Forward Step Fails, the Engine Runs the Compensations in Reverse — Undoing Only the Steps That Already Succeeded.

**Sync vs. Async.** In TPC Mode All Steps Run Synchronously — the Engine Sends a Message, Waits for the Reply, and Moves On. In SAGA Mode Each Step Publishes a Message and Calls `suspend()`, Pausing the Engine Until a Background Consumer Calls `resume()` (Success) or `fail()` (Failure) When the Reply Arrives. Compensation Steps in SAGA Mode Work the Same Way.

**Recovery.** On Every Startup the Engine Scans Redis for Workflow Instances That Never Reached `completed` or `failed`, Reads Their Stored Position, and Continues From Exactly Where They Stopped.

## Test Suite

The Test Suite Lives in `test/`. It Is a Standalone CLI — No External Test Framework. It Builds the Stack, Runs All Cases in Order, and Prints a Pass/Fail Report.

### Run

We require that the Python packages in `requirements.txt` are installed. Best to do so in a virtual environment like so:

```bash
python -m venv venv

# Activate with
source venv/bin/activate 
# Or on Windows:
.\venv\Scripts\Activate.ps1 

pip install -r requirements.txt

# You can deactivate with:
deactivate
```

Run via `python start.py` → Select **Run Tests**. The Launcher Builds the Stack, Runs All Suites in Order, and Tears Everything Down When Finished.

### Suite Order

1. Common — Mode-Agnostic Correctness and Durability
2. TPC or SAGA — Protocol-Specific Correctness (Driven by `TRANSACTION_MODE`)
3. Replication — Sentinel Failover and Data Durability (Always Runs Last; It Changes Cluster Topology)

### Test Cases

#### Common Suite


| Test                                   | What It Verifies                                                                                                 |
| -------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| **Multi-Item Checkout**                | A checkout with multiple distinct items deducts the correct quantity from each and charges the exact total.      |
| **Double Checkout**                    | A second checkout on an already-paid order is a no-op — no duplicate charge, no extra stock deduction.           |
| **Post-Checkout Tampering**            | Adding items to a paid order and retrying checkout produces no extra charge and no stock change.                 |
| **Empty Order Checkout**               | Checking out an order with no items returns 200 without crashing anything.                                       |
| **10 Concurrent Checkouts For 1 Unit** | Ten users race for the last unit; exactly one succeeds and stock never goes negative.                            |
| **Sequential Drain**                   | Sequential checkouts exhaust stock in order; the request that would exceed available stock is rejected cleanly.  |
| **5 Independent Parallel Checkouts**   | Five isolated concurrent checkouts each succeed without interfering with each other.                             |
| **External Stock Change**              | Reducing stock between order creation and checkout causes the checkout to fail correctly.                        |
| **Fund User After Order**              | Adding sufficient credit after order creation but before checkout allows the checkout to succeed.                |
| **Balance Exactly Equals Total**       | Checkout succeeds when credit equals the order cost exactly — not one unit to spare.                             |
| **Stock Exactly Equals Quantity**      | Checkout succeeds when available stock equals the ordered quantity exactly.                                      |
| **One Credit Short**                   | Checkout is rejected when the user is exactly one credit unit short.                                             |
| **One Stock Unit Short**               | Checkout is rejected when stock is exactly one unit below the ordered quantity.                                  |
| **Non-Existent Resource Lookup**       | GET requests for unknown IDs return 4xx, not 5xx.                                                                |
| **addItem Idempotency**                | The same add-item request sent twice with the same idempotency key applies the change only once.                 |
| **Partial Stock Failure**              | If any item in a multi-item batch is out of stock, the entire checkout fails atomically — no partial deductions. |
| **Batch Init**                         | `batch_init` creates integer-keyed items and users that are immediately readable via the API.                    |
| **Add-Stock Idempotency**              | Duplicate add-stock requests with the same idempotency key do not double the stock.                              |
| **Pay Idempotency**                    | Duplicate add-funds requests with the same idempotency key do not double the credit.                             |
| **Order Item Merge**                   | Adding the same item to an order twice merges quantities into a single line item.                                |
| **Zero/Negative Amount Rejected**      | `add_stock` and `add_funds` reject amounts ≤ 0 with a 4xx response.                                              |
| **Item Not Found Subtract**            | Subtracting stock from a non-existent item returns 4xx, not 5xx.                                                 |
| **User Not Found Pay**                 | Charging a non-existent user returns 4xx, not 5xx.                                                               |
| **Malformed Stream Message**           | A garbage message injected into the stream does not crash consumers; the next valid request still succeeds.      |
| **Payment Redis AOF Durability**       | Credit written to Redis survives a container restart; checkout succeeds after the restart.                       |
| **Stock Redis AOF Durability**         | Stock written to Redis survives a container restart; checkout succeeds after the restart.                        |
| **Concurrent Oversell Prevention**     | 20 users racing to buy 3 units result in exactly 3 successful checkouts and exactly 3 charges — no oversell.     |
| **Stock Service Crash Mid-Batch**      | Killing and restarting the stock service during a concurrent batch produces no oversell and no phantom charges.  |
| **Redis Bus Restart Recovery**         | Restarting the shared message bus does not prevent subsequent checkouts from completing.                         |


#### TPC Suite


| Test                           | What It Verifies                                                                                                     |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------- |
| **PREPARE → COMMIT**           | PREPARE holds the reservation and reduces visible stock/credit; COMMIT makes the deduction permanent.                |
| **PREPARE → ABORT**            | PREPARE holds the reservation; ABORT returns all reserved resources to their original values.                        |
| **Vote NO**                    | PREPARE for more than available is rejected with 4xx; no reservation is made and nothing changes.                    |
| **PREPARE Locks Resources**    | A PREPARE-held reservation is unavailable to concurrent regular subtract operations.                                 |
| **ABORT Releases Resources**   | After ABORT, the previously locked resources are available for new operations.                                       |
| **Competing PREPAREs**         | Two concurrent PREPAREs on the same insufficient stock — the first wins, the second votes NO.                        |
| **Idempotent COMMIT**          | Calling COMMIT twice on the same transaction produces no double deduction.                                           |
| **ABORT Safety**               | ABORT on a non-existent or already-committed transaction returns 200 and changes nothing.                            |
| **Stock Votes NO**             | When stock cannot fulfil, the coordinator aborts the entire checkout — payment is untouched.                         |
| **Payment Votes NO**           | When payment cannot fulfil, the coordinator aborts and fully reverses the stock reservation.                         |
| **Participant Crash Recovery** | Stock service killed mid-checkout restarts and the coordinator successfully retries the operation.                   |
| **Coordinator Crash Recovery** | A stuck in-doubt workflow injected directly into the log is deterministically resolved by `recovery_tpc` on restart. |


#### SAGA Suite


| Test                                 | What It Verifies                                                                                                          |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| **Compensating Transaction**         | Stock is reserved, then payment fails; the saga fires a compensating transaction that fully restores stock.               |
| **Stock Fails, No Payment**          | Immediate stock failure causes early rejection — the payment step is never attempted.                                     |
| **Participant Crash Recovery**       | Stock service killed mid-saga; the stream's pending re-delivery mechanism completes the saga after restart.               |
| **Coordinator Crash Recovery**       | A stuck workflow injected into the log is consistently resolved (committed or compensated) by `recovery_saga` on restart. |
| **Double-Checkout Prevention**       | A second checkout on an already-paid order is detected and skipped; stock and credit are unchanged.                       |
| **Multi-Item Compensation**          | All items from a failed multi-item checkout are fully restored by the compensating transaction — no partial rollback.     |
| **Concurrent Correlation Isolation** | 20 simultaneous checkouts each receive the correct, isolated response via their correlation ID.                           |
| **Pending-Message Re-Delivery**      | A stream message left unACKed due to a crash is re-delivered and processed correctly after restart.                       |


#### Replication Suite


| Test                                         | What It Verifies                                                                                                                      |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| **Payment Primary SIGKILL**                  | Data written before a hard kill of the payment primary survives on the promoted replica; checkouts continue through the new master.   |
| **Coordinator DB SIGKILL Mid-Checkout**      | Killing the order-service's Redis mid-checkout and restarting order-service resolves the transaction consistently with no data loss.  |
| **Bus Primary SIGKILL With In-Flight**       | Killing the message bus during 10 concurrent checkouts results in no phantom charges or oversell after Sentinel promotes the replica. |
| **One Sentinel Down**                        | Killing one of three sentinels still leaves quorum intact; a subsequent primary failure is correctly failed over.                     |
| **50 Writes Before Primary SIGKILL**         | All 50 items written to a primary before a hard kill are readable from the promoted replica — zero data loss on promotion.            |
| **Service Scale-Out Correctness**            | Scaling stock-service to three replicas does not corrupt data; a full checkout deducts stock and credit exactly once.                 |
| **Replica Killed, System Continues**         | Killing one of three order-service replicas does not interrupt the system; the remaining two handle new checkouts without errors.     |
| **Concurrent Correctness Under Replication** | Ten concurrent checkouts with all services at three replicas produce no oversell and no phantom charges.                              |


