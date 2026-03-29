# replication tests — Redis Sentinel failover, replica promotion, data durability under hard kills
# also covers service-level replication: multiple instances of order/stock/payment services

import concurrent.futures
import time

from run import api, check, json_field, docker_cmd, wait_for_service

_SCALE_SETTLE_S = 8  # seconds to let new replicas join consumer groups


def _scale(service: str, count: int):
    """Scale a docker compose service to `count` replicas."""
    docker_cmd(f"docker compose up -d --no-recreate --scale {service}={count}")

# container names follow docker compose's <project>-<service>-<replica> pattern
_C_ORDER_PRIMARY = "wdm-project-group24-redis-order-1"
_C_STOCK_PRIMARY = "wdm-project-group24-redis-stock-1"
_C_PAYMENT_PRIMARY = "wdm-project-group24-redis-payment-1"
_C_BUS_PRIMARY = "wdm-project-group24-redis-bus-1"
_C_SENTINEL_1 = "wdm-project-group24-sentinel-1-1"
_C_ORDER_SVC = "wdm-project-group24-order-service-1"

# sentinel detects a dead master after down-after-milliseconds (5 s) then
# runs the election. allow 10 s total so the pool reconnects before we assert.
_FAILOVER_WAIT_S = 10


def test_sentinel_storage_primary_failover():
    # write credit to redis-payment → SIGKILL the primary → sentinel promotes
    # the replica → data must be intact and checkouts must still work
    CREDIT = 777
    PRICE = 50
    STOCK = 5

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    check(
        "Credit Is 777 Before Primary Kill",
        json_field(api("GET", f"/payment/find_user/{user}"), "credit") == CREDIT,
    )

    # SIGKILL: no flush, no graceful shutdown — hardest possible failure
    docker_cmd(f"docker kill {_C_PAYMENT_PRIMARY}")
    time.sleep(_FAILOVER_WAIT_S)
    wait_for_service(f"/payment/find_user/{user}", timeout=30)

    check(
        "Credit Intact On Promoted Replica After Primary Kill",
        json_field(api("GET", f"/payment/find_user/{user}"), "credit") == CREDIT,
    )

    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/1")
    r = api("POST", f"/orders/checkout/{order}")
    check(
        "Checkout Succeeds Through Promoted Payment Replica",
        r.status_code == 200,
        f"got {r.status_code}",
    )
    check(
        "Credit Deducted Correctly On Promoted Replica",
        json_field(api("GET", f"/payment/find_user/{user}"), "credit")
        == CREDIT - PRICE,
    )

    # bring the old primary back; sentinel will demote it to replica automatically
    docker_cmd(f"docker start {_C_PAYMENT_PRIMARY}")
    time.sleep(5)


def test_sentinel_coordinator_db_failover():
    # start a checkout then SIGKILL redis-order (coordinator's txn log) mid-flight
    # restart order-service to trigger recovery_tpc against the promoted replica
    # final state must be fully committed or fully aborted — no partial charge
    PRICE = 30
    STOCK = 10
    CREDIT = 500

    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/1")

    def do_checkout():
        return api("POST", f"/orders/checkout/{order}").status_code

    def kill_order_db():
        time.sleep(0.05)  # let checkout enter the tpc flow first
        docker_cmd(f"docker kill {_C_ORDER_PRIMARY}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        killer_fut = pool.submit(kill_order_db)
        checkout_fut = pool.submit(do_checkout)
        killer_fut.result()
        checkout_fut.result(timeout=30)

    time.sleep(_FAILOVER_WAIT_S)

    # restart order-service: recovery_tpc runs on startup and resolves the
    # incomplete txn against the now-promoted redis-order replica
    docker_cmd(f"docker restart {_C_ORDER_SVC}")
    wait_for_service(f"/orders/find/{order}", timeout=60)
    time.sleep(3)

    paid = json_field(api("GET", f"/orders/find/{order}"), "paid")
    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")

    # the only two valid outcomes after recovery:
    # committed → stock -1, credit -price, paid=true
    # aborted   → stock unchanged, credit unchanged, paid=false
    if paid:
        check("Committed: Stock Deducted By 1", stock == STOCK - 1, f"stock={stock}")
        check(
            "Committed: Credit Charged By Price",
            credit == CREDIT - PRICE,
            f"credit={credit}",
        )
    else:
        check("Aborted: Stock Fully Restored", stock == STOCK, f"stock={stock}")
        check("Aborted: Credit Fully Restored", credit == CREDIT, f"credit={credit}")

    docker_cmd(f"docker start {_C_ORDER_PRIMARY}")
    time.sleep(5)


def test_sentinel_bus_primary_failover_inflight():
    # flood 10 concurrent checkouts then SIGKILL redis-bus mid-flight
    # sentinel promotes the bus replica; all transactions must settle with
    # no phantom deductions and no oversell — at-least-once + idempotency must hold
    PRICE = 20
    STOCK = 20
    CREDIT = 500
    N = 10

    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    pairs = []
    for _ in range(N):
        user = json_field(api("POST", "/payment/create_user"), "user_id")
        api("POST", f"/payment/add_funds/{user}/{CREDIT}")
        order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
        api("POST", f"/orders/addItem/{order}/{item}/1")
        pairs.append((user, order))

    def checkout(pair):
        return api("POST", f"/orders/checkout/{pair[1]}").status_code

    def kill_bus():
        time.sleep(0.1)
        docker_cmd(f"docker kill {_C_BUS_PRIMARY}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=N + 1) as pool:
        killer = pool.submit(kill_bus)
        futures = [pool.submit(checkout, p) for p in pairs]
        killer.result()
        results = [f.result() for f in futures]

    time.sleep(_FAILOVER_WAIT_S)
    wait_for_service(f"/stock/find/{item}", timeout=60)
    time.sleep(3)

    winners = results.count(200)
    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    charged = sum(
        1
        for u, _ in pairs
        if json_field(api("GET", f"/payment/find_user/{u}"), "credit") != CREDIT
    )

    check(
        "Winners Equals Charged Users — No Checkout Without Payment",
        winners == charged,
        f"winners={winners} charged={charged}",
    )
    check(
        "Stock + Winners Equals Initial — No Oversell Or Lost Units",
        stock + winners == STOCK,
        f"stock={stock} winners={winners} initial={STOCK}",
    )

    docker_cmd(f"docker start {_C_BUS_PRIMARY}")
    time.sleep(5)


def test_sentinel_quorum_one_sentinel_down():
    # kill sentinel-1 so only 2 of 3 sentinels remain (quorum=2 still reachable)
    # then kill redis-stock primary — the remaining pair must still elect a new master
    PRICE = 15
    STOCK = 5
    CREDIT = 300

    docker_cmd(f"docker kill {_C_SENTINEL_1}")
    time.sleep(2)  # let the cluster notice one sentinel is gone

    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/1")

    docker_cmd(f"docker kill {_C_STOCK_PRIMARY}")
    time.sleep(_FAILOVER_WAIT_S)
    wait_for_service(f"/stock/find/{item}", timeout=30)

    check(
        "Stock Readable After Primary Killed With Only 2 Of 3 Sentinels Alive",
        json_field(api("GET", f"/stock/find/{item}"), "stock") == STOCK,
    )

    r = api("POST", f"/orders/checkout/{order}")
    check(
        "Checkout Succeeds With 2-Of-3 Sentinels After Failover",
        r.status_code == 200,
        f"got {r.status_code}",
    )
    check(
        "Stock Decremented On Promoted Replica",
        json_field(api("GET", f"/stock/find/{item}"), "stock") == STOCK - 1,
    )
    check(
        "Credit Charged — Full Transaction Completed Under Reduced Sentinel Quorum",
        json_field(api("GET", f"/payment/find_user/{user}"), "credit")
        == CREDIT - PRICE,
    )

    docker_cmd(f"docker start {_C_SENTINEL_1}")
    docker_cmd(f"docker start {_C_STOCK_PRIMARY}")
    time.sleep(5)


def test_sentinel_no_data_loss_on_promotion():
    # write 50 stock items then SIGKILL redis-stock primary
    # sentinel promotes the replica; every item written before the crash must be
    # readable from the new primary (proves aof replication was not behind at kill time)
    N = 50

    item_ids = []
    for i in range(N):
        iid = json_field(api("POST", f"/stock/item/create/{i + 1}"), "item_id")
        api("POST", f"/stock/add/{iid}/100")
        item_ids.append(iid)

    docker_cmd(f"docker kill {_C_STOCK_PRIMARY}")
    time.sleep(_FAILOVER_WAIT_S)
    wait_for_service(f"/stock/find/{item_ids[0]}", timeout=30)

    surviving = sum(
        1 for iid in item_ids if api("GET", f"/stock/find/{iid}").status_code == 200
    )
    check(
        f"All {N} Items Readable From Promoted Replica — Zero Data Loss On Failover",
        surviving == N,
        f"{surviving}/{N} survived",
    )

    docker_cmd(f"docker start {_C_STOCK_PRIMARY}")
    time.sleep(5)


def test_service_scaling_basic_correctness():
    # Scale stock-service to 3 instances, verify that CRUD and checkout still
    # work correctly — requests are correctly routed across replicas.
    PRICE = 25
    STOCK = 10
    CREDIT = 500

    _scale("stock-service", 3)
    time.sleep(_SCALE_SETTLE_S)

    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    check(
        "Service Scaling: Stock Readable After Scale-Out",
        json_field(api("GET", f"/stock/find/{item}"), "stock") == STOCK,
    )

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/1")

    r = api("POST", f"/orders/checkout/{order}")
    check(
        "Service Scaling: Checkout Succeeds With 3 Stock Replicas",
        r.status_code == 200,
        f"got {r.status_code}",
    )
    check(
        "Service Scaling: Stock Decremented Exactly Once",
        json_field(api("GET", f"/stock/find/{item}"), "stock") == STOCK - 1,
    )
    check(
        "Service Scaling: Credit Charged Exactly Once",
        json_field(api("GET", f"/payment/find_user/{user}"), "credit") == CREDIT - PRICE,
    )

    _scale("stock-service", 1)
    time.sleep(_SCALE_SETTLE_S)


def test_service_replica_killed_system_continues():
    # Scale order-service to 3, kill one replica, verify remaining 2 handle
    # new requests without errors.
    PRICE = 15
    STOCK = 5
    CREDIT = 300

    _scale("order-service", 3)
    time.sleep(_SCALE_SETTLE_S)

    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    # Kill the third replica (container name follows compose naming)
    docker_cmd("docker kill wdm-project-group24-order-service-3")
    time.sleep(2)

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/1")

    r = api("POST", f"/orders/checkout/{order}")
    check(
        "Service Replica Kill: Checkout Succeeds With 1-Of-3 Order Replica Killed",
        r.status_code == 200,
        f"got {r.status_code}",
    )
    check(
        "Service Replica Kill: Stock Decremented Correctly",
        json_field(api("GET", f"/stock/find/{item}"), "stock") == STOCK - 1,
    )

    _scale("order-service", 1)
    time.sleep(_SCALE_SETTLE_S)


def test_concurrent_correctness_under_replication():
    # Scale all services to 3, run N concurrent checkouts for a single item
    # with exactly N units of stock. Verify no oversell and no phantom charges.
    PRICE = 20
    STOCK = 5
    CREDIT = 500
    N = 10  # more contenders than stock units to stress the idempotency path

    _scale("order-service", 3)
    _scale("stock-service", 3)
    _scale("payment-service", 3)
    time.sleep(_SCALE_SETTLE_S)

    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    pairs = []
    for _ in range(N):
        user = json_field(api("POST", "/payment/create_user"), "user_id")
        api("POST", f"/payment/add_funds/{user}/{CREDIT}")
        order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
        api("POST", f"/orders/addItem/{order}/{item}/1")
        pairs.append((user, order))

    with concurrent.futures.ThreadPoolExecutor(max_workers=N) as pool:
        results = list(pool.map(lambda p: api("POST", f"/orders/checkout/{p[1]}").status_code, pairs))

    time.sleep(3)  # let any in-flight saga steps settle

    winners = results.count(200)
    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    charged = sum(
        1
        for u, _ in pairs
        if json_field(api("GET", f"/payment/find_user/{u}"), "credit") != CREDIT
    )

    check(
        "Concurrent Replication: Stock + Winners Equals Initial — No Oversell",
        stock + winners == STOCK,
        f"stock={stock} winners={winners} initial={STOCK}",
    )
    check(
        "Concurrent Replication: Charged Users Equals Winners — No Phantom Charge",
        charged == winners,
        f"charged={charged} winners={winners}",
    )

    _scale("order-service", 1)
    _scale("stock-service", 1)
    _scale("payment-service", 1)
    time.sleep(_SCALE_SETTLE_S)


TESTS = [
    (
        "redis-payment",
        "Sentinel: Payment Primary SIGKILL — Replica Promoted, Data Intact",
        test_sentinel_storage_primary_failover,
    ),
    (
        "redis-order",
        "Sentinel: Coordinator DB SIGKILL Mid-Checkout — Recovery Resolves",
        test_sentinel_coordinator_db_failover,
    ),
    (
        "redis-bus",
        "Sentinel: Bus Primary SIGKILL With 10 In-Flight — No Phantom Charge",
        test_sentinel_bus_primary_failover_inflight,
    ),
    (
        "sentinel-1",
        "Sentinel: One Sentinel Down — Quorum Still Elects New Stock Primary",
        test_sentinel_quorum_one_sentinel_down,
    ),
    (
        "redis-stock",
        "Sentinel: 50 Writes Before Primary SIGKILL — Zero Data Loss",
        test_sentinel_no_data_loss_on_promotion,
    ),
    # service replication
    (
        "order-service",
        "Service Replication: Scale Stock To 3 — CRUD And Checkout Correct",
        test_service_scaling_basic_correctness,
    ),
    (
        "order-service",
        "Service Replication: Kill One Order Replica — Remaining 2 Handle Requests",
        test_service_replica_killed_system_continues,
    ),
    (
        "order-service",
        "Service Replication: 10 Concurrent Checkouts With All Services At 3 — No Oversell Or Phantom Charge",
        test_concurrent_correctness_under_replication,
    ),
]