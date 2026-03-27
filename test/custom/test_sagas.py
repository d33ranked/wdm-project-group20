"""
SAGA Tests
==========
Tests specific to the SAGA transaction mode.
Covers compensating transactions, participant crash recovery,
and coordinator crash recovery via saga state persistence.
"""

import json
import subprocess
import time
import uuid

from run import api, check, json_field, PROJECT_ROOT, docker_cmd, docker_exec_redis, wait_for_service


# ---------------------------------------------------------------------------
# 1. Compensating Transaction — Payment Fails, Stock Rolled Back
# ---------------------------------------------------------------------------
def test_compensation_payment_fails():
    """Stock is reserved, payment fails, saga fires compensating rollback."""
    PRICE = 100
    STOCK = 10
    CREDIT = 5

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/1")

    r = api("POST", f"/orders/checkout/{order}")
    check("Checkout Rejected — User Has 5 Credit But Item Costs 100",
          400 <= r.status_code < 500, f"got {r.status_code}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check(f"Stock Restored To {STOCK} After Compensation — Rollback Reversed The Reservation",
          stock == STOCK, f"got {stock}")

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"Credit Unchanged At {CREDIT} — Payment Was Never Charged",
          credit == CREDIT, f"got {credit}")

    paid = json_field(api("GET", f"/orders/find/{order}"), "paid")
    check("Order Not Marked As Paid After Failed Checkout",
          paid is not True, f"got {paid}")


# ---------------------------------------------------------------------------
# 2. Stock Reservation Fails — No Payment Attempted
# ---------------------------------------------------------------------------
def test_stock_fails_no_payment():
    """Insufficient stock causes immediate failure, payment is never attempted."""
    PRICE = 10
    STOCK = 2
    QTY = 50
    CREDIT = 500

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")

    r = api("POST", f"/orders/checkout/{order}")
    check(f"Checkout Rejected — Requested {QTY} Units But Only {STOCK} Available",
          400 <= r.status_code < 500, f"got {r.status_code}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check(f"Stock Unchanged At {STOCK} — No Reservation Was Made",
          stock == STOCK, f"got {stock}")

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"Credit Unchanged At {CREDIT} — Payment Was Never Attempted",
          credit == CREDIT, f"got {credit}")


# ---------------------------------------------------------------------------
# 3. Participant Crash — Stock Service Dies Mid-Saga, Recovers
# ---------------------------------------------------------------------------
def test_participant_crash_recovery():
    """Stop stock service, restart after 3s, saga completes via redis stream pending re-delivery."""
    ITEM_PRICE = 10
    ITEM_QTY = 2
    STOCK = 5
    CREDIT = 100
    CONTAINER = "wdm-project-group24-stock-service-1"

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{ITEM_QTY}")

    docker_cmd(f"docker stop {CONTAINER}")
    subprocess.Popen(
        f"sleep 3 && docker start {CONTAINER}",
        shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    r = api("POST", f"/orders/checkout/{order}")
    expected_stock = STOCK - ITEM_QTY
    expected_credit = CREDIT - (ITEM_PRICE * ITEM_QTY)

    check("Checkout Completed After Stock Service Recovered — Stream Message Re-Delivered And Processed",
          r.status_code == 200, f"got {r.status_code}")

    wait_for_service(f"/stock/find/{item}")
    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")

    check(f"Stock Decreased To {expected_stock} After Recovery — {ITEM_QTY} Units Sold",
          stock == expected_stock, f"got {stock}")
    check(f"Credit Decreased To {expected_credit} After Recovery — "
          f"Charged {ITEM_PRICE}x{ITEM_QTY}",
          credit == expected_credit, f"got {credit}")


# ---------------------------------------------------------------------------
# 4. Coordinator Crash — Stuck Saga Recovered On Order Service Restart
# ---------------------------------------------------------------------------
def test_coordinator_crash_recovery():
    """Inject a stuck saga into the DB, restart order service, verify recovery resolves it.

    Simulates the narrow race condition where stock already processed the
    subtract_batch message but the order service crashed before advancing the
    saga state. Without recovery_saga(), the saga stays stuck in
    STOCK_REQUESTED: stock is deducted but payment is never charged — an
    inconsistent state that is neither fully committed nor fully rolled back.
    """
    ITEM_PRICE = 20
    ITEM_QTY = 2
    STOCK = 10
    CREDIT = 200
    ORDER_CONTAINER = "wdm-project-group24-order-service-1"
    ORDER_DB = "wdm-project-group24-redis-order-1"
    STOCK_DB = "wdm-project-group24-redis-stock-1"

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{ITEM_QTY}")

    docker_cmd(f"docker stop {ORDER_CONTAINER}")

    saga_id = str(uuid.uuid4())
    # items_quantities stored as {item_id: qty} dict — matches order/db.py format
    items_quantities = json.dumps({item: ITEM_QTY})
    stock_idem_key = f"{saga_id}:stock:subtract_batch"
    new_stock = STOCK - ITEM_QTY
    cached_body = json.dumps({"updated_stock": {item: new_stock}})

    # Order Redis: write a saga:* hash in STOCK_REQUESTED state
    docker_exec_redis(
        ORDER_DB,
        "HSET", f"saga:{saga_id}",
        "order_id",                order,
        "state",                   "STOCK_REQUESTED",
        "items_quantities",        items_quantities,
        "original_correlation_id", "recovery-test",
        "idempotency_key",         "",
    )

    # Stock Redis: deduct units (simulates stock subtract_batch already ran)
    docker_exec_redis(STOCK_DB, "HINCRBY", f"item:{item}", "stock", str(-ITEM_QTY))

    # Stock Redis: write idempotency key so recovery re-publish is a no-op for stock
    docker_exec_redis(
        STOCK_DB,
        "HSET", f"idem:{stock_idem_key}",
        "status_code", "200",
        "body",        cached_body,
    )
    docker_exec_redis(STOCK_DB, "EXPIRE", f"idem:{stock_idem_key}", "3600")

    docker_cmd(f"docker start {ORDER_CONTAINER}")

    wait_for_service(f"/orders/find/{order}", timeout=90)
    time.sleep(15)

    stock_val = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit_val = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    paid_val = json_field(api("GET", f"/orders/find/{order}"), "paid")

    expected_stock = STOCK - ITEM_QTY
    expected_credit = CREDIT - (ITEM_PRICE * ITEM_QTY)

    committed = (stock_val == expected_stock and credit_val == expected_credit and paid_val is True)
    rolled_back = (stock_val == STOCK and credit_val == CREDIT and paid_val is not True)

    check(
        "After Coordinator Crash And Recovery, State Is Consistent — "
        "Either Fully Committed Or Fully Rolled Back",
        committed or rolled_back,
        f"stock={stock_val}, credit={credit_val}, paid={paid_val}"
    )

    if committed:
        check("Recovery Resolved Stuck Saga By Completing — "
              "Stock, Credit, And Order All Reflect The Checkout", True)
    elif rolled_back:
        check("Recovery Resolved Stuck Saga By Compensating — "
              "All Services Restored To Original State", True)


# ---------------------------------------------------------------------------
# Ordered test list — imported by run.py
# ---------------------------------------------------------------------------
TESTS = [
    ("Compensating Transaction: Payment Fails, Stock Rolled Back", test_compensation_payment_fails),
    ("Stock Reservation Fails — No Payment Attempted", test_stock_fails_no_payment),
    ("Participant Crash: Stock Dies Mid-Saga And Recovers", test_participant_crash_recovery),
    ("Coordinator Crash: Stuck Saga Recovered On Order Service Restart", test_coordinator_crash_recovery),
]
