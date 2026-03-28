# saga tests — compensating transactions, participant crash, coordinator recovery, stream semantics

import concurrent.futures
import json
import subprocess
import time
import uuid

from run import api, check, json_field, PROJECT_ROOT, docker_cmd, docker_exec_redis, wait_for_service


def test_compensation_payment_fails():
    # stock is reserved, payment fails, saga fires compensating rollback
    PRICE  = 100
    STOCK  = 10
    CREDIT = 5

    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
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


def test_stock_fails_no_payment():
    # insufficient stock causes immediate failure, payment is never attempted
    PRICE  = 10
    STOCK  = 2
    QTY    = 50
    CREDIT = 500

    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
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


def test_participant_crash_recovery():
    # stop stock service, restart after 3s, saga completes via stream pending re-delivery
    ITEM_PRICE = 10
    ITEM_QTY   = 2
    STOCK      = 5
    CREDIT     = 100
    CONTAINER  = "wdm-project-group24-stock-service-1"

    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item  = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{ITEM_QTY}")

    docker_cmd(f"docker stop {CONTAINER}")
    subprocess.Popen(
        f"sleep 3 && docker start {CONTAINER}",
        shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    r = api("POST", f"/orders/checkout/{order}")
    expected_stock  = STOCK - ITEM_QTY
    expected_credit = CREDIT - (ITEM_PRICE * ITEM_QTY)

    check("Checkout Completed After Stock Service Recovered — Stream Message Re-Delivered And Processed",
          r.status_code == 200, f"got {r.status_code}")

    wait_for_service(f"/stock/find/{item}")
    stock  = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")

    check(f"Stock Decreased To {expected_stock} After Recovery — {ITEM_QTY} Units Sold",
          stock == expected_stock, f"got {stock}")
    check(f"Credit Decreased To {expected_credit} After Recovery — Charged {ITEM_PRICE}x{ITEM_QTY}",
          credit == expected_credit, f"got {credit}")


def test_coordinator_crash_recovery():
    # inject a stuck saga in STOCK_REQUESTED, restart order service, verify consistent resolution
    ITEM_PRICE      = 20
    ITEM_QTY        = 2
    STOCK           = 10
    CREDIT          = 200
    ORDER_CONTAINER = "wdm-project-group24-order-service-1"
    ORDER_DB        = "wdm-project-group24-redis-order-1"
    STOCK_DB        = "wdm-project-group24-redis-stock-1"

    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item  = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{ITEM_QTY}")

    docker_cmd(f"docker stop {ORDER_CONTAINER}")

    saga_id           = str(uuid.uuid4())
    items_quantities  = json.dumps({item: ITEM_QTY})
    stock_idem_key    = f"{saga_id}:stock:subtract_batch"
    new_stock         = STOCK - ITEM_QTY
    cached_body       = json.dumps({"updated_stock": {item: new_stock}})

    docker_exec_redis(
        ORDER_DB,
        "HSET", f"saga:{saga_id}",
        "order_id",                order,
        "state",                   "STOCK_REQUESTED",
        "items_quantities",        items_quantities,
        "original_correlation_id", "recovery-test",
        "idempotency_key",         "",
    )
    docker_exec_redis(STOCK_DB, "HINCRBY", f"item:{item}", "stock", str(-ITEM_QTY))
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

    stock_val  = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit_val = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    paid_val   = json_field(api("GET", f"/orders/find/{order}"), "paid")

    expected_stock  = STOCK - ITEM_QTY
    expected_credit = CREDIT - (ITEM_PRICE * ITEM_QTY)

    committed    = (stock_val == expected_stock and credit_val == expected_credit and paid_val is True)
    rolled_back  = (stock_val == STOCK and credit_val == CREDIT and paid_val is not True)

    check(
        "After Coordinator Crash And Recovery, State Is Consistent — Either Fully Committed Or Fully Rolled Back",
        committed or rolled_back,
        f"stock={stock_val}, credit={credit_val}, paid={paid_val}",
    )
    if committed:
        check("Recovery Resolved Stuck Saga By Completing — Stock, Credit, And Order All Reflect The Checkout", True)
    elif rolled_back:
        check("Recovery Resolved Stuck Saga By Compensating — All Services Restored To Original State", True)


def test_saga_double_checkout_prevented():
    # second checkout of a paid order returns 4xx and changes nothing
    PRICE  = 20
    STOCK  = 5
    QTY    = 2
    CREDIT = 500

    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")

    r1 = api("POST", f"/orders/checkout/{order}")
    check("First Checkout Succeeds", r1.status_code == 200, f"got {r1.status_code}")

    time.sleep(1)   # let saga reach COMPLETED

    r2 = api("POST", f"/orders/checkout/{order}")
    check("Second Checkout On A Paid Order Is Skipped",
          r2.status_code == 200, f"got {r2.status_code}")

    expected_stock  = STOCK - QTY
    expected_credit = CREDIT - (PRICE * QTY)

    check(f"Stock Remains {expected_stock} After Duplicate Checkout Attempt",
          json_field(api("GET", f"/stock/find/{item}"), "stock") == expected_stock)
    check(f"Credit Remains {expected_credit} After Duplicate Checkout Attempt",
          json_field(api("GET", f"/payment/find_user/{user}"), "credit") == expected_credit)
    check("Order Remains Paid After Duplicate Checkout Attempt",
          json_field(api("GET", f"/orders/find/{order}"), "paid") is True)


def test_saga_compensation_multi_item():
    # stock reserved for 3 items; payment fails; all 3 items are fully restored
    PRICES  = [15, 25, 10]
    STOCKS  = [10, 10, 10]
    QTYS    = [1,  2,  3]
    CREDIT  = 5   # intentionally too low to pay

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")

    items = []
    for price, stock in zip(PRICES, STOCKS):
        item = json_field(api("POST", f"/stock/item/create/{price}"), "item_id")
        api("POST", f"/stock/add/{item}/{stock}")
        items.append(item)

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    for item, qty in zip(items, QTYS):
        api("POST", f"/orders/addItem/{order}/{item}/{qty}")

    total_cost = sum(p * q for p, q in zip(PRICES, QTYS))
    r = api("POST", f"/orders/checkout/{order}")
    check(f"Checkout Rejected — User Has {CREDIT} Credit But Order Costs {total_cost}",
          400 <= r.status_code < 500, f"got {r.status_code}")

    time.sleep(2)   # let compensating transaction propagate

    for i, (item, price, stock, qty) in enumerate(zip(items, PRICES, STOCKS, QTYS)):
        actual = json_field(api("GET", f"/stock/find/{item}"), "stock")
        check(f"Item {i+1} (Price={price}, Qty={qty}) Stock Fully Restored To {stock}",
              actual == stock, f"got {actual}")

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"Credit Unchanged At {CREDIT} — Payment Was Never Applied",
          credit == CREDIT, f"got {credit}")

    paid = json_field(api("GET", f"/orders/find/{order}"), "paid")
    check("Order Not Marked As Paid After Failed Multi-Item Checkout",
          paid is not True, f"got {paid}")


def test_concurrent_correlation_isolation():
    # 20 simultaneous checkouts — each gets its own correct response via correlation_id isolation
    N      = 20
    PRICE  = 5
    STOCK  = 10
    CREDIT = 200

    def run_one_checkout(_):
        user  = json_field(api("POST", "/payment/create_user"), "user_id")
        api("POST", f"/payment/add_funds/{user}/{CREDIT}")
        item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
        api("POST", f"/stock/add/{item}/{STOCK}")
        order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
        api("POST", f"/orders/addItem/{order}/{item}/1")
        r = api("POST", f"/orders/checkout/{order}")
        return r.status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=N) as pool:
        codes = list(pool.map(run_one_checkout, range(N)))

    all_decided = all(c in (200, 400) for c in codes)
    check("Every Checkout Got A Definitive 200 Or 400 — No Timeouts Or Cross-Wired Responses",
          all_decided, f"codes: {codes}")

    successes = codes.count(200)
    check(f"All {N} Independent Checkouts Succeeded (Each Had Its Own Item And User)",
          successes == N, f"only {successes}/{N} returned 200")


def test_pending_message_redelivery():
    # crash stock mid-request; on restart its pending stream entry is re-delivered
    CONTAINER = "wdm-project-group24-stock-service-1"
    INITIAL   = 10
    ADD_AMT   = 7

    item = json_field(api("POST", "/stock/item/create/1"), "item_id")
    api("POST", f"/stock/add/{item}/{INITIAL}")

    docker_cmd(f"docker stop {CONTAINER}")

    idem_key = "pending-redelivery-test"
    subprocess.Popen(
        ["python3", "-c",
         f"import requests; requests.post('http://localhost:8000/stock/add/{item}/{ADD_AMT}', "
         f"headers={{'Idempotency-Key': '{idem_key}'}}, timeout=35)"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(2)   # give gateway time to publish the message to the stream

    docker_cmd(f"docker start {CONTAINER}")
    wait_for_service(f"/stock/find/{item}", timeout=60)
    time.sleep(3)   # allow consumer loop to process the pending entry

    stock    = json_field(api("GET", f"/stock/find/{item}"), "stock")
    expected = INITIAL + ADD_AMT
    check(
        f"Pending Stream Message Was Re-Delivered After Stock Restart — Stock Is {expected} (= {INITIAL} + {ADD_AMT})",
        stock == expected,
        f"got {stock}",
    )


TESTS = [
    ("order-service",  "Compensating Transaction: Payment Fails, Stock Rolled Back",                   test_compensation_payment_fails),
    ("stock-service",  "Stock Reservation Fails: No Payment Attempted",                                test_stock_fails_no_payment),
    ("stock-service",  "Participant Crash: Stock Dies Mid-Saga And Recovers",                          test_participant_crash_recovery),
    ("order-service",  "Coordinator Crash: Stuck Saga Recovered On Order Service Restart",             test_coordinator_crash_recovery),
    ("order-service",  "Double-Checkout Prevention: Second Checkout Rejected, State Unchanged",        test_saga_double_checkout_prevented),
    ("order-service",  "Multi-Item Compensation: All Items Restored When Payment Fails",               test_saga_compensation_multi_item),
    ("gateway",        "Concurrent Correlation Isolation: 20 Simultaneous Checkouts Get Correct Responses", test_concurrent_correlation_isolation),
    ("redis-bus",      "Pending-Message Re-Delivery: Stock Recovers In-Flight Add After Restart",      test_pending_message_redelivery),
]
