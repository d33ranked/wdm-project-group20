"""
SAGA Tests
==========
Tests specific to the SAGA transaction mode.
Covers compensating transactions, participant crash recovery,
and coordinator crash recovery via saga state persistence.
"""

import json
import subprocess
import threading
import time
import uuid

import requests

from run import api, check, json_field, PROJECT_ROOT, BASE_URL, docker_cmd, docker_exec_sql, wait_for_service

# ---------------------------------------------------------------------------
# 1. Saved idempotency: checkout is successful, nothing changes
# ---------------------------------------------------------------------------
def test_idempotency_saved():
    PRICE = 100
    STOCK = 10
    CREDIT = 200

    ORDER_DB = "wdm-project-group24-order-db-1"

    order_idemp_key = "I-already-paid-this-order"
    order_body = "Also-Doesnt-Matter"

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/1")


    docker_exec_sql(ORDER_DB, "orders",
         f"INSERT INTO idempotency_keys (key, status_code, body) VALUES ('{order_idemp_key}', 200, '{order_body}') ON CONFLICT DO NOTHING")

    # TODO fix this, not working
    headers = {
        "Idempotency-Key":order_idemp_key
    }
    r = api("POST", f"/orders/checkout/{order}", headers=headers)

    check("Checkout Successful — Same Idempotency Key",
          r.status_code == 200, f"got {r.status_code}")
    check("Checkout Returns Saved Body — Same Idempotency Key",
          r.text == order_body, f"got {r.text}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check(f"Stock Is Still {STOCK}  — Nothing Changed",
          stock == STOCK, f"got {stock}")

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"Credit Is Still {CREDIT} — Nothing Changed",
          credit == CREDIT, f"got {credit}")

    paid = json_field(api("GET", f"/orders/find/{order}"), "paid")
    check("Order Still Marked As Not Paid",
          paid is not True, f"got {paid}")

# ---------------------------------------------------------------------------
# 2. Order already paid: SAGA returns 4xx
# ---------------------------------------------------------------------------
def test_order_already_paid():
    PRICE = 100
    STOCK = 10
    CREDIT = 200

    ORDER_DB = "wdm-project-group24-order-db-1"

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/1")

    docker_exec_sql(ORDER_DB, "orders",
         f"UPDATE orders SET paid = TRUE WHERE id = '{order}'")

    r = api("POST", f"/orders/checkout/{order}")
    check("Checkout Rejected — Order Already Paid",
          400 <= r.status_code < 500, f"got {r.status_code}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check(f"Stock Is Still {STOCK}  — Nothing Changed",
          stock == STOCK, f"got {stock}")

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"Credit Is Still {CREDIT} — Nothing Changed",
          credit == CREDIT, f"got {credit}")

    paid = json_field(api("GET", f"/orders/find/{order}"), "paid")
    check("Order Still Marked As Paid",
          paid is True, f"got {paid}")

# ---------------------------------------------------------------------------
# 3. Compensating Transaction — Payment Fails, Stock Rolled Back
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
# 4. Stock Reservation Fails — No Payment Attempted
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
# 5. Participant Crash — Stock Service Dies Mid-Saga, Recovers
# ---------------------------------------------------------------------------
def test_stock_crash_recovery():
    """Stop stock service, restart after 3s, saga completes via Kafka persistence."""
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

    check("Checkout Completed After Stock Service Recovered — Kafka Message Persisted And Processed",
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
# 6. Participant Crash — Payment Service Dies Mid-Saga, Recovers
# ---------------------------------------------------------------------------
def test_payment_crash_recovery():
    """Stop payment service, restart after 3s, saga completes via Kafka persistence."""
    ITEM_PRICE = 10
    ITEM_QTY = 2
    STOCK = 5
    CREDIT = 100
    CONTAINER = "wdm-project-group24-payment-service-1"

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

    check("Checkout Completed After Payment Service Recovered — Kafka Message Persisted And Processed",
          r.status_code == 200, f"got {r.status_code}")

    wait_for_service(f"/payment/find_user/{user}")
    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")

    check(f"Stock Decreased To {expected_stock} After Recovery — {ITEM_QTY} Units Sold",
          stock == expected_stock, f"got {stock}")
    check(f"Credit Decreased To {expected_credit} After Recovery — "
          f"Charged {ITEM_PRICE}x{ITEM_QTY}",
          credit == expected_credit, f"got {credit}")

# ---------------------------------------------------------------------------
# 7. Coordinator Crash — Saga crashes after sending stock request
# ---------------------------------------------------------------------------
def test_coordinator_crash_after_stock():
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
    ORDER_DB = "wdm-project-group24-order-db-1"
    STOCK_DB = "wdm-project-group24-stock-db-1"

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{ITEM_QTY}")

    docker_cmd(f"docker stop {ORDER_CONTAINER}")

    saga_id = str(uuid.uuid4())
    items_quantities = json.dumps({item: ITEM_QTY})
    stock_idem_key = f"{saga_id}:stock:subtract_batch"
    new_stock = STOCK - ITEM_QTY
    cached_body = json.dumps({"updated_stock": {item: new_stock}})

    # Process stock as if the order did it
    docker_exec_sql(ORDER_DB, "orders",
        f"INSERT INTO sagas (id, order_id, state, items_quantities, "
        f"original_correlation_id) VALUES "
        f"('{saga_id}', '{order}', 'STOCK_REQUESTED', '{items_quantities}', 'recovery-test')")

    docker_exec_sql(STOCK_DB, "stock",
        f"UPDATE items SET stock = stock - {ITEM_QTY} WHERE id = '{item}'")

    docker_exec_sql(STOCK_DB, "stock",
        f"INSERT INTO idempotency_keys (key, status_code, body) VALUES "
        f"('{stock_idem_key}', 200, '{cached_body}')")

    # Then it recovers and will see that stock was requested
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
# 8. Coordinator Crash — Saga crashes before sending stock request
# ---------------------------------------------------------------------------
def test_coordinator_crash_before_stock():
    ITEM_PRICE = 20
    ITEM_QTY = 2
    STOCK = 10
    CREDIT = 200
    ORDER_CONTAINER = "wdm-project-group24-order-service-1"
    ORDER_DB = "wdm-project-group24-order-db-1"
    STOCK_DB = "wdm-project-group24-stock-db-1"

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{ITEM_QTY}")

    docker_cmd(f"docker stop {ORDER_CONTAINER}")

    saga_id = str(uuid.uuid4())
    items_quantities = json.dumps({item: ITEM_QTY})
    stock_idem_key = f"{saga_id}:stock:subtract_batch"
    new_stock = STOCK - ITEM_QTY
    cached_body = json.dumps({"updated_stock": {item: new_stock}})

    # Process stock as if the order did it
    docker_exec_sql(ORDER_DB, "orders",
        f"INSERT INTO sagas (id, order_id, state, items_quantities, "
        f"original_correlation_id) VALUES "
        f"('{saga_id}', '{order}', 'STOCK_REQUESTED', '{items_quantities}', 'recovery-test')")

    # Then it recovers and will see that stock was requested
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
# 9. Coordinator Crash — Saga crashes before sending payment request
# ---------------------------------------------------------------------------
def test_coordinator_crash_before_payment():
    ITEM_PRICE = 20
    ITEM_QTY = 2
    STOCK = 10
    CREDIT = 200
    ORDER_CONTAINER = "wdm-project-group24-order-service-1"
    ORDER_DB = "wdm-project-group24-order-db-1"
    STOCK_DB = "wdm-project-group24-stock-db-1"

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{ITEM_QTY}")

    docker_cmd(f"docker stop {ORDER_CONTAINER}")

    saga_id = str(uuid.uuid4())
    items_quantities = json.dumps({item: ITEM_QTY})
    stock_idem_key = f"{saga_id}:stock:subtract_batch"
    new_stock = STOCK - ITEM_QTY
    cached_body = json.dumps({"updated_stock": {item: new_stock}})

    docker_exec_sql(ORDER_DB, "orders",
        f"INSERT INTO sagas (id, order_id, state, items_quantities, "
        f"original_correlation_id) VALUES "
        f"('{saga_id}', '{order}', 'STOCK_REQUESTED', '{items_quantities}', 'recovery-test')")

    docker_exec_sql(STOCK_DB, "stock",
        f"UPDATE items SET stock = stock - {ITEM_QTY} WHERE id = '{item}'")

    docker_exec_sql(STOCK_DB, "stock",
        f"INSERT INTO idempotency_keys (key, status_code, body) VALUES "
        f"('{stock_idem_key}', 200, '{cached_body}')")

    docker_exec_sql(ORDER_DB, "orders",
         f"UPDATE sagas SET state = 'PAYMENT_REQUESTED' WHERE id = '{saga_id}'")

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
# 10. Coordinator Crash — Saga crashes after sending payment request
# ---------------------------------------------------------------------------
def test_coordinator_crash_after_payment():
    ITEM_PRICE = 20
    ITEM_QTY = 2
    ORDER_PRICE = ITEM_PRICE * ITEM_QTY
    STOCK = 10
    CREDIT = 200

    ORDER_CONTAINER = "wdm-project-group24-order-service-1"
    ORDER_DB = "wdm-project-group24-order-db-1"
    STOCK_DB = "wdm-project-group24-stock-db-1"
    PAYMENT_DB = "wdm-project-group24-payment-db-1"

    expected_stock = STOCK - ITEM_QTY
    expected_credit = CREDIT - ORDER_PRICE

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{ITEM_QTY}")

    docker_cmd(f"docker stop {ORDER_CONTAINER}")

    saga_id = str(uuid.uuid4())
    items_quantities = json.dumps({item: ITEM_QTY})
    stock_idem_key = f"{saga_id}:stock:subtract_batch"
    payment_idem_key = f"{saga_id}:payment:pay"

    new_stock = STOCK - ITEM_QTY
    stock_cached_body = json.dumps({"updated_stock": {item: new_stock}})
    payment_cached_body = f"User: {user} credit updated to: {expected_credit}"

    docker_exec_sql(ORDER_DB, "orders",
        f"INSERT INTO sagas (id, order_id, state, items_quantities, "
        f"original_correlation_id) VALUES "
        f"('{saga_id}', '{order}', 'STOCK_REQUESTED', '{items_quantities}', 'recovery-test')")

    docker_exec_sql(STOCK_DB, "stock",
        f"UPDATE items SET stock = stock - {ITEM_QTY} WHERE id = '{item}'")

    docker_exec_sql(STOCK_DB, "stock",
        f"INSERT INTO idempotency_keys (key, status_code, body) VALUES "
        f"('{stock_idem_key}', 200, '{stock_cached_body}')")

    docker_exec_sql(ORDER_DB, "orders",
         f"UPDATE sagas SET state = 'PAYMENT_REQUESTED' WHERE id = '{saga_id}'")

    docker_exec_sql(PAYMENT_DB, "payments",
         f"UPDATE users SET credit = credit - {ORDER_PRICE} WHERE id = '{user}'")

    docker_exec_sql(PAYMENT_DB, "payments",
         f"INSERT INTO idempotency_keys (key, status_code, body) "
            f"VALUES ('{payment_idem_key}', 200, '{payment_cached_body}') ON CONFLICT DO NOTHING")

    docker_cmd(f"docker start {ORDER_CONTAINER}")

    wait_for_service(f"/orders/find/{order}", timeout=90)
    time.sleep(15)

    stock_val = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit_val = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    paid_val = json_field(api("GET", f"/orders/find/{order}"), "paid")

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
# 11. Coordinator Crash — Saga crashes after stock failed
# ---------------------------------------------------------------------------
def test_coordinator_crash_stock_failed():
    # The stock failed so basically nothing is done when recovering.

    ITEM_PRICE = 20
    ITEM_QTY = 2
    STOCK = 10
    CREDIT = 200
    ORDER_CONTAINER = "wdm-project-group24-order-service-1"
    ORDER_DB = "wdm-project-group24-order-db-1"

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{ITEM_QTY}")

    docker_cmd(f"docker stop {ORDER_CONTAINER}")

    saga_id = str(uuid.uuid4())
    items_quantities = json.dumps({item: ITEM_QTY})
    stock_idem_key = f"{saga_id}:stock:subtract_batch"
    new_stock = STOCK - ITEM_QTY

    docker_exec_sql(ORDER_DB, "orders",
        f"INSERT INTO sagas (id, order_id, state, items_quantities, "
        f"original_correlation_id) VALUES "
        f"('{saga_id}', '{order}', 'STOCK_REQUESTED', '{items_quantities}', 'recovery-test')")

    docker_exec_sql(ORDER_DB, "orders",
         f"UPDATE sagas SET state = 'STOCK_FAILED' WHERE id = '{saga_id}'")

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
# 12. Coordinator Crash — Saga crashes after it completed
# ---------------------------------------------------------------------------
def test_coordinator_crash_after_completed():
    # The saga was completed, so again, nothing needs to be done.

    ITEM_PRICE = 20
    ITEM_QTY = 2
    ORDER_PRICE = ITEM_PRICE * ITEM_QTY
    STOCK = 10
    CREDIT = 200

    ORDER_CONTAINER = "wdm-project-group24-order-service-1"
    ORDER_DB = "wdm-project-group24-order-db-1"
    STOCK_DB = "wdm-project-group24-stock-db-1"
    PAYMENT_DB = "wdm-project-group24-payment-db-1"

    expected_stock = STOCK - ITEM_QTY
    expected_credit = CREDIT - ORDER_PRICE

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{ITEM_QTY}")

    docker_cmd(f"docker stop {ORDER_CONTAINER}")

    saga_id = str(uuid.uuid4())
    items_quantities = json.dumps({item: ITEM_QTY})
    stock_idem_key = f"{saga_id}:stock:subtract_batch"
    payment_idem_key = f"{saga_id}:payment:pay"

    new_stock = STOCK - ITEM_QTY
    stock_cached_body = json.dumps({"updated_stock": {item: new_stock}})
    payment_cached_body = f"User: {user} credit updated to: {expected_credit}"

    docker_exec_sql(ORDER_DB, "orders",
        f"INSERT INTO sagas (id, order_id, state, items_quantities, "
        f"original_correlation_id) VALUES "
        f"('{saga_id}', '{order}', 'STOCK_REQUESTED', '{items_quantities}', 'recovery-test')")

    docker_exec_sql(STOCK_DB, "stock",
        f"UPDATE items SET stock = stock - {ITEM_QTY} WHERE id = '{item}'")

    docker_exec_sql(STOCK_DB, "stock",
        f"INSERT INTO idempotency_keys (key, status_code, body) VALUES "
        f"('{stock_idem_key}', 200, '{stock_cached_body}')")

    docker_exec_sql(ORDER_DB, "orders",
         f"UPDATE sagas SET state = 'PAYMENT_REQUESTED' WHERE id = '{saga_id}'")

    docker_exec_sql(PAYMENT_DB, "payments",
         f"UPDATE users SET credit = credit - {int(ORDER_PRICE)} WHERE id = '{user}'")

    docker_exec_sql(PAYMENT_DB, "payments",
         f"INSERT INTO idempotency_keys (key, status_code, body) "
            f"VALUES ('{payment_idem_key}', 200, '{payment_cached_body}') ON CONFLICT DO NOTHING")

    docker_exec_sql(ORDER_DB, "orders",
         f"UPDATE sagas SET state = 'COMPLETED' WHERE id = '{saga_id}'")

    docker_exec_sql(ORDER_DB, "orders",
         f"UPDATE orders SET paid = TRUE WHERE id = '{order}'")

    docker_cmd(f"docker start {ORDER_CONTAINER}")

    wait_for_service(f"/orders/find/{order}", timeout=90)
    time.sleep(15)

    stock_val = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit_val = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    paid_val = json_field(api("GET", f"/orders/find/{order}"), "paid")

    print(paid_val)

    committed = (stock_val == expected_stock and credit_val == expected_credit and paid_val is True)

    check(
        "After Coordinator Crash And Recovery, State Is Fully Committed — ",
        committed,
        f"stock={stock_val}, credit={credit_val}, paid={paid_val}"
    )

# ---------------------------------------------------------------------------
# 13. Coordinator Crash — Saga crashes after rollback requested
# ---------------------------------------------------------------------------
def test_coordinator_crash_before_rolled_back():
    ITEM_PRICE = 20
    ITEM_QTY = 2
    STOCK = 10
    CREDIT = 200
    ORDER_CONTAINER = "wdm-project-group24-order-service-1"
    ORDER_DB = "wdm-project-group24-order-db-1"
    STOCK_DB = "wdm-project-group24-stock-db-1"

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{ITEM_QTY}")

    docker_cmd(f"docker stop {ORDER_CONTAINER}")

    saga_id = str(uuid.uuid4())
    items_quantities = json.dumps({item: ITEM_QTY})
    stock_idem_key = f"{saga_id}:stock:subtract_batch"
    new_stock = STOCK - ITEM_QTY
    cached_body = json.dumps({"updated_stock": {item: new_stock}})

    docker_exec_sql(ORDER_DB, "orders",
        f"INSERT INTO sagas (id, order_id, state, items_quantities, "
        f"original_correlation_id) VALUES "
        f"('{saga_id}', '{order}', 'STOCK_REQUESTED', '{items_quantities}', 'recovery-test')")

    docker_exec_sql(STOCK_DB, "stock",
        f"UPDATE items SET stock = stock - {ITEM_QTY} WHERE id = '{item}'")

    docker_exec_sql(STOCK_DB, "stock",
        f"INSERT INTO idempotency_keys (key, status_code, body) VALUES "
        f"('{stock_idem_key}', 200, '{cached_body}')")

    print("Will set payment requested")
    docker_exec_sql(ORDER_DB, "orders",
         f"UPDATE sagas SET state = 'PAYMENT_REQUESTED' WHERE id = '{saga_id}'")
    print("Set payment requested")

    docker_exec_sql(ORDER_DB, "orders",
         f"UPDATE sagas SET state = 'ROLLBACK_REQUESTED' WHERE id = '{saga_id}'")

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
# 14. Coordinator Crash — Saga crashes after rollback successful
# ---------------------------------------------------------------------------
def test_coordinator_crash_after_rolled_back():
    ITEM_PRICE = 20
    ITEM_QTY = 2
    STOCK = 10
    CREDIT = 200
    ORDER_CONTAINER = "wdm-project-group24-order-service-1"
    ORDER_DB = "wdm-project-group24-order-db-1"
    STOCK_DB = "wdm-project-group24-stock-db-1"

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{ITEM_QTY}")

    docker_cmd(f"docker stop {ORDER_CONTAINER}")

    saga_id = str(uuid.uuid4())
    items_quantities = json.dumps({item: ITEM_QTY})
    stock_idem_key = f"{saga_id}:stock:subtract_batch"
    new_stock = STOCK - ITEM_QTY
    cached_body_request = json.dumps({"updated_stock": {item: new_stock}})
    cached_body_rollback = json.dumps({"updated_stock": {item: STOCK}})

    docker_exec_sql(ORDER_DB, "orders",
        f"INSERT INTO sagas (id, order_id, state, items_quantities, "
        f"original_correlation_id) VALUES "
        f"('{saga_id}', '{order}', 'STOCK_REQUESTED', '{items_quantities}', 'recovery-test')")

    docker_exec_sql(STOCK_DB, "stock",
        f"UPDATE items SET stock = stock - {ITEM_QTY} WHERE id = '{item}'")

    docker_exec_sql(STOCK_DB, "stock",
        f"INSERT INTO idempotency_keys (key, status_code, body) VALUES "
        f"('{stock_idem_key}', 200, '{cached_body_request}')")

    docker_exec_sql(ORDER_DB, "orders",
         f"UPDATE sagas SET state = 'PAYMENT_REQUESTED' WHERE id = '{saga_id}'")

    docker_exec_sql(ORDER_DB, "orders",
         f"UPDATE sagas SET state = 'ROLLBACK_REQUESTED' WHERE id = '{saga_id}'")

    docker_exec_sql(STOCK_DB, "stock",
         f"UPDATE items SET stock = stock + {ITEM_QTY} WHERE id = '{item}'")

    docker_exec_sql(STOCK_DB, "stock",
         f"INSERT INTO idempotency_keys (key, status_code, body) VALUES "
         f"('{stock_idem_key}', 200, '{cached_body_rollback}')")

    docker_exec_sql(ORDER_DB, "orders",
         f"UPDATE sagas SET state = 'ROLLED_BACK' WHERE id = '{saga_id}'")

    docker_cmd(f"docker start {ORDER_CONTAINER}")

    wait_for_service(f"/orders/find/{order}", timeout=90)
    time.sleep(15)

    stock_val = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit_val = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    paid_val = json_field(api("GET", f"/orders/find/{order}"), "paid")

    rolled_back = (stock_val == STOCK and credit_val == CREDIT and paid_val is not True)
    check(
        "After Coordinator Crash And Recovery, State Is Fully Rolled Back",
        rolled_back,
        f"stock={stock_val}, credit={credit_val}, paid={paid_val}"
    )

# ---------------------------------------------------------------------------
# Ordered test list — imported by run.py
# ---------------------------------------------------------------------------
TESTS = [
    ("Transaction Saved in Idempotency ID: Checkout Successful, Nothing Changes", test_idempotency_saved),
    ("Order Already Paid: Checkout Fails, Nothing Changes", test_order_already_paid),
    ("Compensating Transaction: Payment Fails, Stock Rolled Back", test_compensation_payment_fails),
    ("Stock Reservation Fails — No Payment Attempted", test_stock_fails_no_payment),
    ("Participant Crash: Stock Dies Mid-Saga And Recovers", test_stock_crash_recovery),
    ("Participant Crash: Payment Dies Mid-Saga And Recovers", test_payment_crash_recovery),
    ("Coordinator Crash: Saga Crashes Before Stock Request", test_coordinator_crash_before_stock),
    ("Coordinator Crash: Saga Crashes After Stock Request", test_coordinator_crash_after_stock),
    ("Coordinator Crash: Saga Crashes Before Payment Request", test_coordinator_crash_before_payment),
    ("Coordinator Crash: Saga Crashes After Payment Request", test_coordinator_crash_after_payment),
    ("Coordinator Crash: Saga Crashes After Stock Failed", test_coordinator_crash_stock_failed),
    ("Coordinator Crash: Saga Crashes After Saga Completed", test_coordinator_crash_after_completed),
    ("Coordinator Crash: Saga Crashes After Rollback Requested", test_coordinator_crash_before_rolled_back),
    ("Coordinator Crash: Saga Crashes After Rollback Successful", test_coordinator_crash_after_rolled_back),
]
