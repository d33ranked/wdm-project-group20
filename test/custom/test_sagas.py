"""
SAGA Tests
==========
Tests specific to the SAGA transaction mode.
Covers compensating transactions, participant crash recovery,
and coordinator crash recovery via saga state persistence.
"""

import subprocess

from run import api, check, json_field, docker_cmd, wait_for_service


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

    docker_cmd(f"sudo docker stop {CONTAINER}")
    subprocess.Popen(
        f"sleep 3 && sudo docker start {CONTAINER}",
        shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    r = api("POST", f"/orders/checkout/{order}")
    expected_stock = STOCK - ITEM_QTY
    expected_credit = CREDIT - (ITEM_PRICE * ITEM_QTY)

    check("Checkout Completed After Stock Service Recovered — Kafka Message Persisted And Processed",
          r.status_code == 200, f"got {r.status_code} with text: {r.text}")

    wait_for_service(f"/stock/find/{item}")
    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")

    check(f"Stock Decreased To {expected_stock} After Recovery — {ITEM_QTY} Units Sold",
          stock == expected_stock, f"got {stock}")
    check(f"Credit Decreased To {expected_credit} After Recovery — "
          f"Charged {ITEM_PRICE}x{ITEM_QTY}",
          credit == expected_credit, f"got {credit}")


# ---------------------------------------------------------------------------
# Ordered test list — imported by run.py
# ---------------------------------------------------------------------------
TESTS = [
    ("Compensating Transaction: Payment Fails, Stock Rolled Back", test_compensation_payment_fails),
    ("Stock Reservation Fails — No Payment Attempted", test_stock_fails_no_payment),
    ("Participant Crash: Stock Dies Mid-Saga And Recovers", test_participant_crash_recovery),
]
