"""
Common Tests
=============
Tests that apply regardless of transaction mode (TPC or SAGA).
Each test function is self-contained: creates its own resources, runs assertions, cleans up.
"""

import concurrent.futures
import uuid

from run import api, check, json_field


# ---------------------------------------------------------------------------
# 1. Stock CRUD
# ---------------------------------------------------------------------------
def test_stock_crud():
    """Create an item, add stock, verify, subtract, verify."""
    r = api("POST", "/stock/item/create/25")
    item_id = json_field(r, "item_id")
    check("POST /stock/item/create returns 200 and a valid item_id", r.status_code == 200 and item_id is not None)

    r = api("POST", f"/stock/add/{item_id}/10")
    check("POST /stock/add adds 10 units to the newly created item", r.status_code == 200)

    r = api("GET", f"/stock/find/{item_id}")
    stock = json_field(r, "stock")
    price = json_field(r, "price")
    check("GET /stock/find confirms stock count is 10 after adding", stock == 10, f"got {stock}")
    check("GET /stock/find confirms item price persisted as 25", price == 25, f"got {price}")

    r = api("POST", f"/stock/subtract/{item_id}/4")
    check("POST /stock/subtract deducts 4 units successfully (200)", r.status_code == 200)

    r = api("GET", f"/stock/find/{item_id}")
    stock = json_field(r, "stock")
    check("Stock count is now 6 after subtracting 4 from 10", stock == 6, f"got {stock}")

    r = api("POST", f"/stock/subtract/{item_id}/999")
    check("Subtracting 999 from 6 stock is rejected with 4xx (prevents oversell)", 400 <= r.status_code < 500)


# ---------------------------------------------------------------------------
# 2. Payment CRUD
# ---------------------------------------------------------------------------
def test_payment_crud():
    """Create a user, add funds, verify, pay, verify."""
    r = api("POST", "/payment/create_user")
    user_id = json_field(r, "user_id")
    check("POST /payment/create_user returns 200 and a valid user_id", r.status_code == 200 and user_id is not None)

    r = api("POST", f"/payment/add_funds/{user_id}/150")
    check("POST /payment/add_funds adds 150 credits to the new user", r.status_code == 200)

    r = api("GET", f"/payment/find_user/{user_id}")
    credit = json_field(r, "credit")
    check("GET /payment/find_user confirms credit balance is 150", credit == 150, f"got {credit}")

    r = api("POST", f"/payment/pay/{user_id}/50")
    check("POST /payment/pay deducts 50 credits successfully (200)", r.status_code == 200)

    r = api("GET", f"/payment/find_user/{user_id}")
    credit = json_field(r, "credit")
    check("Credit balance is now 100 after paying 50 from 150", credit == 100, f"got {credit}")

    r = api("POST", f"/payment/pay/{user_id}/999")
    check("Paying 999 from 100 credit is rejected with 4xx (prevents overdraft)", 400 <= r.status_code < 500)


# ---------------------------------------------------------------------------
# 3. Order lifecycle
# ---------------------------------------------------------------------------
def test_order_lifecycle():
    """Create order, add items, verify structure."""
    user = json_field(api("POST", "/payment/create_user"), "user_id")
    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/20")
    api("POST", f"/payment/add_funds/{user}/500")

    r = api("POST", f"/orders/create/{user}")
    order_id = json_field(r, "order_id")
    check("POST /orders/create returns 200 and a valid order_id", r.status_code == 200 and order_id is not None)

    r = api("POST", f"/orders/addItem/{order_id}/{item}/3")
    check("POST /orders/addItem attaches 3 units of item to the order", r.status_code == 200)

    r = api("GET", f"/orders/find/{order_id}")
    check("GET /orders/find retrieves the order successfully (200)", r.status_code == 200)
    check("Order's user_id matches the user who created it", json_field(r, "user_id") == user)


# ---------------------------------------------------------------------------
# 4. Successful checkout (end-to-end)
# ---------------------------------------------------------------------------
def test_checkout_success():
    """Full checkout: stock decreases, credit decreases, order marked paid."""
    ITEM_PRICE = 20
    ITEM_QTY = 3
    STARTING_CREDIT = 500
    STARTING_STOCK = 10

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{STARTING_CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STARTING_STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{ITEM_QTY}")

    r = api("POST", f"/orders/checkout/{order}")
    check("POST /orders/checkout completes successfully (200)", r.status_code == 200, f"got {r.status_code}")

    expected_stock = STARTING_STOCK - ITEM_QTY
    expected_credit = STARTING_CREDIT - (ITEM_PRICE * ITEM_QTY)

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    paid = json_field(api("GET", f"/orders/find/{order}"), "paid")

    check(f"Stock decreased from {STARTING_STOCK} to {expected_stock} ({ITEM_QTY} units sold)", stock == expected_stock, f"got {stock}")
    check(f"Credit decreased from {STARTING_CREDIT} to {expected_credit} (charged {ITEM_PRICE}×{ITEM_QTY})", credit == expected_credit, f"got {credit}")
    check("Order is now marked as paid=True in the database", paid is True, f"got {paid}")


# ---------------------------------------------------------------------------
# 5. Checkout rollback — insufficient credit
# ---------------------------------------------------------------------------
def test_checkout_rollback_insufficient_credit():
    """Checkout fails when user cannot afford the order; stock is restored."""
    ITEM_PRICE = 100
    STARTING_STOCK = 10

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/5")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STARTING_STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/1")

    r = api("POST", f"/orders/checkout/{order}")
    check("Checkout rejected with 4xx — user has 5 credit but item costs 100", 400 <= r.status_code < 500, f"got {r.status_code}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"Stock rolled back to {STARTING_STOCK} — no items were deducted", stock == STARTING_STOCK, f"got {stock}")
    check("Credit stayed at 5 — no charge was applied", credit == 5, f"got {credit}")


# ---------------------------------------------------------------------------
# 6. Checkout rollback — insufficient stock
# ---------------------------------------------------------------------------
def test_checkout_rollback_insufficient_stock():
    """Checkout fails when stock is insufficient; credit is untouched."""
    ITEM_PRICE = 10
    STARTING_CREDIT = 500
    STARTING_STOCK = 2

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{STARTING_CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STARTING_STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/50")

    r = api("POST", f"/orders/checkout/{order}")
    check("Checkout rejected with 4xx — wants 50 units but only 2 in stock", 400 <= r.status_code < 500, f"got {r.status_code}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"Stock unchanged at {STARTING_STOCK} — nothing was reserved", stock == STARTING_STOCK, f"got {stock}")
    check(f"Credit unchanged at {STARTING_CREDIT} — no payment was attempted", credit == STARTING_CREDIT, f"got {credit}")


# ---------------------------------------------------------------------------
# 7. Atomicity — concurrent stock subtract (no oversell)
# ---------------------------------------------------------------------------
def test_atomicity_no_oversell():
    """Fire N concurrent subtract-1 requests on stock=K. Exactly K succeed."""
    STOCK = 5
    CONCURRENT = 10

    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    def subtract_one(_):
        return api("POST", f"/stock/subtract/{item}/1").status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT) as pool:
        results = list(pool.map(subtract_one, range(CONCURRENT)))

    successes = results.count(200)
    final = json_field(api("GET", f"/stock/find/{item}"), "stock")

    check(f"Exactly {STOCK} of {CONCURRENT} concurrent subtract-1 requests succeeded (stock was {STOCK})", successes == STOCK, f"got {successes}")
    check("Final stock is 0 — no oversell occurred despite concurrent access", final == 0, f"got {final}")

    rejects = [c for c in results if c != 200]
    if rejects:
        check("All rejected requests returned 4xx (not 5xx server errors)", all(400 <= c < 500 for c in rejects), f"got {rejects}")


# ---------------------------------------------------------------------------
# 8. Idempotency — stock
# ---------------------------------------------------------------------------
def test_idempotency_stock():
    """Same Idempotency-Key twice subtracts stock only once."""
    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/10")

    key = f"idem-{uuid.uuid4()}"
    headers = {"Idempotency-Key": key}

    r1 = api("POST", f"/stock/subtract/{item}/3", headers=headers)
    r2 = api("POST", f"/stock/subtract/{item}/3", headers=headers)

    check("First subtract with Idempotency-Key returns 200", r1.status_code == 200)
    check("Replayed subtract with same Idempotency-Key also returns 200 (cached response)", r2.status_code == 200)

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock is 7 — subtracted only once despite two calls with same key", stock == 7, f"got {stock}")


# ---------------------------------------------------------------------------
# 9. Idempotency — payment
# ---------------------------------------------------------------------------
def test_idempotency_payment():
    """Same Idempotency-Key twice deducts credit only once."""
    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/100")

    key = f"idem-{uuid.uuid4()}"
    headers = {"Idempotency-Key": key}

    api("POST", f"/payment/pay/{user}/30", headers=headers)
    api("POST", f"/payment/pay/{user}/30", headers=headers)

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check("Credit is 70 — deducted only once despite two calls with same key", credit == 70, f"got {credit}")


# ---------------------------------------------------------------------------
# Ordered test list — imported by run.py
# ---------------------------------------------------------------------------
TESTS = [
    ("Stock Service CRUD — create item, add/subtract stock, reject over-subtract", test_stock_crud),
    ("Payment Service CRUD — create user, add/deduct funds, reject overdraft", test_payment_crud),
    ("Order Lifecycle — create order, attach items, verify ownership", test_order_lifecycle),
    ("End-to-End Checkout — stock decreases, credit charged, order marked paid", test_checkout_success),
    ("Checkout Rollback (insufficient credit) — user can't afford, nothing changes", test_checkout_rollback_insufficient_credit),
    ("Checkout Rollback (insufficient stock) — not enough stock, credit untouched", test_checkout_rollback_insufficient_stock),
    ("Atomicity — 10 concurrent subtract-1 on stock=5, exactly 5 succeed, no oversell", test_atomicity_no_oversell),
    ("Stock Idempotency — same Idempotency-Key twice, stock subtracted only once", test_idempotency_stock),
    ("Payment Idempotency — same Idempotency-Key twice, credit deducted only once", test_idempotency_payment),
]
