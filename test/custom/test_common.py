"""
Common Tests
=============
Mode-agnostic suite validating API correctness, consistency invariants,
concurrency under contention, boundary conditions, and edge cases.
"""

import concurrent.futures
import uuid

from run import api, check, json_field


# ---------------------------------------------------------------------------
# 1. Multi-Item Checkout Math
# ---------------------------------------------------------------------------
def test_multi_item_checkout():
    """Three items at different prices, verify per-item stock and total charge."""
    PRICES = [10, 25, 50]
    QTYS = [2, 3, 1]
    STOCK = 20
    CREDIT = 1000

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")

    items = []
    for price in PRICES:
        item = json_field(api("POST", f"/stock/item/create/{price}"), "item_id")
        api("POST", f"/stock/add/{item}/{STOCK}")
        items.append(item)

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    for item, qty in zip(items, QTYS):
        api("POST", f"/orders/addItem/{order}/{item}/{qty}")

    r = api("POST", f"/orders/checkout/{order}")
    total_cost = sum(p * q for p, q in zip(PRICES, QTYS))

    check("Checkout Succeeds For An Order Containing Three Different Items",
          r.status_code == 200, f"got {r.status_code}")

    for i, (item, price, qty) in enumerate(zip(items, PRICES, QTYS)):
        expected = STOCK - qty
        actual = json_field(api("GET", f"/stock/find/{item}"), "stock")
        check(f"Item {i+1} (Price={price}, Qty={qty}): Stock Decreased From {STOCK} To {expected}",
              actual == expected, f"got {actual}")

    balance = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    expected_bal = CREDIT - total_cost
    check(f"User Balance Decreased From {CREDIT} To {expected_bal} "
          f"(Charge: 10×2 + 25×3 + 50×1 = {total_cost})",
          balance == expected_bal, f"got {balance}")

    paid = json_field(api("GET", f"/orders/find/{order}"), "paid")
    check("Order Marked As Paid After Multi-Item Checkout", paid is True, f"got {paid}")


# ---------------------------------------------------------------------------
# 2. Double Checkout Prevention
# ---------------------------------------------------------------------------
def test_double_checkout():
    """Checkout same order twice — second must be rejected, no double charge."""
    PRICE = 20
    QTY = 2
    STOCK = 10
    CREDIT = 500

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")

    r1 = api("POST", f"/orders/checkout/{order}")
    check("First Checkout Succeeds", r1.status_code == 200, f"got {r1.status_code}")

    stock_after_first = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit_after_first = json_field(api("GET", f"/payment/find_user/{user}"), "credit")

    r2 = api("POST", f"/orders/checkout/{order}")
    check("Second Checkout On A Paid Order Is Skipped",
          r2.status_code == 200, f"got {r2.status_code}")

    stock_now = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit_now = json_field(api("GET", f"/payment/find_user/{user}"), "credit")

    check(f"Stock Unchanged At {stock_after_first} After Second Checkout — No Double Deduction",
          stock_now == stock_after_first, f"got {stock_now}")
    check(f"Balance Unchanged At {credit_after_first} After Second Checkout — No Double Charge",
          credit_now == credit_after_first, f"got {credit_now}")


# ---------------------------------------------------------------------------
# 3. Post-Checkout Tampering
# ---------------------------------------------------------------------------
def test_post_checkout_tampering():
    """After checkout, add an expensive item and re-checkout — no additional charges."""
    PRICE = 10
    QTY = 2
    STOCK = 20
    CREDIT = 500
    EXPENSIVE_PRICE = 200

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    expensive = json_field(api("POST", f"/stock/item/create/{EXPENSIVE_PRICE}"), "item_id")
    api("POST", f"/stock/add/{expensive}/10")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")

    r = api("POST", f"/orders/checkout/{order}")
    check("Initial Checkout Succeeds Before Tampering Attempt",
          r.status_code == 200, f"got {r.status_code}")

    credit_after = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    stock_after = json_field(api("GET", f"/stock/find/{item}"), "stock")

    api("POST", f"/orders/addItem/{order}/{expensive}/5")
    api("POST", f"/orders/checkout/{order}")

    credit_now = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    stock_now = json_field(api("GET", f"/stock/find/{item}"), "stock")
    expensive_stock = json_field(api("GET", f"/stock/find/{expensive}"), "stock")

    check(f"Balance Still {credit_after} — Adding Items To Paid Order Caused No Extra Charge",
          credit_now == credit_after, f"got {credit_now}")
    check(f"Original Item Stock Still {stock_after} — No Additional Units Deducted",
          stock_now == stock_after, f"got {stock_now}")
    check("Expensive Item Stock Still 10 — Was Never Sold Through Tampered Order",
          expensive_stock == 10, f"got {expensive_stock}")


# ---------------------------------------------------------------------------
# 4. Checkout Empty Order
# ---------------------------------------------------------------------------
def test_checkout_empty_order():
    """Checkout an order with no items — should not crash the server."""
    user = json_field(api("POST", "/payment/create_user"), "user_id")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")

    r = api("POST", f"/orders/checkout/{order}")
    check("Empty Order Checkout Is Skipped",
          r.status_code == 200, f"got {r.status_code}")


# ---------------------------------------------------------------------------
# 5. Last-Item Contention
# ---------------------------------------------------------------------------
def test_concurrent_fight_for_last_item():
    """10 users race for 1 item — exactly 1 wins, no oversell."""
    PRICE = 50
    N_USERS = 10
    CREDIT = 1000

    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/1")

    users_orders = []
    for _ in range(N_USERS):
        user = json_field(api("POST", "/payment/create_user"), "user_id")
        api("POST", f"/payment/add_funds/{user}/{CREDIT}")
        order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
        api("POST", f"/orders/addItem/{order}/{item}/1")
        users_orders.append((user, order))

    def checkout(pair):
        _, o = pair
        return api("POST", f"/orders/checkout/{o}").status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=N_USERS) as pool:
        results = list(pool.map(checkout, users_orders))

    winners = results.count(200)
    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")

    check(f"Exactly 1 Of {N_USERS} Concurrent Checkouts Won The Single Available Unit",
          winners == 1, f"got {winners} winners")
    check("Final Stock Is 0 — Exactly One Unit Was Sold, No Oversell",
          stock == 0, f"got {stock}")

    charged = sum(
        1 for u, _ in users_orders
        if json_field(api("GET", f"/payment/find_user/{u}"), "credit") != CREDIT
    )
    check(f"Only 1 User Was Charged — Remaining {N_USERS - 1} Retain Full {CREDIT} Credit",
          charged == 1, f"got {charged} users charged")


# ---------------------------------------------------------------------------
# 6. Sequential Stock Drain
# ---------------------------------------------------------------------------
def test_sequential_drain():
    """Three orders of qty=2 on stock=5 — first two succeed, third rejected."""
    PRICE = 10
    STOCK = 5
    QTY = 2

    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    users = []
    orders = []
    for _ in range(3):
        user = json_field(api("POST", "/payment/create_user"), "user_id")
        api("POST", f"/payment/add_funds/{user}/500")
        order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
        api("POST", f"/orders/addItem/{order}/{item}/{QTY}")
        users.append(user)
        orders.append(order)

    r1 = api("POST", f"/orders/checkout/{orders[0]}")
    check(f"First Checkout (Qty={QTY}) Succeeds — Stock Goes From {STOCK} To {STOCK - QTY}",
          r1.status_code == 200, f"got {r1.status_code}")

    r2 = api("POST", f"/orders/checkout/{orders[1]}")
    check(f"Second Checkout (Qty={QTY}) Succeeds — Stock Goes From {STOCK - QTY} To {STOCK - 2*QTY}",
          r2.status_code == 200, f"got {r2.status_code}")

    r3 = api("POST", f"/orders/checkout/{orders[2]}")
    remaining = STOCK - 2 * QTY
    check(f"Third Checkout (Qty={QTY}) Rejected — Only {remaining} Unit Left But {QTY} Needed",
          400 <= r3.status_code < 500, f"got {r3.status_code}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check(f"Final Stock Is {remaining} — Two Orders Fulfilled, Third Correctly Rejected",
          stock == remaining, f"got {stock}")

    credit3 = json_field(api("GET", f"/payment/find_user/{users[2]}"), "credit")
    check("Third User Balance Unchanged At 500 — Not Charged For Rejected Checkout",
          credit3 == 500, f"got {credit3}")


# ---------------------------------------------------------------------------
# 7. Concurrent Isolation
# ---------------------------------------------------------------------------
def test_concurrent_independent_checkouts():
    """5 independent users/items checkout simultaneously — all succeed, no cross-contamination."""
    N = 5
    PRICE = 20
    QTY = 3
    STOCK = 10
    CREDIT = 500

    users, items, orders = [], [], []
    for _ in range(N):
        user = json_field(api("POST", "/payment/create_user"), "user_id")
        api("POST", f"/payment/add_funds/{user}/{CREDIT}")
        item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
        api("POST", f"/stock/add/{item}/{STOCK}")
        order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
        api("POST", f"/orders/addItem/{order}/{item}/{QTY}")
        users.append(user)
        items.append(item)
        orders.append(order)

    def checkout(order_id):
        return api("POST", f"/orders/checkout/{order_id}").status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=N) as pool:
        results = list(pool.map(checkout, orders))

    check(f"All {N} Independent Checkouts Succeeded Concurrently",
          all(r == 200 for r in results), f"got {results}")

    expected_stock = STOCK - QTY
    expected_credit = CREDIT - (PRICE * QTY)
    for i in range(N):
        s = json_field(api("GET", f"/stock/find/{items[i]}"), "stock")
        c = json_field(api("GET", f"/payment/find_user/{users[i]}"), "credit")
        check(f"User {i+1}: Stock={expected_stock}, Balance={expected_credit} — "
              f"Isolated From Other Users",
              s == expected_stock and c == expected_credit,
              f"stock={s}, credit={c}")


# ---------------------------------------------------------------------------
# 8. Stale Stock
# ---------------------------------------------------------------------------
def test_stock_modified_before_checkout():
    """Create order when stock=10, externally subtract 5, then checkout needing 8 fails."""
    PRICE = 10
    STOCK = 10
    QTY = 8
    SUBTRACT = 5
    CREDIT = 500

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")

    api("POST", f"/stock/subtract/{item}/{SUBTRACT}")
    remaining = STOCK - SUBTRACT

    r = api("POST", f"/orders/checkout/{order}")
    check(f"Checkout Rejected — Order Needs {QTY} Units But Only {remaining} Remain "
          f"After External Subtraction",
          400 <= r.status_code < 500, f"got {r.status_code}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check(f"Stock Stays At {remaining} — No Partial Deduction From Failed Checkout",
          stock == remaining, f"got {stock}")

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"Balance Stays At {CREDIT} — Not Charged For A Failed Checkout",
          credit == CREDIT, f"got {credit}")


# ---------------------------------------------------------------------------
# 9. Late Funding
# ---------------------------------------------------------------------------
def test_fund_user_after_order():
    """User starts at 0 credit, creates order, adds funds, then checkout succeeds."""
    PRICE = 50
    QTY = 2
    STOCK = 10
    COST = PRICE * QTY

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")

    api("POST", f"/payment/add_funds/{user}/{COST}")

    r = api("POST", f"/orders/checkout/{order}")
    check(f"Checkout Succeeds — {COST} Credits Added After Order Was Created But Before Checkout",
          r.status_code == 200, f"got {r.status_code}")

    balance = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check("Balance Is 0 After Checkout — All Funds Spent On The Order",
          balance == 0, f"got {balance}")


# ---------------------------------------------------------------------------
# 10. Boundary — Exact Balance
# ---------------------------------------------------------------------------
def test_exact_balance_boundary():
    """User balance exactly equals order total — checkout succeeds, balance becomes 0."""
    PRICE = 30
    QTY = 2
    STOCK = 10
    COST = PRICE * QTY

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{COST}")
    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")

    r = api("POST", f"/orders/checkout/{order}")
    check(f"Checkout Succeeds When Balance ({COST}) Exactly Equals Order Total ({COST})",
          r.status_code == 200, f"got {r.status_code}")

    balance = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check("Balance Is Exactly 0 — No Off-By-One At The Boundary",
          balance == 0, f"got {balance}")


# ---------------------------------------------------------------------------
# 11. Boundary — Exact Stock
# ---------------------------------------------------------------------------
def test_exact_stock_boundary():
    """Stock exactly equals order quantity — checkout succeeds, stock becomes 0."""
    PRICE = 10
    QTY = 5
    STOCK = 5
    CREDIT = 500

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")

    r = api("POST", f"/orders/checkout/{order}")
    check(f"Checkout Succeeds When Stock ({STOCK}) Exactly Equals Order Quantity ({QTY})",
          r.status_code == 200, f"got {r.status_code}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock Is Exactly 0 — No Off-By-One At The Boundary",
          stock == 0, f"got {stock}")


# ---------------------------------------------------------------------------
# 12. Boundary — One Credit Short
# ---------------------------------------------------------------------------
def test_one_credit_short():
    """Order costs 100, user has 99 — checkout rejected."""
    PRICE = 100
    STOCK = 10
    CREDIT = 99

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/1")

    r = api("POST", f"/orders/checkout/{order}")
    check(f"Checkout Rejected — User Has {CREDIT} Credit But Order Costs {PRICE} (1 Short)",
          400 <= r.status_code < 500, f"got {r.status_code}")

    balance = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"Balance Unchanged At {CREDIT} — No Charge For Rejected Checkout",
          balance == CREDIT, f"got {balance}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check(f"Stock Unchanged At {STOCK} — No Deduction For Rejected Checkout",
          stock == STOCK, f"got {stock}")


# ---------------------------------------------------------------------------
# 13. Boundary — One Stock Unit Short
# ---------------------------------------------------------------------------
def test_one_stock_short():
    """Order needs 5, stock has 4 — checkout rejected."""
    PRICE = 10
    STOCK = 4
    QTY = 5
    CREDIT = 500

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")

    r = api("POST", f"/orders/checkout/{order}")
    check(f"Checkout Rejected — Order Needs {QTY} Units But Stock Has {STOCK} (1 Short)",
          400 <= r.status_code < 500, f"got {r.status_code}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check(f"Stock Unchanged At {STOCK} — No Partial Deduction",
          stock == STOCK, f"got {stock}")

    balance = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"Balance Unchanged At {CREDIT} — No Charge Applied",
          balance == CREDIT, f"got {balance}")


# ---------------------------------------------------------------------------
# 14. Non-Existent Resources
# ---------------------------------------------------------------------------
def test_find_nonexistent():
    """GET on random UUIDs should return 4xx, not 5xx."""
    fake = str(uuid.uuid4())

    r = api("GET", f"/stock/find/{fake}")
    check("GET Non-Existent Item Returns 4xx, Not A Server Error",
          400 <= r.status_code < 500, f"got {r.status_code}")

    r = api("GET", f"/payment/find_user/{fake}")
    check("GET Non-Existent User Returns 4xx, Not A Server Error",
          400 <= r.status_code < 500, f"got {r.status_code}")

    r = api("GET", f"/orders/find/{fake}")
    check("GET Non-Existent Order Returns 4xx, Not A Server Error",
          400 <= r.status_code < 500, f"got {r.status_code}")


# ---------------------------------------------------------------------------
# 15. addItem Idempotency — Same Key Sent Twice, Quantity Added Once
# ---------------------------------------------------------------------------
def test_add_item_idempotency():
    """Sending the same addItem request twice with the same idempotency key
    must add the item only once — no quantity doubling, no cost doubling."""
    PRICE = 10
    STOCK = 5

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")

    headers = {"Idempotency-Key": "add-item-idem-test-001"}

    r1 = api("POST", f"/orders/addItem/{order}/{item}/2", headers=headers)
    check("First addItem With Idempotency Key Succeeds",
          r1.status_code == 200, f"got {r1.status_code}")

    r2 = api("POST", f"/orders/addItem/{order}/{item}/2", headers=headers)
    check("Second addItem With Same Idempotency Key Returns 200 — Cached, Not Re-Applied",
          r2.status_code == 200, f"got {r2.status_code}")

    check("Both Responses Are Identical — Confirms Second Was Served From Cache",
          r1.text == r2.text, f"r1={r1.text!r} r2={r2.text!r}")

    order_data = api("GET", f"/orders/find/{order}").json()
    items = order_data.get("items", [])
    total_cost = order_data.get("total_cost", 0)

    quantity_in_order = sum(q for (iid, q) in items if iid == item)
    check("Order Contains Item With Quantity 2 — Not 4 (Not Added Twice)",
          quantity_in_order == 2, f"got quantity={quantity_in_order}")
    check(f"Total Cost Is {PRICE * 2} — Reflects One addItem Call, Not Two",
          total_cost == PRICE * 2, f"got total_cost={total_cost}")


# ---------------------------------------------------------------------------
# 16. Multi-Item Checkout Atomic Rollback — One Item Out Of Stock
# ---------------------------------------------------------------------------
def test_multi_item_checkout_partial_stock_failure():
    """Order has 3 items; the second item has 0 stock.
    Checkout must fail AND all stock levels must stay unchanged —
    no partial deduction from the items that did have stock."""
    CREDIT = 1000

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")

    item1 = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item1}/5")               # has stock

    item2 = json_field(api("POST", "/stock/item/create/20"), "item_id")
    # deliberately leave item2 at 0 stock — no add_funds call

    item3 = json_field(api("POST", "/stock/item/create/30"), "item_id")
    api("POST", f"/stock/add/{item3}/5")               # has stock

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item1}/2")
    api("POST", f"/orders/addItem/{order}/{item2}/1")  # will cause the failure
    api("POST", f"/orders/addItem/{order}/{item3}/1")

    r = api("POST", f"/orders/checkout/{order}")
    check("Checkout Rejected — Item2 Has 0 Stock, Entire Batch Must Fail",
          400 <= r.status_code < 500, f"got {r.status_code}")

    s1 = json_field(api("GET", f"/stock/find/{item1}"), "stock")
    s2 = json_field(api("GET", f"/stock/find/{item2}"), "stock")
    s3 = json_field(api("GET", f"/stock/find/{item3}"), "stock")

    check("Item1 Stock Still 5 — Not Partially Deducted Despite Having Enough Stock",
          s1 == 5, f"got {s1}")
    check("Item2 Stock Still 0 — Confirms It Was The Cause Of Failure",
          s2 == 0, f"got {s2}")
    check("Item3 Stock Still 5 — Not Partially Deducted Despite Having Enough Stock",
          s3 == 5, f"got {s3}")

    balance = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"User Balance Unchanged At {CREDIT} — Not Charged For A Failed Checkout",
          balance == CREDIT, f"got {balance}")


# ---------------------------------------------------------------------------
# Ordered test list — imported by run.py
# ---------------------------------------------------------------------------
TESTS = [
    ("Multi-Item Checkout With Per-Item Stock Verification", test_multi_item_checkout),
    ("Double Checkout On A Paid Order", test_double_checkout),
    ("Add Items To A Paid Order And Re-Checkout", test_post_checkout_tampering),
    ("Checkout An Empty Order (No Items)", test_checkout_empty_order),
    ("10 Concurrent Checkouts For 1 Unit Of Stock", test_concurrent_fight_for_last_item),
    ("Sequential Checkouts Until Stock Exhausted", test_sequential_drain),
    ("5 Independent Checkouts In Parallel", test_concurrent_independent_checkouts),
    ("External Stock Change Between Order And Checkout", test_stock_modified_before_checkout),
    ("Fund User After Order Creation, Then Checkout", test_fund_user_after_order),
    ("Boundary: Balance Exactly Equals Order Total", test_exact_balance_boundary),
    ("Boundary: Stock Exactly Equals Order Quantity", test_exact_stock_boundary),
    ("Boundary: One Credit Short Of Order Total", test_one_credit_short),
    ("Boundary: One Stock Unit Short Of Order Quantity", test_one_stock_short),
    ("GET On Non-Existent Stock, User, And Order IDs", test_find_nonexistent),
    ("addItem Idempotency — Same Key Prevents Duplicate Quantity", test_add_item_idempotency),
    ("Multi-Item Checkout Atomic Rollback When One Item Is Out Of Stock", test_multi_item_checkout_partial_stock_failure),
]
