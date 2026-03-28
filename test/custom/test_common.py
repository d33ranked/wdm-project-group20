# common tests — mode-agnostic: checkout correctness, idempotency, storage, fault injection

import concurrent.futures
import subprocess
import time
import uuid

import run
from run import api, check, json_field, docker_cmd, docker_exec_redis, wait_for_service


def test_multi_item_checkout():
    # three items at different prices, verify per-item stock and total charge
    PRICES = [10, 25, 50]
    QTYS   = [2, 3, 1]
    STOCK  = 20
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
        check(f"Item {i+1} (Price={price}, Qty={qty}) Stock Decreased From {STOCK} To {expected}",
              actual == expected, f"got {actual}")

    balance = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    expected_bal = CREDIT - total_cost
    check(f"Balance Decreased From {CREDIT} To {expected_bal} (Charge: {total_cost})",
          balance == expected_bal, f"got {balance}")

    paid = json_field(api("GET", f"/orders/find/{order}"), "paid")
    check("Order Marked As Paid After Multi-Item Checkout", paid is True, f"got {paid}")


def test_double_checkout():
    # checkout same order twice — second must be rejected, no double charge
    PRICE  = 20
    QTY    = 2
    STOCK  = 10
    CREDIT = 500

    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")

    r1 = api("POST", f"/orders/checkout/{order}")
    check("First Checkout Succeeds", r1.status_code == 200, f"got {r1.status_code}")

    stock_after  = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit_after = json_field(api("GET", f"/payment/find_user/{user}"), "credit")

    r2 = api("POST", f"/orders/checkout/{order}")
    check("Second Checkout On A Paid Order Is Skipped",
          r2.status_code == 200, f"got {r2.status_code}")
    check(f"Stock Unchanged At {stock_after} After Second Checkout",
          json_field(api("GET", f"/stock/find/{item}"), "stock") == stock_after)
    check(f"Balance Unchanged At {credit_after} After Second Checkout",
          json_field(api("GET", f"/payment/find_user/{user}"), "credit") == credit_after)


def test_post_checkout_tampering():
    # after checkout, add expensive item and re-checkout — no additional charges
    PRICE           = 10
    QTY             = 2
    STOCK           = 20
    CREDIT          = 500
    EXPENSIVE_PRICE = 200

    user      = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item      = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    expensive = json_field(api("POST", f"/stock/item/create/{EXPENSIVE_PRICE}"), "item_id")
    api("POST", f"/stock/add/{expensive}/10")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")
    api("POST", f"/orders/checkout/{order}")

    credit_after = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    stock_after  = json_field(api("GET", f"/stock/find/{item}"), "stock")

    api("POST", f"/orders/addItem/{order}/{expensive}/5")
    api("POST", f"/orders/checkout/{order}")

    check(f"Balance Still {credit_after} — Adding Items To Paid Order Caused No Extra Charge",
          json_field(api("GET", f"/payment/find_user/{user}"), "credit") == credit_after)
    check(f"Original Item Stock Still {stock_after} — No Additional Units Deducted",
          json_field(api("GET", f"/stock/find/{item}"), "stock") == stock_after)
    check("Expensive Item Stock Still 10 — Was Never Sold Through Tampered Order",
          json_field(api("GET", f"/stock/find/{expensive}"), "stock") == 10)


def test_checkout_empty_order():
    # checkout an order with no items — should not crash
    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    r     = api("POST", f"/orders/checkout/{order}")
    check("Empty Order Checkout Is Skipped", r.status_code == 200, f"got {r.status_code}")


def test_concurrent_fight_for_last_item():
    # 10 users race for 1 item — exactly 1 wins, no oversell
    PRICE   = 50
    N_USERS = 10
    CREDIT  = 1000

    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/1")

    pairs = []
    for _ in range(N_USERS):
        user  = json_field(api("POST", "/payment/create_user"), "user_id")
        api("POST", f"/payment/add_funds/{user}/{CREDIT}")
        order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
        api("POST", f"/orders/addItem/{order}/{item}/1")
        pairs.append((user, order))

    with concurrent.futures.ThreadPoolExecutor(max_workers=N_USERS) as pool:
        results = list(pool.map(
            lambda p: api("POST", f"/orders/checkout/{p[1]}").status_code, pairs))

    winners = results.count(200)
    stock   = json_field(api("GET", f"/stock/find/{item}"), "stock")
    charged = sum(1 for u, _ in pairs
                  if json_field(api("GET", f"/payment/find_user/{u}"), "credit") != CREDIT)

    check(f"Exactly 1 Of {N_USERS} Concurrent Checkouts Won The Single Available Unit",
          winners == 1, f"got {winners} winners")
    check("Final Stock Is 0 — Exactly One Unit Was Sold, No Oversell",
          stock == 0, f"got {stock}")
    check(f"Only 1 User Was Charged — Remaining {N_USERS - 1} Retain Full Credit",
          charged == 1, f"got {charged} users charged")


def test_sequential_drain():
    # three orders of qty=2 on stock=5 — first two succeed, third rejected
    PRICE = 10
    STOCK = 5
    QTY   = 2

    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    users, orders = [], []
    for _ in range(3):
        user  = json_field(api("POST", "/payment/create_user"), "user_id")
        api("POST", f"/payment/add_funds/{user}/500")
        order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
        api("POST", f"/orders/addItem/{order}/{item}/{QTY}")
        users.append(user)
        orders.append(order)

    r1 = api("POST", f"/orders/checkout/{orders[0]}")
    check(f"First Checkout (Qty={QTY}) Succeeds", r1.status_code == 200, f"got {r1.status_code}")

    r2 = api("POST", f"/orders/checkout/{orders[1]}")
    check(f"Second Checkout (Qty={QTY}) Succeeds", r2.status_code == 200, f"got {r2.status_code}")

    r3 = api("POST", f"/orders/checkout/{orders[2]}")
    remaining = STOCK - 2 * QTY
    check(f"Third Checkout (Qty={QTY}) Rejected — Only {remaining} Unit Left",
          400 <= r3.status_code < 500, f"got {r3.status_code}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check(f"Final Stock Is {remaining} — Two Orders Fulfilled, Third Correctly Rejected",
          stock == remaining, f"got {stock}")
    check("Third User Balance Unchanged — Not Charged For Rejected Checkout",
          json_field(api("GET", f"/payment/find_user/{users[2]}"), "credit") == 500)


def test_concurrent_independent_checkouts():
    # 5 independent users/items checkout simultaneously — all succeed, no cross-contamination
    N      = 5
    PRICE  = 20
    QTY    = 3
    STOCK  = 10
    CREDIT = 500

    users, items, orders = [], [], []
    for _ in range(N):
        user  = json_field(api("POST", "/payment/create_user"), "user_id")
        api("POST", f"/payment/add_funds/{user}/{CREDIT}")
        item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
        api("POST", f"/stock/add/{item}/{STOCK}")
        order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
        api("POST", f"/orders/addItem/{order}/{item}/{QTY}")
        users.append(user)
        items.append(item)
        orders.append(order)

    with concurrent.futures.ThreadPoolExecutor(max_workers=N) as pool:
        results = list(pool.map(
            lambda o: api("POST", f"/orders/checkout/{o}").status_code, orders))

    check(f"All {N} Independent Checkouts Succeeded Concurrently",
          all(r == 200 for r in results), f"got {results}")

    for i in range(N):
        s = json_field(api("GET", f"/stock/find/{items[i]}"), "stock")
        c = json_field(api("GET", f"/payment/find_user/{users[i]}"), "credit")
        check(f"User {i+1}: Stock={STOCK - QTY}, Balance={CREDIT - PRICE * QTY} — Isolated",
              s == STOCK - QTY and c == CREDIT - PRICE * QTY, f"stock={s}, credit={c}")


def test_stock_modified_before_checkout():
    # create order when stock=10, externally subtract 5, then checkout needing 8 fails
    PRICE    = 10
    STOCK    = 10
    QTY      = 8
    SUBTRACT = 5
    CREDIT   = 500

    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")
    api("POST", f"/stock/subtract/{item}/{SUBTRACT}")

    remaining = STOCK - SUBTRACT
    r = api("POST", f"/orders/checkout/{order}")
    check(f"Checkout Rejected — Order Needs {QTY} Units But Only {remaining} Remain",
          400 <= r.status_code < 500, f"got {r.status_code}")
    check(f"Stock Stays At {remaining} — No Partial Deduction From Failed Checkout",
          json_field(api("GET", f"/stock/find/{item}"), "stock") == remaining)
    check(f"Balance Stays At {CREDIT} — Not Charged For A Failed Checkout",
          json_field(api("GET", f"/payment/find_user/{user}"), "credit") == CREDIT)


def test_fund_user_after_order():
    # user starts at 0 credit, creates order, adds funds, then checkout succeeds
    PRICE = 50
    QTY   = 2
    STOCK = 10
    COST  = PRICE * QTY

    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")
    api("POST", f"/payment/add_funds/{user}/{COST}")

    r = api("POST", f"/orders/checkout/{order}")
    check(f"Checkout Succeeds — {COST} Credits Added After Order Was Created But Before Checkout",
          r.status_code == 200, f"got {r.status_code}")
    check("Balance Is 0 After Checkout — All Funds Spent On The Order",
          json_field(api("GET", f"/payment/find_user/{user}"), "credit") == 0)


def test_exact_balance_boundary():
    # balance exactly equals order total — checkout succeeds, balance becomes 0
    PRICE = 30
    QTY   = 2
    STOCK = 10
    COST  = PRICE * QTY

    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{COST}")
    item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")

    r = api("POST", f"/orders/checkout/{order}")
    check(f"Checkout Succeeds When Balance ({COST}) Exactly Equals Order Total ({COST})",
          r.status_code == 200, f"got {r.status_code}")
    check("Balance Is Exactly 0 — No Off-By-One At The Boundary",
          json_field(api("GET", f"/payment/find_user/{user}"), "credit") == 0)


def test_exact_stock_boundary():
    # stock exactly equals order quantity — checkout succeeds, stock becomes 0
    PRICE  = 10
    QTY    = 5
    STOCK  = 5
    CREDIT = 500

    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")

    r = api("POST", f"/orders/checkout/{order}")
    check(f"Checkout Succeeds When Stock ({STOCK}) Exactly Equals Order Quantity ({QTY})",
          r.status_code == 200, f"got {r.status_code}")
    check("Stock Is Exactly 0 — No Off-By-One At The Boundary",
          json_field(api("GET", f"/stock/find/{item}"), "stock") == 0)


def test_one_credit_short():
    # order costs 100, user has 99 — checkout rejected
    PRICE  = 100
    STOCK  = 10
    CREDIT = 99

    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/1")

    r = api("POST", f"/orders/checkout/{order}")
    check(f"Checkout Rejected — User Has {CREDIT} Credit But Order Costs {PRICE}",
          400 <= r.status_code < 500, f"got {r.status_code}")
    check(f"Balance Unchanged At {CREDIT} — No Charge For Rejected Checkout",
          json_field(api("GET", f"/payment/find_user/{user}"), "credit") == CREDIT)
    check(f"Stock Unchanged At {STOCK} — No Deduction For Rejected Checkout",
          json_field(api("GET", f"/stock/find/{item}"), "stock") == STOCK)


def test_one_stock_short():
    # order needs 5, stock has 4 — checkout rejected
    PRICE  = 10
    STOCK  = 4
    QTY    = 5
    CREDIT = 500

    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY}")

    r = api("POST", f"/orders/checkout/{order}")
    check(f"Checkout Rejected — Order Needs {QTY} Units But Stock Has {STOCK}",
          400 <= r.status_code < 500, f"got {r.status_code}")
    check(f"Stock Unchanged At {STOCK} — No Partial Deduction",
          json_field(api("GET", f"/stock/find/{item}"), "stock") == STOCK)
    check(f"Balance Unchanged At {CREDIT} — No Charge Applied",
          json_field(api("GET", f"/payment/find_user/{user}"), "credit") == CREDIT)


def test_find_nonexistent():
    # GET on random UUIDs should return 4xx, not 5xx
    fake = str(uuid.uuid4())
    check("GET Non-Existent Item Returns 4xx",
          400 <= api("GET", f"/stock/find/{fake}").status_code < 500)
    check("GET Non-Existent User Returns 4xx",
          400 <= api("GET", f"/payment/find_user/{fake}").status_code < 500)
    check("GET Non-Existent Order Returns 4xx",
          400 <= api("GET", f"/orders/find/{fake}").status_code < 500)


def test_add_item_idempotency():
    # same addItem with same idempotency key adds quantity only once
    PRICE = 10
    STOCK = 5

    user    = json_field(api("POST", "/payment/create_user"), "user_id")
    item    = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order   = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    headers = {"Idempotency-Key": "add-item-idem-test-001"}

    r1 = api("POST", f"/orders/addItem/{order}/{item}/2", headers=headers)
    r2 = api("POST", f"/orders/addItem/{order}/{item}/2", headers=headers)

    check("First addItem Succeeds", r1.status_code == 200, f"got {r1.status_code}")
    check("Second addItem With Same Key Returns 200 — Served From Cache",
          r2.status_code == 200, f"got {r2.status_code}")
    check("Both Responses Are Identical — Confirms Second Was Cached",
          r1.text == r2.text)

    order_data   = api("GET", f"/orders/find/{order}").json()
    items_list   = order_data.get("items", [])
    qty_in_order = sum(q for (iid, q) in items_list if iid == item)
    check("Order Contains Item With Quantity 2 — Not 4 (Not Added Twice)",
          qty_in_order == 2, f"got qty={qty_in_order}")
    check(f"Total Cost Is {PRICE * 2} — Reflects One addItem Call, Not Two",
          order_data.get("total_cost", 0) == PRICE * 2)


def test_multi_item_checkout_partial_stock_failure():
    # order has 3 items; second has 0 stock — entire batch must atomically fail
    CREDIT = 1000

    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item1 = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item1}/5")
    item2 = json_field(api("POST", "/stock/item/create/20"), "item_id")   # left at 0
    item3 = json_field(api("POST", "/stock/item/create/30"), "item_id")
    api("POST", f"/stock/add/{item3}/5")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item1}/2")
    api("POST", f"/orders/addItem/{order}/{item2}/1")
    api("POST", f"/orders/addItem/{order}/{item3}/1")

    r = api("POST", f"/orders/checkout/{order}")
    check("Checkout Rejected — Item2 Has 0 Stock, Entire Batch Must Fail",
          400 <= r.status_code < 500, f"got {r.status_code}")
    check("Item1 Stock Still 5 — Not Partially Deducted",
          json_field(api("GET", f"/stock/find/{item1}"), "stock") == 5)
    check("Item2 Stock Still 0 — Confirms It Was The Cause Of Failure",
          json_field(api("GET", f"/stock/find/{item2}"), "stock") == 0)
    check("Item3 Stock Still 5 — Not Partially Deducted",
          json_field(api("GET", f"/stock/find/{item3}"), "stock") == 5)
    check(f"User Balance Unchanged At {CREDIT} — Not Charged For Failed Checkout",
          json_field(api("GET", f"/payment/find_user/{user}"), "credit") == CREDIT)


def test_batch_init_stock_and_payment():
    # batch_init creates integer-keyed items and users readable immediately
    N           = 5
    START_STOCK = 50
    ITEM_PRICE  = 10
    START_MONEY = 200

    r_stock = api("POST", f"/stock/batch_init/{N}/{START_STOCK}/{ITEM_PRICE}")
    check("Batch Init Stock Returns 200", r_stock.status_code == 200, f"got {r_stock.status_code}")
    r_pay = api("POST", f"/payment/batch_init/{N}/{START_MONEY}")
    check("Batch Init Payment Returns 200", r_pay.status_code == 200, f"got {r_pay.status_code}")

    for i in range(N):
        data = api("GET", f"/stock/find/{i}")
        check(f"Batch-Init Item {i} Readable", data.status_code == 200, f"got {data.status_code}")
        check(f"Item {i} Has Stock={START_STOCK}", json_field(data, "stock") == START_STOCK)
        check(f"Item {i} Has Price={ITEM_PRICE}",  json_field(data, "price") == ITEM_PRICE)

    for i in range(N):
        data = api("GET", f"/payment/find_user/{i}")
        check(f"Batch-Init User {i} Readable", data.status_code == 200, f"got {data.status_code}")
        check(f"User {i} Has Credit={START_MONEY}", json_field(data, "credit") == START_MONEY)


def test_add_stock_idempotency():
    # same add_stock with same idempotency key does not double-add
    INITIAL  = 10
    ADD_AMT  = 5
    IDEM_KEY = str(uuid.uuid4())

    item    = json_field(api("POST", "/stock/item/create/1"), "item_id")
    api("POST", f"/stock/add/{item}/{INITIAL}")
    headers = {"Idempotency-Key": IDEM_KEY}

    r1 = api("POST", f"/stock/add/{item}/{ADD_AMT}", headers=headers)
    r2 = api("POST", f"/stock/add/{item}/{ADD_AMT}", headers=headers)
    check("First Add-Stock Request Succeeds", r1.status_code == 200)
    check("Second Add-Stock With Same Key Returns 200 (Cached)", r2.status_code == 200)

    stock    = json_field(api("GET", f"/stock/find/{item}"), "stock")
    expected = INITIAL + ADD_AMT
    check(f"Stock Is {expected} — Idempotency Key Prevented The Second Add",
          stock == expected, f"got {stock}")


def test_pay_idempotency():
    # same /pay request with same idempotency key does not double-deduct
    INITIAL  = 100
    PAY_AMT  = 30
    IDEM_KEY = str(uuid.uuid4())

    user    = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{INITIAL}")
    headers = {"Idempotency-Key": IDEM_KEY}

    r1 = api("POST", f"/payment/pay/{user}/{PAY_AMT}", headers=headers)
    r2 = api("POST", f"/payment/pay/{user}/{PAY_AMT}", headers=headers)
    check("First Pay Request Succeeds", r1.status_code == 200)
    check("Second Pay With Same Key Returns 200 (Cached)", r2.status_code == 200)

    credit   = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    expected = INITIAL - PAY_AMT
    check(f"Credit Is {expected} — Idempotency Key Prevented The Second Deduction",
          credit == expected, f"got {credit}")


def test_order_item_quantity_merge():
    # adding same item twice merges quantities, not double-lists
    PRICE  = 10
    STOCK  = 20
    QTY_1  = 3
    QTY_2  = 4
    CREDIT = 1000

    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY_1}")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY_2}")

    order_data   = api("GET", f"/orders/find/{order}").json()
    items_list   = order_data.get("items", [])
    total_cost   = order_data.get("total_cost", 0)

    check("Order Items List Has Exactly One Entry After Adding Same Item Twice",
          len(items_list) == 1, f"got {len(items_list)} entries")
    expected_qty = QTY_1 + QTY_2
    merged_qty   = items_list[0][1] if items_list else 0
    check(f"Merged Quantity Is {expected_qty} (= {QTY_1} + {QTY_2})",
          merged_qty == expected_qty, f"got {merged_qty}")
    check(f"Total Cost Reflects Merged Quantity: {PRICE * expected_qty}",
          total_cost == PRICE * expected_qty, f"got {total_cost}")


def test_zero_amount_rejected():
    # add_stock and add_funds reject amount <= 0 with 4xx
    item = json_field(api("POST", "/stock/item/create/5"), "item_id")
    user = json_field(api("POST", "/payment/create_user"), "user_id")

    for amt in ("0", "-1", "-100"):
        r_s = api("POST", f"/stock/add/{item}/{amt}")
        check(f"Add Stock With Amount={amt} Returns 4xx",
              400 <= r_s.status_code < 500, f"got {r_s.status_code}")
        r_p = api("POST", f"/payment/add_funds/{user}/{amt}")
        check(f"Add Funds With Amount={amt} Returns 4xx",
              400 <= r_p.status_code < 500, f"got {r_p.status_code}")

    check("Stock Remains 0 After All Rejected Adds",
          json_field(api("GET", f"/stock/find/{item}"), "stock") == 0)
    check("Credit Remains 0 After All Rejected Adds",
          json_field(api("GET", f"/payment/find_user/{user}"), "credit") == 0)


def test_item_not_found_subtract():
    # subtracting from a nonexistent item returns 4xx, not 500
    ghost = str(uuid.uuid4())
    check("Subtract From Nonexistent Item Returns 4xx",
          400 <= api("POST", f"/stock/subtract/{ghost}/1").status_code < 500)
    check("Find Nonexistent Item Returns 4xx",
          400 <= api("GET", f"/stock/find/{ghost}").status_code < 500)


def test_user_not_found_pay():
    # paying from a nonexistent user returns 4xx, not 500
    ghost = str(uuid.uuid4())
    check("Pay From Nonexistent User Returns 4xx",
          400 <= api("POST", f"/payment/pay/{ghost}/50").status_code < 500)
    check("Find Nonexistent User Returns 4xx",
          400 <= api("GET", f"/payment/find_user/{ghost}").status_code < 500)


def test_malformed_stream_message():
    # inject garbage into the stream; verify consumer survives and processes real messages
    stream = "tpc.stock" if run.MODE == "TPC" else "gateway.stock"
    docker_exec_redis(
        "wdm-project-group24-redis-bus-1",
        "XADD", stream, "*",
        "not_a_command", "garbage",
        "correlation_id", "chaos-malformed-001",
    )
    time.sleep(3)

    PRICE  = 10
    STOCK  = 5
    CREDIT = 200
    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/1")

    r = api("POST", f"/orders/checkout/{order}")
    check("Checkout Succeeds After Malformed Message Injected Into Stream",
          r.status_code == 200, f"got {r.status_code}")
    check("Stock Decremented Correctly — Consumer Recovered From Bad Message",
          json_field(api("GET", f"/stock/find/{item}"), "stock") == STOCK - 1)


def test_payment_redis_aof_durability():
    # write credit, restart payment redis, verify credit survived via AOF
    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/999")
    check("Credit Is 999 Before Redis Restart",
          json_field(api("GET", f"/payment/find_user/{user}"), "credit") == 999)

    docker_cmd("docker restart wdm-project-group24-redis-payment-1")
    wait_for_service(f"/payment/find_user/{user}")
    time.sleep(2)

    check("Credit Still 999 After Payment Redis Restart — AOF Persisted Write",
          json_field(api("GET", f"/payment/find_user/{user}"), "credit") == 999)

    PRICE = 50
    STOCK = 5
    item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/1")
    r = api("POST", f"/orders/checkout/{order}")
    check("Checkout Succeeds After Payment Redis Restart",
          r.status_code == 200, f"got {r.status_code}")


def test_stock_redis_aof_durability():
    # write stock, restart stock redis, verify stock survived via AOF
    item = json_field(api("POST", "/stock/item/create/15"), "item_id")
    api("POST", f"/stock/add/{item}/42")
    check("Stock Is 42 Before Redis Restart",
          json_field(api("GET", f"/stock/find/{item}"), "stock") == 42)

    docker_cmd("docker restart wdm-project-group24-redis-stock-1")
    wait_for_service(f"/stock/find/{item}")
    time.sleep(2)

    check("Stock Still 42 After Stock Redis Restart — AOF Persisted Write",
          json_field(api("GET", f"/stock/find/{item}"), "stock") == 42)

    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/1000")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/2")
    r = api("POST", f"/orders/checkout/{order}")
    check("Checkout Succeeds After Stock Redis Restart",
          r.status_code == 200, f"got {r.status_code}")
    check("Stock Decremented Correctly Post-Restart",
          json_field(api("GET", f"/stock/find/{item}"), "stock") == 40)


def test_concurrent_oversell_prevention():
    # 20 users all checkout simultaneously for an item with only 3 units
    N_USERS = 20
    UNITS   = 3
    PRICE   = 25
    CREDIT  = 500

    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{UNITS}")

    pairs = []
    for _ in range(N_USERS):
        user  = json_field(api("POST", "/payment/create_user"), "user_id")
        api("POST", f"/payment/add_funds/{user}/{CREDIT}")
        order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
        api("POST", f"/orders/addItem/{order}/{item}/1")
        pairs.append((user, order))

    with concurrent.futures.ThreadPoolExecutor(max_workers=N_USERS) as pool:
        results = list(pool.map(
            lambda p: api("POST", f"/orders/checkout/{p[1]}").status_code, pairs))

    winners = results.count(200)
    stock   = json_field(api("GET", f"/stock/find/{item}"), "stock")
    charged = sum(1 for u, _ in pairs
                  if json_field(api("GET", f"/payment/find_user/{u}"), "credit") != CREDIT)

    check(f"Exactly {UNITS} Of {N_USERS} Concurrent Checkouts Succeeded — No Oversell",
          winners == UNITS, f"got {winners} winners")
    check("Stock Is 0 — All Available Units Sold Exactly Once",
          stock == 0, f"got {stock}")
    check(f"Exactly {UNITS} Users Were Charged — No Double-Charge",
          charged == UNITS, f"got {charged} charged")


def test_stock_service_crash_mid_batch():
    # queue 5 checkouts; kill stock service mid-flight; verify all settle consistently
    PRICE  = 10
    STOCK  = 20
    CREDIT = 500
    N      = 5
    SVC    = "wdm-project-group24-stock-service-1"

    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    pairs = []
    for _ in range(N):
        user  = json_field(api("POST", "/payment/create_user"), "user_id")
        api("POST", f"/payment/add_funds/{user}/{CREDIT}")
        order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
        api("POST", f"/orders/addItem/{order}/{item}/1")
        pairs.append((user, order))

    def checkout(pair):
        return api("POST", f"/orders/checkout/{pair[1]}").status_code

    def kill_and_restart():
        time.sleep(0.1)
        docker_cmd(f"docker stop {SVC}")
        time.sleep(3)
        docker_cmd(f"docker start {SVC}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=N + 1) as pool:
        killer = pool.submit(kill_and_restart)
        futs   = [pool.submit(checkout, p) for p in pairs]
        killer.result()
        results = [f.result() for f in futs]

    wait_for_service(f"/stock/find/{item}", timeout=60)
    time.sleep(2)

    winners = results.count(200)
    stock   = json_field(api("GET", f"/stock/find/{item}"), "stock")
    charged = sum(1 for u, _ in pairs
                  if json_field(api("GET", f"/payment/find_user/{u}"), "credit") != CREDIT)

    check("Winners Equals Charged Users — No Checkout Without Payment",
          winners == charged, f"winners={winners} charged={charged}")
    check("Stock + Winners Equals Initial Stock — No Units Lost Or Oversold",
          stock + winners == STOCK, f"stock={stock} winners={winners} initial={STOCK}")


def test_redis_bus_restart_recovery():
    # kill the message bus, let it restart, verify checkouts still work
    docker_cmd("docker restart wdm-project-group24-redis-bus-1")
    wait_for_service("/orders/create/healthcheck", timeout=90)
    time.sleep(5)

    PRICE  = 20
    STOCK  = 10
    CREDIT = 500
    user  = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item  = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/2")

    r = api("POST", f"/orders/checkout/{order}")
    check("Checkout Succeeds After Redis Bus Restart",
          r.status_code == 200, f"got {r.status_code}")
    check("Stock Decremented After Bus Restart",
          json_field(api("GET", f"/stock/find/{item}"), "stock") == STOCK - 2)
    check("Credit Decremented After Bus Restart",
          json_field(api("GET", f"/payment/find_user/{user}"), "credit") == CREDIT - PRICE * 2)


TESTS = [
    ("order-service",   "Multi-Item Checkout With Per-Item Stock Verification",              test_multi_item_checkout),
    ("order-service",   "Double Checkout On A Paid Order",                                   test_double_checkout),
    ("order-service",   "Add Items To A Paid Order And Re-Checkout",                         test_post_checkout_tampering),
    ("order-service",   "Checkout An Empty Order",                                           test_checkout_empty_order),
    ("order-service",   "10 Concurrent Checkouts For 1 Unit Of Stock",                      test_concurrent_fight_for_last_item),
    ("order-service",   "Sequential Checkouts Until Stock Is Exhausted",                    test_sequential_drain),
    ("order-service",   "5 Independent Checkouts In Parallel",                              test_concurrent_independent_checkouts),
    ("stock-service",   "External Stock Change Between Order And Checkout",                  test_stock_modified_before_checkout),
    ("payment-service", "Fund User After Order Creation Then Checkout",                      test_fund_user_after_order),
    ("payment-service", "Boundary: Balance Exactly Equals Order Total",                      test_exact_balance_boundary),
    ("stock-service",   "Boundary: Stock Exactly Equals Order Quantity",                     test_exact_stock_boundary),
    ("payment-service", "Boundary: One Credit Short Of Order Total",                         test_one_credit_short),
    ("stock-service",   "Boundary: One Stock Unit Short Of Order Quantity",                  test_one_stock_short),
    ("order-service",   "GET On Non-Existent Stock, User, And Order IDs",                   test_find_nonexistent),
    ("order-service",   "addItem Idempotency: Same Key Prevents Duplicate Quantity",         test_add_item_idempotency),
    ("stock-service",   "Multi-Item Checkout Atomic Rollback When One Item Is Out Of Stock", test_multi_item_checkout_partial_stock_failure),
    ("stock-service",   "Batch Init: Integer-Keyed Items And Users Are Readable",            test_batch_init_stock_and_payment),
    ("stock-service",   "Add-Stock Idempotency: Same Key Does Not Double-Add",               test_add_stock_idempotency),
    ("payment-service", "Pay Idempotency: Same Key Does Not Double-Deduct",                  test_pay_idempotency),
    ("order-service",   "Order Item Merge: Same Item Added Twice Merges Quantities",         test_order_item_quantity_merge),
    ("stock-service",   "Zero Or Negative Amount Rejected By Stock And Payment",             test_zero_amount_rejected),
    ("stock-service",   "Item Not Found: Subtract Returns 4xx Not 5xx",                     test_item_not_found_subtract),
    ("payment-service", "User Not Found: Pay Returns 4xx Not 5xx",                          test_user_not_found_pay),
    ("redis-bus",       "Malformed Stream Message: Consumer Survives And Continues",         test_malformed_stream_message),
    ("redis-payment",   "Payment Redis Restart: AOF Durability",                             test_payment_redis_aof_durability),
    ("redis-stock",     "Stock Redis Restart: AOF Durability",                               test_stock_redis_aof_durability),
    ("order-service",   "Concurrent Oversell Prevention: 20 Users For 3 Units",             test_concurrent_oversell_prevention),
    ("stock-service",   "Stock Service Crash Mid-Batch: Consistency Survives",               test_stock_service_crash_mid_batch),
    ("redis-bus",       "Redis Bus Restart: Stream Consumers Recover",                       test_redis_bus_restart_recovery),
]
