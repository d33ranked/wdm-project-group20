"""
2PC (Two-Phase Commit) Tests
=============================
Tests specific to the TPC transaction mode.
Covers prepare/commit/abort correctness, resource locking during prepare,
competing transactions, idempotent commit/abort, mixed-vote checkout scenarios,
and coordinator/participant failure recovery.
"""

import subprocess
import threading
import time

import requests

from run import api, check, json_field, PROJECT_ROOT, BASE_URL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _docker(cmd: str):
    """Run a docker command silently."""
    subprocess.run(
        cmd, shell=True, cwd=PROJECT_ROOT,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def _wait_for_service(probe_path: str, timeout: int = 60):
    """Poll until a service endpoint responds with a non-5xx status."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(f"{BASE_URL}{probe_path}", timeout=3)
            if r.status_code < 500:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


# ---------------------------------------------------------------------------
# 1. PREPARE → COMMIT Finalises Permanently (Stock And Payment)
# ---------------------------------------------------------------------------
def test_prepare_commit():
    """Prepare reserves resources, commit makes the deduction permanent."""
    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/10")

    r = api("POST", f"/stock/prepare/txn-pc-s/{item}/3")
    check("Stock PREPARE Reserves 3 Of 10 Units — Service Votes YES",
          r.status_code == 200)

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock Shows 7 After PREPARE — 3 Units Held In Reservation",
          stock == 7, f"got {stock}")

    r = api("POST", "/stock/commit/txn-pc-s")
    check("Stock COMMIT Finalises The Transaction",
          r.status_code == 200)

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock Remains 7 After COMMIT — Deduction Is Now Permanent",
          stock == 7, f"got {stock}")

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/100")

    r = api("POST", f"/payment/prepare/txn-pc-p/{user}/30")
    check("Payment PREPARE Reserves 30 Of 100 Credits — Service Votes YES",
          r.status_code == 200)

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check("Credit Shows 70 After PREPARE — 30 Credits Held In Reservation",
          credit == 70, f"got {credit}")

    r = api("POST", "/payment/commit/txn-pc-p")
    check("Payment COMMIT Finalises The Transaction",
          r.status_code == 200)

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check("Credit Remains 70 After COMMIT — Charge Is Now Permanent",
          credit == 70, f"got {credit}")


# ---------------------------------------------------------------------------
# 2. PREPARE → ABORT Restores Fully (Stock And Payment)
# ---------------------------------------------------------------------------
def test_prepare_abort():
    """Prepare reserves resources, abort returns everything."""
    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/10")

    api("POST", f"/stock/prepare/txn-pa-s/{item}/4")
    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock Shows 6 After PREPARE Reserved 4 Units",
          stock == 6, f"got {stock}")

    r = api("POST", "/stock/abort/txn-pa-s")
    check("Stock ABORT Releases The Reservation", r.status_code == 200)

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock Restored To 10 After ABORT — All 4 Reserved Units Returned",
          stock == 10, f"got {stock}")

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/100")

    api("POST", f"/payment/prepare/txn-pa-p/{user}/40")
    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check("Credit Shows 60 After PREPARE Reserved 40 Credits",
          credit == 60, f"got {credit}")

    r = api("POST", "/payment/abort/txn-pa-p")
    check("Payment ABORT Releases The Reservation", r.status_code == 200)

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check("Credit Restored To 100 After ABORT — All 40 Reserved Credits Returned",
          credit == 100, f"got {credit}")


# ---------------------------------------------------------------------------
# 3. Vote NO When Insufficient (Stock And Payment)
# ---------------------------------------------------------------------------
def test_vote_no():
    """Prepare for more than available — service votes NO, nothing changes."""
    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/2")

    r = api("POST", f"/stock/prepare/txn-vno-s/{item}/999")
    check("Stock PREPARE For 999 Units Rejected — Only 2 Available, Service Votes NO",
          400 <= r.status_code < 500)

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock Unchanged At 2 — No Reservation Made After Vote NO",
          stock == 2, f"got {stock}")

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/10")

    r = api("POST", f"/payment/prepare/txn-vno-p/{user}/999")
    check("Payment PREPARE For 999 Credits Rejected — Only 10 Available, Service Votes NO",
          400 <= r.status_code < 500)

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check("Credit Unchanged At 10 — No Reservation Made After Vote NO",
          credit == 10, f"got {credit}")


# ---------------------------------------------------------------------------
# 4. Prepared Resources Are Locked From Regular Operations
# ---------------------------------------------------------------------------
def test_prepare_locks_resources():
    """PREPARE holds 8 of 10 stock; a regular subtract of 5 must fail because only 2 are free."""
    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/10")

    api("POST", f"/stock/prepare/txn-lock/{item}/8")
    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock Shows 2 After PREPARE Locked 8 Of 10 Units",
          stock == 2, f"got {stock}")

    r = api("POST", f"/stock/subtract/{item}/5")
    check("Regular Subtract Of 5 Fails — Only 2 Units Free, 8 Are Locked By PREPARE",
          400 <= r.status_code < 500, f"got {r.status_code}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock Still Shows 2 — Failed Subtract Did Not Corrupt Reserved State",
          stock == 2, f"got {stock}")

    api("POST", "/stock/abort/txn-lock")


# ---------------------------------------------------------------------------
# 5. ABORT Frees Locked Resources For New Operations
# ---------------------------------------------------------------------------
def test_abort_frees_resources():
    """PREPARE 8 of 10 → ABORT → subtract all 10 succeeds (everything freed)."""
    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/10")

    api("POST", f"/stock/prepare/txn-free/{item}/8")
    api("POST", "/stock/abort/txn-free")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock Restored To 10 After ABORT Released 8 Reserved Units",
          stock == 10, f"got {stock}")

    r = api("POST", f"/stock/subtract/{item}/10")
    check("Subtract All 10 Units Succeeds After ABORT — Full Capacity Available Again",
          r.status_code == 200, f"got {r.status_code}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock Is 0 — Confirms ABORT Genuinely Released All Reserved Units",
          stock == 0, f"got {stock}")


# ---------------------------------------------------------------------------
# 6. Competing PREPAREs On The Same Resource
# ---------------------------------------------------------------------------
def test_competing_prepares():
    """Two transactions each try to reserve 7 from stock=10 — second must vote NO."""
    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/10")

    r1 = api("POST", f"/stock/prepare/txn-comp-a/{item}/7")
    check("First PREPARE Reserves 7 Of 10 — Votes YES",
          r1.status_code == 200)

    r2 = api("POST", f"/stock/prepare/txn-comp-b/{item}/7")
    check("Second PREPARE For 7 Votes NO — Only 3 Unreserved, Cannot Fulfil",
          400 <= r2.status_code < 500)

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock Shows 3 — Only First Reservation Held, Second Was Rejected",
          stock == 3, f"got {stock}")

    api("POST", "/stock/abort/txn-comp-a")


# ---------------------------------------------------------------------------
# 7. Idempotent COMMIT — Re-COMMIT Is A Safe No-Op
# ---------------------------------------------------------------------------
def test_idempotent_commit():
    """COMMIT same txn twice — second is a no-op, no double deduction."""
    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/10")
    api("POST", f"/stock/prepare/txn-idem-c/{item}/3")
    api("POST", "/stock/commit/txn-idem-c")

    r = api("POST", "/stock/commit/txn-idem-c")
    check("Stock: Second COMMIT On Same Transaction Returns 200 — Idempotent",
          r.status_code == 200)

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock Is 7 — Committed Once Despite Two COMMIT Calls, No Double Deduction",
          stock == 7, f"got {stock}")

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/100")
    api("POST", f"/payment/prepare/txn-idem-cp/{user}/20")
    api("POST", "/payment/commit/txn-idem-cp")

    r = api("POST", "/payment/commit/txn-idem-cp")
    check("Payment: Second COMMIT Returns 200 — No Double Charge",
          r.status_code == 200)

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check("Credit Is 80 — Charged Once Despite Two COMMIT Calls",
          credit == 80, f"got {credit}")


# ---------------------------------------------------------------------------
# 8. ABORT Safety — Non-Existent And Already-Resolved Transactions
# ---------------------------------------------------------------------------
def test_abort_safety():
    """ABORT on unknown txn and already-committed txn — both return 200, no state change."""
    r = api("POST", "/stock/abort/txn-never-existed")
    check("Stock: ABORT Of Non-Existent Transaction Returns 200 — Safe No-Op",
          r.status_code == 200)

    r = api("POST", "/payment/abort/txn-never-existed")
    check("Payment: ABORT Of Non-Existent Transaction Returns 200 — Safe No-Op",
          r.status_code == 200)

    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/5")
    api("POST", f"/stock/prepare/txn-already/{item}/2")
    api("POST", "/stock/commit/txn-already")

    r = api("POST", "/stock/abort/txn-already")
    check("Stock: ABORT Of Already-Committed Transaction Returns 200 — Does Not Reverse The Commit",
          r.status_code == 200)

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock Remains 3 — ABORT After COMMIT Did Not Undo The Permanent Deduction",
          stock == 3, f"got {stock}")


# ---------------------------------------------------------------------------
# 9. 2PC Checkout — Stock Votes NO, Entire Checkout Aborts
# ---------------------------------------------------------------------------
def test_checkout_stock_votes_no():
    """Stock cannot fulfil → coordinator aborts all → payment untouched."""
    CREDIT = 500
    STOCK = 2

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/50")

    r = api("POST", f"/orders/checkout/{order}")
    check("Checkout Rejected — Stock Votes NO (Wants 50, Has 2), Coordinator Aborts",
          400 <= r.status_code < 500, f"got {r.status_code}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check(f"Stock Unchanged At {STOCK} — No Reservation Was Made",
          stock == STOCK, f"got {stock}")

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"Credit Unchanged At {CREDIT} — Payment Was Never Prepared Or Was Aborted",
          credit == CREDIT, f"got {credit}")


# ---------------------------------------------------------------------------
# 10. 2PC Checkout — Payment Votes NO, Stock Reservation Rolled Back
# ---------------------------------------------------------------------------
def test_checkout_payment_votes_no():
    """Payment cannot fulfil → coordinator aborts → stock reservation reversed."""
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
    check("Checkout Rejected — Stock Votes YES But Payment Votes NO (5 < 100), Coordinator Aborts",
          400 <= r.status_code < 500, f"got {r.status_code}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check(f"Stock Restored To {STOCK} — ABORT Reversed The Stock Reservation",
          stock == STOCK, f"got {stock}")

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"Credit Unchanged At {CREDIT} — Payment Never Reserved Any Funds",
          credit == CREDIT, f"got {credit}")


# ---------------------------------------------------------------------------
# 11. Participant Crash — Stock Service Dies During Checkout, Recovers
# ---------------------------------------------------------------------------
def test_participant_crash_recovery():
    """Stop stock service, restart after 3s, checkout retries and succeeds."""
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

    _docker(f"docker stop {CONTAINER}")
    subprocess.Popen(
        f"sleep 3 && docker start {CONTAINER}",
        shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    r = api("POST", f"/orders/checkout/{order}")
    expected_stock = STOCK - ITEM_QTY
    expected_credit = CREDIT - (ITEM_PRICE * ITEM_QTY)

    check("Checkout Completed After Stock Service Recovered — Order Service Retried Successfully",
          r.status_code == 200, f"got {r.status_code}")

    _wait_for_service(f"/stock/find/{item}")
    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")

    check(f"Stock Decreased To {expected_stock} After Recovery — {ITEM_QTY} Units Sold",
          stock == expected_stock, f"got {stock}")
    check(f"Credit Decreased To {expected_credit} After Recovery — "
          f"Charged {ITEM_PRICE}×{ITEM_QTY}",
          credit == expected_credit, f"got {credit}")


# ---------------------------------------------------------------------------
# 12. Coordinator Crash — Order Service Killed After PREPARE, Recovery On Startup
# ---------------------------------------------------------------------------
def test_coordinator_crash_recovery():
    """Kill the coordinator mid-checkout, restart, verify consistent final state."""
    ITEM_PRICE = 20
    ITEM_QTY = 2
    STOCK = 10
    CREDIT = 200
    ORDER_CONTAINER = "wdm-project-group24-order-service-1"

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{ITEM_QTY}")

    checkout_done = threading.Event()

    def do_checkout():
        try:
            api("POST", f"/orders/checkout/{order}")
        except Exception:
            pass
        checkout_done.set()

    t = threading.Thread(target=do_checkout, daemon=True)
    t.start()

    time.sleep(0.3)
    _docker(f"docker kill {ORDER_CONTAINER}")

    checkout_done.wait(timeout=35)

    time.sleep(2)
    _docker(f"docker start {ORDER_CONTAINER}")

    _wait_for_service(f"/orders/find/{order}", timeout=90)
    time.sleep(5)

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    paid = json_field(api("GET", f"/orders/find/{order}"), "paid")

    expected_stock = STOCK - ITEM_QTY
    expected_credit = CREDIT - (ITEM_PRICE * ITEM_QTY)

    committed = (stock == expected_stock and credit == expected_credit and paid is True)
    rolled_back = (stock == STOCK and credit == CREDIT and paid is not True)

    check(
        "After Coordinator Crash And Recovery, State Is Consistent — "
        "Either Fully Committed Or Fully Rolled Back",
        committed or rolled_back,
        f"stock={stock}, credit={credit}, paid={paid}"
    )

    if committed:
        check("Recovery Resolved In-Doubt Transaction By Committing — "
              "Stock, Credit, And Order All Reflect The Checkout", True)
    elif rolled_back:
        check("Recovery Resolved In-Doubt Transaction By Rolling Back — "
              "All Participants Restored To Original State", True)


# ---------------------------------------------------------------------------
# Ordered test list — imported by run.py
# ---------------------------------------------------------------------------
TESTS = [
    ("PREPARE → COMMIT On Stock And Payment", test_prepare_commit),
    ("PREPARE → ABORT On Stock And Payment", test_prepare_abort),
    ("Vote NO When Resources Are Insufficient", test_vote_no),
    ("PREPARE Locks Resources From Regular Operations", test_prepare_locks_resources),
    ("ABORT Releases Locked Resources For Reuse", test_abort_frees_resources),
    ("Two Competing PREPAREs On The Same Stock", test_competing_prepares),
    ("Idempotent COMMIT — No Double Deduction", test_idempotent_commit),
    ("ABORT On Non-Existent And Already-Committed Transactions", test_abort_safety),
    ("2PC Checkout: Stock Votes NO, Coordinator Aborts All", test_checkout_stock_votes_no),
    ("2PC Checkout: Payment Votes NO, Stock Reservation Rolled Back", test_checkout_payment_votes_no),
    ("Participant Crash: Stock Dies Mid-Checkout And Recovers", test_participant_crash_recovery),
    ("Coordinator Crash: Order Service Killed After PREPARE, Recovers Consistently", test_coordinator_crash_recovery),
]
