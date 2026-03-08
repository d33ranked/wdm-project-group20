"""
2PC (Two-Phase Commit) Tests
=============================
Tests specific to the TPC transaction mode.
Covers prepare/commit/abort on individual services, end-to-end checkout,
vote-NO scenarios, idempotent commit/abort, and fault-tolerance recovery.
"""

import subprocess
import time

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


def _wait_for_service(probe_path: str, timeout: int = 30):
    """Poll until a service endpoint responds 200."""
    import requests
    start = time.time()
    while time.time() - start < timeout:
        try:
            if requests.get(f"{BASE_URL}{probe_path}", timeout=3).status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


# ---------------------------------------------------------------------------
# 1. Stock prepare → commit
# ---------------------------------------------------------------------------
def test_stock_prepare_commit():
    """Prepare reserves stock, commit finalises it."""
    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/10")

    r = api("POST", f"/stock/prepare/txn-sc1/{item}/3")
    check("PREPARE reserves 3 units — stock service votes YES (200)", r.status_code == 200)

    stock_mid = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock reduced from 10 to 7 immediately after PREPARE (reservation held)", stock_mid == 7, f"got {stock_mid}")

    r = api("POST", "/stock/commit/txn-sc1")
    check("COMMIT finalises the transaction — reservation becomes permanent (200)", r.status_code == 200)

    stock_final = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock remains 7 after COMMIT — deduction is now permanent", stock_final == 7, f"got {stock_final}")


# ---------------------------------------------------------------------------
# 2. Stock prepare → abort (rollback)
# ---------------------------------------------------------------------------
def test_stock_prepare_abort():
    """Abort restores the stock reserved by prepare."""
    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/10")

    api("POST", f"/stock/prepare/txn-sa1/{item}/4")
    stock_mid = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock reduced from 10 to 6 after PREPARE reserved 4 units", stock_mid == 6, f"got {stock_mid}")

    r = api("POST", "/stock/abort/txn-sa1")
    check("ABORT releases the reservation successfully (200)", r.status_code == 200)

    stock_final = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock restored to 10 after ABORT — reserved units returned", stock_final == 10, f"got {stock_final}")


# ---------------------------------------------------------------------------
# 3. Stock vote NO (insufficient stock)
# ---------------------------------------------------------------------------
def test_stock_vote_no():
    """Prepare with more units than available returns 4xx."""
    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/2")

    r = api("POST", f"/stock/prepare/txn-sno/{item}/999")
    check("PREPARE for 999 units rejected with 4xx — stock votes NO (only 2 available)", 400 <= r.status_code < 500)

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    check("Stock unchanged at 2 — no reservation was made after vote NO", stock == 2, f"got {stock}")


# ---------------------------------------------------------------------------
# 4. Payment prepare → commit
# ---------------------------------------------------------------------------
def test_payment_prepare_commit():
    """Prepare reserves credit, commit finalises it."""
    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/100")

    r = api("POST", f"/payment/prepare/txn-pc1/{user}/30")
    check("PREPARE reserves 30 credits — payment service votes YES (200)", r.status_code == 200)

    credit_mid = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check("Credit reduced from 100 to 70 immediately after PREPARE (funds held)", credit_mid == 70, f"got {credit_mid}")

    r = api("POST", "/payment/commit/txn-pc1")
    check("COMMIT finalises the payment — reservation becomes permanent (200)", r.status_code == 200)

    credit_final = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check("Credit remains 70 after COMMIT — deduction is now permanent", credit_final == 70, f"got {credit_final}")


# ---------------------------------------------------------------------------
# 5. Payment prepare → abort (rollback)
# ---------------------------------------------------------------------------
def test_payment_prepare_abort():
    """Abort restores the credit reserved by prepare."""
    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/100")

    api("POST", f"/payment/prepare/txn-pa1/{user}/40")
    credit_mid = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check("Credit reduced from 100 to 60 after PREPARE reserved 40 credits", credit_mid == 60, f"got {credit_mid}")

    r = api("POST", "/payment/abort/txn-pa1")
    check("ABORT releases the credit reservation successfully (200)", r.status_code == 200)

    credit_final = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check("Credit restored to 100 after ABORT — reserved funds returned to user", credit_final == 100, f"got {credit_final}")


# ---------------------------------------------------------------------------
# 6. Payment vote NO (insufficient credit)
# ---------------------------------------------------------------------------
def test_payment_vote_no():
    """Prepare with more credit than available returns 4xx."""
    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/10")

    r = api("POST", f"/payment/prepare/txn-pno/{user}/999")
    check("PREPARE for 999 credits rejected with 4xx — payment votes NO (only 10 available)", 400 <= r.status_code < 500)

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check("Credit unchanged at 10 — no reservation was made after vote NO", credit == 10, f"got {credit}")


# ---------------------------------------------------------------------------
# 7. Idempotent commit and abort
# ---------------------------------------------------------------------------
def test_idempotent_commit_abort():
    """Committing/aborting an already-resolved or unknown txn returns 200."""
    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/5")
    api("POST", f"/stock/prepare/txn-idem/{item}/1")
    api("POST", "/stock/commit/txn-idem")

    r = api("POST", "/stock/commit/txn-idem")
    check("Stock: re-committing an already-committed txn returns 200 (idempotent)", r.status_code == 200)

    r = api("POST", "/stock/abort/txn-nonexistent")
    check("Stock: aborting a non-existent txn returns 200 (safe no-op)", r.status_code == 200)

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/50")
    api("POST", f"/payment/prepare/txn-pidem/{user}/10")
    api("POST", "/payment/commit/txn-pidem")

    r = api("POST", "/payment/commit/txn-pidem")
    check("Payment: re-committing an already-committed txn returns 200 (idempotent)", r.status_code == 200)

    r = api("POST", "/payment/abort/txn-pnonexistent")
    check("Payment: aborting a non-existent txn returns 200 (safe no-op)", r.status_code == 200)


# ---------------------------------------------------------------------------
# 8. 2PC checkout — stock votes NO, payment untouched
# ---------------------------------------------------------------------------
def test_checkout_stock_votes_no():
    """When stock cannot fulfil, entire checkout aborts. Payment stays intact."""
    STARTING_CREDIT = 500
    STARTING_STOCK = 2

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{STARTING_CREDIT}")
    item = json_field(api("POST", "/stock/item/create/10"), "item_id")
    api("POST", f"/stock/add/{item}/{STARTING_STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/50")

    r = api("POST", f"/orders/checkout/{order}")
    check("2PC checkout rejected — stock PREPARE fails (wants 50, has 2), coordinator ABORTs all", 400 <= r.status_code < 500)

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"Stock unchanged at {STARTING_STOCK} — no reservation was made", stock == STARTING_STOCK, f"got {stock}")
    check(f"Credit unchanged at {STARTING_CREDIT} — payment PREPARE was never attempted", credit == STARTING_CREDIT, f"got {credit}")


# ---------------------------------------------------------------------------
# 9. 2PC checkout — payment votes NO, stock restored
# ---------------------------------------------------------------------------
def test_checkout_payment_votes_no():
    """When payment cannot fulfil, stock reservation is aborted."""
    ITEM_PRICE = 100
    STARTING_STOCK = 10
    STARTING_CREDIT = 5

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{STARTING_CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STARTING_STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/1")

    r = api("POST", f"/orders/checkout/{order}")
    check("2PC checkout rejected — stock votes YES but payment votes NO (5 < 100), coordinator ABORTs", 400 <= r.status_code < 500)

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"Stock restored to {STARTING_STOCK} — ABORT reversed the stock reservation", stock == STARTING_STOCK, f"got {stock}")
    check(f"Credit unchanged at {STARTING_CREDIT} — payment never reserved any funds", credit == STARTING_CREDIT, f"got {credit}")


# ---------------------------------------------------------------------------
# 10. Fault tolerance — resilience during brief stock outage
# ---------------------------------------------------------------------------
def test_resilience_stock_outage():
    """Checkout retries when stock-service is briefly down, succeeds after restart."""
    ITEM_PRICE = 10
    ITEM_QTY = 2
    STARTING_STOCK = 5
    STARTING_CREDIT = 100
    CONTAINER = "wdm-project-group24-stock-service-1"

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{STARTING_CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{ITEM_PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STARTING_STOCK}")
    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{ITEM_QTY}")

    _docker(f"docker stop {CONTAINER}")
    subprocess.Popen(
        f"sleep 3 && docker start {CONTAINER}",
        shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    r = api("POST", f"/orders/checkout/{order}")

    expected_stock = STARTING_STOCK - ITEM_QTY
    expected_credit = STARTING_CREDIT - (ITEM_PRICE * ITEM_QTY)

    check("Checkout succeeded after retrying — order service recovered once stock came back", r.status_code == 200, f"got {r.status_code}")

    _wait_for_service(f"/stock/find/{item}")
    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check(f"Stock decreased from {STARTING_STOCK} to {expected_stock} ({ITEM_QTY} units sold after recovery)", stock == expected_stock, f"got {stock}")
    check(f"Credit decreased from {STARTING_CREDIT} to {expected_credit} (charged {ITEM_PRICE}×{ITEM_QTY} after recovery)", credit == expected_credit, f"got {credit}")


# ---------------------------------------------------------------------------
# Ordered test list — imported by run.py
# ---------------------------------------------------------------------------
TESTS = [
    ("Stock PREPARE → COMMIT: reserve 3 units, commit makes deduction permanent", test_stock_prepare_commit),
    ("Stock PREPARE → ABORT: reserve 4 units, abort restores all reserved stock", test_stock_prepare_abort),
    ("Stock VOTE NO: PREPARE rejected when requesting more than available stock", test_stock_vote_no),
    ("Payment PREPARE → COMMIT: reserve 30 credits, commit makes charge permanent", test_payment_prepare_commit),
    ("Payment PREPARE → ABORT: reserve 40 credits, abort returns funds to user", test_payment_prepare_abort),
    ("Payment VOTE NO: PREPARE rejected when requesting more than available credit", test_payment_vote_no),
    ("Idempotent COMMIT & ABORT: duplicate commits and aborts on unknown txns return 200", test_idempotent_commit_abort),
    ("2PC Checkout — Stock Votes NO: order wants 50 units (only 2), entire checkout aborts", test_checkout_stock_votes_no),
    ("2PC Checkout — Payment Votes NO: stock votes YES, payment votes NO, stock reservation rolled back", test_checkout_payment_votes_no),
    ("Resilience: stock-service stopped for 3s during checkout, order-service retries and succeeds", test_resilience_stock_outage),
]
