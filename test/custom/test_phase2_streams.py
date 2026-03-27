# phase 2 stream tests — validates double-checkout prevention, multi-item compensation,
# concurrent correlation isolation, and pending-message re-delivery after participant crash

import concurrent.futures
import time
import subprocess

from run import api, check, json_field, docker_cmd, wait_for_service


# ---------------------------------------------------------------------------
# 1. Double-Checkout Prevention — Second Checkout Rejected, State Unchanged
# ---------------------------------------------------------------------------
def test_saga_double_checkout_prevented():
    """Checking out a paid order a second time returns 4xx and changes nothing.

    After the first checkout succeeds the order is marked paid=true and a saga
    state machine reaches COMPLETED.  A second checkout request for the same
    order should be rejected immediately; stock and credit must stay at their
    post-checkout values.
    """
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

    # Brief pause so the saga reaches COMPLETED before the second attempt
    time.sleep(1)

    r2 = api("POST", f"/orders/checkout/{order}")
    check("Second Checkout On A Paid Order Returns 4xx",
          400 <= r2.status_code < 500, f"got {r2.status_code}")

    # State must be exactly equal to what the first checkout left behind
    stock_after  = json_field(api("GET", f"/stock/find/{item}"),        "stock")
    credit_after = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    paid_after   = json_field(api("GET", f"/orders/find/{order}"),      "paid")

    expected_stock  = STOCK  - QTY
    expected_credit = CREDIT - (PRICE * QTY)

    check(f"Stock Remains {expected_stock} After Duplicate Checkout Attempt",
          stock_after == expected_stock, f"got {stock_after}")
    check(f"Credit Remains {expected_credit} After Duplicate Checkout Attempt",
          credit_after == expected_credit, f"got {credit_after}")
    check("Order Remains Paid After Duplicate Checkout Attempt",
          paid_after is True, f"got {paid_after}")


# ---------------------------------------------------------------------------
# 2. Multi-Item Compensation — All Items Restored When Payment Fails
# ---------------------------------------------------------------------------
def test_saga_compensation_multi_item():
    """Stock is reserved for 3 items; payment fails; ALL 3 items are restored.

    The compensating transaction (add_batch) must be issued for the entire
    items_quantities set stored in saga:{saga_id}, not just the first item.
    This validates the batch-rollback path in the saga orchestrator.
    """
    PRICES  = [15, 25, 10]
    STOCKS  = [10, 10, 10]
    QTYS    = [1,  2,  3]
    CREDIT  = 5             # intentionally too low to pay

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

    # Give the compensating transaction time to propagate
    time.sleep(2)

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


# ---------------------------------------------------------------------------
# 3. Concurrent Correlation Isolation — 20 Simultaneous Checkouts
# ---------------------------------------------------------------------------
def test_concurrent_correlation_isolation():
    """20 checkout requests fire simultaneously; each gets its own response.

    Each request is assigned a unique correlation_id by the gateway.
    The StreamClient.send_request() registers an Event keyed on that id
    before publishing to the stream.  The background response consumer wakes
    ONLY the matching Event.

    If the correlation logic is broken, responses cross-wire: some callers
    never wake (timeout) or wake with the wrong result.  We verify that all
    20 checkouts get a clean 200 or 400 — no ambiguous or leaked responses.
    """
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
    check(f"Every Checkout Got A Definitive 200 Or 400 — No Timeouts Or Errors",
          all_decided, f"codes: {codes}")

    successes = codes.count(200)
    check(f"All {N} Independent Checkouts Succeeded (Each Had Its Own Item And User)",
          successes == N, f"only {successes}/{N} returned 200")


# ---------------------------------------------------------------------------
# 4. Pending-Message Re-Delivery After Participant Crash
# ---------------------------------------------------------------------------
def test_pending_message_redelivery():
    """Crash stock mid-request; on restart its pending stream entry is re-delivered.

    Redis Streams' consumer group maintains a Pending Entry List (PEL) for
    every delivered-but-unACKed message.  When start_gateway_consumer starts
    up it calls read_pending_then_new() which reads with id="0" first —
    draining the PEL before fetching new messages.

    Flow:
      1. Stop stock service.
      2. Send a /stock/add request (message lands in gateway.stock, stays
         pending forever since nobody ACKs it).
      3. Start stock service → pending message re-delivered → stock is added.

    We verify the final stock value reflects the operation that was in-flight
    when stock was down.
    """
    CONTAINER = "wdm-project-group24-stock-service-1"
    INITIAL   = 10
    ADD_AMT   = 7

    item = json_field(api("POST", "/stock/item/create/1"), "item_id")
    api("POST", f"/stock/add/{item}/{INITIAL}")

    # Stop stock — the next request will sit in the PEL
    docker_cmd(f"docker stop {CONTAINER}")

    # Fire the add-stock request asynchronously (gateway will block until
    # REQUEST_TIMEOUT_S, then time out — that is expected here).
    # What matters is that the message landed in the stream before we restart.
    idem_key = "pending-redelivery-test"
    subprocess.Popen(
        ["python3", "-c",
         f"import requests; requests.post('http://localhost:8000/stock/add/{item}/{ADD_AMT}', "
         f"headers={{'Idempotency-Key': '{idem_key}'}}, timeout=35)"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Give the gateway a moment to publish the message to the stream
    time.sleep(2)

    # Restart stock — pending message in gateway.stock will be re-delivered
    docker_cmd(f"docker start {CONTAINER}")
    wait_for_service(f"/stock/find/{item}", timeout=60)
    time.sleep(3)   # allow consumer loop to process the pending entry

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    expected = INITIAL + ADD_AMT
    check(
        f"Pending Stream Message Was Re-Delivered After Stock Restart — "
        f"Stock Is {expected} (= {INITIAL} + {ADD_AMT})",
        stock == expected,
        f"got {stock}",
    )


# ---------------------------------------------------------------------------
# Ordered test list — imported by run.py
# ---------------------------------------------------------------------------
TESTS = [
    ("Double-Checkout Prevention: Second Checkout Rejected, State Unchanged", test_saga_double_checkout_prevented),
    ("Multi-Item Compensation: All Items Restored When Payment Fails", test_saga_compensation_multi_item),
    ("Concurrent Correlation Isolation: 20 Simultaneous Checkouts Get Correct Responses", test_concurrent_correlation_isolation),
    ("Pending-Message Re-Delivery: Stock Recovers In-Flight Add After Restart", test_pending_message_redelivery),
]
