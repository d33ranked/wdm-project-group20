"""Order service 2PC coordinator — checkout, commit/abort helpers, recovery."""

import os
import json
import uuid
import time
import logging
from collections import defaultdict
from time import perf_counter

import requests
from flask import g, abort, Response

GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://nginx:80")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP Helpers (Retry With Exponential Backoff)
# ---------------------------------------------------------------------------

def send_post_request(url, idempotency_key=None, max_retries=7, json_body=None):
    headers = {"Idempotency-Key": idempotency_key} if idempotency_key else {}
    start = perf_counter()
    for attempt in range(max_retries + 1):
        try:
            response = requests.post(url, headers=headers, json=json_body, timeout=5)
            if response.status_code < 500:
                logger.debug("ORDER: POST took %.7fs", perf_counter() - start)
                return response
        except requests.exceptions.RequestException:
            if attempt == max_retries:
                abort(400, "Requests error")
        if attempt < max_retries:
            time.sleep(min(0.5 * (2 ** attempt), 5))
    abort(400, "Requests error")


def send_get_request(url):
    try:
        start = perf_counter()
        response = requests.get(url, timeout=5)
        logger.debug("ORDER: GET took %.7fs", perf_counter() - start)
        return response
    except requests.exceptions.RequestException:
        abort(400, "Requests error")


# ---------------------------------------------------------------------------
# 2PC Participant Helpers
# ---------------------------------------------------------------------------

def rollback_stock(removed_items, transaction_id):
    for item_id, quantity in removed_items:
        send_post_request(
            f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}",
            idempotency_key=f"{transaction_id}:stock:rollback:{item_id}",
        )


def commit_tpc(txn_id, prepared_stock, prepared_payment):
    if prepared_stock:
        send_post_request(f"{GATEWAY_URL}/stock/commit/{txn_id}", f"{txn_id}:stock:commit")
    if prepared_payment:
        send_post_request(f"{GATEWAY_URL}/payment/commit/{txn_id}", f"{txn_id}:payment:commit")


def abort_tpc(txn_id, prepared_stock, prepared_payment):
    if prepared_stock:
        send_post_request(f"{GATEWAY_URL}/stock/abort/{txn_id}", f"{txn_id}:stock:abort")
    if prepared_payment:
        send_post_request(f"{GATEWAY_URL}/payment/abort/{txn_id}", f"{txn_id}:payment:abort")


# ---------------------------------------------------------------------------
# 2PC Checkout
#
# State Machine:
#   started -> preparing_stock -> preparing_payment -> committing -> committed
#   Any vote-NO or failure -> aborting -> aborted
#
# Every State Transition Is Persisted Before The Next External Call.
# ---------------------------------------------------------------------------

def checkout_tpc(order_id):
    conn = g.conn
    txn_id = str(uuid.uuid4())

    with conn.cursor() as cur:
        # Step 1: Lock Order Row
        cur.execute("SELECT paid, items, user_id, total_cost FROM orders WHERE id = %s FOR UPDATE", (order_id,))
        row = cur.fetchone()
        if row is None:
            abort(400, f"Order: {order_id} not found!")
        paid, items, user_id, total_cost = row

        if paid:
            return Response("Order is already paid for!", status=200)

        items_quantities = defaultdict(int)
        for item_id, qty in items:
            items_quantities[item_id] += qty
        if not items_quantities:
            return Response("Order has no items.", status=200)

        # Step 2: Create Transaction Log Entry
        cur.execute(
            "INSERT INTO transaction_log (txn_id, order_id, status, prepared_stock, prepared_payment, user_id, total_cost) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (txn_id, order_id, "started", json.dumps([]), False, user_id, total_cost),
        )
        conn.commit()

        # Step 3: Prepare All Stock Items In One Batch Request
        cur.execute("UPDATE transaction_log SET status = 'preparing_stock' WHERE txn_id = %s", (txn_id,))
        conn.commit()

        batch_items = [{"item_id": iid, "quantity": qty} for iid, qty in sorted(items_quantities.items())]
        prepared_stock = [[e["item_id"], e["quantity"]] for e in batch_items]

        stock_reply = send_post_request(
            f"{GATEWAY_URL}/stock/prepare_batch/{txn_id}",
            idempotency_key=f"{txn_id}:stock:prepare_batch",
            json_body={"items": batch_items},
        )
        if stock_reply.status_code != 200:
            # Stock Voted NO — Abort
            cur.execute("UPDATE transaction_log SET status = 'aborting' WHERE txn_id = %s", (txn_id,))
            conn.commit()
            abort_tpc(txn_id, [], False)
            cur.execute("UPDATE transaction_log SET status = 'aborted' WHERE txn_id = %s", (txn_id,))
            conn.commit()
            abort(400, "Failed to PREPARE stock")

        # Persist Prepared Items For Crash Recovery
        cur.execute("UPDATE transaction_log SET prepared_stock = %s WHERE txn_id = %s",
                    (json.dumps(prepared_stock), txn_id))
        conn.commit()

        # Step 4: Prepare Payment
        cur.execute("UPDATE transaction_log SET status = 'preparing_payment' WHERE txn_id = %s", (txn_id,))
        conn.commit()

        payment_reply = send_post_request(
            f"{GATEWAY_URL}/payment/prepare/{txn_id}/{user_id}/{total_cost}",
            idempotency_key=f"{txn_id}:payment:prepare",
        )
        if payment_reply.status_code != 200:
            # Payment Voted NO — Abort All
            cur.execute("UPDATE transaction_log SET status = 'aborting' WHERE txn_id = %s", (txn_id,))
            conn.commit()
            abort_tpc(txn_id, prepared_stock, False)
            cur.execute("UPDATE transaction_log SET status = 'aborted' WHERE txn_id = %s", (txn_id,))
            conn.commit()
            abort(400, "Failed to PREPARE payment")

        cur.execute("UPDATE transaction_log SET prepared_payment = TRUE WHERE txn_id = %s", (txn_id,))
        conn.commit()

        # Step 5: Commit (Decision Is Final Once 'committing' Is Persisted)
        cur.execute("UPDATE transaction_log SET status = 'committing' WHERE txn_id = %s", (txn_id,))
        conn.commit()

        commit_tpc(txn_id, prepared_stock, True)

        cur.execute("UPDATE orders SET paid = TRUE WHERE id = %s", (order_id,))
        cur.execute("UPDATE transaction_log SET status = 'committed' WHERE txn_id = %s", (txn_id,))
        conn.commit()

    return Response("Checkout successful", status=200)


# ---------------------------------------------------------------------------
# Recovery: Scan Transaction Log For Non-Terminal States On Startup
#   started / preparing_stock / preparing_payment / aborting -> Abort
#   committing -> Commit (Decision Was Already Made)
# ---------------------------------------------------------------------------

def recovery_tpc(conn_pool):
    conn = conn_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT txn_id, order_id, status, prepared_stock, prepared_payment, user_id, total_cost "
                "FROM transaction_log WHERE status NOT IN ('committed', 'aborted')"
            )
            rows = cur.fetchall()
            if not rows:
                print("RECOVERY: No incomplete transactions found", flush=True)
                return

            for (txn_id, order_id, status, prepared_stock, prepared_payment, user_id, total_cost) in rows:
                if prepared_stock is None:
                    prepared_stock = []
                elif isinstance(prepared_stock, str):
                    try:
                        prepared_stock = json.loads(prepared_stock) if prepared_stock else []
                    except (TypeError, ValueError):
                        prepared_stock = []

                print(f"RECOVERY: txn={txn_id}, status={status}", flush=True)

                if status in ("started", "preparing_stock", "preparing_payment", "aborting"):
                    abort_tpc(txn_id, prepared_stock, prepared_payment)
                    cur.execute("UPDATE transaction_log SET status = 'aborted' WHERE txn_id = %s", (txn_id,))
                    conn.commit()
                elif status == "committing":
                    commit_tpc(txn_id, prepared_stock, prepared_payment)
                    cur.execute("UPDATE orders SET paid = TRUE WHERE id = %s", (order_id,))
                    cur.execute("UPDATE transaction_log SET status = 'committed' WHERE txn_id = %s", (txn_id,))
                    conn.commit()
    finally:
        conn_pool.putconn(conn)
