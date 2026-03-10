"""
Order Service
=============
Manages orders and acts as the transaction coordinator for checkout.

Supports two transaction modes controlled by the TRANSACTION_MODE env var:

  TPC  — Synchronous Two-Phase Commit over HTTP. The order service is the
         coordinator: it PREPAREs stock and payment participants, then
         COMMITs or ABORTs based on their votes. Every state transition
         is persisted to the transaction_log table before the next external
         call, providing a durable coordinator log for crash recovery.

  SAGA — Asynchronous orchestration over Kafka. The order service publishes
         saga commands to internal.stock and internal.payment, advances
         state based on responses, and fires compensating transactions
         (rollbacks) on failure. State is persisted to the sagas table.

In both modes, the order service is the single source of truth for
checkout atomicity. Stock and payment are passive participants that
execute commands and report results.
"""

# gevent monkey-patching must happen before any other imports so that
# stdlib blocking calls (socket, time.sleep, threading) are replaced
# with cooperative greenlet-friendly versions.
import gevent.monkey
gevent.monkey.patch_all()

import os
import json
import uuid
import time
import atexit
import random
import logging
import threading
from collections import defaultdict
from typing import Any

import psycopg2
import requests
import psycopg2.pool

from time import perf_counter
from flask import Flask, jsonify, abort, Response, g

from common.db import create_conn_pool, setup_flask_lifecycle, setup_gunicorn_logging
from common.idempotency import check_idempotency_kafka, save_idempotency_kafka
from common.kafka_helpers import build_producer

GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://nginx:80")
TRANSACTION_MODE = os.environ.get("TRANSACTION_MODE", "TPC")

GATEWAY_KAFKA = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-external:9092")
INTERNAL_KAFKA = os.environ.get("INTERNAL_KAFKA_BOOTSTRAP_SERVERS", "kafka-internal:9092")

app = Flask("order-service")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Kafka topic names
# ---------------------------------------------------------------------------

ORDER_TOPIC = "gateway.orders"
GATEWAY_RESPONSE_TOPIC = "gateway.responses"

INTERNAL_STOCK_TOPIC = "internal.stock"
INTERNAL_PAYMENT_TOPIC = "internal.payment"
INTERNAL_RESPONSE_TOPIC = "internal.responses"

# ---------------------------------------------------------------------------
# Saga states
# ---------------------------------------------------------------------------

class SagaState:
    STOCK_REQUESTED = "STOCK_REQUESTED"
    PAYMENT_REQUESTED = "PAYMENT_REQUESTED"
    ROLLBACK_REQUESTED = "ROLLBACK_REQUESTED"
    COMPLETED = "COMPLETED"
    STOCK_FAILED = "STOCK_FAILED"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    ROLLED_BACK = "ROLLED_BACK"
    ROLLBACK_FAILED = "ROLLBACK_FAILED"

# ---------------------------------------------------------------------------
# DB pool and Flask lifecycle
# ---------------------------------------------------------------------------

conn_pool = create_conn_pool("ORDER")
setup_flask_lifecycle(app, conn_pool, "ORDER")

# ---------------------------------------------------------------------------
# DB helpers — orders
#
# A single db_get_order() that takes a connection parameter is used by both
# Flask routes and Kafka handlers. Flask routes catch ValueError and convert
# it to abort(400).
# ---------------------------------------------------------------------------

def db_get_order(conn, order_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT paid, items, user_id, total_cost FROM orders WHERE id = %s",
            (order_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"Order {order_id} not found")
    return {"paid": row[0], "items": row[1], "user_id": row[2], "total_cost": row[3]}


def db_get_order_for_update(conn, order_id: str) -> dict:
    """Same as db_get_order but acquires a row-level lock (FOR UPDATE).

    Serialises concurrent checkouts of the same order — the second thread
    blocks until the first completes.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT paid, items, user_id, total_cost FROM orders WHERE id = %s FOR UPDATE",
            (order_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"Order {order_id} not found")
    return {"paid": row[0], "items": row[1], "user_id": row[2], "total_cost": row[3]}


def db_mark_paid(conn, order_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE orders SET paid = TRUE WHERE id = %s", (order_id,))

# ---------------------------------------------------------------------------
# DB helpers — sagas
# ---------------------------------------------------------------------------

def db_create_saga(conn, saga_id, order_id, state, items_quantities,
                   original_correlation_id, idempotency_key):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO sagas
                (id, order_id, state, items_quantities, original_correlation_id, idempotency_key)
            VALUES (%s, %s, %s, %s, %s, %s)""",
            (saga_id, order_id, state, json.dumps(items_quantities),
             original_correlation_id, idempotency_key),
        )


def db_get_saga_for_update(conn, saga_id):
    with conn.cursor() as cur:
        cur.execute(
            """SELECT id, order_id, state, items_quantities,
                      original_correlation_id, idempotency_key
            FROM sagas WHERE id = %s FOR UPDATE""",
            (saga_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "id": row[0], "order_id": row[1], "state": row[2],
        "items_quantities": row[3], "original_correlation_id": row[4],
        "idempotency_key": row[5],
    }


def db_advance_saga(conn, saga_id, new_state):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE sagas SET state = %s, updated_at = NOW() WHERE id = %s",
            (new_state, saga_id),
        )

# ---------------------------------------------------------------------------
# Flask HTTP endpoints (shared by both modes)
# ---------------------------------------------------------------------------

@app.post("/create/<user_id>")
def create_order(user_id: str):
    key = str(uuid.uuid4())
    cur = g.conn.cursor()
    cur.execute(
        "INSERT INTO orders (id, paid, items, user_id, total_cost) VALUES (%s, %s, %s, %s, %s)",
        (key, False, json.dumps([]), user_id, 0),
    )
    cur.close()
    return jsonify({"order_id": key})


@app.post("/batch_init/<n>/<n_items>/<n_users>/<item_price>")
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n, n_items, n_users, item_price = int(n), int(n_items), int(n_users), int(item_price)
    cur = g.conn.cursor()
    for i in range(n):
        user_id = str(random.randint(0, n_users - 1))
        item1_id = str(random.randint(0, n_items - 1))
        item2_id = str(random.randint(0, n_items - 1))
        items = [[item1_id, 1], [item2_id, 1]]
        cur.execute(
            "INSERT INTO orders (id, paid, items, user_id, total_cost) VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (id) DO UPDATE SET paid = EXCLUDED.paid, items = EXCLUDED.items, "
            "user_id = EXCLUDED.user_id, total_cost = EXCLUDED.total_cost",
            (str(i), False, json.dumps(items), user_id, 2 * item_price),
        )
    cur.close()
    return jsonify({"msg": "Batch init for orders successful"})


@app.get("/find/<order_id>")
def find_order(order_id: str):
    try:
        order = db_get_order(g.conn, order_id)
    except ValueError as exc:
        abort(400, str(exc))
    return jsonify(
        {
            "order_id": order_id,
            "paid": order["paid"],
            "items": order["items"],
            "user_id": order["user_id"],
            "total_cost": order["total_cost"],
        }
    )


@app.post("/addItem/<order_id>/<item_id>/<quantity>")
def add_item(order_id: str, item_id: str, quantity: int):
    cur = g.conn.cursor()
    cur.execute(
        "SELECT items, total_cost FROM orders WHERE id = %s FOR UPDATE", (order_id,)
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"Order: {order_id} not found!")
    items, total_cost = row

    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        cur.close()
        abort(400, f"Item: {item_id} does not exist!")

    item_json = item_reply.json()
    items.append([item_id, int(quantity)])
    total_cost += int(quantity) * item_json["price"]

    cur.execute(
        "UPDATE orders SET items = %s, total_cost = %s WHERE id = %s",
        (json.dumps(items), total_cost, order_id),
    )
    cur.close()
    return Response(
        f"Item: {item_id} added to: {order_id} price updated to: {total_cost}",
        status=200,
    )


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})

# ---------------------------------------------------------------------------
# HTTP helpers (used in TPC mode for inter-service calls)
# ---------------------------------------------------------------------------

def send_post_request(url: str, idempotency_key: str = None, max_retries: int = 3):
    """POST with exponential backoff: 100ms, 200ms, 400ms.

    2xx and 4xx are terminal (business success or business failure — no retry).
    5xx triggers retry because it may be a transient infrastructure issue.
    """
    headers = {}
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key

    start_time = perf_counter()
    for attempt in range(max_retries + 1):
        try:
            response = requests.post(url, headers=headers, timeout=5)
            if response.status_code < 500:
                duration = perf_counter() - start_time
                print(f"ORDER: POST request took {duration:.7f} seconds")
                return response
            app.logger.warning(
                f"Retry {attempt + 1}/{max_retries} for {url} returned {response.status_code}"
            )
        except requests.exceptions.RequestException as e:
            app.logger.warning(
                f"Retry {attempt + 1}/{max_retries} for {url} failed: {e}"
            )
            if attempt == max_retries:
                abort(400, "Requests error")
        if attempt < max_retries:
            wait = 0.1 * (2**attempt)
            time.sleep(wait)
    abort(400, "Requests error")


def send_get_request(url: str):
    try:
        start_time = perf_counter()
        response = requests.get(url, timeout=5)
        duration = perf_counter() - start_time
        print(f"ORDER: GET request took {duration:.7f} seconds")
    except requests.exceptions.RequestException:
        abort(400, "Requests error")
    else:
        return response

# ---------------------------------------------------------------------------
# Checkout dispatcher
# ---------------------------------------------------------------------------

@app.post("/checkout/<order_id>")
def checkout(order_id: str):
    if TRANSACTION_MODE == "TPC":
        return checkout_tpc(order_id)
    else:
        return checkout_saga_http(order_id)

# ============================================================================
# TPC (Two-Phase Commit) — synchronous HTTP coordinator
#
# State machine: started -> preparing_stock -> preparing_payment ->
#                committing -> committed (happy path)
#                Any vote-NO or failure -> aborting -> aborted
#
# Every state transition is committed to PostgreSQL BEFORE the next external
# call. This is the durable coordinator log — if the process crashes at any
# point, recovery_tpc() reads the last persisted state and resumes.
# ============================================================================

def rollback_stock(removed_items: list[tuple[str, int]], transaction_id: str):
    for item_id, quantity in removed_items:
        send_post_request(
            f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}",
            idempotency_key=f"{transaction_id}:stock:rollback:{item_id}",
        )


def commit_tpc(txn_id: str, prepared_stock: list, prepared_payment: bool):
    if prepared_stock:
        send_post_request(
            f"{GATEWAY_URL}/stock/commit/{txn_id}",
            idempotency_key=f"{txn_id}:stock:commit",
        )
    if prepared_payment:
        send_post_request(
            f"{GATEWAY_URL}/payment/commit/{txn_id}",
            idempotency_key=f"{txn_id}:payment:commit",
        )


def abort_tpc(txn_id: str, prepared_stock: list, prepared_payment: bool):
    if prepared_stock:
        send_post_request(
            f"{GATEWAY_URL}/stock/abort/{txn_id}",
            idempotency_key=f"{txn_id}:stock:abort",
        )
    if prepared_payment:
        send_post_request(
            f"{GATEWAY_URL}/payment/abort/{txn_id}",
            idempotency_key=f"{txn_id}:payment:abort",
        )


def checkout_tpc(order_id: str):
    conn = g.conn
    cur = conn.cursor()

    txn_id = str(uuid.uuid4())

    cur.execute(
        "SELECT paid, items, user_id, total_cost FROM orders WHERE id = %s FOR UPDATE",
        (order_id,),
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"Order: {order_id} not found!")
    paid, items, user_id, total_cost = row

    if paid:
        cur.close()
        return Response("Order is already paid for!", status=200)

    items_quantities = defaultdict(int)
    for item_id, qty in items:
        items_quantities[item_id] += qty

    if not items_quantities:
        cur.close()
        abort(400, "Order has no items!")

    # --- Coordinator log: record transaction start ---
    cur.execute(
        "INSERT INTO transaction_log (txn_id, order_id, status, prepared_stock, prepared_payment, user_id, total_cost) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (txn_id, order_id, "started", json.dumps([]), False, user_id, total_cost),
    )
    conn.commit()

    # --- Phase 1: PREPARE stock (one item at a time) ---
    cur.execute(
        "UPDATE transaction_log SET status = 'preparing_stock' WHERE txn_id = %s",
        (txn_id,),
    )
    conn.commit()

    prepared_stock = []
    for item_id, qty in sorted(items_quantities.items()):
        reply = send_post_request(
            f"{GATEWAY_URL}/stock/prepare/{txn_id}/{item_id}/{qty}",
            idempotency_key=f"{txn_id}:stock:prepare:{item_id}",
        )
        if reply.status_code != 200:
            cur.execute(
                "UPDATE transaction_log SET status = 'aborting' WHERE txn_id = %s",
                (txn_id,),
            )
            conn.commit()
            abort_tpc(txn_id, prepared_stock, False)
            cur.execute(
                "UPDATE transaction_log SET status = 'aborted' WHERE txn_id = %s",
                (txn_id,),
            )
            conn.commit()
            cur.close()
            abort(400, "Failed to PREPARE stock")

        # Persist which items are prepared so recovery knows what to abort
        prepared_stock.append([item_id, qty])
        cur.execute(
            "UPDATE transaction_log SET prepared_stock = %s WHERE txn_id = %s",
            (json.dumps(prepared_stock), txn_id),
        )
        conn.commit()

    # --- Phase 1: PREPARE payment ---
    cur.execute(
        "UPDATE transaction_log SET status = 'preparing_payment' WHERE txn_id = %s",
        (txn_id,),
    )
    conn.commit()

    payment_reply = send_post_request(
        f"{GATEWAY_URL}/payment/prepare/{txn_id}/{user_id}/{total_cost}",
        idempotency_key=f"{txn_id}:payment:prepare",
    )
    if payment_reply.status_code != 200:
        cur.execute(
            "UPDATE transaction_log SET status = 'aborting' WHERE txn_id = %s",
            (txn_id,),
        )
        conn.commit()
        abort_tpc(txn_id, prepared_stock, False)
        cur.execute(
            "UPDATE transaction_log SET status = 'aborted' WHERE txn_id = %s", (txn_id,)
        )
        conn.commit()
        cur.close()
        abort(400, "Failed to PREPARE payment")

    cur.execute(
        "UPDATE transaction_log SET prepared_payment = TRUE WHERE txn_id = %s",
        (txn_id,),
    )
    conn.commit()

    # --- Phase 2: COMMIT ---
    # Once we persist 'committing', the decision is final. Even if we crash
    # here, recovery_tpc() will see 'committing' and finish the commit.
    cur.execute(
        "UPDATE transaction_log SET status = 'committing' WHERE txn_id = %s", (txn_id,)
    )
    conn.commit()

    commit_tpc(txn_id, prepared_stock, True)

    cur.execute("UPDATE orders SET paid = TRUE WHERE id = %s", (order_id,))
    cur.execute(
        "UPDATE transaction_log SET status = 'committed' WHERE txn_id = %s", (txn_id,)
    )
    conn.commit()
    cur.close()
    return Response("Checkout successful", status=200)


def recovery_tpc():
    """Recover incomplete transactions on startup by reading the coordinator log.

    Scans transaction_log for non-terminal states:
      started, preparing_stock, preparing_payment, aborting -> abort
      committing -> commit (the decision was already made)

    This guarantees that no in-doubt transaction survives a restart.
    """
    conn = conn_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT txn_id, order_id, status, prepared_stock, prepared_payment, user_id, total_cost "
            "FROM transaction_log WHERE status NOT IN ('committed', 'aborted')"
        )
        rows = cur.fetchall()
        if not rows:
            cur.close()
            app.logger.info("RECOVERY: No incomplete transactions found")
            return

        for (txn_id, order_id, status, prepared_stock, prepared_payment, user_id, total_cost) in rows:
            if prepared_stock is None:
                prepared_stock = []
            elif isinstance(prepared_stock, str):
                try:
                    prepared_stock = json.loads(prepared_stock) if prepared_stock else []
                except (TypeError, ValueError):
                    prepared_stock = []

            app.logger.warning(
                f"RECOVERY: Found incomplete transaction txn={txn_id}, status={status}"
            )

            if status in ("started", "preparing_stock", "preparing_payment"):
                abort_tpc(txn_id, prepared_stock, prepared_payment)
                cur.execute(
                    "UPDATE transaction_log SET status = 'aborted' WHERE txn_id = %s",
                    (txn_id,),
                )
                conn.commit()
            elif status == "committing":
                commit_tpc(txn_id, prepared_stock, prepared_payment)
                cur.execute("UPDATE orders SET paid = TRUE WHERE id = %s", (order_id,))
                cur.execute(
                    "UPDATE transaction_log SET status = 'committed' WHERE txn_id = %s",
                    (txn_id,),
                )
                conn.commit()
            elif status == "aborting":
                abort_tpc(txn_id, prepared_stock, prepared_payment)
                cur.execute(
                    "UPDATE transaction_log SET status = 'aborted' WHERE txn_id = %s",
                    (txn_id,),
                )
                conn.commit()
            else:
                app.logger.warning(
                    f"RECOVERY: Unknown status txn={txn_id}, status={status}"
                )
        cur.close()
    finally:
        conn_pool.putconn(conn)

# ============================================================================
# SAGA — synchronous HTTP fallback (used when Kafka is unavailable)
#
# A lightweight saga that uses direct HTTP calls instead of Kafka. Stock is
# subtracted per-item, and if any step fails, previously subtracted items
# are rolled back. No saga log is persisted because the HTTP request itself
# is synchronous — there is nothing to recover.
# ============================================================================

def checkout_saga_http(order_id: str):
    transaction_id = str(uuid.uuid4())

    cur = g.conn.cursor()
    cur.execute(
        "SELECT paid, items, user_id, total_cost FROM orders WHERE id = %s FOR UPDATE",
        (order_id,),
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"Order: {order_id} not found!")
    paid, items, user_id, total_cost = row

    if paid:
        cur.close()
        return Response("Order is already paid for!", status=200)

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in items:
        items_quantities[item_id] += quantity

    if not items_quantities:
        cur.close()
        abort(400, "Order has no items!")

    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        stock_reply = send_post_request(
            f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}",
            idempotency_key=f"{transaction_id}:stock:subtract:{item_id}",
        )
        if stock_reply.status_code != 200:
            rollback_stock(removed_items, transaction_id)
            cur.close()
            abort(400, f"Out of stock on item_id: {item_id}")
        removed_items.append((item_id, quantity))

    user_reply = send_post_request(
        f"{GATEWAY_URL}/payment/pay/{user_id}/{total_cost}",
        idempotency_key=f"{transaction_id}:payment:pay",
    )
    if user_reply.status_code != 200:
        rollback_stock(removed_items, transaction_id)
        cur.close()
        abort(400, "User out of credit")

    cur.execute("UPDATE orders SET paid = TRUE WHERE id = %s", (order_id,))
    cur.close()
    return Response("Checkout successful", status=200)

# ============================================================================
# SAGA — Kafka-driven checkout (used when requests come via api-gateway)
#
# The order service is the saga orchestrator. When a checkout arrives via
# the gateway Kafka consumer, it initiates a multi-step saga:
#
#   1. Publish subtract_batch to internal.stock
#   2. On stock success -> publish pay to internal.payment
#   3. On payment success -> mark order paid, saga COMPLETED
#   3b. On payment failure -> publish add_batch to internal.stock (compensate)
#   4. On rollback success -> saga ROLLED_BACK
#
# Each state transition is persisted to the sagas table before publishing
# the next Kafka message. This is the durable saga log.
# ============================================================================

gateway_producer = None
internal_producer = None

# Pending price lookups for addItem via Kafka (SAGA mode)
_pending_price_lookups: dict[str, tuple[threading.Event, dict | None]] = {}
_pending_price_lookups_lock = threading.Lock()


def publish_gateway_response(correlation_id: str, status_code: int, body: Any) -> None:
    """Send a response back to the API gateway so it can unblock the HTTP client."""
    import kafka as kafka_mod
    payload = {"correlation_id": correlation_id, "status_code": status_code, "body": body}
    try:
        gateway_producer.send(GATEWAY_RESPONSE_TOPIC, key=correlation_id, value=payload)
        gateway_producer.flush(timeout=5)
    except kafka_mod.errors.KafkaError as exc:
        logger.error("Failed to publish gateway response for %s: %s", correlation_id, exc)


def publish_internal_request(topic: str, correlation_id: str, method: str,
                             path: str, body: dict | None = None,
                             headers: dict | None = None) -> None:
    """Publish a saga command to a participant service via the internal Kafka cluster."""
    import kafka as kafka_mod
    payload = {
        "correlation_id": correlation_id,
        "method": method.upper(),
        "path": path,
        "headers": headers or {},
        "body": body or {},
    }
    try:
        internal_producer.send(topic, key=correlation_id, value=payload)
        internal_producer.flush(timeout=5)
    except kafka_mod.errors.KafkaError as exc:
        logger.error("Failed to publish internal request to %s: %s", topic, exc)
        raise

# ---------------------------------------------------------------------------
# SAGA gateway message routing (Kafka consumer calls these)
#
# When running in SAGA mode, all HTTP requests are routed through the API
# gateway -> Kafka -> this consumer. The routing logic here mirrors the
# Flask endpoints above but operates on Kafka message payloads instead of
# HTTP requests.
# ---------------------------------------------------------------------------

def handle_checkout_saga(conn, order_id, headers):
    """Initiates a saga-based checkout. Returns None (response sent async)."""
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")
    correlation_id = headers.get("X-Correlation-Id") or headers.get("x-correlation-id")

    cached = check_idempotency_kafka(conn, idem_key)
    if cached:
        return cached

    try:
        order = db_get_order_for_update(conn, order_id)
    except ValueError as exc:
        conn.rollback()
        return 400, {"error": str(exc)}

    if order["paid"]:
        conn.rollback()
        return 409, {"error": f"Order {order_id} is already paid"}

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order["items"]:
        items_quantities[item_id] += quantity

    if not items_quantities:
        conn.rollback()
        return 400, {"error": "Order has no items"}

    saga_id = idem_key or str(uuid.uuid4())
    stock_corr_id = f"{saga_id}:stock:subtract_batch"

    try:
        db_create_saga(
            conn, saga_id=saga_id, order_id=order_id,
            state=SagaState.STOCK_REQUESTED,
            items_quantities=dict(items_quantities),
            original_correlation_id=correlation_id,
            idempotency_key=idem_key,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    batch_items = [{"item_id": iid, "amount": qty} for iid, qty in items_quantities.items()]
    publish_internal_request(
        topic=INTERNAL_STOCK_TOPIC,
        correlation_id=stock_corr_id,
        method="POST",
        path="/subtract_batch",
        body={"items": batch_items},
        headers={"Idempotency-Key": stock_corr_id},
    )

    return None  # response sent asynchronously by saga handlers


def route_gateway_message(payload: dict, conn):
    """Route a message from gateway.orders to the correct handler."""
    method = payload.get("method", "GET").upper()
    path = payload.get("path", "/")
    body = payload.get("body") or {}
    headers = payload.get("headers") or {}
    headers["X-Correlation-Id"] = payload.get("correlation_id", "")

    segments = [s for s in path.strip("/").split("/") if s]

    if method == "POST" and len(segments) >= 2 and segments[0] == "create":
        user_id = segments[1]
        order_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO orders (id, paid, items, user_id, total_cost) VALUES (%s, %s, %s, %s, %s)",
                (order_id, False, json.dumps([]), user_id, 0),
            )
        conn.commit()
        return 201, {"order_id": order_id}

    if method == "POST" and len(segments) >= 5 and segments[0] == "batch_init":
        n, n_items, n_users, item_price = int(segments[1]), int(segments[2]), int(segments[3]), int(segments[4])
        with conn.cursor() as cur:
            for i in range(n):
                uid = str(random.randint(0, n_users - 1))
                i1 = str(random.randint(0, n_items - 1))
                i2 = str(random.randint(0, n_items - 1))
                cur.execute(
                    "INSERT INTO orders (id, paid, items, user_id, total_cost) VALUES (%s, %s, %s, %s, %s) "
                    "ON CONFLICT (id) DO UPDATE SET paid = EXCLUDED.paid, items = EXCLUDED.items, "
                    "user_id = EXCLUDED.user_id, total_cost = EXCLUDED.total_cost",
                    (str(i), False, json.dumps([[i1, 1], [i2, 1]]), uid, 2 * item_price),
                )
        conn.commit()
        return 200, {"msg": "Batch init for orders successful"}

    if method == "GET" and len(segments) >= 2 and segments[0] == "find":
        order_id = segments[1]
        try:
            order = db_get_order(conn, order_id)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "order_id": order_id, "paid": order["paid"],
            "items": order["items"], "user_id": order["user_id"],
            "total_cost": order["total_cost"],
        }

    if method == "POST" and len(segments) >= 4 and segments[0] == "addItem":
        order_id, item_id, quantity = segments[1], segments[2], int(segments[3])
        stock_response = _fetch_item_price(item_id)
        if stock_response is None:
            return 503, {"error": "Stock service timeout fetching item price"}
        if stock_response.get("status_code") != 200:
            return 400, {"error": f"Item {item_id} not found in stock service"}
        item_price = stock_response["body"]["price"]
        with conn.cursor() as cur:
            cur.execute("SELECT items, total_cost FROM orders WHERE id = %s FOR UPDATE", (order_id,))
            row = cur.fetchone()
            if row is None:
                return 400, {"error": f"Order {order_id} not found"}
            items_list, total_cost = row
            items_list.append([item_id, quantity])
            total_cost += quantity * item_price
            cur.execute(
                "UPDATE orders SET items = %s, total_cost = %s WHERE id = %s",
                (json.dumps(items_list), total_cost, order_id),
            )
        conn.commit()
        return 200, f"Item: {item_id} added to: {order_id} price updated to: {total_cost}"

    if method == "POST" and len(segments) >= 2 and segments[0] == "checkout":
        order_id = segments[1]
        return handle_checkout_saga(conn, order_id, headers)

    return 404, {"error": f"No handler for {method} {path}"}


def _fetch_item_price(item_id: str):
    """Synchronously fetch item price from stock service via internal Kafka.

    Publishes a GET /find request to internal.stock and blocks until the
    response arrives on internal.responses. The internal consumer thread
    detects this correlation ID in _pending_price_lookups and sets the event
    instead of routing it as a saga response.
    """
    corr_id = str(uuid.uuid4())
    event = threading.Event()

    with _pending_price_lookups_lock:
        _pending_price_lookups[corr_id] = (event, None)

    publish_internal_request(
        topic=INTERNAL_STOCK_TOPIC,
        correlation_id=corr_id,
        method="GET",
        path=f"/find/{item_id}",
    )

    if not event.wait(timeout=10):
        with _pending_price_lookups_lock:
            _pending_price_lookups.pop(corr_id, None)
        return None

    with _pending_price_lookups_lock:
        _, response = _pending_price_lookups.pop(corr_id)
    return response

# ---------------------------------------------------------------------------
# Saga advancement handlers
#
# Each handler is called when a response arrives on internal.responses
# matching the expected correlation ID suffix. The handler acquires a
# FOR UPDATE lock on the saga row and verifies the current state before
# advancing — this guards against duplicate or out-of-order Kafka messages.
# ---------------------------------------------------------------------------

def _on_stock_response(conn, saga, response):
    if response.get("status_code") == 200:
        payment_corr_id = f"{saga['id']}:payment:pay"
        try:
            order = db_get_order(conn, saga["order_id"])
        except ValueError:
            _fire_rollback(conn, saga)
            return
        db_advance_saga(conn, saga["id"], SagaState.PAYMENT_REQUESTED)
        conn.commit()
        publish_internal_request(
            topic=INTERNAL_PAYMENT_TOPIC,
            correlation_id=payment_corr_id,
            method="POST",
            path=f"/pay/{order['user_id']}/{order['total_cost']}",
            headers={"Idempotency-Key": payment_corr_id},
        )
    else:
        error = (response.get("body") or {}).get("error", "Stock subtraction failed")
        db_advance_saga(conn, saga["id"], SagaState.STOCK_FAILED)
        conn.commit()
        publish_gateway_response(saga["original_correlation_id"], 400, {"error": error})


def _on_payment_response(conn, saga, response):
    if response.get("status_code") == 200:
        try:
            db_mark_paid(conn, saga["order_id"])
            save_idempotency_kafka(conn, saga["idempotency_key"], 200, "Checkout successful")
            db_advance_saga(conn, saga["id"], SagaState.COMPLETED)
            conn.commit()
        except Exception:
            logger.critical(
                "SAGA INCONSISTENCY: payment committed but order %s not marked paid. saga_id=%s",
                saga["order_id"], saga["id"],
            )
            conn.rollback()
            raise
        publish_gateway_response(saga["original_correlation_id"], 200, "Checkout successful")
    else:
        _fire_rollback(conn, saga)


def _fire_rollback(conn, saga):
    """Fire compensating transaction: re-add stock that was subtracted."""
    rollback_corr_id = f"{saga['id']}:stock:rollback"
    items_quantities = saga["items_quantities"]
    batch_items = [{"item_id": iid, "amount": qty} for iid, qty in items_quantities.items()]

    db_advance_saga(conn, saga["id"], SagaState.ROLLBACK_REQUESTED)
    conn.commit()

    publish_internal_request(
        topic=INTERNAL_STOCK_TOPIC,
        correlation_id=rollback_corr_id,
        method="POST",
        path="/add_batch",
        body={"items": batch_items},
        headers={"Idempotency-Key": rollback_corr_id},
    )


def _on_rollback_response(conn, saga, response):
    if response.get("status_code") == 200:
        db_advance_saga(conn, saga["id"], SagaState.ROLLED_BACK)
        conn.commit()
        publish_gateway_response(
            saga["original_correlation_id"], 400, {"error": "User has insufficient credit"},
        )
    else:
        logger.critical(
            "SAGA INCONSISTENCY: stock rollback failed. saga_id=%s order_id=%s",
            saga["id"], saga["order_id"],
        )
        db_advance_saga(conn, saga["id"], SagaState.ROLLBACK_FAILED)
        conn.commit()
        publish_gateway_response(
            saga["original_correlation_id"], 500,
            {"error": "Checkout failed and rollback encountered an error."},
        )


# Correlation ID suffix -> (handler function, expected saga state)
_SAGA_HANDLERS = {
    ":stock:subtract_batch": (_on_stock_response, SagaState.STOCK_REQUESTED),
    ":payment:pay": (_on_payment_response, SagaState.PAYMENT_REQUESTED),
    ":stock:rollback": (_on_rollback_response, SagaState.ROLLBACK_REQUESTED),
}


def handle_internal_response(payload):
    """Routes an internal.responses message to the correct saga handler.

    First checks if this is a price-lookup response (non-saga). If not,
    matches the correlation ID suffix to determine which saga step completed,
    acquires a FOR UPDATE lock on the saga row, verifies state, and calls
    the handler.
    """
    correlation_id = payload.get("correlation_id", "")

    # Check if this is a response to a synchronous price lookup (addItem)
    with _pending_price_lookups_lock:
        if correlation_id in _pending_price_lookups:
            event, _ = _pending_price_lookups[correlation_id]
            _pending_price_lookups[correlation_id] = (event, payload)
            event.set()
            return

    handler_fn = None
    saga_id = None
    expected_state = None

    for suffix, (fn, state) in _SAGA_HANDLERS.items():
        if correlation_id.endswith(suffix):
            saga_id = correlation_id[: -len(suffix)]
            handler_fn = fn
            expected_state = state
            break

    if handler_fn is None:
        logger.warning("No handler found for internal correlation_id: %s", correlation_id)
        return

    conn = conn_pool.getconn()
    try:
        saga = db_get_saga_for_update(conn, saga_id)
        if saga is None:
            logger.error("Saga %s not found (correlation_id=%s)", saga_id, correlation_id)
            conn.rollback()
            return
        if saga["state"] != expected_state:
            logger.warning(
                "Saga %s: expected state %s but got %s — skipping",
                saga_id, expected_state, saga["state"],
            )
            conn.rollback()
            return
        handler_fn(conn, saga, payload)
    except Exception as exc:
        logger.error("Error advancing saga %s: %s", saga_id, exc, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn_pool.putconn(conn)

# ---------------------------------------------------------------------------
# SAGA recovery
# ---------------------------------------------------------------------------

def recovery_saga():
    """Recover incomplete sagas by re-publishing the pending Kafka message.

    Safe because all downstream consumers are idempotent — re-publishing
    a subtract_batch, pay, or add_batch with the same idempotency key
    returns the cached result without double-processing.
    """
    terminal_states = (
        SagaState.COMPLETED, SagaState.STOCK_FAILED,
        SagaState.PAYMENT_FAILED, SagaState.ROLLED_BACK,
        SagaState.ROLLBACK_FAILED,
    )
    conn = conn_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, order_id, state, items_quantities FROM sagas "
                "WHERE state NOT IN %s",
                (terminal_states,),
            )
            rows = cur.fetchall()

        if not rows:
            app.logger.info("SAGA RECOVERY: No incomplete sagas found")
            return

        for saga_id, order_id, state, items_quantities in rows:
            app.logger.warning(
                "SAGA RECOVERY: Found incomplete saga %s in state %s (order %s)",
                saga_id, state, order_id,
            )

            if isinstance(items_quantities, str):
                items_quantities = json.loads(items_quantities)

            batch_items = [
                {"item_id": iid, "amount": qty}
                for iid, qty in items_quantities.items()
            ]

            if state == SagaState.STOCK_REQUESTED:
                corr_id = f"{saga_id}:stock:subtract_batch"
                publish_internal_request(
                    topic=INTERNAL_STOCK_TOPIC,
                    correlation_id=corr_id,
                    method="POST",
                    path="/subtract_batch",
                    body={"items": batch_items},
                    headers={"Idempotency-Key": corr_id},
                )

            elif state == SagaState.PAYMENT_REQUESTED:
                try:
                    order = db_get_order(conn, order_id)
                except ValueError:
                    app.logger.error(
                        "SAGA RECOVERY: Order %s not found for saga %s, skipping",
                        order_id, saga_id,
                    )
                    continue
                corr_id = f"{saga_id}:payment:pay"
                publish_internal_request(
                    topic=INTERNAL_PAYMENT_TOPIC,
                    correlation_id=corr_id,
                    method="POST",
                    path=f"/pay/{order['user_id']}/{order['total_cost']}",
                    headers={"Idempotency-Key": corr_id},
                )

            elif state == SagaState.ROLLBACK_REQUESTED:
                corr_id = f"{saga_id}:stock:rollback"
                publish_internal_request(
                    topic=INTERNAL_STOCK_TOPIC,
                    correlation_id=corr_id,
                    method="POST",
                    path="/add_batch",
                    body={"items": batch_items},
                    headers={"Idempotency-Key": corr_id},
                )

            app.logger.info(
                "SAGA RECOVERY: Re-published message for saga %s (state=%s)",
                saga_id, state,
            )
    finally:
        conn_pool.putconn(conn)

# ---------------------------------------------------------------------------
# Kafka consumer loops (SAGA mode only)
# ---------------------------------------------------------------------------

def start_gateway_consumer():
    """Consume messages from gateway.orders and route them to handlers.

    Unlike stock/payment which use the generic run_consumer_loop, the order
    service needs custom logic: checkout returns None (response sent async
    by saga handlers), while other operations return a result immediately.

    auto_offset_reset="earliest" ensures no messages are missed after a
    restart — in contrast to the gateway which uses "latest" because it only
    cares about responses to currently in-flight HTTP requests.
    """
    import kafka
    while True:
        try:
            consumer = kafka.KafkaConsumer(
                ORDER_TOPIC,
                bootstrap_servers=GATEWAY_KAFKA,
                group_id="order-service",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("Gateway consumer started on '%s'", ORDER_TOPIC)

            for message in consumer:
                payload = message.value
                correlation_id = payload.get("correlation_id")

                if not correlation_id:
                    consumer.commit()
                    continue

                conn = conn_pool.getconn()
                result = None
                try:
                    result = route_gateway_message(payload, conn)
                except Exception as exc:
                    logger.error("Unhandled error processing %s: %s", correlation_id, exc, exc_info=True)
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    result = (500, {"error": "Internal server error"})
                finally:
                    conn_pool.putconn(conn)

                if result is not None:
                    status_code, body = result
                    publish_gateway_response(correlation_id, status_code, body)

                try:
                    consumer.commit()
                except Exception:
                    pass

        except Exception as exc:
            logger.error("Gateway consumer crashed, reconnecting in 3s: %s", exc)
            time.sleep(3)


def start_internal_consumer():
    """Consume saga responses from internal.responses and advance saga state."""
    import kafka
    while True:
        try:
            consumer = kafka.KafkaConsumer(
                INTERNAL_RESPONSE_TOPIC,
                bootstrap_servers=INTERNAL_KAFKA,
                group_id="order-service-internal",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("Internal response consumer started on '%s'", INTERNAL_RESPONSE_TOPIC)

            for message in consumer:
                try:
                    handle_internal_response(message.value)
                except Exception as exc:
                    logger.error("Unhandled error in internal consumer: %s", exc, exc_info=True)
                try:
                    consumer.commit()
                except Exception:
                    pass

        except Exception as exc:
            logger.error("Internal consumer crashed, reconnecting in 3s: %s", exc)
            time.sleep(3)

# ---------------------------------------------------------------------------
# Startup
#
# In SAGA mode, gunicorn runs with -w 1 (single worker) because the Kafka
# consumer threads and saga state are in-process. Multiple workers would
# each spin up their own consumers, creating duplicate message processing.
# In TPC mode, multiple workers are safe because all state coordination
# happens via the database.
# ---------------------------------------------------------------------------

with app.app_context():
    if TRANSACTION_MODE == "TPC":
        try:
            recovery_tpc()
        except Exception as e:
            app.logger.warning(f"RECOVERY: Error during recovery: {e}")
    elif TRANSACTION_MODE == "SAGA":
        gateway_producer = build_producer(GATEWAY_KAFKA)
        internal_producer = build_producer(INTERNAL_KAFKA)
        try:
            recovery_saga()
        except Exception as e:
            app.logger.warning(f"SAGA RECOVERY: Error during recovery: {e}")
        threading.Thread(target=start_gateway_consumer, daemon=True, name="gateway-consumer").start()
        threading.Thread(target=start_internal_consumer, daemon=True, name="internal-consumer").start()
        app.logger.info("SAGA mode: Kafka producers and consumers started")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    setup_gunicorn_logging(app)
