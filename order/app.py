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

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

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
# DB pool
# ---------------------------------------------------------------------------

def create_conn_pool(retries=10, delay=2):
    for attempt in range(retries):
        try:
            return psycopg2.pool.ThreadedConnectionPool(
                minconn=10,
                maxconn=100,
                host=os.environ["POSTGRES_HOST"],
                port=int(os.environ["POSTGRES_PORT"]),
                dbname=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
            )
        except psycopg2.OperationalError:
            if attempt < retries - 1:
                print(
                    f"ORDER: PostgreSQL not ready, retrying in {delay}s... (attempt {attempt+1}/{retries})"
                )
                time.sleep(delay)
            else:
                raise


conn_pool = create_conn_pool()


def close_db_connection():
    conn_pool.closeall()


atexit.register(close_db_connection)

# ---------------------------------------------------------------------------
# Flask request lifecycle (used in TPC mode for HTTP endpoints)
# ---------------------------------------------------------------------------

@app.before_request
def before_req():
    g.start_time = perf_counter()
    g.conn = conn_pool.getconn()


@app.after_request
def log_response(response):
    duration = perf_counter() - g.start_time
    print(f"ORDER: Request took {duration:.7f} seconds")
    return response


@app.teardown_request
def teardown_req(exception):
    conn = g.pop("conn", None)
    if conn is not None:
        if exception:
            conn.rollback()
        else:
            conn.commit()
        conn_pool.putconn(conn)

# ---------------------------------------------------------------------------
# DB helpers — orders
# ---------------------------------------------------------------------------

def get_order_from_db(order_id: str):
    cur = g.conn.cursor()
    cur.execute(
        "SELECT paid, items, user_id, total_cost FROM orders WHERE id = %s", (order_id,)
    )
    row = cur.fetchone()
    cur.close()
    if row is None:
        abort(400, f"Order: {order_id} not found!")
    return {"paid": row[0], "items": row[1], "user_id": row[2], "total_cost": row[3]}


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
# DB helpers — idempotency (for SAGA mode)
# ---------------------------------------------------------------------------

def check_saga_idempotency(conn, idem_key):
    if not idem_key:
        return None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status_code, body FROM idempotency_keys WHERE key = %s",
            (idem_key,),
        )
        row = cur.fetchone()
    return (row[0], row[1]) if row else None


def save_saga_idempotency(conn, idem_key, status_code, body):
    if not idem_key:
        return
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO idempotency_keys (key, status_code, body) "
            "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (idem_key, status_code, body),
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
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)
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
    order = get_order_from_db(order_id)
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
# HTTP helpers (used in TPC mode)
# ---------------------------------------------------------------------------

def send_post_request(url: str, idempotency_key: str = None, max_retries: int = 3):
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
                abort(400, REQ_ERROR_STR)
        if attempt < max_retries:
            wait = 0.1 * (2**attempt)
            time.sleep(wait)
    abort(400, REQ_ERROR_STR)


def send_get_request(url: str):
    try:
        start_time = perf_counter()
        response = requests.get(url, timeout=5)
        duration = perf_counter() - start_time
        print(f"ORDER: GET request took {duration:.7f} seconds")
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
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
        abort(400, "Order is already paid for!")

    items_quantities = defaultdict(int)
    for item_id, qty in items:
        items_quantities[item_id] += qty

    if not items_quantities:
        cur.close()
        abort(400, "Order has no items!")

    cur.execute(
        "INSERT INTO transaction_log (txn_id, order_id, status, prepared_stock, prepared_payment, user_id, total_cost) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (txn_id, order_id, "started", json.dumps([]), False, user_id, total_cost),
    )
    conn.commit()

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

        prepared_stock.append([item_id, qty])
        cur.execute(
            "UPDATE transaction_log SET prepared_stock = %s WHERE txn_id = %s",
            (json.dumps(prepared_stock), txn_id),
        )
        conn.commit()

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
# ============================================================================

def checkout_saga_http(order_id: str):
    """Synchronous saga checkout via HTTP — fallback when not using Kafka gateway."""
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
        abort(400, "Order is already paid for!")

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
# ============================================================================

gateway_producer = None
internal_producer = None

# Pending price lookups for addItem via Kafka (SAGA mode)
_pending_price_lookups: dict[str, tuple[threading.Event, dict | None]] = {}
_pending_price_lookups_lock = threading.Lock()


def _build_kafka_producer(bootstrap_servers: str):
    import kafka
    return kafka.KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=5,
        batch_size=32_768,
    )


def publish_gateway_response(correlation_id: str, status_code: int, body: Any) -> None:
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
# ---------------------------------------------------------------------------

def handle_checkout_saga(conn, order_id, headers):
    """Initiates a saga-based checkout. Returns None (response sent async)."""
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")
    correlation_id = headers.get("X-Correlation-Id") or headers.get("x-correlation-id")

    cached = check_saga_idempotency(conn, idem_key)
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
    """Synchronously fetch item price from stock service via internal Kafka."""
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
            save_saga_idempotency(conn, saga["idempotency_key"], 200, "Checkout successful")
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


_SAGA_HANDLERS = {
    ":stock:subtract_batch": (_on_stock_response, SagaState.STOCK_REQUESTED),
    ":payment:pay": (_on_payment_response, SagaState.PAYMENT_REQUESTED),
    ":stock:rollback": (_on_rollback_response, SagaState.ROLLBACK_REQUESTED),
}


def handle_internal_response(payload):
    """Routes an internal.responses message to the correct saga handler."""
    correlation_id = payload.get("correlation_id", "")

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
# Kafka consumer loops (SAGA mode only)
# ---------------------------------------------------------------------------

def start_gateway_consumer():
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
# ---------------------------------------------------------------------------

with app.app_context():
    if TRANSACTION_MODE == "TPC":
        try:
            recovery_tpc()
        except Exception as e:
            app.logger.warning(f"RECOVERY: Error during recovery: {e}")
    elif TRANSACTION_MODE == "SAGA":
        gateway_producer = _build_kafka_producer(GATEWAY_KAFKA)
        internal_producer = _build_kafka_producer(INTERNAL_KAFKA)
        threading.Thread(target=start_gateway_consumer, daemon=True, name="gateway-consumer").start()
        threading.Thread(target=start_internal_consumer, daemon=True, name="internal-consumer").start()
        app.logger.info("SAGA mode: Kafka producers and consumers started")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
