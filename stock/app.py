"""
Stock Service
=============
Manages item inventory (stock levels and prices).

Supports two transaction modes controlled by the TRANSACTION_MODE env var:

  TPC  — Flask HTTP endpoints for CRUD + 2PC prepare/commit/abort.
         The order service (coordinator) calls these endpoints directly
         via Nginx during checkout.

  SAGA — Same Flask endpoints for direct CRUD, plus Kafka consumers that
         handle saga commands (subtract_batch, add_batch) from the order
         service orchestrator. Batch operations are atomic: all items
         succeed or none do.
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
import logging
import threading

import psycopg2

from flask import Flask, jsonify, abort, request, Response, g

from common.db import create_conn_pool, setup_flask_lifecycle, setup_gunicorn_logging
from common.idempotency import (
    check_idempotency_http, save_idempotency_http,
    check_idempotency_kafka, save_idempotency_kafka,
)
from common.kafka_helpers import build_producer, publish_response, run_consumer_loop

TRANSACTION_MODE = os.environ.get("TRANSACTION_MODE", "TPC")

GATEWAY_KAFKA = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-external:9092")
INTERNAL_KAFKA = os.environ.get("INTERNAL_KAFKA_BOOTSTRAP_SERVERS", "kafka-internal:9092")

GATEWAY_STOCK_TOPIC = "gateway.stock"
GATEWAY_RESPONSE_TOPIC = "gateway.responses"
INTERNAL_STOCK_TOPIC = "internal.stock"
INTERNAL_RESPONSE_TOPIC = "internal.responses"

app = Flask("stock-service")
logger = logging.getLogger(__name__)

conn_pool = create_conn_pool("STOCK")
setup_flask_lifecycle(app, conn_pool, "STOCK")

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_item_from_db(item_id: str):
    cur = g.conn.cursor()
    cur.execute("SELECT stock, price FROM items WHERE id = %s", (item_id,))
    row = cur.fetchone()
    cur.close()
    if row is None:
        abort(400, f"Item: {item_id} not found!")
    return {"stock": row[0], "price": row[1]}

# ---------------------------------------------------------------------------
# Flask HTTP endpoints
# ---------------------------------------------------------------------------

@app.post("/item/create/<price>")
def create_item(price: int):
    key = str(uuid.uuid4())
    cur = g.conn.cursor()
    cur.execute(
        "INSERT INTO items (id, stock, price) VALUES (%s, %s, %s)", (key, 0, int(price))
    )
    cur.close()
    return jsonify({"item_id": key})


@app.post("/batch_init/<n>/<starting_stock>/<item_price>")
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n, starting_stock, item_price = int(n), int(starting_stock), int(item_price)
    cur = g.conn.cursor()
    for i in range(n):
        cur.execute(
            "INSERT INTO items (id, stock, price) VALUES (%s, %s, %s) "
            "ON CONFLICT (id) DO UPDATE SET stock = EXCLUDED.stock, price = EXCLUDED.price",
            (str(i), starting_stock, item_price),
        )
    cur.close()
    return jsonify({"msg": "Batch init for stock successful"})


@app.get("/find/<item_id>")
def find_item(item_id: str):
    item = get_item_from_db(item_id)
    return jsonify({"stock": item["stock"], "price": item["price"]})


@app.post("/add/<item_id>/<amount>")
def add_stock(item_id: str, amount: int):
    idem_key = request.headers.get("Idempotency-Key")
    cached = check_idempotency_http(g.conn, idem_key)
    if cached is not None:
        return Response(cached[1], status=cached[0])

    cur = g.conn.cursor()
    cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"Item: {item_id} not found!")
    cur.execute(
        "UPDATE items SET stock = stock + %s WHERE id = %s RETURNING stock",
        (int(amount), item_id),
    )
    new_stock = cur.fetchone()[0]
    cur.close()

    body = f"Item: {item_id} stock updated to: {new_stock}"
    save_idempotency_http(g.conn, idem_key, 200, body)
    return Response(body, status=200)


@app.post("/subtract/<item_id>/<amount>")
def remove_stock(item_id: str, amount: int):
    idem_key = request.headers.get("Idempotency-Key")
    cached = check_idempotency_http(g.conn, idem_key)
    if cached is not None:
        return Response(cached[1], status=cached[0])

    cur = g.conn.cursor()
    cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"Item: {item_id} not found!")
    if row[0] - int(amount) < 0:
        cur.close()
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    cur.execute(
        "UPDATE items SET stock = stock - %s WHERE id = %s RETURNING stock",
        (int(amount), item_id),
    )
    new_stock = cur.fetchone()[0]
    cur.close()

    body = f"Item: {item_id} stock updated to: {new_stock}"
    save_idempotency_http(g.conn, idem_key, 200, body)
    return Response(body, status=200)


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})

# ---------------------------------------------------------------------------
# 2PC endpoints (TPC mode)
#
# Pessimistic reservation pattern: PREPARE immediately deducts stock and
# records the reservation. This makes the resource unavailable to other
# transactions, which is the desired isolation behaviour.
#
# COMMIT simply deletes the reservation record — the deduction is already
# in place, so commit is just cleanup. This makes commit inherently
# idempotent: committing a non-existent record is a no-op DELETE.
#
# ABORT reads the reservation, restores the stock, then deletes the record.
# Also idempotent: aborting a non-existent record changes nothing.
# ---------------------------------------------------------------------------

@app.post("/prepare/<txn_id>/<item_id>/<quantity>")
def prepare_transaction(txn_id: str, item_id: str, quantity: int):
    quantity = int(quantity)
    cur = g.conn.cursor()

    cur.execute(
        "SELECT 1 FROM prepared_transactions WHERE txn_id = %s AND item_id = %s",
        (txn_id, item_id),
    )
    if cur.fetchone() is not None:
        cur.close()
        return Response("Transaction already prepared", status=200)

    cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"Item: {item_id} not found!")
    if row[0] < quantity:
        cur.close()
        abort(400, f"Item: {item_id} has insufficient stock!")

    cur.execute(
        "UPDATE items SET stock = stock - %s WHERE id = %s", (quantity, item_id)
    )
    cur.execute(
        "INSERT INTO prepared_transactions (txn_id, item_id, quantity) VALUES (%s, %s, %s)",
        (txn_id, item_id, quantity),
    )
    cur.close()
    return Response("Transaction prepared", status=200)


@app.post("/commit/<txn_id>")
def commit_transaction(txn_id: str):
    cur = g.conn.cursor()
    cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
    cur.close()
    return Response("Transaction committed", status=200)


@app.post("/abort/<txn_id>")
def abort_transaction(txn_id: str):
    cur = g.conn.cursor()
    cur.execute(
        "SELECT item_id, quantity FROM prepared_transactions WHERE txn_id = %s",
        (txn_id,),
    )
    rows = cur.fetchall()
    for item_id, quantity in rows:
        cur.execute(
            "UPDATE items SET stock = stock + %s WHERE id = %s", (quantity, item_id)
        )
    cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
    cur.close()
    return Response("Transaction aborted", status=200)


def recovery_tpc():
    """Auto-abort prepared transactions older than 5 minutes on startup.

    Safety net for the case where the coordinator (order service) crashed
    and never sent commit/abort. Without this, stock would remain reserved
    indefinitely.
    """
    conn = conn_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT txn_id, item_id, quantity FROM prepared_transactions "
            "WHERE created_at < NOW() - INTERVAL '5 minutes'"
        )
        rows = cur.fetchall()
        if not rows:
            cur.close()
            app.logger.info("RECOVERY: No stale prepared transactions found")
            return
        for txn_id, item_id, quantity in rows:
            app.logger.warning(
                f"RECOVERY: Aborting stale prepared transaction txn={txn_id}, item={item_id}"
            )
            cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
            cur.execute(
                "UPDATE items SET stock = stock + %s WHERE id = %s", (quantity, item_id)
            )
            cur.execute(
                "DELETE FROM prepared_transactions WHERE txn_id = %s AND item_id = %s",
                (txn_id, item_id),
            )
            conn.commit()
        cur.close()
    finally:
        conn_pool.putconn(conn)

# ---------------------------------------------------------------------------
# Batch operations (SAGA mode — atomic multi-item subtract/add)
#
# These are used by saga commands (subtract_batch / add_batch). All items
# are locked in sorted order (ORDER BY id) to prevent deadlocks when
# concurrent transactions touch overlapping item sets.
# ---------------------------------------------------------------------------

def db_subtract_stock_batch(conn, items):
    """Atomically subtract stock for multiple items. Returns {item_id: new_stock}."""
    item_ids = [item_id for item_id, _ in items]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, stock FROM items WHERE id = ANY(%s) ORDER BY id FOR UPDATE",
            (item_ids,),
        )
        rows = {row[0]: row[1] for row in cur.fetchall()}
        for item_id, amount in items:
            if item_id not in rows:
                raise ValueError(f"Item {item_id} not found")
            if rows[item_id] - amount < 0:
                raise ValueError(f"Item {item_id} has insufficient stock")
        results = {}
        for item_id, amount in items:
            cur.execute(
                "UPDATE items SET stock = stock - %s WHERE id = %s RETURNING stock",
                (amount, item_id),
            )
            results[item_id] = cur.fetchone()[0]
    return results


def db_add_stock_batch(conn, items):
    """Atomically add stock for multiple items. Returns {item_id: new_stock}."""
    item_ids = [item_id for item_id, _ in items]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, stock FROM items WHERE id = ANY(%s) ORDER BY id FOR UPDATE",
            (item_ids,),
        )
        rows = {row[0]: row[1] for row in cur.fetchall()}
        for item_id, _ in items:
            if item_id not in rows:
                raise ValueError(f"Item {item_id} not found")
        results = {}
        for item_id, amount in items:
            cur.execute(
                "UPDATE items SET stock = stock + %s WHERE id = %s RETURNING stock",
                (amount, item_id),
            )
            results[item_id] = cur.fetchone()[0]
    return results

# ---------------------------------------------------------------------------
# Kafka message routing (SAGA mode)
#
# Mirrors the Flask routing logic but for Kafka messages. This duplication
# is inherent to the architecture: Kafka messages arrive as JSON envelopes
# with method/path fields that must be manually dispatched, since Flask's
# URL routing only applies to HTTP requests.
# ---------------------------------------------------------------------------

def route_kafka_message(payload, conn):
    """Route a Kafka message to the correct handler, return (status_code, body)."""
    method = payload.get("method", "GET").upper()
    path = payload.get("path", "/")
    body = payload.get("body") or {}
    headers = payload.get("headers") or {}

    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    if method == "POST" and len(segments) >= 2 and segments[0] == "item" and segments[1] == "create":
        price = int(segments[2]) if len(segments) > 2 else 0
        item_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute("INSERT INTO items (id, stock, price) VALUES (%s, %s, %s)", (item_id, 0, price))
        conn.commit()
        return 201, {"item_id": item_id}

    if method == "POST" and len(segments) >= 4 and segments[0] == "batch_init":
        n, starting_stock, item_price = int(segments[1]), int(segments[2]), int(segments[3])
        with conn.cursor() as cur:
            for i in range(n):
                cur.execute(
                    "INSERT INTO items (id, stock, price) VALUES (%s, %s, %s) "
                    "ON CONFLICT (id) DO UPDATE SET stock = EXCLUDED.stock, price = EXCLUDED.price",
                    (str(i), starting_stock, item_price),
                )
        conn.commit()
        return 200, {"msg": "Batch init for stock successful"}

    if method == "GET" and len(segments) >= 2 and segments[0] == "find":
        item_id = segments[1]
        with conn.cursor() as cur:
            cur.execute("SELECT stock, price FROM items WHERE id = %s", (item_id,))
            row = cur.fetchone()
        if row is None:
            return 400, {"error": f"Item {item_id} not found"}
        return 200, {"stock": row[0], "price": row[1]}

    if method == "POST" and len(segments) >= 3 and segments[0] == "add":
        item_id, amount = segments[1], int(segments[2])
        cached = check_idempotency_kafka(conn, idem_key)
        if cached:
            return cached
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
                if cur.fetchone() is None:
                    return 400, {"error": f"Item {item_id} not found"}
                cur.execute("UPDATE items SET stock = stock + %s WHERE id = %s RETURNING stock", (amount, item_id))
                new_stock = cur.fetchone()[0]
            resp_body = f"Item: {item_id} stock updated to: {new_stock}"
            save_idempotency_kafka(conn, idem_key, 200, resp_body)
            conn.commit()
            return 200, resp_body
        except Exception:
            conn.rollback()
            raise

    if method == "POST" and len(segments) >= 3 and segments[0] == "subtract":
        item_id, amount = segments[1], int(segments[2])
        cached = check_idempotency_kafka(conn, idem_key)
        if cached:
            return cached
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
                row = cur.fetchone()
                if row is None:
                    return 400, {"error": f"Item {item_id} not found"}
                if row[0] - amount < 0:
                    return 400, {"error": f"Item {item_id} has insufficient stock"}
                cur.execute("UPDATE items SET stock = stock - %s WHERE id = %s RETURNING stock", (amount, item_id))
                new_stock = cur.fetchone()[0]
            resp_body = f"Item: {item_id} stock updated to: {new_stock}"
            save_idempotency_kafka(conn, idem_key, 200, resp_body)
            conn.commit()
            return 200, resp_body
        except Exception:
            conn.rollback()
            raise

    if method == "POST" and segments and segments[0] == "subtract_batch":
        cached = check_idempotency_kafka(conn, idem_key)
        if cached:
            return cached
        try:
            raw_items = body["items"]
            items = [(entry["item_id"], int(entry["amount"])) for entry in raw_items]
        except (KeyError, TypeError, ValueError):
            return 400, {"error": 'Expected body: {"items": [{"item_id": str, "amount": int}, ...]}'}
        try:
            results = db_subtract_stock_batch(conn, items)
            response_body = {"updated_stock": results}
            save_idempotency_kafka(conn, idem_key, 200, json.dumps(response_body))
            conn.commit()
            return 200, response_body
        except ValueError as exc:
            conn.rollback()
            return 400, {"error": str(exc)}

    if method == "POST" and segments and segments[0] == "add_batch":
        cached = check_idempotency_kafka(conn, idem_key)
        if cached:
            return cached
        try:
            raw_items = body["items"]
            items = [(entry["item_id"], int(entry["amount"])) for entry in raw_items]
        except (KeyError, TypeError, ValueError):
            return 400, {"error": 'Expected body: {"items": [{"item_id": str, "amount": int}, ...]}'}
        try:
            results = db_add_stock_batch(conn, items)
            response_body = {"updated_stock": results}
            save_idempotency_kafka(conn, idem_key, 200, json.dumps(response_body))
            conn.commit()
            return 200, response_body
        except ValueError as exc:
            conn.rollback()
            return 400, {"error": str(exc)}

    return 404, {"error": f"No handler for {method} {path}"}

# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

with app.app_context():
    if TRANSACTION_MODE == "TPC":
        try:
            recovery_tpc()
        except Exception as e:
            app.logger.warning(f"RECOVERY STOCK: Error during recovery: {e}")
    elif TRANSACTION_MODE == "SAGA":
        gateway_producer = build_producer(GATEWAY_KAFKA)
        internal_producer = build_producer(INTERNAL_KAFKA)

        threading.Thread(
            target=run_consumer_loop,
            args=(conn_pool, INTERNAL_KAFKA, INTERNAL_STOCK_TOPIC,
                  "stock-service-internal", internal_producer,
                  INTERNAL_RESPONSE_TOPIC, route_kafka_message, "Stock"),
            daemon=True, name="internal-consumer",
        ).start()

        threading.Thread(
            target=run_consumer_loop,
            args=(conn_pool, GATEWAY_KAFKA, GATEWAY_STOCK_TOPIC,
                  "stock-service-gateway", gateway_producer,
                  GATEWAY_RESPONSE_TOPIC, route_kafka_message, "Stock"),
            daemon=True, name="gateway-consumer",
        ).start()

        app.logger.info("SAGA mode: Kafka producers and consumers started")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    setup_gunicorn_logging(app)
