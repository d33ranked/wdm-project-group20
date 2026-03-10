"""
Payment Service
================
Manages user accounts and credit balances.

Supports two transaction modes controlled by the TRANSACTION_MODE env var:

  TPC  — Flask HTTP endpoints for CRUD + 2PC prepare/commit/abort.
         The order service (coordinator) calls these endpoints directly
         via Nginx during checkout.

  SAGA — Same Flask endpoints for direct CRUD, plus Kafka consumers that
         handle saga commands (pay) from the order service orchestrator.
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

GATEWAY_PAYMENT_TOPIC = "gateway.payment"
GATEWAY_RESPONSE_TOPIC = "gateway.responses"
INTERNAL_PAYMENT_TOPIC = "internal.payment"
INTERNAL_RESPONSE_TOPIC = "internal.responses"

app = Flask("payment-service")
logger = logging.getLogger(__name__)

conn_pool = create_conn_pool("PAYMENT")
setup_flask_lifecycle(app, conn_pool, "PAYMENT")

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_user_from_db(user_id: str):
    cur = g.conn.cursor()
    cur.execute("SELECT credit FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    if row is None:
        abort(400, f"User: {user_id} not found!")
    return {"credit": row[0]}

# ---------------------------------------------------------------------------
# Flask HTTP endpoints
# ---------------------------------------------------------------------------

@app.post("/create_user")
def create_user():
    key = str(uuid.uuid4())
    cur = g.conn.cursor()
    cur.execute("INSERT INTO users (id, credit) VALUES (%s, %s)", (key, 0))
    cur.close()
    return jsonify({"user_id": key})


@app.post("/batch_init/<n>/<starting_money>")
def batch_init_users(n: int, starting_money: int):
    n, starting_money = int(n), int(starting_money)
    cur = g.conn.cursor()
    for i in range(n):
        cur.execute(
            "INSERT INTO users (id, credit) VALUES (%s, %s) "
            "ON CONFLICT (id) DO UPDATE SET credit = EXCLUDED.credit",
            (str(i), starting_money),
        )
    cur.close()
    return jsonify({"msg": "Batch init for users successful"})


@app.get("/find_user/<user_id>")
def find_user(user_id: str):
    user = get_user_from_db(user_id)
    return jsonify({"user_id": user_id, "credit": user["credit"]})


@app.post("/add_funds/<user_id>/<amount>")
def add_credit(user_id: str, amount: int):
    idem_key = request.headers.get("Idempotency-Key")
    cached = check_idempotency_http(g.conn, idem_key)
    if cached is not None:
        return Response(cached[1], status=cached[0])

    cur = g.conn.cursor()
    cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"User: {user_id} not found!")
    cur.execute(
        "UPDATE users SET credit = credit + %s WHERE id = %s RETURNING credit",
        (int(amount), user_id),
    )
    new_credit = cur.fetchone()[0]
    cur.close()

    body = f"User: {user_id} credit updated to: {new_credit}"
    save_idempotency_http(g.conn, idem_key, 200, body)
    return Response(body, status=200)


@app.post("/pay/<user_id>/<amount>")
def remove_credit(user_id: str, amount: int):
    idem_key = request.headers.get("Idempotency-Key")
    cached = check_idempotency_http(g.conn, idem_key)
    if cached is not None:
        return Response(cached[1], status=cached[0])

    cur = g.conn.cursor()
    cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"User: {user_id} not found!")
    if row[0] - int(amount) < 0:
        cur.close()
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    cur.execute(
        "UPDATE users SET credit = credit - %s WHERE id = %s RETURNING credit",
        (int(amount), user_id),
    )
    new_credit = cur.fetchone()[0]
    cur.close()

    body = f"User: {user_id} credit updated to: {new_credit}"
    save_idempotency_http(g.conn, idem_key, 200, body)
    return Response(body, status=200)


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})

# ---------------------------------------------------------------------------
# 2PC endpoints (TPC mode)
#
# Same pessimistic reservation pattern as stock: PREPARE immediately deducts
# credit and records the reservation. COMMIT deletes the record (deduction
# is already permanent). ABORT restores credit and deletes the record.
# All three operations are idempotent by design.
# ---------------------------------------------------------------------------

@app.post("/prepare/<txn_id>/<user_id>/<amount>")
def prepare_transaction(txn_id: str, user_id: str, amount: int):
    amount = int(amount)
    cur = g.conn.cursor()

    cur.execute("SELECT 1 FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
    if cur.fetchone() is not None:
        cur.close()
        return Response("Transaction already prepared", status=200)

    cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"User: {user_id} not found!")
    if row[0] < amount:
        cur.close()
        abort(400, f"User: {user_id} has insufficient credit!")

    cur.execute(
        "UPDATE users SET credit = credit - %s WHERE id = %s", (amount, user_id)
    )
    cur.execute(
        "INSERT INTO prepared_transactions (txn_id, user_id, amount) VALUES (%s, %s, %s)",
        (txn_id, user_id, amount),
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
        "SELECT user_id, amount FROM prepared_transactions WHERE txn_id = %s", (txn_id,)
    )
    row = cur.fetchone()
    if row is not None:
        user_id, amount = row
        cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
        if cur.fetchone() is not None:
            cur.execute(
                "UPDATE users SET credit = credit + %s WHERE id = %s", (amount, user_id)
            )
        cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
    cur.close()
    return Response("Transaction aborted", status=200)


def recovery_tpc():
    """Auto-abort prepared transactions older than 5 minutes on startup.

    Safety net for the case where the coordinator (order service) crashed
    and never sent commit/abort. Without this, user credit would remain
    reserved indefinitely.
    """
    conn = conn_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT txn_id, user_id, amount FROM prepared_transactions "
            "WHERE created_at < NOW() - INTERVAL '5 minutes'"
        )
        rows = cur.fetchall()
        if not rows:
            cur.close()
            app.logger.info("RECOVERY: No stale prepared transactions found")
            return
        for txn_id, user_id, amount in rows:
            app.logger.warning(
                f"RECOVERY: Aborting stale prepared transaction txn={txn_id}, user={user_id}"
            )
            cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
            if cur.fetchone() is not None:
                cur.execute(
                    "UPDATE users SET credit = credit + %s WHERE id = %s",
                    (amount, user_id),
                )
            cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
            conn.commit()
        cur.close()
    finally:
        conn_pool.putconn(conn)

# ---------------------------------------------------------------------------
# Kafka message routing (SAGA mode)
# ---------------------------------------------------------------------------

def route_kafka_message(payload, conn):
    """Route a Kafka message to the correct handler, return (status_code, body)."""
    method = payload.get("method", "GET").upper()
    path = payload.get("path", "/")
    headers = payload.get("headers") or {}

    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    if method == "POST" and segments and segments[0] == "create_user":
        user_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (id, credit) VALUES (%s, %s)", (user_id, 0))
        conn.commit()
        return 201, {"user_id": user_id}

    if method == "POST" and len(segments) >= 3 and segments[0] == "batch_init":
        n, starting_money = int(segments[1]), int(segments[2])
        with conn.cursor() as cur:
            for i in range(n):
                cur.execute(
                    "INSERT INTO users (id, credit) VALUES (%s, %s) "
                    "ON CONFLICT (id) DO UPDATE SET credit = EXCLUDED.credit",
                    (str(i), starting_money),
                )
        conn.commit()
        return 200, {"msg": "Batch init for users successful"}

    if method == "GET" and len(segments) >= 2 and segments[0] == "find_user":
        user_id = segments[1]
        with conn.cursor() as cur:
            cur.execute("SELECT credit FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
        if row is None:
            return 400, {"error": f"User {user_id} not found"}
        return 200, {"user_id": user_id, "credit": row[0]}

    if method == "POST" and len(segments) >= 3 and segments[0] == "add_funds":
        user_id, amount = segments[1], int(segments[2])
        cached = check_idempotency_kafka(conn, idem_key)
        if cached:
            return cached
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
                if cur.fetchone() is None:
                    return 400, {"error": f"User {user_id} not found"}
                cur.execute("UPDATE users SET credit = credit + %s WHERE id = %s RETURNING credit", (amount, user_id))
                new_credit = cur.fetchone()[0]
            resp_body = f"User: {user_id} credit updated to: {new_credit}"
            save_idempotency_kafka(conn, idem_key, 200, resp_body)
            conn.commit()
            return 200, resp_body
        except Exception:
            conn.rollback()
            raise

    if method == "POST" and len(segments) >= 3 and segments[0] == "pay":
        user_id, amount = segments[1], int(segments[2])
        cached = check_idempotency_kafka(conn, idem_key)
        if cached:
            return cached
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
                row = cur.fetchone()
                if row is None:
                    return 400, {"error": f"User {user_id} not found"}
                if row[0] - amount < 0:
                    return 400, {"error": f"User {user_id} has insufficient funds"}
                cur.execute("UPDATE users SET credit = credit - %s WHERE id = %s RETURNING credit", (amount, user_id))
                new_credit = cur.fetchone()[0]
            resp_body = f"User: {user_id} credit updated to: {new_credit}"
            save_idempotency_kafka(conn, idem_key, 200, resp_body)
            conn.commit()
            return 200, resp_body
        except Exception:
            conn.rollback()
            raise

    return 404, {"error": f"No handler for {method} {path}"}

# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

with app.app_context():
    if TRANSACTION_MODE == "TPC":
        try:
            recovery_tpc()
        except Exception as e:
            app.logger.warning(f"RECOVERY PAYMENT: Error during recovery: {e}")
    elif TRANSACTION_MODE == "SAGA":
        gateway_producer = build_producer(GATEWAY_KAFKA)
        internal_producer = build_producer(INTERNAL_KAFKA)

        threading.Thread(
            target=run_consumer_loop,
            args=(conn_pool, INTERNAL_KAFKA, INTERNAL_PAYMENT_TOPIC,
                  "payment-service-internal", internal_producer,
                  INTERNAL_RESPONSE_TOPIC, route_kafka_message, "Payment"),
            daemon=True, name="internal-consumer",
        ).start()

        threading.Thread(
            target=run_consumer_loop,
            args=(conn_pool, GATEWAY_KAFKA, GATEWAY_PAYMENT_TOPIC,
                  "payment-service-gateway", gateway_producer,
                  GATEWAY_RESPONSE_TOPIC, route_kafka_message, "Payment"),
            daemon=True, name="gateway-consumer",
        ).start()

        app.logger.info("SAGA mode: Kafka producers and consumers started")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    setup_gunicorn_logging(app)
