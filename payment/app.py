import gevent.monkey
gevent.monkey.patch_all()

import os
import uuid
import logging
import threading

from flask import Flask, jsonify, abort, request, Response, g

from common.db import create_conn_pool, setup_flask_lifecycle, setup_gunicorn_logging
from common.idempotency import check_idempotency_http, save_idempotency_http
from common.kafka_helpers import build_producer, run_consumer_loop

TRANSACTION_MODE = os.environ.get("TRANSACTION_MODE", "TPC")
GATEWAY_KAFKA = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-external:9092")
INTERNAL_KAFKA = os.environ.get("INTERNAL_KAFKA_BOOTSTRAP_SERVERS", "kafka-internal:9092")

app = Flask("payment-service")
logger = logging.getLogger(__name__)

conn_pool = create_conn_pool("PAYMENT")
setup_flask_lifecycle(app, conn_pool, "PAYMENT")

# ---------------------------------------------------------------------------
# Flask Endpoints (Both Modes)
# ---------------------------------------------------------------------------

@app.post("/create_user")
def create_user():
    key = str(uuid.uuid4())
    with g.conn.cursor() as cur:
        cur.execute("INSERT INTO users (id, credit) VALUES (%s, %s)", (key, 0))
    return jsonify({"user_id": key})


@app.post("/batch_init/<n>/<starting_money>")
def batch_init_users(n: int, starting_money: int):
    n, starting_money = int(n), int(starting_money)
    with g.conn.cursor() as cur:
        for i in range(n):
            cur.execute(
                "INSERT INTO users (id, credit) VALUES (%s, %s) "
                "ON CONFLICT (id) DO UPDATE SET credit = EXCLUDED.credit",
                (str(i), starting_money),
            )
    return jsonify({"msg": "Batch init for users successful"})


@app.get("/find_user/<user_id>")
def find_user(user_id: str):
    with g.conn.cursor() as cur:
        cur.execute("SELECT credit FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
    if row is None:
        abort(400, f"User: {user_id} not found!")
    return jsonify({"user_id": user_id, "credit": row[0]})


@app.post("/add_funds/<user_id>/<amount>")
def add_credit(user_id: str, amount: int):
    if int(amount) <= 0:
        abort(400, "Amount must be positive!")
    idem_key = request.headers.get("Idempotency-Key")
    cached = check_idempotency_http(g.conn, idem_key)
    if cached is not None:
        return Response(cached[1], status=cached[0])

    with g.conn.cursor() as cur:
        cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
        row = cur.fetchone()
        if row is None:
            abort(400, f"User: {user_id} not found!")
        cur.execute("UPDATE users SET credit = credit + %s WHERE id = %s RETURNING credit", (int(amount), user_id))
        new_credit = cur.fetchone()[0]

    body = f"User: {user_id} credit updated to: {new_credit}"
    save_idempotency_http(g.conn, idem_key, 200, body)
    return Response(body, status=200)


@app.post("/pay/<user_id>/<amount>")
def remove_credit(user_id: str, amount: int):
    if int(amount) <= 0:
        abort(400, "Amount must be positive!")
    idem_key = request.headers.get("Idempotency-Key")
    cached = check_idempotency_http(g.conn, idem_key)
    if cached is not None:
        return Response(cached[1], status=cached[0])

    with g.conn.cursor() as cur:
        cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
        row = cur.fetchone()
        if row is None:
            abort(400, f"User: {user_id} not found!")
        if row[0] - int(amount) < 0:
            abort(400, f"User: {user_id} credit cannot get reduced below zero!")
        cur.execute("UPDATE users SET credit = credit - %s WHERE id = %s RETURNING credit", (int(amount), user_id))
        new_credit = cur.fetchone()[0]

    body = f"User: {user_id} credit updated to: {new_credit}"
    save_idempotency_http(g.conn, idem_key, 200, body)
    return Response(body, status=200)


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})

# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

import tpc
import saga

with app.app_context():
    if TRANSACTION_MODE == "TPC":
        tpc.init_routes(app)
        try:
            tpc.recovery(conn_pool)
        except Exception as e:
            print(f"RECOVERY PAYMENT: {e}", flush=True)

    elif TRANSACTION_MODE == "SAGA":
        gateway_producer = build_producer(GATEWAY_KAFKA)
        internal_producer = build_producer(INTERNAL_KAFKA)

        threading.Thread(
            target=run_consumer_loop,
            args=(conn_pool, INTERNAL_KAFKA, "internal.payment",
                  "payment-service-internal", internal_producer,
                  "internal.responses", saga.route_kafka_message, "Payment"),
            daemon=True, name="internal-consumer",
        ).start()

        threading.Thread(
            target=run_consumer_loop,
            args=(conn_pool, GATEWAY_KAFKA, "gateway.payment",
                  "payment-service-gateway", gateway_producer,
                  "gateway.responses", saga.route_kafka_message, "Payment"),
            daemon=True, name="gateway-consumer",
        ).start()

        print("SAGA mode: Kafka consumers started", flush=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    setup_gunicorn_logging(app)
