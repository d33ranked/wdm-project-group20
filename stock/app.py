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

app = Flask("stock-service")
logger = logging.getLogger(__name__)

conn_pool = create_conn_pool("STOCK")
setup_flask_lifecycle(app, conn_pool, "STOCK")

# ---------------------------------------------------------------------------
# Flask Endpoints (Both Modes)
# ---------------------------------------------------------------------------

@app.post("/item/create/<price>")
def create_item(price: int):
    key = str(uuid.uuid4())
    with g.conn.cursor() as cur:
        cur.execute("INSERT INTO items (id, stock, price) VALUES (%s, %s, %s)", (key, 0, int(price)))
    return jsonify({"item_id": key})


@app.post("/batch_init/<n>/<starting_stock>/<item_price>")
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n, starting_stock, item_price = int(n), int(starting_stock), int(item_price)
    with g.conn.cursor() as cur:
        for i in range(n):
            cur.execute(
                "INSERT INTO items (id, stock, price) VALUES (%s, %s, %s) "
                "ON CONFLICT (id) DO UPDATE SET stock = EXCLUDED.stock, price = EXCLUDED.price",
                (str(i), starting_stock, item_price),
            )
    return jsonify({"msg": "Batch init for stock successful"})


@app.get("/find/<item_id>")
def find_item(item_id: str):
    with g.conn.cursor() as cur:
        cur.execute("SELECT stock, price FROM items WHERE id = %s", (item_id,))
        row = cur.fetchone()
    if row is None:
        abort(400, f"Item: {item_id} not found!")
    return jsonify({"stock": row[0], "price": row[1]})


@app.post("/add/<item_id>/<amount>")
def add_stock(item_id: str, amount: int):
    if int(amount) <= 0:
        abort(400, "Amount must be positive!")
    idem_key = request.headers.get("Idempotency-Key")
    cached = check_idempotency_http(g.conn, idem_key)
    if cached is not None:
        return Response(cached[1], status=cached[0])

    with g.conn.cursor() as cur:
        cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
        row = cur.fetchone()
        if row is None:
            abort(400, f"Item: {item_id} not found!")
        cur.execute("UPDATE items SET stock = stock + %s WHERE id = %s RETURNING stock", (int(amount), item_id))
        new_stock = cur.fetchone()[0]

    body = f"Item: {item_id} stock updated to: {new_stock}"
    save_idempotency_http(g.conn, idem_key, 200, body)
    return Response(body, status=200)


@app.post("/subtract/<item_id>/<amount>")
def remove_stock(item_id: str, amount: int):
    if int(amount) <= 0:
        abort(400, "Amount must be positive!")
    idem_key = request.headers.get("Idempotency-Key")
    cached = check_idempotency_http(g.conn, idem_key)
    if cached is not None:
        return Response(cached[1], status=cached[0])

    with g.conn.cursor() as cur:
        cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
        row = cur.fetchone()
        if row is None:
            abort(400, f"Item: {item_id} not found!")
        if row[0] - int(amount) < 0:
            abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
        cur.execute("UPDATE items SET stock = stock - %s WHERE id = %s RETURNING stock", (int(amount), item_id))
        new_stock = cur.fetchone()[0]

    body = f"Item: {item_id} stock updated to: {new_stock}"
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
            print(f"RECOVERY STOCK: {e}", flush=True)

    elif TRANSACTION_MODE == "SAGA":
        gateway_producer = build_producer(GATEWAY_KAFKA)
        internal_producer = build_producer(INTERNAL_KAFKA)

        threading.Thread(
            target=run_consumer_loop,
            args=(conn_pool, INTERNAL_KAFKA, "internal.stock",
                  "stock-service-internal", internal_producer,
                  "internal.responses", saga.route_kafka_message, "Stock"),
            daemon=True, name="internal-consumer",
        ).start()

        threading.Thread(
            target=run_consumer_loop,
            args=(conn_pool, GATEWAY_KAFKA, "gateway.stock",
                  "stock-service-gateway", gateway_producer,
                  "gateway.responses", saga.route_kafka_message, "Stock"),
            daemon=True, name="gateway-consumer",
        ).start()

        print("SAGA mode: Kafka consumers started", flush=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    setup_gunicorn_logging(app)
