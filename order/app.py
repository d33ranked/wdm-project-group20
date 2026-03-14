import gevent.monkey
gevent.monkey.patch_all()

import os
import json
import time
import uuid
import random
import logging
import threading

from flask import Flask, jsonify, abort, request, Response, g

from common.db import create_conn_pool, setup_flask_lifecycle, setup_gunicorn_logging
from common.idempotency import check_idempotency_http, save_idempotency_http

import tpc
import saga
from db import get_order

TRANSACTION_MODE = os.environ.get("TRANSACTION_MODE", "TPC")
GATEWAY_KAFKA = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-external:9092")
INTERNAL_KAFKA = os.environ.get("INTERNAL_KAFKA_BOOTSTRAP_SERVERS", "kafka-internal:9092")
STOCK_SERVICE_URL = os.environ.get("STOCK_SERVICE_URL", "http://stock-service:5000")

app = Flask("order-service")
logger = logging.getLogger(__name__)

conn_pool = create_conn_pool("ORDER")
setup_flask_lifecycle(app, conn_pool, "ORDER")


@app.before_request
def _start_timer():
    g.start_time = time.time()


@app.after_request
def _log_request_time(response):
    duration_ms = (time.time() - g.start_time) * 1000
    print(f"[ORDER] {request.method} {request.path} {response.status_code} {duration_ms:.0f}ms", flush=True)
    return response


# ---------------------------------------------------------------------------
# Flask Endpoints
# ---------------------------------------------------------------------------

@app.post("/create/<user_id>")
def create_order(user_id: str):
    key = str(uuid.uuid4())
    with g.conn.cursor() as cur:
        cur.execute(
            "INSERT INTO orders (id, paid, items, user_id, total_cost) VALUES (%s, %s, %s, %s, %s)",
            (key, False, json.dumps([]), user_id, 0),
        )
    return jsonify({"order_id": key})


@app.post("/batch_init/<n>/<n_items>/<n_users>/<item_price>")
def batch_init(n: int, n_items: int, n_users: int, item_price: int):
    n, n_items, n_users, item_price = int(n), int(n_items), int(n_users), int(item_price)
    with g.conn.cursor() as cur:
        for i in range(n):
            uid = str(random.randint(0, n_users - 1))
            i1, i2 = str(random.randint(0, n_items - 1)), str(random.randint(0, n_items - 1))
            cur.execute(
                "INSERT INTO orders (id, paid, items, user_id, total_cost) VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (id) DO UPDATE SET paid = EXCLUDED.paid, items = EXCLUDED.items, "
                "user_id = EXCLUDED.user_id, total_cost = EXCLUDED.total_cost",
                (str(i), False, json.dumps([[i1, 1], [i2, 1]]), uid, 2 * item_price),
            )
    return jsonify({"msg": "Batch init for orders successful"})


@app.get("/find/<order_id>")
def find_order(order_id: str):
    try:
        order = get_order(g.conn, order_id)
    except ValueError as exc:
        abort(400, str(exc))
    return jsonify({
        "order_id": order_id, "paid": order["paid"], "items": order["items"],
        "user_id": order["user_id"], "total_cost": order["total_cost"],
    })


@app.post("/addItem/<order_id>/<item_id>/<quantity>")
def add_item(order_id: str, item_id: str, quantity: int):
    quantity = int(quantity)
    if quantity <= 0:
        abort(400, "Quantity must be positive!")
    idem_key = request.headers.get("Idempotency-Key")
    cached = check_idempotency_http(g.conn, idem_key)
    if cached is not None:
        return Response(cached[1], status=cached[0])

    stock_reply = tpc.send_get_request(f"{STOCK_SERVICE_URL}/find/{item_id}")
    if stock_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")
    item_price = stock_reply.json()["price"]

    with g.conn.cursor() as cur:
        cur.execute("SELECT items, total_cost FROM orders WHERE id = %s FOR UPDATE", (order_id,))
        row = cur.fetchone()
        if row is None:
            abort(400, f"Order: {order_id} not found!")
        items_list, total_cost = row
        merged = False
        for entry in items_list:
            if entry[0] == item_id:
                entry[1] += quantity
                merged = True
                break
        if not merged:
            items_list.append([item_id, quantity])
        total_cost += quantity * item_price
        cur.execute("UPDATE orders SET items = %s, total_cost = %s WHERE id = %s",
                    (json.dumps(items_list), total_cost, order_id))
    body = f"Item: {item_id} added to: {order_id} price updated to: {total_cost}"
    save_idempotency_http(g.conn, idem_key, 200, body)
    return Response(body, status=200)


@app.post("/checkout/<order_id>")
def checkout(order_id: str):
    if TRANSACTION_MODE == "TPC":
        return tpc.checkout_tpc(order_id)
    else:
        abort(400, "Something went wrong! The checkout failed in SAGA Mode.")


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})

# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

with app.app_context():
    if TRANSACTION_MODE == "TPC":
        try:
            tpc.recovery_tpc(conn_pool)
        except Exception as e:
            print(f"RECOVERY ORDER: {e}", flush=True)

    elif TRANSACTION_MODE == "SAGA":
        # Single Worker Required — Saga State Machine Needs Consistent In-Memory State
        saga.init(conn_pool, GATEWAY_KAFKA, INTERNAL_KAFKA)

        try:
            saga.recovery_saga(conn_pool)
        except Exception as e:
            print(f"SAGA RECOVERY ORDER: {e}", flush=True)

        threading.Thread(
            target=saga.start_gateway_consumer,
            args=(GATEWAY_KAFKA,),
            daemon=True, name="gateway-consumer",
        ).start()

        threading.Thread(
            target=saga.start_internal_consumer,
            args=(INTERNAL_KAFKA,),
            daemon=True, name="internal-consumer",
        ).start()

        print("SAGA mode: Kafka consumers started", flush=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    setup_gunicorn_logging(app)
