import gevent.monkey

gevent.monkey.patch_all()

import os
import tpc
import json
import time
import saga
import logging
import threading
from db import get_order
from flask import Flask, jsonify, abort, request, Response, g
from common.idempotency import check_idempotency, save_idempotency
from common.redis_db import (
    create_redis_pool,
    setup_flask_lifecycle,
    setup_gunicorn_logging,
    warmup_pool,
)
from common.streams import create_bus_pool
from operations import create_order, batch_init_orders, add_item_to_order

TRANSACTION_MODE = os.environ.get("TRANSACTION_MODE", "SAGA")
STOCK_SERVICE_URL = os.environ.get("STOCK_SERVICE_URL", "http://stock-service:5000")

app = Flask("order-service")
logger = logging.getLogger(__name__)

redis_pool = create_redis_pool("ORDER")
setup_flask_lifecycle(app, redis_pool, "ORDER")
bus_pool = create_bus_pool()


@app.after_request
def _log_request_time(response):
    duration_ms = (time.perf_counter() - g.start_time) * 1000
    print(
        f"[ORDER] {request.method} {request.path} {response.status_code} {duration_ms:.0f}ms",
        flush=True,
    )
    return response


@app.post("/create/<user_id>")
def create_order_route(user_id: str):
    order_id = create_order(g.redis, user_id)
    return jsonify({"order_id": order_id}), 201


@app.post("/batch_init/<n>/<n_items>/<n_users>/<item_price>")
def batch_init(n: int, n_items: int, n_users: int, item_price: int):
    batch_init_orders(g.redis, int(n), int(n_items), int(n_users), int(item_price))
    return jsonify({"msg": "Batch init for orders successful"})


@app.get("/find/<order_id>")
def find_order(order_id: str):
    try:
        order = get_order(g.redis, order_id)
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
    quantity = int(quantity)
    if quantity <= 0:
        abort(400, "Quantity must be positive!")

    idem_key = request.headers.get("Idempotency-Key")
    cached = check_idempotency(g.redis, idem_key)
    if cached is not None:
        return Response(cached[1], status=cached[0])

    stock_reply = tpc.send_get_request(f"{STOCK_SERVICE_URL}/find/{item_id}")
    if stock_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")
    item_price = stock_reply.json()["price"]

    total_cost, error = add_item_to_order(g.redis, order_id, item_id, quantity, item_price)
    if error:
        abort(400, error)

    body = f"Item: {item_id} added to: {order_id} price updated to: {total_cost}"
    save_idempotency(g.redis, idem_key, 200, body)
    return Response(body, status=200)


@app.post("/checkout/<order_id>")
def checkout(order_id: str):
    if TRANSACTION_MODE == "TPC":
        return tpc.checkout_tpc(order_id)
    else:
        abort(400, "Checkout failed in SAGA mode — use the SAGA gateway endpoint.")


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


with app.app_context():
    if TRANSACTION_MODE == "TPC":
        tpc.init_bus(bus_pool, redis_pool)  # must come before recovery
        try:
            tpc.recovery_tpc()
        except Exception as e:
            print(f"RECOVERY ORDER TPC: {e}", flush=True)

    elif TRANSACTION_MODE == "SAGA":
        saga.init(redis_pool, bus_pool)
        warmup_pool(redis_pool)
        warmup_pool(bus_pool)

        try:
            saga.recovery_saga()
        except Exception as e:
            print(f"SAGA RECOVERY ORDER: {e}", flush=True)

        threading.Thread(
            target=saga.start_gateway_consumer,
            daemon=True,
            name="gateway-consumer",
        ).start()

        threading.Thread(
            target=saga.start_internal_consumer,
            daemon=True,
            name="internal-consumer",
        ).start()

        print("SAGA mode: Redis Streams consumers started", flush=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    setup_gunicorn_logging(app)
