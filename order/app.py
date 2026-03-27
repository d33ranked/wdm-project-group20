# green event patching for concurrency
import gevent.monkey

gevent.monkey.patch_all()

import os
import tpc
import json
import time
import uuid
import saga
import random
import logging
import threading
from db import get_order
from flask import Flask, jsonify, abort, request, Response, g
from common.idempotency import check_idempotency_http, save_idempotency_http
from common.redis_db import (
    create_redis_pool,
    setup_flask_lifecycle,
    setup_gunicorn_logging,
)
from common.streams import create_bus_pool

# extract environment variables
TRANSACTION_MODE = os.environ.get("TRANSACTION_MODE", "TPC")
STOCK_SERVICE_URL = os.environ.get("STOCK_SERVICE_URL", "http://stock-service:5000")

# flask app
app = Flask("order-service")
logger = logging.getLogger(__name__)

# create redis connection pool (order data) and message bus pool
redis_pool = create_redis_pool("ORDER")
setup_flask_lifecycle(app, redis_pool, "ORDER")
bus_pool = create_bus_pool()


# log request duration
@app.after_request
def _log_request_time(response):
    duration_ms = (time.perf_counter() - g.start_time) * 1000
    print(
        f"[ORDER] {request.method} {request.path} {response.status_code} {duration_ms:.0f}ms",
        flush=True,
    )
    return response


# create a new order
@app.post("/create/<user_id>")
def create_order(user_id: str):
    # creates a new empty order for a user
    # stores it as a hash : order:{order_id}
    order_id = str(uuid.uuid4())
    g.redis.hset(
        f"order:{order_id}",
        mapping={
            "paid": "false",
            "items": json.dumps([]),
            "user_id": user_id,
            "total_cost": "0",
        },
    )
    return jsonify({"order_id": order_id}), 201


# bulk-create N test orders
@app.post("/batch_init/<n>/<n_items>/<n_users>/<item_price>")
def batch_init(n: int, n_items: int, n_users: int, item_price: int):
    # used by consistency test harness
    # each order gets two random items and a random user
    n, n_items, n_users, item_price = (
        int(n),
        int(n_items),
        int(n_users),
        int(item_price),
    )

    # batch all hset commands in a single round-trip to redis
    pipe = g.redis.pipeline(transaction=False)
    for i in range(n):
        uid = str(random.randint(0, n_users - 1))
        i1 = str(random.randint(0, n_items - 1))
        i2 = str(random.randint(0, n_items - 1))
        pipe.hset(
            f"order:{i}",
            mapping={
                "paid": "false",
                "items": json.dumps([[i1, 1], [i2, 1]]),
                "user_id": uid,
                "total_cost": str(2 * item_price),
            },
        )
    pipe.execute()
    return jsonify({"msg": "Batch init for orders successful"})


# find an order
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


# add an item to an order
@app.post("/addItem/<order_id>/<item_id>/<quantity>")
def add_item(order_id: str, item_id: str, quantity: int):
    # fetch the item price from stock service
    # update order items and total cost atomically
    # idempotency key prevents double-add on retry

    # check if quantity is positive
    quantity = int(quantity)
    if quantity <= 0:
        abort(400, "Quantity must be positive!")

    # idempotency checks
    idem_key = request.headers.get("Idempotency-Key")
    cached = check_idempotency_http(g.redis, idem_key)
    if cached is not None:
        return Response(cached[1], status=cached[0])

    # fetch item price from stock service
    stock_reply = tpc.send_get_request(f"{STOCK_SERVICE_URL}/find/{item_id}")
    if stock_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")
    item_price = stock_reply.json()["price"]

    # read current order state
    order_data = g.redis.hgetall(f"order:{order_id}")
    if not order_data:
        abort(400, f"Order: {order_id} not found!")

    items_list = json.loads(order_data.get("items", "[]"))
    total_cost = int(order_data.get("total_cost", 0))

    # merge if the same item already exists and increment its quantity
    merged = False
    for entry in items_list:
        if entry[0] == item_id:
            entry[1] += quantity
            merged = True
            break
    if not merged:
        items_list.append([item_id, quantity])
    total_cost += quantity * item_price

    # write back to redis
    pipe = g.redis.pipeline(transaction=False)
    pipe.hset(
        f"order:{order_id}",
        mapping={
            "items": json.dumps(items_list),
            "total_cost": str(total_cost),
        },
    )
    pipe.execute()

    body = f"Item: {item_id} added to: {order_id} price updated to: {total_cost}"
    save_idempotency_http(g.redis, idem_key, 200, body)
    return Response(body, status=200)


# checkout an order
@app.post("/checkout/<order_id>")
def checkout(order_id: str):
    if TRANSACTION_MODE == "TPC":
        return tpc.checkout_tpc(order_id)
    else:
        abort(400, "Checkout failed in SAGA mode — use the SAGA gateway endpoint.")


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


# start-up recovery and background consumers
with app.app_context():
    if TRANSACTION_MODE == "TPC":
        # init_bus must come before recovery so commit_tpc/abort_tpc can use streams
        tpc.init_bus(bus_pool)
        try:
            tpc.recovery_tpc(redis_pool)
        except Exception as e:
            print(f"RECOVERY ORDER TPC: {e}", flush=True)

    elif TRANSACTION_MODE == "SAGA":
        saga.init(redis_pool, bus_pool)

        try:
            saga.recovery_saga(redis_pool)
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