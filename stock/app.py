import gevent.monkey

gevent.monkey.patch_all()

import os
import uuid
import logging
import threading

import redis as redis_lib
from flask import Flask, jsonify, abort, request, Response, g

from common.redis_db import (
    create_redis_pool,
    setup_flask_lifecycle,
    setup_gunicorn_logging,
    warmup_pool,
    LuaScripts,
)
from common.idempotency import check_idempotency, save_idempotency
from common.streams import create_bus_pool

TRANSACTION_MODE = os.environ.get("TRANSACTION_MODE", "SAGA")

app = Flask("stock-service")
logger = logging.getLogger(__name__)

redis_pool = create_redis_pool("STOCK")
setup_flask_lifecycle(app, redis_pool, "STOCK")
bus_pool = create_bus_pool()

_scripts = LuaScripts(redis_lib.Redis(connection_pool=redis_pool))


@app.post("/item/create/<price>")
def create_item(price: int):
    key = str(uuid.uuid4())
    g.redis.hset(f"item:{key}", mapping={"stock": "0", "price": str(int(price))})
    return jsonify({"item_id": key})


@app.post("/batch_init/<n>/<starting_stock>/<item_price>")
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n, starting_stock, item_price = int(n), int(starting_stock), int(item_price)
    pipe = g.redis.pipeline(transaction=False)
    for i in range(n):
        pipe.hset(
            f"item:{i}",
            mapping={"stock": str(starting_stock), "price": str(item_price)},
        )
    pipe.execute()
    return jsonify({"msg": "Batch init for stock successful"})


@app.get("/find/<item_id>")
def find_item(item_id: str):
    data = g.redis.hgetall(f"item:{item_id}")
    if not data:
        abort(400, f"Item: {item_id} not found!")
    return jsonify({"stock": int(data["stock"]), "price": int(data["price"])})


@app.post("/add/<item_id>/<amount>")
def add_stock(item_id: str, amount: int):
    if int(amount) <= 0:
        abort(400, "Amount must be positive!")
    idem_key = request.headers.get("Idempotency-Key")
    cached = check_idempotency(g.redis, idem_key)
    if cached is not None:
        return Response(cached[1], status=cached[0])

    if not g.redis.hexists(f"item:{item_id}", "stock"):
        abort(400, f"Item: {item_id} not found!")
    new_stock = g.redis.hincrby(f"item:{item_id}", "stock", int(amount))

    body = f"Item: {item_id} stock updated to: {new_stock}"
    save_idempotency(g.redis, idem_key, 200, body)
    return Response(body, status=200)


@app.post("/subtract/<item_id>/<amount>")
def remove_stock(item_id: str, amount: int):
    if int(amount) <= 0:
        abort(400, "Amount must be positive!")
    idem_key = request.headers.get("Idempotency-Key")
    cached = check_idempotency(g.redis, idem_key)
    if cached is not None:
        return Response(cached[1], status=cached[0])

    try:
        _scripts.deduct_stock_batch(
            keys=[f"item:{item_id}"],
            args=[int(amount)],
            client=g.redis,
        )
    except redis_lib.exceptions.ResponseError as exc:
        err = str(exc)
        if "NOT_FOUND" in err:
            abort(400, f"Item: {item_id} not found!")
        if "INSUFFICIENT" in err:
            abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
        raise

    new_stock = int(g.redis.hget(f"item:{item_id}", "stock"))
    body = f"Item: {item_id} stock updated to: {new_stock}"
    save_idempotency(g.redis, idem_key, 200, body)
    return Response(body, status=200)


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


import tpc
import saga

with app.app_context():
    if TRANSACTION_MODE == "TPC":
        tpc.init_routes(app, redis_pool, _scripts)
        tpc.init_tpc_stream(bus_pool)
        try:
            tpc.recovery(redis_pool, _scripts)
        except Exception as e:
            print(f"RECOVERY STOCK: {e}", flush=True)

        threading.Thread(
            target=tpc.start_tpc_consumer,
            daemon=True,
            name="tpc-consumer",
        ).start()
        print("TPC mode: Redis Streams consumer started", flush=True)

    elif TRANSACTION_MODE == "SAGA":
        saga.init(redis_pool, _scripts, bus_pool)
        warmup_pool(redis_pool)
        warmup_pool(bus_pool)

        try:
            saga.recovery_saga(redis_pool)
        except Exception as e:
            print(f"SAGA RECOVERY STOCK: {e}", flush=True)

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