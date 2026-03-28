# stock saga participant — redis streams, at-least-once delivery via pending re-read

import json
import uuid
import time
import logging

import redis as redis_lib

from common.idempotency import check_idempotency, save_idempotency
from common.streams import (
    get_bus,
    ensure_groups,
    publish,
    read_pending_then_new,
    ack,
)

logger = logging.getLogger(__name__)

GATEWAY_STREAM = "gateway.stock"
GATEWAY_RESPONSE_STREAM = "gateway.responses"
INTERNAL_STREAM = "internal.stock"
INTERNAL_RESPONSE_STREAM = "internal.responses"

GROUP_GATEWAY = "stock-service"
GROUP_INTERNAL = "stock-service"

_redis_pool = None
_bus_pool = None
_scripts = None


def init(redis_pool, scripts, bus_pool):
    global _redis_pool, _scripts, _bus_pool
    _redis_pool = redis_pool
    _scripts = scripts
    _bus_pool = bus_pool
    bus = get_bus(bus_pool)
    ensure_groups(
        bus,
        [
            (GATEWAY_STREAM, GROUP_GATEWAY),
            (INTERNAL_STREAM, GROUP_INTERNAL),
        ],
    )


def _get_r():
    return redis_lib.Redis(connection_pool=_redis_pool)


def _get_bus():
    return get_bus(_bus_pool)


def db_subtract_stock_batch(r, items):
    # all-or-nothing lua: checks all items have sufficient stock before deducting any
    keys = [f"item:{item_id}" for item_id, _ in items]
    args = [qty for _, qty in items]
    _scripts.deduct_stock_batch(keys=keys, args=args, client=r)
    pipe = r.pipeline(transaction=False)
    for item_id, _ in items:
        pipe.hget(f"item:{item_id}", "stock")
    stocks = pipe.execute()
    return {
        item_id: int(s) if s is not None else 0
        for (item_id, _), s in zip(items, stocks)
    }


def db_add_stock_batch(r, items):
    # restore stock atomically; always succeeds
    keys = [f"item:{item_id}" for item_id, _ in items]
    args = [qty for _, qty in items]
    _scripts.restore_stock_batch(keys=keys, args=args, client=r)
    pipe = r.pipeline(transaction=False)
    for item_id, _ in items:
        pipe.hget(f"item:{item_id}", "stock")
    stocks = pipe.execute()
    return {
        item_id: int(s) if s is not None else 0
        for (item_id, _), s in zip(items, stocks)
    }


def route_stream_message(payload, r):
    # dispatch inbound stream message; returns (status_code, body)
    method = payload.get("method", "GET").upper()
    path = payload.get("path", "/")
    body = payload.get("body") or {}
    headers = payload.get("headers") or {}
    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    # POST /item/create/<price>
    if (
        method == "POST"
        and len(segments) >= 2
        and segments[0] == "item"
        and segments[1] == "create"
    ):
        price = int(segments[2]) if len(segments) > 2 else 0
        item_id = str(uuid.uuid4())
        r.hset(f"item:{item_id}", mapping={"stock": "0", "price": str(price)})
        return 201, {"item_id": item_id}

    # POST /batch_init/<n>/<starting_stock>/<item_price>
    if method == "POST" and len(segments) >= 4 and segments[0] == "batch_init":
        n, starting_stock, item_price = (
            int(segments[1]),
            int(segments[2]),
            int(segments[3]),
        )
        pipe = r.pipeline(transaction=False)
        for i in range(n):
            pipe.hset(
                f"item:{i}",
                mapping={"stock": str(starting_stock), "price": str(item_price)},
            )
        pipe.execute()
        return 200, {"msg": "Batch init for stock successful"}

    # GET /find/<item_id>
    if method == "GET" and len(segments) >= 2 and segments[0] == "find":
        item_id = segments[1]
        data = r.hgetall(f"item:{item_id}")
        if not data:
            return 400, {"error": f"Item {item_id} not found"}
        return 200, {"stock": int(data["stock"]), "price": int(data["price"])}

    # POST /add/<item_id>/<amount>
    if method == "POST" and len(segments) >= 3 and segments[0] == "add":
        item_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive!"}
        cached = check_idempotency(r, idem_key)
        if cached:
            return cached
        if not r.hexists(f"item:{item_id}", "stock"):
            return 400, {"error": f"Item {item_id} not found"}
        new_stock = r.hincrby(f"item:{item_id}", "stock", amount)
        resp = f"Item: {item_id} stock updated to: {new_stock}"
        save_idempotency(r, idem_key, 200, resp)
        return 200, resp

    # POST /subtract/<item_id>/<amount>
    if method == "POST" and len(segments) >= 3 and segments[0] == "subtract":
        item_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive!"}
        cached = check_idempotency(r, idem_key)
        if cached:
            return cached
        try:
            _scripts.deduct_stock_batch(
                keys=[f"item:{item_id}"], args=[amount], client=r
            )
        except redis_lib.exceptions.ResponseError as exc:
            err = str(exc)
            if "NOT_FOUND" in err:
                return 400, {"error": f"Item {item_id} not found"}
            if "INSUFFICIENT" in err:
                return 400, {"error": f"Item {item_id} has insufficient stock"}
            raise
        new_stock = int(r.hget(f"item:{item_id}", "stock"))
        resp = f"Item: {item_id} stock updated to: {new_stock}"
        save_idempotency(r, idem_key, 200, resp)
        return 200, resp

    # POST /subtract_batch
    if method == "POST" and segments and segments[0] == "subtract_batch":
        cached = check_idempotency(r, idem_key)
        if cached:
            return cached
        try:
            items = [(e["item_id"], int(e["amount"])) for e in body["items"]]
        except (KeyError, TypeError, ValueError):
            return 400, {
                "error": 'Expected {"items": [{"item_id": str, "amount": int}, ...]}'
            }
        try:
            results = db_subtract_stock_batch(r, items)
            resp = {"updated_stock": results}
            save_idempotency(r, idem_key, 200, json.dumps(resp))
            return 200, resp
        except redis_lib.exceptions.ResponseError as exc:
            err = str(exc)
            if "NOT_FOUND" in err:
                item_id = err.split("item:")[-1]
                return 400, {"error": f"Item {item_id} not found"}
            if "INSUFFICIENT" in err:
                item_id = err.split("item:")[-1]
                return 400, {"error": f"Item {item_id} has insufficient stock"}
            return 400, {"error": str(exc)}

    # POST /add_batch
    if method == "POST" and segments and segments[0] == "add_batch":
        cached = check_idempotency(r, idem_key)
        if cached:
            return cached
        try:
            items = [(e["item_id"], int(e["amount"])) for e in body["items"]]
        except (KeyError, TypeError, ValueError):
            return 400, {
                "error": 'Expected {"items": [{"item_id": str, "amount": int}, ...]}'
            }
        try:
            results = db_add_stock_batch(r, items)
            resp = {"updated_stock": results}
            save_idempotency(r, idem_key, 200, json.dumps(resp))
            return 200, resp
        except Exception as exc:
            return 400, {"error": str(exc)}

    return 404, {"error": f"No handler for {method} {path}"}


def _publish_response(
    response_stream: str, correlation_id: str, status_code: int, body
):
    publish(
        _get_bus(),
        response_stream,
        {
            "correlation_id": correlation_id,
            "status_code": status_code,
            "body": body,
        },
    )


def _handle_gateway_message(msg_id: str, payload: dict):
    # runs in its own greenlet — all messages in a batch execute concurrently
    # gevent yields on every redis i/o call so 500 messages overlap their network waits
    correlation_id = payload.get("correlation_id")
    bus = _get_bus()
    if not correlation_id:
        ack(bus, GATEWAY_STREAM, GROUP_GATEWAY, msg_id)
        return
    r = _get_r()
    try:
        status_code, body = route_stream_message(payload, r)
    except Exception as exc:
        logger.error("Error processing %s: %s", correlation_id, exc, exc_info=True)
        status_code, body = 400, {"error": "Internal server error"}
    _publish_response(GATEWAY_RESPONSE_STREAM, correlation_id, status_code, body)
    ack(bus, GATEWAY_STREAM, GROUP_GATEWAY, msg_id)


def _handle_internal_message(msg_id: str, payload: dict):
    # runs in its own greenlet — same concurrent pattern as gateway handler
    correlation_id = payload.get("correlation_id")
    bus = _get_bus()
    if not correlation_id:
        ack(bus, INTERNAL_STREAM, GROUP_INTERNAL, msg_id)
        return
    r = _get_r()
    try:
        status_code, body = route_stream_message(payload, r)
    except Exception as exc:
        logger.error("Error processing %s: %s", correlation_id, exc, exc_info=True)
        status_code, body = 400, {"error": "Internal server error"}
    _publish_response(INTERNAL_RESPONSE_STREAM, correlation_id, status_code, body)
    ack(bus, INTERNAL_STREAM, GROUP_INTERNAL, msg_id)


def start_gateway_consumer():
    # consume gateway.stock; spawn one greenlet per message so the batch runs concurrently
    import gevent
    logger.info("Stock gateway consumer started on stream '%s'", GATEWAY_STREAM)
    while True:
        try:
            msgs = read_pending_then_new(get_bus(_bus_pool), GATEWAY_STREAM, GROUP_GATEWAY)
            if msgs:
                gevent.joinall(
                    [gevent.spawn(_handle_gateway_message, mid, pl) for mid, pl in msgs]
                )
        except Exception as exc:
            logger.error("Stock gateway consumer error, retrying in 1s: %s", exc)
            time.sleep(1)


def start_internal_consumer():
    # consume internal.stock; same concurrent pattern as gateway consumer
    import gevent
    logger.info("Stock internal consumer started on stream '%s'", INTERNAL_STREAM)
    while True:
        try:
            msgs = read_pending_then_new(get_bus(_bus_pool), INTERNAL_STREAM, GROUP_INTERNAL)
            if msgs:
                gevent.joinall(
                    [gevent.spawn(_handle_internal_message, mid, pl) for mid, pl in msgs]
                )
        except Exception as exc:
            logger.error("Stock internal consumer error, retrying in 1s: %s", exc)
            time.sleep(1)


def recovery_saga(redis_pool):
    # participant only — orchestrator (order service) drives recovery
    print(
        "SAGA RECOVERY STOCK: participant only — no saga state to recover", flush=True
    )