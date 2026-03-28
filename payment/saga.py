import uuid
import time
import json
import logging

import redis as redis_lib

from common.idempotency import check_idempotency, save_idempotency
from common.streams import (
    get_bus,
    ensure_groups,
    publish_response,
    make_message_handler,
    run_gevent_consumer,
)

logger = logging.getLogger(__name__)

GATEWAY_STREAM = "gateway.payment"
GATEWAY_RESPONSE_STREAM = "gateway.responses"
INTERNAL_STREAM = "internal.payment"
INTERNAL_RESPONSE_STREAM = "internal.responses"

GROUP_GATEWAY = "payment-service"
GROUP_INTERNAL = "payment-service"

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


def route_stream_message(payload, r):
    method = payload.get("method", "GET").upper()
    path = payload.get("path", "/")
    headers = payload.get("headers") or {}
    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    if method == "POST" and segments and segments[0] == "create_user":
        user_id = str(uuid.uuid4())
        r.hset(f"user:{user_id}", mapping={"credit": "0"})
        return 201, {"user_id": user_id}

    if method == "POST" and len(segments) >= 3 and segments[0] == "batch_init":
        n, starting_money = int(segments[1]), int(segments[2])
        pipe = r.pipeline(transaction=False)
        for i in range(n):
            pipe.hset(f"user:{i}", mapping={"credit": str(starting_money)})
        pipe.execute()
        return 200, {"msg": "Batch init for users successful"}

    if method == "GET" and len(segments) >= 2 and segments[0] == "find_user":
        user_id = segments[1]
        data = r.hgetall(f"user:{user_id}")
        if not data:
            return 400, {"error": f"User {user_id} not found"}
        return 200, {"user_id": user_id, "credit": int(data["credit"])}

    if method == "POST" and len(segments) >= 3 and segments[0] == "add_funds":
        user_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive!"}
        cached = check_idempotency(r, idem_key)
        if cached:
            return cached
        if not r.hexists(f"user:{user_id}", "credit"):
            return 400, {"error": f"User {user_id} not found"}
        new_credit = r.hincrby(f"user:{user_id}", "credit", amount)
        resp = f"User: {user_id} credit updated to: {new_credit}"
        save_idempotency(r, idem_key, 200, resp)
        return 200, resp

    if method == "POST" and len(segments) >= 3 and segments[0] == "pay":
        user_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive!"}
        cached = check_idempotency(r, idem_key)
        if cached:
            return cached
        try:
            new_credit = _scripts.deduct_credit(
                keys=[f"user:{user_id}"],
                args=[amount],
                client=r,
            )
        except redis_lib.exceptions.ResponseError as exc:
            err = str(exc)
            if "NOT_FOUND" in err:
                return 400, {"error": f"User {user_id} not found"}
            if "INSUFFICIENT_CREDIT" in err:
                return 400, {"error": f"User {user_id} has insufficient funds"}
            raise
        resp = f"User: {user_id} credit updated to: {new_credit}"
        save_idempotency(r, idem_key, 200, resp)
        return 200, resp

    return 404, {"error": f"No handler for {method} {path}"}


_handle_gateway_message = make_message_handler(
    _get_bus, _get_r, GATEWAY_STREAM, GROUP_GATEWAY, GATEWAY_RESPONSE_STREAM, route_stream_message
)
_handle_internal_message = make_message_handler(
    _get_bus, _get_r, INTERNAL_STREAM, GROUP_INTERNAL, INTERNAL_RESPONSE_STREAM, route_stream_message
)


def start_gateway_consumer():
    run_gevent_consumer(_bus_pool, GATEWAY_STREAM, GROUP_GATEWAY, _handle_gateway_message, "Payment gateway")


def start_internal_consumer():
    run_gevent_consumer(_bus_pool, INTERNAL_STREAM, GROUP_INTERNAL, _handle_internal_message, "Payment internal")


def recovery_saga(redis_pool):
    print(
        "SAGA RECOVERY PAYMENT: participant only — no saga state to recover", flush=True
    )
