# payment saga participant — redis streams, at-least-once delivery via pending re-read

import uuid
import time
import json
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
    # dispatch inbound stream message; returns (status_code, body)
    method = payload.get("method", "GET").upper()
    path = payload.get("path", "/")
    headers = payload.get("headers") or {}
    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    # POST /create_user
    if method == "POST" and segments and segments[0] == "create_user":
        user_id = str(uuid.uuid4())
        r.hset(f"user:{user_id}", mapping={"credit": "0"})
        return 201, {"user_id": user_id}

    # POST /batch_init/<n>/<starting_money>
    if method == "POST" and len(segments) >= 3 and segments[0] == "batch_init":
        n, starting_money = int(segments[1]), int(segments[2])
        pipe = r.pipeline(transaction=False)
        for i in range(n):
            pipe.hset(f"user:{i}", mapping={"credit": str(starting_money)})
        pipe.execute()
        return 200, {"msg": "Batch init for users successful"}

    # GET /find_user/<user_id>
    if method == "GET" and len(segments) >= 2 and segments[0] == "find_user":
        user_id = segments[1]
        data = r.hgetall(f"user:{user_id}")
        if not data:
            return 400, {"error": f"User {user_id} not found"}
        return 200, {"user_id": user_id, "credit": int(data["credit"])}

    # POST /add_funds/<user_id>/<amount>
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

    # POST /pay/<user_id>/<amount>
    if method == "POST" and len(segments) >= 3 and segments[0] == "pay":
        user_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive!"}
        cached = check_idempotency(r, idem_key)
        if cached:
            return cached
        # deduct_credit lua: atomically checks credit >= amount then deducts
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


def start_gateway_consumer():
    # consume gateway.payment; ack after response published for at-least-once delivery
    logger.info("Payment gateway consumer started on stream '%s'", GATEWAY_STREAM)
    while True:
        try:
            bus = _get_bus()
            for msg_id, payload in read_pending_then_new(
                bus, GATEWAY_STREAM, GROUP_GATEWAY
            ):
                correlation_id = payload.get("correlation_id")
                if not correlation_id:
                    ack(bus, GATEWAY_STREAM, GROUP_GATEWAY, msg_id)
                    continue

                r = _get_r()
                try:
                    status_code, body = route_stream_message(payload, r)
                except Exception as exc:
                    logger.error(
                        "Error processing %s: %s", correlation_id, exc, exc_info=True
                    )
                    status_code, body = 400, {"error": "Internal server error"}

                _publish_response(
                    GATEWAY_RESPONSE_STREAM, correlation_id, status_code, body
                )
                ack(bus, GATEWAY_STREAM, GROUP_GATEWAY, msg_id)

        except Exception as exc:
            logger.error("Payment gateway consumer error, retrying in 1s: %s", exc)
            time.sleep(1)


def start_internal_consumer():
    # consume internal.payment; same at-least-once contract as gateway consumer
    logger.info("Payment internal consumer started on stream '%s'", INTERNAL_STREAM)
    while True:
        try:
            bus = _get_bus()
            for msg_id, payload in read_pending_then_new(
                bus, INTERNAL_STREAM, GROUP_INTERNAL
            ):
                correlation_id = payload.get("correlation_id")
                if not correlation_id:
                    ack(bus, INTERNAL_STREAM, GROUP_INTERNAL, msg_id)
                    continue

                r = _get_r()
                try:
                    status_code, body = route_stream_message(payload, r)
                except Exception as exc:
                    logger.error(
                        "Error processing %s: %s", correlation_id, exc, exc_info=True
                    )
                    status_code, body = 400, {"error": "Internal server error"}

                _publish_response(
                    INTERNAL_RESPONSE_STREAM, correlation_id, status_code, body
                )
                ack(bus, INTERNAL_STREAM, GROUP_INTERNAL, msg_id)

        except Exception as exc:
            logger.error("Payment internal consumer error, retrying in 1s: %s", exc)
            time.sleep(1)


def recovery_saga(redis_pool):
    # participant only — orchestrator (order service) drives recovery
    print(
        "SAGA RECOVERY PAYMENT: participant only — no saga state to recover", flush=True
    )