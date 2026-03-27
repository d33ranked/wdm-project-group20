# payment 2pc participant — prepare/commit/abort via lua scripts, consumed from tpc.payment stream

import time
import logging

import redis as redis_lib
from flask import g, abort, Response

from common.streams import get_bus, ensure_groups, publish, read_pending_then_new, ack

logger = logging.getLogger(__name__)

TPC_STREAM = "tpc.payment"
TPC_RESPONSE_STREAM = "tpc.responses"
TPC_GROUP = "payment-tpc"

_redis_pool = None
_scripts = None
_bus_pool = None


def init_routes(app, redis_pool, scripts):
    # register http routes (kept for direct test access via nginx) and store pool + scripts
    global _redis_pool, _scripts
    _redis_pool = redis_pool
    _scripts = scripts

    @app.post("/prepare/<txn_id>/<user_id>/<amount>")
    def prepare_transaction(txn_id: str, user_id: str, amount: int):
        amount = int(amount)
        try:
            _scripts.prepare_payment(
                keys=[f"prepared:payment:{txn_id}", f"user:{user_id}"],
                args=[amount, user_id],
                client=g.redis,
            )
        except redis_lib.exceptions.ResponseError as exc:
            err = str(exc)
            if "NOT_FOUND" in err:
                abort(400, f"User: {user_id} not found!")
            if "INSUFFICIENT_CREDIT" in err:
                abort(400, f"User: {user_id} has insufficient credit!")
            raise
        return Response("Transaction prepared", status=200)

    @app.post("/commit/<txn_id>")
    def commit_transaction(txn_id: str):
        _scripts.commit_payment(keys=[f"prepared:payment:{txn_id}"], client=g.redis)
        return Response("Transaction committed", status=200)

    @app.post("/abort/<txn_id>")
    def abort_transaction(txn_id: str):
        _scripts.abort_payment(keys=[f"prepared:payment:{txn_id}"], client=g.redis)
        return Response("Transaction aborted", status=200)


def init_tpc_stream(bus_pool):
    global _bus_pool
    _bus_pool = bus_pool
    ensure_groups(get_bus(bus_pool), [(TPC_STREAM, TPC_GROUP)])


def _dispatch(command: str, payload: dict, r) -> tuple:
    # run lua script for given tpc command; returns (status_code, body)
    txn_id = payload.get("txn_id", "")

    if command == "prepare":
        user_id = payload.get("user_id")
        amount = int(payload.get("amount", 0))
        try:
            _scripts.prepare_payment(
                keys=[f"prepared:payment:{txn_id}", f"user:{user_id}"],
                args=[amount, user_id],
                client=r,
            )
        except redis_lib.exceptions.ResponseError as exc:
            err = str(exc)
            if "NOT_FOUND" in err:
                return 400, {"error": f"User: {user_id} not found!"}
            if "INSUFFICIENT_CREDIT" in err:
                return 400, {"error": f"User: {user_id} has insufficient credit!"}
            raise
        return 200, "Transaction prepared"

    if command == "commit":
        _scripts.commit_payment(keys=[f"prepared:payment:{txn_id}"], client=r)
        return 200, "Transaction committed"

    if command == "abort":
        _scripts.abort_payment(keys=[f"prepared:payment:{txn_id}"], client=r)
        return 200, "Transaction aborted"

    return 400, {"error": f"Unknown TPC command: {command}"}


def start_tpc_consumer():
    # consume tpc.payment commands; ack after response published for at-least-once delivery
    logger.info("Payment TPC consumer started on stream '%s'", TPC_STREAM)
    while True:
        try:
            bus = get_bus(_bus_pool)
            for msg_id, payload in read_pending_then_new(bus, TPC_STREAM, TPC_GROUP):
                correlation_id = payload.get("correlation_id")
                command = payload.get("command")

                r = redis_lib.Redis(connection_pool=_redis_pool)
                try:
                    status_code, body = _dispatch(command, payload, r)
                except Exception as exc:
                    logger.error(
                        "TPC command error %s/%s: %s",
                        command,
                        correlation_id,
                        exc,
                        exc_info=True,
                    )
                    status_code, body = 400, {"error": "Internal TPC error"}

                publish(
                    get_bus(_bus_pool),
                    TPC_RESPONSE_STREAM,
                    {
                        "correlation_id": correlation_id,
                        "status_code": status_code,
                        "body": body,
                    },
                )
                ack(bus, TPC_STREAM, TPC_GROUP, msg_id)

        except Exception as exc:
            logger.error("Payment TPC consumer error, retrying in 1s: %s", exc)
            time.sleep(1)


def recovery(redis_pool, scripts):
    # coordinator (order service) drives all recovery; prepared keys have 600s ttl as safety net
    print(
        "RECOVERY PAYMENT: coordinator-driven — no participant-side action needed",
        flush=True,
    )