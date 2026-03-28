# stock 2pc participant — prepare/commit/abort via lua scripts, consumed from tpc.stock stream

import time
import logging

import redis as redis_lib
from flask import g, abort, Response, request

from common.streams import get_bus, ensure_groups, publish, read_pending_then_new, ack

logger = logging.getLogger(__name__)

TPC_STREAM = "tpc.stock"
TPC_RESPONSE_STREAM = "tpc.responses"
TPC_GROUP = "stock-tpc"

_redis_pool = None
_scripts = None
_bus_pool = None


def init_routes(app, redis_pool, scripts):
    # register http routes (kept for direct test access via nginx) and store pool + scripts
    global _redis_pool, _scripts
    _redis_pool = redis_pool
    _scripts = scripts

    @app.post("/prepare_batch/<txn_id>")
    def prepare_batch(txn_id: str):
        body = request.get_json(silent=True) or {}
        items = body.get("items", [])
        if not items:
            abort(400, "No items provided for prepare_batch")
        n = len(items)
        keys = [f"prepared:stock:{txn_id}"] + [f"item:{e['item_id']}" for e in items]
        args = [n] + [e["item_id"] for e in items] + [int(e["quantity"]) for e in items]
        try:
            _scripts.prepare_stock_batch(keys=keys, args=args, client=g.redis)
        except redis_lib.exceptions.ResponseError as exc:
            err = str(exc)
            if "NOT_FOUND" in err:
                abort(400, f"Item: {err.split('item:')[-1]} not found!")
            if "INSUFFICIENT" in err:
                abort(400, f"Item: {err.split('item:')[-1]} has insufficient stock!")
            raise
        return Response("Transaction prepared", status=200)

    @app.post("/prepare/<txn_id>/<item_id>/<quantity>")
    def prepare_transaction(txn_id: str, item_id: str, quantity: int):
        quantity = int(quantity)
        keys = [f"prepared:stock:{txn_id}", f"item:{item_id}"]
        args = [1, item_id, quantity]
        try:
            _scripts.prepare_stock_batch(keys=keys, args=args, client=g.redis)
        except redis_lib.exceptions.ResponseError as exc:
            err = str(exc)
            if "NOT_FOUND" in err:
                abort(400, f"Item: {item_id} not found!")
            if "INSUFFICIENT" in err:
                abort(400, f"Item: {item_id} has insufficient stock!")
            raise
        return Response("Transaction prepared", status=200)

    @app.post("/commit/<txn_id>")
    def commit_transaction(txn_id: str):
        _scripts.commit_stock(keys=[f"prepared:stock:{txn_id}"], client=g.redis)
        return Response("Transaction committed", status=200)

    @app.post("/abort/<txn_id>")
    def abort_transaction(txn_id: str):
        _scripts.abort_stock(keys=[f"prepared:stock:{txn_id}"], client=g.redis)
        return Response("Transaction aborted", status=200)


def init_tpc_stream(bus_pool):
    global _bus_pool
    _bus_pool = bus_pool
    ensure_groups(get_bus(bus_pool), [(TPC_STREAM, TPC_GROUP)])


def _dispatch(command: str, payload: dict, r) -> tuple:
    # run lua script for given tpc command; returns (status_code, body)
    txn_id = payload.get("txn_id", "")

    if command == "prepare_batch":
        items = payload.get("items", [])
        if not items:
            return 400, {"error": "No items provided for prepare_batch"}
        n = len(items)
        keys = [f"prepared:stock:{txn_id}"] + [f"item:{e['item_id']}" for e in items]
        args = [n] + [e["item_id"] for e in items] + [int(e["quantity"]) for e in items]
        try:
            _scripts.prepare_stock_batch(keys=keys, args=args, client=r)
        except redis_lib.exceptions.ResponseError as exc:
            err = str(exc)
            if "NOT_FOUND" in err:
                return 400, {"error": f"Item: {err.split('item:')[-1]} not found!"}
            if "INSUFFICIENT" in err:
                return 400, {
                    "error": f"Item: {err.split('item:')[-1]} has insufficient stock!"
                }
            raise
        return 200, "Transaction prepared"

    if command == "prepare":
        item_id = payload.get("item_id")
        quantity = int(payload.get("quantity", 0))
        keys = [f"prepared:stock:{txn_id}", f"item:{item_id}"]
        args = [1, item_id, quantity]
        try:
            _scripts.prepare_stock_batch(keys=keys, args=args, client=r)
        except redis_lib.exceptions.ResponseError as exc:
            err = str(exc)
            if "NOT_FOUND" in err:
                return 400, {"error": f"Item: {item_id} not found!"}
            if "INSUFFICIENT" in err:
                return 400, {"error": f"Item: {item_id} has insufficient stock!"}
            raise
        return 200, "Transaction prepared"

    if command == "commit":
        _scripts.commit_stock(keys=[f"prepared:stock:{txn_id}"], client=r)
        return 200, "Transaction committed"

    if command == "abort":
        _scripts.abort_stock(keys=[f"prepared:stock:{txn_id}"], client=r)
        return 200, "Transaction aborted"

    return 400, {"error": f"Unknown TPC command: {command}"}


def _handle_message(msg_id: str, payload: dict):
    # each message runs in its own greenlet so the entire batch is processed concurrently
    # gevent yields during every redis i/o call, so 100 messages overlap their network waits
    # instead of stacking sequentially (100 × 1.5ms → ~1.5ms wall time for the batch)
    correlation_id = payload.get("correlation_id")
    command = payload.get("command")
    r = redis_lib.Redis(connection_pool=_redis_pool)
    try:
        status_code, body = _dispatch(command, payload, r)
    except Exception as exc:
        logger.error(
            "TPC command error %s/%s: %s", command, correlation_id, exc, exc_info=True
        )
        status_code, body = 400, {"error": "Internal TPC error"}

    bus = get_bus(_bus_pool)
    publish(bus, TPC_RESPONSE_STREAM, {
        "correlation_id": correlation_id,
        "status_code": status_code,
        "body": body,
    })
    ack(bus, TPC_STREAM, TPC_GROUP, msg_id)


def start_tpc_consumer():
    # consume tpc.stock commands; ack after response published for at-least-once delivery
    import gevent
    logger.info("Stock TPC consumer started on stream '%s'", TPC_STREAM)
    while True:
        try:
            msgs = read_pending_then_new(get_bus(_bus_pool), TPC_STREAM, TPC_GROUP)
            if msgs:
                # spawn one greenlet per message and wait for all to finish before next read
                gevent.joinall([gevent.spawn(_handle_message, mid, pl) for mid, pl in msgs])
        except Exception as exc:
            logger.error("Stock TPC consumer error, retrying in 1s: %s", exc)
            time.sleep(1)


def recovery(redis_pool, scripts):
    # coordinator (order service) drives all recovery; prepared keys have 600s ttl as safety net
    print(
        "RECOVERY STOCK: coordinator-driven — no participant-side action needed",
        flush=True,
    )