"""Payment 2PC participant — prepare/commit/abort via Redis Lua scripts.

Phase 3 additions
-----------------
A Redis Streams consumer thread is added alongside the existing HTTP routes.
The coordinator (order service) now sends prepare/commit/abort commands via
tpc.payment instead of HTTP.  The HTTP routes remain registered and still work
for direct test access through nginx.

Stream topology
---------------
tpc.payment   ← coordinator publishes commands here (consumer group payment-tpc)
tpc.responses → this service publishes results here (plain XADD)

Command message format
----------------------
{
    "correlation_id": "<txn_id>:<command>",
    "command":        "prepare" | "commit" | "abort",
    "txn_id":         "...",
    # prepare: "user_id": str, "amount": int
}

Response message format (written to tpc.responses)
----------------------------------------------------
{
    "correlation_id": "...",
    "status_code":    200 | 400,
    "body":           str | dict,
}

Key schema (unchanged)
----------
user:{user_id}               Hash  { credit }
prepared:payment:{txn_id}   Hash  { user_id, amount }  TTL 600s
"""

import time
import logging

import redis as redis_lib
from flask import g, abort, Response

from common.streams import get_bus, ensure_groups, publish, read_pending_then_new, ack

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stream / group names
# ---------------------------------------------------------------------------

TPC_STREAM          = "tpc.payment"
TPC_RESPONSE_STREAM = "tpc.responses"
TPC_GROUP           = "payment-tpc"

# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

_redis_pool = None
_scripts    = None
_bus_pool   = None


def init_routes(app, redis_pool, scripts):
    """Register HTTP routes and store the redis pool + scripts."""
    global _redis_pool, _scripts
    _redis_pool = redis_pool
    _scripts    = scripts

    # PREPARE — credit check + deduct + record reservation (all atomic)
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

    # COMMIT — credit was already deducted at prepare time; just delete reservation
    @app.post("/commit/<txn_id>")
    def commit_transaction(txn_id: str):
        _scripts.commit_payment(keys=[f"prepared:payment:{txn_id}"], client=g.redis)
        return Response("Transaction committed", status=200)

    # ABORT — read reservation, restore credit to user, delete reservation
    @app.post("/abort/<txn_id>")
    def abort_transaction(txn_id: str):
        _scripts.abort_payment(keys=[f"prepared:payment:{txn_id}"], client=g.redis)
        return Response("Transaction aborted", status=200)


def init_tpc_stream(bus_pool):
    """Store the bus pool and ensure the consumer group exists."""
    global _bus_pool
    _bus_pool = bus_pool
    bus = get_bus(bus_pool)
    ensure_groups(bus, [(TPC_STREAM, TPC_GROUP)])


# ---------------------------------------------------------------------------
# TPC command dispatcher
# ---------------------------------------------------------------------------

def _dispatch(command: str, payload: dict, r) -> tuple:
    """Run the Lua script for the given TPC command.  Returns (status_code, body)."""
    txn_id = payload.get("txn_id", "")

    if command == "prepare":
        user_id = payload.get("user_id")
        amount  = int(payload.get("amount", 0))
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


# ---------------------------------------------------------------------------
# Redis Streams consumer loop — called from app.py in a daemon thread
# ---------------------------------------------------------------------------

def start_tpc_consumer():
    """Consume TPC commands from tpc.payment and publish results to tpc.responses.

    At-least-once delivery: read_pending_then_new re-delivers any commands that
    were delivered but not yet ACKed (i.e. the service crashed mid-command).
    ack() is called only after the response has been published.
    """
    logger.info("Payment TPC consumer started on stream '%s'", TPC_STREAM)
    while True:
        try:
            bus = get_bus(_bus_pool)
            for msg_id, payload in read_pending_then_new(bus, TPC_STREAM, TPC_GROUP):
                correlation_id = payload.get("correlation_id")
                command        = payload.get("command")

                r = redis_lib.Redis(connection_pool=_redis_pool)
                try:
                    status_code, body = _dispatch(command, payload, r)
                except Exception as exc:
                    logger.error("TPC command error %s/%s: %s",
                                 command, correlation_id, exc, exc_info=True)
                    status_code, body = 400, {"error": "Internal TPC error"}

                publish(get_bus(_bus_pool), TPC_RESPONSE_STREAM, {
                    "correlation_id": correlation_id,
                    "status_code":    status_code,
                    "body":           body,
                })
                ack(bus, TPC_STREAM, TPC_GROUP, msg_id)

        except Exception as exc:
            logger.error("Payment TPC consumer error, retrying in 1s: %s", exc)
            time.sleep(1)


def recovery(redis_pool, scripts):
    # coordinator-driven recovery: order service scans its txn:* keys on startup
    # and explicitly calls commit/abort on all participants including this one.
    # prepared:payment:{txn_id} keys carry a 600s TTL as a last-resort safety net.
    print("RECOVERY PAYMENT: coordinator-driven — no participant-side action needed", flush=True)
