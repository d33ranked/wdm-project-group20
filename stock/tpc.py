"""Stock 2PC participant — prepare/commit/abort via Redis Lua scripts.

Key schema
----------
item:{item_id}             Hash  { stock, price }
prepared:stock:{txn_id}   Hash  { item_id: quantity, ... }  TTL 600s

How prepare works
-----------------
prepare_stock_batch Lua script (atomic):
  1. If prepared:stock:{txn_id} already exists → return 0 (idempotent, already prepared)
  2. Check every item has sufficient stock → error if not
  3. Deduct stock from every item
  4. Record each {item_id: quantity} in the reservation hash
  5. Set 600s TTL on the reservation hash (coordinator-crash safety net)

Commit: just deletes the reservation hash (stock was already deducted at prepare time).
Abort:  reads the reservation hash, restores stock for each item, deletes the hash.

Recovery
--------
The order service (coordinator) scans its own txn:* keys on startup and calls
commit or abort on all participants. No recovery logic is needed here.
The 600s TTL on prepared:stock:* keys is a last-resort fallback if the
coordinator never recovers — in practice the coordinator restarts within seconds.
"""

import redis as redis_lib
from flask import g, abort, Response, request

_redis_pool = None
_scripts = None


def init_routes(app, redis_pool, scripts):
    global _redis_pool, _scripts
    _redis_pool = redis_pool
    _scripts = scripts

    # PREPARE BATCH — check stock, deduct, record reservation (all atomic)
    @app.post("/prepare_batch/<txn_id>")
    def prepare_batch(txn_id: str):
        body = request.get_json(silent=True) or {}
        items = body.get("items", [])
        if not items:
            abort(400, "No items provided for prepare_batch")

        n = len(items)
        # KEYS[1]       = "prepared:stock:{txn_id}"
        # KEYS[2..n+1]  = "item:{id}" for each item
        # ARGV[1]       = n
        # ARGV[2..n+1]  = item_id strings
        # ARGV[n+2..2n+1] = quantities
        keys = [f"prepared:stock:{txn_id}"] + [f"item:{e['item_id']}" for e in items]
        args = [n] + [e["item_id"] for e in items] + [int(e["quantity"]) for e in items]

        try:
            _scripts.prepare_stock_batch(keys=keys, args=args, client=g.redis)
        except redis_lib.exceptions.ResponseError as exc:
            err = str(exc)
            if "NOT_FOUND" in err:
                item_id = err.split("item:")[-1]
                abort(400, f"Item: {item_id} not found!")
            if "INSUFFICIENT" in err:
                item_id = err.split("item:")[-1]
                abort(400, f"Item: {item_id} has insufficient stock!")
            raise
        # result 0 = already prepared (idempotent), result 1 = newly prepared
        return Response("Transaction prepared", status=200)

    # PREPARE (single item — kept for backward compatibility and direct tests)
    @app.post("/prepare/<txn_id>/<item_id>/<quantity>")
    def prepare_transaction(txn_id: str, item_id: str, quantity: int):
        quantity = int(quantity)
        # single-item call: n=1, item_id=ARGV[2], qty=ARGV[3]
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

    # COMMIT — stock was already deducted at prepare time; just delete reservation
    @app.post("/commit/<txn_id>")
    def commit_transaction(txn_id: str):
        _scripts.commit_stock(keys=[f"prepared:stock:{txn_id}"], client=g.redis)
        return Response("Transaction committed", status=200)

    # ABORT — read reservation, restore stock for each item, delete reservation
    @app.post("/abort/<txn_id>")
    def abort_transaction(txn_id: str):
        _scripts.abort_stock(keys=[f"prepared:stock:{txn_id}"], client=g.redis)
        return Response("Transaction aborted", status=200)


def recovery(redis_pool, scripts):
    # coordinator-driven recovery: order service scans its txn:* keys on startup
    # and explicitly calls commit/abort on all participants including this one
    # prepared:stock:{txn_id} keys carry a 600s TTL as a last-resort safety net
    print("RECOVERY STOCK: coordinator-driven — no participant-side action needed", flush=True)
