# order service 2PC coordinator — Redis version
# state machine (same as before, now persisted in Redis):
# started → preparing_stock → preparing_payment → committing → committed
# Any vote-NO or timeout at any point → aborting → aborted
# transaction log key: txn:{txn_id}  (Redis Hash)
# fields: order_id, status, prepared_stock (JSON), prepared_payment, user_id, total_cost
# checkout lock: checkout-lock:{order_id}  (Redis String, TTL 60s)
# prevents two concurrent HTTP requests from double-checking out the same order
# PostgreSQL handled this with SELECT FOR UPDATE; Redis uses SETNX

import os
import json
import uuid
import time
import logging
from collections import defaultdict
from time import perf_counter

import requests
from flask import g, abort, Response

GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://nginx:80")
logger = logging.getLogger(__name__)


def send_post_request(url, idempotency_key=None, max_retries=7, json_body=None):
    headers = {"Idempotency-Key": idempotency_key} if idempotency_key else {}
    start = perf_counter()
    for attempt in range(max_retries + 1):
        try:
            response = requests.post(url, headers=headers, json=json_body, timeout=5)
            if response.status_code < 500:
                logger.debug("ORDER: POST took %.7fs", perf_counter() - start)
                return response
        except requests.exceptions.RequestException:
            if attempt == max_retries:
                abort(400, "Requests error")
        if attempt < max_retries:
            time.sleep(min(0.5 * (2**attempt), 5))
    abort(400, "Requests error")


def send_get_request(url):
    try:
        start = perf_counter()
        response = requests.get(url, timeout=5)
        logger.debug("ORDER: GET took %.7fs", perf_counter() - start)
        return response
    except requests.exceptions.RequestException:
        abort(400, "Requests error")


_LOCK_TTL_S = 60  # seconds — long enough for a full checkout round-trip


def _acquire_checkout_lock(r, order_id: str, lock_token: str) -> bool:
    # try to acquire an exclusive lock on this order's checkout
    # SET NX EX is atomic: sets the key only if it does not already exist
    # returns True if we got the lock, False if someone else holds it
    return bool(r.set(f"checkout-lock:{order_id}", lock_token, nx=True, ex=_LOCK_TTL_S))


def _release_checkout_lock(r, order_id: str, lock_token: str):
    # release the lock only if we still own it
    # uses a Lua script to make the check-and-delete atomic
    # if we checked ownership and then deleted in two separate commands, a race could let us delete someone else's lock
    release_script = r.register_script(
        """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            redis.call('DEL', KEYS[1])
            return 1
        end
        return 0
    """
    )
    release_script(keys=[f"checkout-lock:{order_id}"], args=[lock_token])


def _txn_key(txn_id: str) -> str:
    return f"txn:{txn_id}"


def _set_txn_status(r, txn_id: str, status: str):
    # update the transaction status
    # in PostgreSQL this was UPDATE + conn.commit()
    # in Redis, HSET is immediately durable — no commit needed
    r.hset(_txn_key(txn_id), "status", status)


def _create_txn(r, txn_id: str, order_id: str, user_id: str, total_cost: int):
    # create the initial transaction log entry
    r.hset(
        _txn_key(txn_id),
        mapping={
            "order_id": order_id,
            "status": "started",
            "prepared_stock": json.dumps([]),
            "prepared_payment": "false",
            "user_id": user_id,
            "total_cost": str(total_cost),
        },
    )


def _get_txn(r, txn_id: str) -> dict | None:
    # read back a transaction log entry for recovery
    # returns None if the transaction does not exist
    data = r.hgetall(_txn_key(txn_id))
    if not data:
        return None
    return {
        "txn_id": txn_id,
        "order_id": data["order_id"],
        "status": data["status"],
        "prepared_stock": json.loads(data.get("prepared_stock", "[]")),
        "prepared_payment": data.get("prepared_payment", "false") == "true",
        "user_id": data["user_id"],
        "total_cost": int(data["total_cost"]),
    }


def rollback_stock(removed_items, transaction_id):
    for item_id, quantity in removed_items:
        send_post_request(
            f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}",
            idempotency_key=f"{transaction_id}:stock:rollback:{item_id}",
        )


def commit_tpc(txn_id, prepared_stock, prepared_payment):
    if prepared_stock:
        send_post_request(
            f"{GATEWAY_URL}/stock/commit/{txn_id}",
            f"{txn_id}:stock:commit",
        )
    if prepared_payment:
        send_post_request(
            f"{GATEWAY_URL}/payment/commit/{txn_id}",
            f"{txn_id}:payment:commit",
        )


def abort_tpc(txn_id, prepared_stock, prepared_payment):
    if prepared_stock:
        send_post_request(
            f"{GATEWAY_URL}/stock/abort/{txn_id}",
            f"{txn_id}:stock:abort",
        )
    if prepared_payment:
        send_post_request(
            f"{GATEWAY_URL}/payment/abort/{txn_id}",
            f"{txn_id}:payment:abort",
        )


def checkout_tpc(order_id: str):
    r = g.redis
    txn_id = str(uuid.uuid4())
    lock_token = str(uuid.uuid4())

    # ── Step 0: Acquire checkout lock ──────────────────────────────────────
    # Prevents two concurrent requests from double-checking out the same
    # order. The lock is released at the end (or expires after 60s on crash).
    if not _acquire_checkout_lock(r, order_id, lock_token):
        abort(409, f"Checkout already in progress for order {order_id}")

    try:
        # ── Step 1: Read and validate order ────────────────────────────────
        order_data = r.hgetall(f"order:{order_id}")
        if not order_data:
            abort(400, f"Order: {order_id} not found!")

        if order_data.get("paid") == "true":
            return Response("Order is already paid for!", status=200)

        items = json.loads(order_data.get("items", "[]"))
        user_id = order_data["user_id"]
        total_cost = int(order_data.get("total_cost", 0))

        items_quantities = defaultdict(int)
        for item_id, qty in items:
            items_quantities[item_id] += qty
        if not items_quantities:
            return Response("Order has no items.", status=200)

        # ── Step 2: Create transaction log entry ───────────────────────────
        _create_txn(r, txn_id, order_id, user_id, total_cost)

        # ── Step 3: Prepare stock ──────────────────────────────────────────
        _set_txn_status(r, txn_id, "preparing_stock")

        batch_items = [
            {"item_id": iid, "quantity": qty}
            for iid, qty in sorted(items_quantities.items())
        ]
        prepared_stock = [[e["item_id"], e["quantity"]] for e in batch_items]

        stock_reply = send_post_request(
            f"{GATEWAY_URL}/stock/prepare_batch/{txn_id}",
            idempotency_key=f"{txn_id}:stock:prepare_batch",
            json_body={"items": batch_items},
        )
        if stock_reply.status_code != 200:
            _set_txn_status(r, txn_id, "aborting")
            abort_tpc(txn_id, [], False)
            _set_txn_status(r, txn_id, "aborted")
            abort(400, "Failed to PREPARE stock")

        # Persist which items were prepared (needed for abort in recovery)
        r.hset(_txn_key(txn_id), "prepared_stock", json.dumps(prepared_stock))

        # ── Step 4: Prepare payment ────────────────────────────────────────
        _set_txn_status(r, txn_id, "preparing_payment")

        payment_reply = send_post_request(
            f"{GATEWAY_URL}/payment/prepare/{txn_id}/{user_id}/{total_cost}",
            idempotency_key=f"{txn_id}:payment:prepare",
        )
        if payment_reply.status_code != 200:
            _set_txn_status(r, txn_id, "aborting")
            abort_tpc(txn_id, prepared_stock, False)
            _set_txn_status(r, txn_id, "aborted")
            abort(400, "Failed to PREPARE payment")

        r.hset(_txn_key(txn_id), "prepared_payment", "true")

        # ── Step 5: Commit ─────────────────────────────────────────────────
        # Once this HSET lands on disk (AOF), the decision is final.
        # If the coordinator crashes after this point, recovery will re-commit.
        _set_txn_status(r, txn_id, "committing")

        commit_tpc(txn_id, prepared_stock, True)

        r.hset(f"order:{order_id}", "paid", "true")
        _set_txn_status(r, txn_id, "committed")

    finally:
        # Always release the checkout lock, even on abort/exception
        _release_checkout_lock(r, order_id, lock_token)

    return Response("Checkout successful", status=200)


# ---------------------------------------------------------------------------
# Recovery — runs at startup, resolves any non-terminal transactions
# ---------------------------------------------------------------------------


def recovery_tpc(redis_pool):
    """
    Scan all txn:* keys. Any transaction not in a terminal state
    (committed or aborted) is either re-committed or aborted depending on
    how far it got before the crash.

    Rule:
      committing  → always commit  (decision was made, must be honoured)
      anything else → abort         (safer to undo than to re-drive forward)
    """
    import redis as redis_lib

    r = redis_lib.Redis(connection_pool=redis_pool)

    # SCAN iterates keys matching the pattern without blocking Redis.
    # cursor=0 starts a new scan; we stop when cursor returns to 0.
    cursor = 0
    recovered = 0
    while True:
        cursor, keys = r.scan(cursor, match="txn:*", count=100)
        for key in keys:
            txn_id = key[len("txn:") :]
            txn = _get_txn(r, txn_id)
            if txn is None:
                continue
            status = txn["status"]
            if status in ("committed", "aborted"):
                continue

            print(f"RECOVERY TPC: txn={txn_id} status={status}", flush=True)
            recovered += 1

            if status == "committing":
                commit_tpc(txn_id, txn["prepared_stock"], txn["prepared_payment"])
                r.hset(f"order:{txn['order_id']}", "paid", "true")
                _set_txn_status(r, txn_id, "committed")
            else:
                # started / preparing_stock / preparing_payment / aborting
                abort_tpc(txn_id, txn["prepared_stock"], txn["prepared_payment"])
                _set_txn_status(r, txn_id, "aborted")

        if cursor == 0:
            break

    if recovered == 0:
        print("RECOVERY TPC: No incomplete transactions found", flush=True)


# ---------------------------------------------------------------------------
# Module-level init — called from app.py to pass the bus client
# (placeholder for Phase 3 when 2PC moves to Redis Streams)
# ---------------------------------------------------------------------------


def init_bus(bus_redis):
    # reserved for Phase 3 — 2PC over Redis Streams
    pass