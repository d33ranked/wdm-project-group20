# order service 2PC coordinator — Redis Streams messaging (Phase 3).
#
# What changed from Phase 2
# --------------------------
# All coordinator→participant communication has moved from HTTP (via nginx)
# to Redis Streams on redis-bus.  The participant HTTP endpoints on stock and
# payment are still registered and still work for direct test access; only the
# coordinator's outbound calls changed.
#
# Stream topology (all streams on redis-bus)
# ------------------------------------------
# tpc.stock     ← coordinator publishes prepare/commit/abort commands here
# tpc.payment   ← same for payment
# tpc.responses → stock and payment publish vote/ack responses here
#
# Request-response pattern (same as gateway/SAGA)
# ------------------------------------------------
# 1. Assign a correlation_id to the command.
# 2. Register it in _tpc_client._pending (Event).
# 3. XADD the command to tpc.stock or tpc.payment.
# 4. Block on Event (up to TPC_TIMEOUT_S).
# 5. Background thread reads tpc.responses, wakes the matching Event.
#
# Crash recovery
# ---------------
# tpc.stock and tpc.payment use XREADGROUP with consumer groups so unACKed
# commands survive participant restarts.  On restart, read_pending_then_new
# re-delivers any in-flight command before fetching new ones.
# The coordinator itself uses plain XREAD with last_id="$" on tpc.responses
# (only sees responses produced after it started — same reasoning as gateway).
# On coordinator restart, recovery_tpc re-publishes commit/abort commands
# which trigger fresh responses from participants.

import json
import uuid
import time
import logging
import threading
from collections import defaultdict
from time import perf_counter

import requests
from flask import g, abort, Response

from common.streams import get_bus, ensure_groups, publish

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stream names
# ---------------------------------------------------------------------------

TPC_STOCK_STREAM    = "tpc.stock"
TPC_PAYMENT_STREAM  = "tpc.payment"
TPC_RESPONSE_STREAM = "tpc.responses"

# How long to wait for a participant response before treating it as a vote-NO.
# Stock restarts in ~3 s in the crash-recovery test; 15 s gives plenty of margin.
TPC_TIMEOUT_S = 15


# ---------------------------------------------------------------------------
# TpcStreamClient — publish a command and block for a correlated response
# ---------------------------------------------------------------------------

class TpcStreamClient:
    """Mirrors the gateway's StreamClient but for TPC coordinator→participant calls."""

    def __init__(self, bus_pool):
        self._pool = bus_pool
        self._pending: dict = {}
        self._pending_lock = threading.Lock()
        self._start_response_consumer()

    def send(self, stream: str, payload: dict, correlation_id: str) -> dict:
        """Publish a TPC command and block until the participant responds."""
        event = threading.Event()
        with self._pending_lock:
            self._pending[correlation_id] = (event, None)

        bus = get_bus(self._pool)
        try:
            publish(bus, stream, payload)
        except Exception as exc:
            self._remove_pending(correlation_id)
            logger.error("Failed to publish TPC command to '%s': %s", stream, exc)
            return {"status_code": 400, "body": f"Bus publish error: {exc}"}

        if not event.wait(timeout=TPC_TIMEOUT_S):
            self._remove_pending(correlation_id)
            logger.warning("TPC timeout waiting for response to %s", correlation_id)
            return {"status_code": 400, "body": "TPC request timed out"}

        with self._pending_lock:
            _, response = self._pending.pop(correlation_id)
        return response

    def _remove_pending(self, correlation_id: str):
        with self._pending_lock:
            self._pending.pop(correlation_id, None)

    def _start_response_consumer(self):
        """Background thread reading tpc.responses and waking waiting commands."""
        def consume():
            bus = get_bus(self._pool)
            last_id = "$"   # only responses produced after this instance started
            while True:
                try:
                    result = bus.xread(
                        {TPC_RESPONSE_STREAM: last_id},
                        count=100,
                        block=2000,
                    )
                    if not result:
                        continue
                    for _stream, entries in result:
                        for msg_id, fields in entries:
                            last_id = msg_id
                            try:
                                self._handle_response(json.loads(fields["data"]))
                            except (KeyError, json.JSONDecodeError) as exc:
                                logger.error("Malformed TPC response %s: %s", msg_id, exc)
                except Exception as exc:
                    logger.error("TPC response consumer error, retrying in 1s: %s", exc)
                    time.sleep(1)

        threading.Thread(
            target=consume, daemon=True, name="tpc-response-consumer"
        ).start()

    def _handle_response(self, payload: dict):
        correlation_id = payload.get("correlation_id")
        if not correlation_id:
            return
        with self._pending_lock:
            entry = self._pending.get(correlation_id)
            if entry is None:
                return   # response to a timed-out command — discard
            event, _ = entry
            self._pending[correlation_id] = (event, payload)
        event.set()


# ---------------------------------------------------------------------------
# Module-level client — created by init_bus() at startup
# ---------------------------------------------------------------------------

_tpc_client: TpcStreamClient | None = None


def init_bus(bus_pool):
    """Create the TpcStreamClient.  Must be called before checkout or recovery."""
    global _tpc_client
    # Pre-create the response stream so XREAD doesn't error before any
    # participant has ever published a response.
    bus = get_bus(bus_pool)
    ensure_groups(bus, [(TPC_RESPONSE_STREAM, "tpc-init")])
    _tpc_client = TpcStreamClient(bus_pool)


def _send(stream: str, payload: dict, correlation_id: str) -> dict:
    """Send a TPC command and return the participant's response dict."""
    return _tpc_client.send(stream, payload, correlation_id)


# ---------------------------------------------------------------------------
# HTTP helper — kept for the addItem price lookup (stock find, not TPC)
# ---------------------------------------------------------------------------

def send_get_request(url):
    try:
        start = perf_counter()
        response = requests.get(url, timeout=5)
        logger.debug("ORDER: GET took %.7fs", perf_counter() - start)
        return response
    except requests.exceptions.RequestException:
        abort(400, "Requests error")


# ---------------------------------------------------------------------------
# Checkout lock (Redis SETNX)
# ---------------------------------------------------------------------------

_LOCK_TTL_S = 60


def _acquire_checkout_lock(r, order_id: str, lock_token: str) -> bool:
    return bool(r.set(f"checkout-lock:{order_id}", lock_token, nx=True, ex=_LOCK_TTL_S))


def _release_checkout_lock(r, order_id: str, lock_token: str):
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


# ---------------------------------------------------------------------------
# Transaction log helpers
# ---------------------------------------------------------------------------

def _txn_key(txn_id: str) -> str:
    return f"txn:{txn_id}"


def _set_txn_status(r, txn_id: str, status: str):
    r.hset(_txn_key(txn_id), "status", status)


def _create_txn(r, txn_id: str, order_id: str, user_id: str, total_cost: int):
    r.hset(
        _txn_key(txn_id),
        mapping={
            "order_id":        order_id,
            "status":          "started",
            "prepared_stock":  json.dumps([]),
            "prepared_payment": "false",
            "user_id":         user_id,
            "total_cost":      str(total_cost),
        },
    )


def _get_txn(r, txn_id: str) -> dict | None:
    data = r.hgetall(_txn_key(txn_id))
    if not data:
        return None
    return {
        "txn_id":           txn_id,
        "order_id":         data["order_id"],
        "status":           data["status"],
        "prepared_stock":   json.loads(data.get("prepared_stock", "[]")),
        "prepared_payment": data.get("prepared_payment", "false") == "true",
        "user_id":          data["user_id"],
        "total_cost":       int(data["total_cost"]),
    }


# ---------------------------------------------------------------------------
# Commit / Abort helpers (used by checkout and recovery)
# ---------------------------------------------------------------------------

def commit_tpc(txn_id, prepared_stock, prepared_payment):
    """Send commit to stock and/or payment via Redis Streams."""
    if prepared_stock:
        corr_id = f"{txn_id}:stock:commit"
        _send(TPC_STOCK_STREAM, {
            "correlation_id": corr_id,
            "command":        "commit",
            "txn_id":         txn_id,
        }, corr_id)

    if prepared_payment:
        corr_id = f"{txn_id}:payment:commit"
        _send(TPC_PAYMENT_STREAM, {
            "correlation_id": corr_id,
            "command":        "commit",
            "txn_id":         txn_id,
        }, corr_id)


def abort_tpc(txn_id, prepared_stock, prepared_payment):
    """Send abort to stock and/or payment via Redis Streams."""
    if prepared_stock:
        corr_id = f"{txn_id}:stock:abort"
        _send(TPC_STOCK_STREAM, {
            "correlation_id": corr_id,
            "command":        "abort",
            "txn_id":         txn_id,
        }, corr_id)

    if prepared_payment:
        corr_id = f"{txn_id}:payment:abort"
        _send(TPC_PAYMENT_STREAM, {
            "correlation_id": corr_id,
            "command":        "abort",
            "txn_id":         txn_id,
        }, corr_id)


# ---------------------------------------------------------------------------
# Checkout
# ---------------------------------------------------------------------------

def checkout_tpc(order_id: str):
    r = g.redis
    txn_id     = str(uuid.uuid4())
    lock_token = str(uuid.uuid4())

    # ── Step 0: Acquire checkout lock ──────────────────────────────────────
    if not _acquire_checkout_lock(r, order_id, lock_token):
        return Response("Checkout already in progress", status=200)

    try:
        # ── Step 1: Read and validate order ────────────────────────────────
        order_data = r.hgetall(f"order:{order_id}")
        if not order_data:
            abort(400, f"Order: {order_id} not found!")

        if order_data.get("paid") == "true":
            return Response("Order is already paid for!", status=200)

        items      = json.loads(order_data.get("items", "[]"))
        user_id    = order_data["user_id"]
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

        batch_items    = [
            {"item_id": iid, "quantity": qty}
            for iid, qty in sorted(items_quantities.items())
        ]
        prepared_stock = [[e["item_id"], e["quantity"]] for e in batch_items]

        corr_id = f"{txn_id}:stock:prepare_batch"
        stock_resp = _send(TPC_STOCK_STREAM, {
            "correlation_id": corr_id,
            "command":        "prepare_batch",
            "txn_id":         txn_id,
            "items":          batch_items,
        }, corr_id)

        if stock_resp.get("status_code") != 200:
            _set_txn_status(r, txn_id, "aborting")
            abort_tpc(txn_id, [], False)
            _set_txn_status(r, txn_id, "aborted")
            abort(400, "Failed to PREPARE stock")

        r.hset(_txn_key(txn_id), "prepared_stock", json.dumps(prepared_stock))

        # ── Step 4: Prepare payment ────────────────────────────────────────
        _set_txn_status(r, txn_id, "preparing_payment")

        corr_id = f"{txn_id}:payment:prepare"
        payment_resp = _send(TPC_PAYMENT_STREAM, {
            "correlation_id": corr_id,
            "command":        "prepare",
            "txn_id":         txn_id,
            "user_id":        user_id,
            "amount":         total_cost,
        }, corr_id)

        if payment_resp.get("status_code") != 200:
            _set_txn_status(r, txn_id, "aborting")
            abort_tpc(txn_id, prepared_stock, False)
            _set_txn_status(r, txn_id, "aborted")
            abort(400, "Failed to PREPARE payment")

        r.hset(_txn_key(txn_id), "prepared_payment", "true")

        # ── Step 5: Commit ─────────────────────────────────────────────────
        # Once this HSET lands (AOF), the decision is final.
        # If the coordinator crashes here, recovery re-commits.
        _set_txn_status(r, txn_id, "committing")

        commit_tpc(txn_id, prepared_stock, True)

        r.hset(f"order:{order_id}", "paid", "true")
        _set_txn_status(r, txn_id, "committed")

    finally:
        _release_checkout_lock(r, order_id, lock_token)

    return Response("Checkout successful", status=200)


# ---------------------------------------------------------------------------
# Recovery — runs at startup, resolves non-terminal transactions
# ---------------------------------------------------------------------------

def recovery_tpc(redis_pool):
    """
    Scan all txn:* keys.  Any transaction not in a terminal state is either
    re-committed or aborted depending on how far it got before the crash.

    Rule:
      committing  → always commit  (decision was already made — must honour it)
      anything else → abort        (safer to undo than to re-drive forward)
    """
    import redis as redis_lib

    r = redis_lib.Redis(connection_pool=redis_pool)

    cursor    = 0
    recovered = 0
    while True:
        cursor, keys = r.scan(cursor, match="txn:*", count=100)
        for key in keys:
            txn_id = key[len("txn:"):]
            txn    = _get_txn(r, txn_id)
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
                abort_tpc(txn_id, txn["prepared_stock"], txn["prepared_payment"])
                _set_txn_status(r, txn_id, "aborted")

        if cursor == 0:
            break

    if recovered == 0:
        print("RECOVERY TPC: No incomplete transactions found", flush=True)
