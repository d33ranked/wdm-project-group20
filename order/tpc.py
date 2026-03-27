# order 2pc coordinator — publishes prepare/commit/abort to tpc.stock and tpc.payment streams

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

TPC_STOCK_STREAM = "tpc.stock"
TPC_PAYMENT_STREAM = "tpc.payment"
TPC_RESPONSE_STREAM = "tpc.responses"

TPC_TIMEOUT_S = 15  # stock restarts in ~3s; 15s gives plenty of margin


class TpcStreamClient:
    # mirrors gateway's StreamClient for tpc coordinator→participant calls

    def __init__(self, bus_pool):
        self._pool = bus_pool
        self._pending: dict = {}
        self._pending_lock = threading.Lock()
        self._start_response_consumer()

    def send(self, stream: str, payload: dict, correlation_id: str) -> dict:
        # register before publishing to avoid race where response arrives first
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
        def consume():
            bus = get_bus(self._pool)
            last_id = "$"  # only responses produced after this instance started
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
                                logger.error(
                                    "Malformed TPC response %s: %s", msg_id, exc
                                )
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
                return  # response to a timed-out command — discard
            event, _ = entry
            self._pending[correlation_id] = (event, payload)
        event.set()


_tpc_client: TpcStreamClient | None = None


def init_bus(bus_pool):
    # pre-create response stream so xread doesn't error before any participant has responded
    global _tpc_client
    bus = get_bus(bus_pool)
    ensure_groups(bus, [(TPC_RESPONSE_STREAM, "tpc-init")])
    _tpc_client = TpcStreamClient(bus_pool)


def _send(stream: str, payload: dict, correlation_id: str) -> dict:
    return _tpc_client.send(stream, payload, correlation_id)


# http helper — used only for addItem price lookup (stock find, not tpc)
def send_get_request(url):
    try:
        start = perf_counter()
        response = requests.get(url, timeout=5)
        logger.debug("ORDER: GET took %.7fs", perf_counter() - start)
        return response
    except requests.exceptions.RequestException:
        abort(400, "Requests error")


_LOCK_TTL_S = 60  # checkout lock ttl in seconds


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


def _txn_key(txn_id: str) -> str:
    return f"txn:{txn_id}"


def _set_txn_status(r, txn_id: str, status: str):
    r.hset(_txn_key(txn_id), "status", status)


def _create_txn(r, txn_id: str, order_id: str, user_id: str, total_cost: int):
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


def commit_tpc(txn_id, prepared_stock, prepared_payment):
    # send commit to stock and/or payment via streams
    if prepared_stock:
        corr_id = f"{txn_id}:stock:commit"
        _send(
            TPC_STOCK_STREAM,
            {
                "correlation_id": corr_id,
                "command": "commit",
                "txn_id": txn_id,
            },
            corr_id,
        )

    if prepared_payment:
        corr_id = f"{txn_id}:payment:commit"
        _send(
            TPC_PAYMENT_STREAM,
            {
                "correlation_id": corr_id,
                "command": "commit",
                "txn_id": txn_id,
            },
            corr_id,
        )


def abort_tpc(txn_id, prepared_stock, prepared_payment):
    # send abort to stock and/or payment via streams
    if prepared_stock:
        corr_id = f"{txn_id}:stock:abort"
        _send(
            TPC_STOCK_STREAM,
            {
                "correlation_id": corr_id,
                "command": "abort",
                "txn_id": txn_id,
            },
            corr_id,
        )

    if prepared_payment:
        corr_id = f"{txn_id}:payment:abort"
        _send(
            TPC_PAYMENT_STREAM,
            {
                "correlation_id": corr_id,
                "command": "abort",
                "txn_id": txn_id,
            },
            corr_id,
        )


def checkout_tpc(order_id: str):
    r = g.redis
    txn_id = str(uuid.uuid4())
    lock_token = str(uuid.uuid4())

    if not _acquire_checkout_lock(r, order_id, lock_token):
        return Response("Checkout already in progress", status=200)

    try:
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

        _create_txn(r, txn_id, order_id, user_id, total_cost)

        _set_txn_status(r, txn_id, "preparing_stock")

        batch_items = [
            {"item_id": iid, "quantity": qty}
            for iid, qty in sorted(items_quantities.items())
        ]
        prepared_stock = [[e["item_id"], e["quantity"]] for e in batch_items]

        corr_id = f"{txn_id}:stock:prepare_batch"
        stock_resp = _send(
            TPC_STOCK_STREAM,
            {
                "correlation_id": corr_id,
                "command": "prepare_batch",
                "txn_id": txn_id,
                "items": batch_items,
            },
            corr_id,
        )

        if stock_resp.get("status_code") != 200:
            _set_txn_status(r, txn_id, "aborting")
            abort_tpc(txn_id, [], False)
            _set_txn_status(r, txn_id, "aborted")
            abort(400, "Failed to PREPARE stock")

        r.hset(_txn_key(txn_id), "prepared_stock", json.dumps(prepared_stock))

        _set_txn_status(r, txn_id, "preparing_payment")

        corr_id = f"{txn_id}:payment:prepare"
        payment_resp = _send(
            TPC_PAYMENT_STREAM,
            {
                "correlation_id": corr_id,
                "command": "prepare",
                "txn_id": txn_id,
                "user_id": user_id,
                "amount": total_cost,
            },
            corr_id,
        )

        if payment_resp.get("status_code") != 200:
            _set_txn_status(r, txn_id, "aborting")
            abort_tpc(txn_id, prepared_stock, False)
            _set_txn_status(r, txn_id, "aborted")
            abort(400, "Failed to PREPARE payment")

        r.hset(_txn_key(txn_id), "prepared_payment", "true")

        # once committing status is persisted (aof), decision is final; recovery re-commits on crash
        _set_txn_status(r, txn_id, "committing")

        commit_tpc(txn_id, prepared_stock, True)

        r.hset(f"order:{order_id}", "paid", "true")
        _set_txn_status(r, txn_id, "committed")

    finally:
        _release_checkout_lock(r, order_id, lock_token)

    return Response("Checkout successful", status=200)


def recovery_tpc(redis_pool):
    # scan txn:* keys; committing → commit (decision final), anything else → abort
    import redis as redis_lib

    r = redis_lib.Redis(connection_pool=redis_pool)

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
                abort_tpc(txn_id, txn["prepared_stock"], txn["prepared_payment"])
                _set_txn_status(r, txn_id, "aborted")

        if cursor == 0:
            break

    if recovered == 0:
        print("RECOVERY TPC: No incomplete transactions found", flush=True)