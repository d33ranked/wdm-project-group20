import json
import uuid
import time
import logging
import requests
import threading
import redis as redis_lib
from time import perf_counter
from collections import defaultdict
from flask import g, abort, Response
from common.orchestrator import Orchestrator, Workflow, StepFailed
from common.stream_rpc import StreamRpc
import os
from common.streams import get_bus, ensure_groups, publish

logger = logging.getLogger(__name__)

TPC_STOCK_STREAM = "tpc.stock"
TPC_PAYMENT_STREAM = "tpc.payment"
TPC_RESPONSE_STREAM = "tpc.responses"
_RESPONSE_BATCH_SIZE = int(os.environ.get("STREAM_BATCH_SIZE", "100"))

TPC_TIMEOUT_S = 15  # stock restarts in ~3s; 15s gives plenty of margin

_tpc_client: "TpcStreamClient | None" = None
_orchestrator: "Orchestrator | None" = None
_redis_pool = None


class TpcStreamClient:

    def __init__(self, bus_pool):
        self._pool = bus_pool
        self._rpc = StreamRpc(default_timeout=TPC_TIMEOUT_S)
        self._start_response_consumer()

    def send(self, stream: str, payload: dict, correlation_id: str) -> dict:
        return self._rpc.send(self._pool, stream, payload, correlation_id)

    def _start_response_consumer(self):
        def consume():
            bus = get_bus(self._pool)
            last_id = "$"
            while True:
                try:
                    result = bus.xread(
                        {TPC_RESPONSE_STREAM: last_id},
                        count=_RESPONSE_BATCH_SIZE,
                        block=2000,
                    )
                    if not result:
                        continue
                    for _stream, entries in result:
                        for msg_id, fields in entries:
                            last_id = msg_id
                            try:
                                payload = json.loads(fields["data"])
                                corr_id = payload.get("correlation_id", "")
                                if corr_id:
                                    self._rpc.handle_response(corr_id, payload)
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


def init_bus(bus_pool, redis_pool):
    global _tpc_client, _orchestrator, _redis_pool
    _redis_pool = redis_pool
    bus = get_bus(bus_pool)
    ensure_groups(bus, [(TPC_RESPONSE_STREAM, "tpc-init")])
    _tpc_client = TpcStreamClient(bus_pool)
    _orchestrator = Orchestrator(redis_pool)


def _send(stream: str, payload: dict, correlation_id: str) -> dict:
    return _tpc_client.send(stream, payload, correlation_id)


def _publish(stream: str, payload: dict):
    bus = get_bus(_tpc_client._pool)
    publish(bus, stream, payload)


def send_get_request(url):
    try:
        start = perf_counter()
        response = requests.get(url, timeout=5)
        logger.debug("ORDER: GET took %.7fs", perf_counter() - start)
        return response
    except requests.exceptions.RequestException:
        abort(400, "Requests error")


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


def _step_prepare_stock(ctx):
    batch_items = [{"item_id": iid, "quantity": qty} for iid, qty in ctx["items"]]
    corr_id = f"{ctx['wf_id']}:stock:prepare_batch"
    resp = _send(
        TPC_STOCK_STREAM,
        {
            "correlation_id": corr_id,
            "command": "prepare_batch",
            "txn_id": ctx["wf_id"],
            "items": batch_items,
        },
        corr_id,
    )
    if resp.get("status_code") != 200:
        raise StepFailed("Failed to PREPARE stock")


def _step_prepare_payment(ctx):
    corr_id = f"{ctx['wf_id']}:payment:prepare"
    resp = _send(
        TPC_PAYMENT_STREAM,
        {
            "correlation_id": corr_id,
            "command": "prepare",
            "txn_id": ctx["wf_id"],
            "user_id": ctx["user_id"],
            "amount": ctx["total_cost"],
        },
        corr_id,
    )
    if resp.get("status_code") != 200:
        raise StepFailed("Failed to PREPARE payment")


def _step_commit(ctx):
    _publish(
        TPC_STOCK_STREAM,
        {
            "correlation_id": f"{ctx['wf_id']}:stock:commit",
            "command": "commit",
            "txn_id": ctx["wf_id"],
        },
    )
    _publish(
        TPC_PAYMENT_STREAM,
        {
            "correlation_id": f"{ctx['wf_id']}:payment:commit",
            "command": "commit",
            "txn_id": ctx["wf_id"],
        },
    )
    r = redis_lib.Redis(connection_pool=_redis_pool)
    r.hset(f"order:{ctx['order_id']}", "paid", "true")


def _comp_abort_stock(ctx):
    corr_id = f"{ctx['wf_id']}:stock:abort"
    _send(
        TPC_STOCK_STREAM,
        {
            "correlation_id": corr_id,
            "command": "abort",
            "txn_id": ctx["wf_id"],
        },
        corr_id,
    )


def _comp_abort_payment(ctx):
    corr_id = f"{ctx['wf_id']}:payment:abort"
    _send(
        TPC_PAYMENT_STREAM,
        {
            "correlation_id": corr_id,
            "command": "abort",
            "txn_id": ctx["wf_id"],
        },
        corr_id,
    )


CHECKOUT_WORKFLOW = Workflow(
    name="checkout_tpc",
    steps=[_step_prepare_stock, _step_prepare_payment, _step_commit],
    compensation=[_comp_abort_stock, _comp_abort_payment],
)


def checkout_tpc(order_id: str):
    r = g.redis
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

        context_items = [[iid, qty] for iid, qty in sorted(items_quantities.items())]

        wf_id = _orchestrator.start(
            CHECKOUT_WORKFLOW,
            {
                "order_id": order_id,
                "user_id": user_id,
                "total_cost": total_cost,
                "items": context_items,
            },
        )

        status, error, _ = _orchestrator.get_status(wf_id)
        if status == _orchestrator.COMPLETED:
            return Response("Checkout successful", status=200)
        else:
            abort(400, error or "Checkout failed")

    finally:
        _release_checkout_lock(r, order_id, lock_token)


def recovery_tpc():
    _orchestrator.recover(CHECKOUT_WORKFLOW)
