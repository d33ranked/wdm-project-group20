import json
import os
import time
import logging
import requests
import redis as redis_lib
from collections import defaultdict
from db import get_order, get_order_for_update, mark_paid
from common.idempotency import check_idempotency, save_idempotency
from common.orchestrator import Orchestrator, Workflow, suspend
from common.streams import (
    get_bus,
    ensure_groups,
    publish,
    publish_response,
    read_pending_then_new,
    ack,
)
from operations import create_order, batch_init_orders, add_item_to_order

logger = logging.getLogger(__name__)

STOCK_SERVICE_URL = os.environ.get("STOCK_SERVICE_URL", "http://stock-service:5000")

GATEWAY_STREAM = "gateway.orders"
GATEWAY_RESPONSE_STREAM = "gateway.responses"
INTERNAL_STOCK_STREAM = "internal.stock"
INTERNAL_PAYMENT_STREAM = "internal.payment"
INTERNAL_RESPONSE_STREAM = "internal.responses"

GROUP_GATEWAY = "order-service"
GROUP_INTERNAL = "order-response-service"

_redis_pool = None
_bus_pool = None
_orchestrator: "Orchestrator | None" = None


def init(redis_pool, bus_pool):
    global _redis_pool, _bus_pool, _orchestrator
    _redis_pool = redis_pool
    _bus_pool = bus_pool
    _orchestrator = Orchestrator(redis_pool)
    bus = get_bus(bus_pool)
    ensure_groups(
        bus,
        [
            (GATEWAY_STREAM, GROUP_GATEWAY),
            (INTERNAL_RESPONSE_STREAM, GROUP_INTERNAL),
        ],
    )


def _get_r():
    return redis_lib.Redis(connection_pool=_redis_pool)


def _get_bus():
    return get_bus(_bus_pool)


def _publish_gateway_response(correlation_id: str, status_code: int, body):
    publish_response(_get_bus(), GATEWAY_RESPONSE_STREAM, correlation_id, status_code, body)


def _publish_internal_request(
    stream, correlation_id, method, path, body=None, headers=None
):
    publish(
        _get_bus(),
        stream,
        {
            "correlation_id": correlation_id,
            "method": method.upper(),
            "path": path,
            "headers": headers or {},
            "body": body or {},
        },
    )


def _step_subtract_stock(ctx):
    wf_id = ctx["wf_id"]
    batch_items = [
        {"item_id": iid, "amount": qty} for iid, qty in ctx["items_quantities"].items()
    ]
    corr_id = f"{wf_id}:stock:subtract_batch"
    _publish_internal_request(
        INTERNAL_STOCK_STREAM,
        corr_id,
        "POST",
        "/subtract_batch",
        body={"items": batch_items},
        headers={"Idempotency-Key": corr_id},
    )
    suspend()


def _step_charge_payment(ctx):
    wf_id = ctx["wf_id"]
    corr_id = f"{wf_id}:payment:pay"
    _publish_internal_request(
        INTERNAL_PAYMENT_STREAM,
        corr_id,
        "POST",
        f"/pay/{ctx['user_id']}/{ctx['total_cost']}",
        headers={"Idempotency-Key": corr_id},
    )
    suspend()


def _step_mark_paid(ctx):
    r = _get_r()
    mark_paid(r, ctx["order_id"])
    save_idempotency(r, ctx["idempotency_key"], 200, "Checkout successful")


def _comp_add_stock(ctx):
    wf_id = ctx["wf_id"]
    batch_items = [
        {"item_id": iid, "amount": qty} for iid, qty in ctx["items_quantities"].items()
    ]
    corr_id = f"{wf_id}:stock:rollback"
    _publish_internal_request(
        INTERNAL_STOCK_STREAM,
        corr_id,
        "POST",
        "/add_batch",
        body={"items": batch_items},
        headers={"Idempotency-Key": corr_id},
    )
    suspend()


def _on_complete(ctx):
    _publish_gateway_response(
        ctx["original_correlation_id"], 200, "Checkout successful"
    )


def _on_failed(ctx):
    _publish_gateway_response(
        ctx["original_correlation_id"],
        400,
        {"error": ctx.get("_error", "Checkout failed")},
    )


CHECKOUT_WORKFLOW = Workflow(
    name="checkout_saga",
    steps=[_step_subtract_stock, _step_charge_payment, _step_mark_paid],
    compensation=[_comp_add_stock],
    on_complete=_on_complete,
    on_failed=_on_failed,
)

_RESUME_SUFFIXES = [":stock:subtract_batch", ":payment:pay"]
_COMP_SUFFIX = ":stock:rollback"


def handle_checkout_saga(r, order_id: str, headers: dict):
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")
    correlation_id = headers.get("X-Correlation-Id") or headers.get("x-correlation-id")

    cached = check_idempotency(r, idem_key)
    if cached:
        return cached

    try:
        order = get_order_for_update(r, order_id)
    except ValueError as exc:
        return 400, {"error": str(exc)}

    if order["paid"]:
        return 200, {"message": f"Order {order_id} is already paid"}

    items_quantities: dict = defaultdict(int)
    for item_id, quantity in order["items"]:
        items_quantities[item_id] += quantity
    if not items_quantities:
        return 200, "Order has no items."

    _orchestrator.start(
        CHECKOUT_WORKFLOW,
        {
            "order_id": order_id,
            "user_id": order["user_id"],
            "total_cost": order["total_cost"],
            "items_quantities": dict(items_quantities),
            "original_correlation_id": correlation_id,
            "idempotency_key": idem_key,
        },
    )

    return None


def handle_internal_response(payload):
    """Dispatch one message from the internal.responses stream."""
    correlation_id = payload.get("correlation_id", "")

    for suffix in _RESUME_SUFFIXES:
        if correlation_id.endswith(suffix):
            wf_id = correlation_id[: -len(suffix)]
            if payload.get("status_code") == 200:
                _orchestrator.resume(CHECKOUT_WORKFLOW, wf_id)
            else:
                body = payload.get("body") or {}
                error = (
                    body.get("error") if isinstance(body, dict) else str(body)
                ) or "Step failed"
                _orchestrator.fail(CHECKOUT_WORKFLOW, wf_id, error)
            return

    if correlation_id.endswith(_COMP_SUFFIX):
        wf_id = correlation_id[: -len(_COMP_SUFFIX)]
        _orchestrator.resume_comp(CHECKOUT_WORKFLOW, wf_id)
        return

    logger.warning("No handler for correlation_id: %s", correlation_id)


def route_gateway_message(payload, r):
    method = payload.get("method", "GET").upper()
    path = payload.get("path", "/")
    body = payload.get("body") or {}
    headers = payload.get("headers") or {}
    headers["X-Correlation-Id"] = payload.get("correlation_id", "")
    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    if method == "POST" and len(segments) >= 2 and segments[0] == "create":
        order_id = create_order(r, segments[1])
        return 201, {"order_id": order_id}

    if method == "POST" and len(segments) >= 5 and segments[0] == "batch_init":
        batch_init_orders(r, int(segments[1]), int(segments[2]), int(segments[3]), int(segments[4]))
        return 200, {"msg": "Batch init for orders successful"}

    if method == "GET" and len(segments) >= 2 and segments[0] == "find":
        try:
            order = get_order(r, segments[1])
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "order_id": segments[1],
            "paid": order["paid"],
            "items": order["items"],
            "user_id": order["user_id"],
            "total_cost": order["total_cost"],
        }

    if method == "POST" and len(segments) >= 4 and segments[0] == "addItem":
        order_id, item_id, quantity = segments[1], segments[2], int(segments[3])
        if quantity <= 0:
            return 400, {"error": "Quantity must be positive!"}
        cached = check_idempotency(r, idem_key)
        if cached:
            return cached

        stock_response = _fetch_item_price(item_id)
        if stock_response is None:
            return 400, {"error": "Stock service timeout fetching item price"}
        if stock_response.get("status_code") != 200:
            return 400, {"error": f"Item {item_id} not found in stock service"}
        item_price = stock_response["body"]["price"]

        total_cost, error = add_item_to_order(r, order_id, item_id, quantity, item_price)
        if error:
            return 400, {"error": error}
        resp = f"Item: {item_id} added to: {order_id} price updated to: {total_cost}"
        save_idempotency(r, idem_key, 200, resp)
        return 200, resp

    if method == "POST" and len(segments) >= 2 and segments[0] == "checkout":
        return handle_checkout_saga(r, segments[1], headers)

    return 404, {"error": f"No handler for {method} {path}"}


def _fetch_item_price(item_id: str):
    """Fetch item price via direct HTTP GET to stock service."""
    try:
        resp = requests.get(f"{STOCK_SERVICE_URL}/find/{item_id}", timeout=5)
        return {"status_code": resp.status_code, "body": resp.json()}
    except Exception:
        return None


def start_gateway_consumer():
    while True:
        try:
            bus = _get_bus()
            for msg_id, payload in read_pending_then_new(
                bus, GATEWAY_STREAM, GROUP_GATEWAY
            ):
                correlation_id = payload.get("correlation_id")
                r = _get_r()
                result = None
                try:
                    result = route_gateway_message(payload, r)
                except Exception as exc:
                    logger.error(
                        "Error processing %s: %s", correlation_id, exc, exc_info=True
                    )
                    result = (400, {"error": "Internal server error"})

                if result is not None:
                    _publish_gateway_response(correlation_id, result[0], result[1])

                ack(bus, GATEWAY_STREAM, GROUP_GATEWAY, msg_id)
        except Exception as exc:
            logger.error("Gateway consumer error, retrying in 1s: %s", exc)
            time.sleep(1)


def start_internal_consumer():
    while True:
        try:
            bus = _get_bus()
            for msg_id, payload in read_pending_then_new(
                bus, INTERNAL_RESPONSE_STREAM, GROUP_INTERNAL
            ):
                try:
                    handle_internal_response(payload)
                except Exception as exc:
                    logger.error("Error in internal consumer: %s", exc, exc_info=True)
                ack(bus, INTERNAL_RESPONSE_STREAM, GROUP_INTERNAL, msg_id)
        except Exception as exc:
            logger.error("Internal consumer error, retrying in 1s: %s", exc)
            time.sleep(1)


def recovery_saga():
    _orchestrator.recover(CHECKOUT_WORKFLOW)
