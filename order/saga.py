# Order service SAGA orchestrator — Redis Streams messaging (Phase 2).
#
# Stream topology (all streams live on redis-bus)
# ------------------------------------------------
# gateway.orders     ← gateway publishes every inbound order request here
# gateway.responses  → order service publishes HTTP responses back to gateway
# internal.stock     → order service publishes stock sub-tasks here
# internal.payment   → order service publishes payment sub-tasks here
# internal.responses ← stock and payment publish results here
#
# Consumer groups
# ---------------
# gateway.orders     / order-service          — processes CRUD + checkout
# internal.responses / order-response-service — advances saga state machine
#
# At-least-once delivery
# ----------------------
# Every message is ACKed only after processing completes.  If the service
# crashes mid-handler, the message is re-delivered on restart via the
# pending re-read in streams.read_pending_then_new.  All handlers are
# idempotent through the existing idempotency key mechanism.

import json
import uuid
import time
import random
import logging
import threading
from collections import defaultdict

import redis as redis_lib

from common.idempotency import check_idempotency_kafka, save_idempotency_kafka
from common.streams import (
    create_bus_pool, get_bus,
    ensure_groups, publish, read_pending_then_new, ack,
)

from db import get_order, get_order_for_update, mark_paid
from db import create_saga, get_saga_for_update, advance_saga

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Saga States
# ---------------------------------------------------------------------------

class SagaState:
    STOCK_REQUESTED    = "STOCK_REQUESTED"
    PAYMENT_REQUESTED  = "PAYMENT_REQUESTED"
    ROLLBACK_REQUESTED = "ROLLBACK_REQUESTED"
    COMPLETED          = "COMPLETED"
    STOCK_FAILED       = "STOCK_FAILED"
    PAYMENT_FAILED     = "PAYMENT_FAILED"
    ROLLED_BACK        = "ROLLED_BACK"
    ROLLBACK_FAILED    = "ROLLBACK_FAILED"


TERMINAL_STATES = (
    SagaState.COMPLETED,
    SagaState.STOCK_FAILED,
    SagaState.PAYMENT_FAILED,
    SagaState.ROLLED_BACK,
    SagaState.ROLLBACK_FAILED,
)

# ---------------------------------------------------------------------------
# Stream names
# ---------------------------------------------------------------------------

GATEWAY_STREAM          = "gateway.orders"
GATEWAY_RESPONSE_STREAM = "gateway.responses"
INTERNAL_STOCK_STREAM   = "internal.stock"
INTERNAL_PAYMENT_STREAM = "internal.payment"
INTERNAL_RESPONSE_STREAM = "internal.responses"

# consumer group names
GROUP_GATEWAY  = "order-service"
GROUP_INTERNAL = "order-response-service"

# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

_redis_pool = None   # order service's own Redis (data storage)
_bus_pool   = None   # shared redis-bus (messaging)

# price-lookup correlation: correlation_id → (Event, response | None)
_pending_price_lookups: dict = {}
_pending_price_lookups_lock = threading.Lock()

_saga_start_times: dict = {}


def _log_saga_duration(saga_id: str, final_state: str):
    start = _saga_start_times.pop(saga_id, None)
    if start is not None:
        print(
            f"[order] SAGA {saga_id} finished [{final_state}] "
            f"in {(time.perf_counter() - start) * 1000:.2f}ms",
            flush=True,
        )


def init(redis_pool, bus_pool):
    global _redis_pool, _bus_pool
    _redis_pool = redis_pool
    _bus_pool   = bus_pool

    # ensure consumer groups exist on startup
    bus = get_bus(bus_pool)
    ensure_groups(bus, [
        (GATEWAY_STREAM,           GROUP_GATEWAY),
        (INTERNAL_RESPONSE_STREAM, GROUP_INTERNAL),
    ])


def _get_r():
    """Redis client for order data (storage Redis)."""
    return redis_lib.Redis(connection_pool=_redis_pool)


def _get_bus():
    """Redis client for the message bus."""
    return get_bus(_bus_pool)


# ---------------------------------------------------------------------------
# Publishing helpers
# ---------------------------------------------------------------------------

def _publish_gateway_response(correlation_id: str, status_code: int, body):
    publish(_get_bus(), GATEWAY_RESPONSE_STREAM, {
        "correlation_id": correlation_id,
        "status_code":    status_code,
        "body":           body,
    })


def _publish_internal_request(stream, correlation_id, method, path,
                               body=None, headers=None):
    publish(_get_bus(), stream, {
        "correlation_id": correlation_id,
        "method":         method.upper(),
        "path":           path,
        "headers":        headers or {},
        "body":           body or {},
    })


# ---------------------------------------------------------------------------
# Checkout — initiates a new SAGA
# ---------------------------------------------------------------------------

def handle_checkout_saga(r, order_id: str, headers: dict):
    idem_key       = headers.get("Idempotency-Key") or headers.get("idempotency-key")
    correlation_id = headers.get("X-Correlation-Id") or headers.get("x-correlation-id")

    cached = check_idempotency_kafka(r, idem_key)
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

    saga_id       = str(uuid.uuid4())
    stock_corr_id = f"{saga_id}:stock:subtract_batch"

    create_saga(r, saga_id, order_id, SagaState.STOCK_REQUESTED,
                dict(items_quantities), correlation_id, idem_key)

    _saga_start_times[saga_id] = time.perf_counter()

    batch_items = [{"item_id": iid, "amount": qty} for iid, qty in items_quantities.items()]
    _publish_internal_request(
        INTERNAL_STOCK_STREAM, stock_corr_id,
        "POST", "/subtract_batch",
        body={"items": batch_items},
        headers={"Idempotency-Key": stock_corr_id},
    )

    return None  # response sent async via _publish_gateway_response


# ---------------------------------------------------------------------------
# Gateway stream message routing
# ---------------------------------------------------------------------------

def route_gateway_message(payload, r):
    """Dispatch one message from the gateway.orders stream."""
    method   = payload.get("method", "GET").upper()
    path     = payload.get("path", "/")
    body     = payload.get("body") or {}
    headers  = payload.get("headers") or {}
    headers["X-Correlation-Id"] = payload.get("correlation_id", "")
    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    # POST /create/<user_id>
    if method == "POST" and len(segments) >= 2 and segments[0] == "create":
        order_id = str(uuid.uuid4())
        r.hset(f"order:{order_id}", mapping={
            "paid":       "false",
            "items":      json.dumps([]),
            "user_id":    segments[1],
            "total_cost": "0",
        })
        return 201, {"order_id": order_id}

    # POST /batch_init/<n>/<n_items>/<n_users>/<item_price>
    if method == "POST" and len(segments) >= 5 and segments[0] == "batch_init":
        n, n_items, n_users, item_price = (
            int(segments[1]), int(segments[2]),
            int(segments[3]), int(segments[4]),
        )
        pipe = r.pipeline(transaction=False)
        for i in range(n):
            uid = str(random.randint(0, n_users - 1))
            i1  = str(random.randint(0, n_items - 1))
            i2  = str(random.randint(0, n_items - 1))
            pipe.hset(f"order:{i}", mapping={
                "paid":       "false",
                "items":      json.dumps([[i1, 1], [i2, 1]]),
                "user_id":    uid,
                "total_cost": str(2 * item_price),
            })
        pipe.execute()
        return 200, {"msg": "Batch init for orders successful"}

    # GET /find/<order_id>
    if method == "GET" and len(segments) >= 2 and segments[0] == "find":
        try:
            order = get_order(r, segments[1])
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "order_id":   segments[1],
            "paid":       order["paid"],
            "items":      order["items"],
            "user_id":    order["user_id"],
            "total_cost": order["total_cost"],
        }

    # POST /addItem/<order_id>/<item_id>/<quantity>
    if method == "POST" and len(segments) >= 4 and segments[0] == "addItem":
        order_id, item_id, quantity = segments[1], segments[2], int(segments[3])
        if quantity <= 0:
            return 400, {"error": "Quantity must be positive!"}
        cached = check_idempotency_kafka(r, idem_key)
        if cached:
            return cached

        stock_response = _fetch_item_price(item_id)
        if stock_response is None:
            return 400, {"error": "Stock service timeout fetching item price"}
        if stock_response.get("status_code") != 200:
            return 400, {"error": f"Item {item_id} not found in stock service"}
        item_price = stock_response["body"]["price"]

        order_data = r.hgetall(f"order:{order_id}")
        if not order_data:
            return 400, {"error": f"Order {order_id} not found"}

        items_list = json.loads(order_data.get("items", "[]"))
        total_cost = int(order_data.get("total_cost", 0))

        merged = False
        for entry in items_list:
            if entry[0] == item_id:
                entry[1] += quantity
                merged = True
                break
        if not merged:
            items_list.append([item_id, quantity])
        total_cost += quantity * item_price

        r.hset(f"order:{order_id}", mapping={
            "items":      json.dumps(items_list),
            "total_cost": str(total_cost),
        })
        resp = f"Item: {item_id} added to: {order_id} price updated to: {total_cost}"
        save_idempotency_kafka(r, idem_key, 200, resp)
        return 200, resp

    # POST /checkout/<order_id>
    if method == "POST" and len(segments) >= 2 and segments[0] == "checkout":
        return handle_checkout_saga(r, segments[1], headers)

    return 404, {"error": f"No handler for {method} {path}"}


def _fetch_item_price(item_id: str):
    """Block until stock service responds with the item price."""
    corr_id = str(uuid.uuid4())
    event   = threading.Event()

    with _pending_price_lookups_lock:
        _pending_price_lookups[corr_id] = (event, None)

    _publish_internal_request(INTERNAL_STOCK_STREAM, corr_id, "GET", f"/find/{item_id}")

    if not event.wait(timeout=10):
        with _pending_price_lookups_lock:
            _pending_price_lookups.pop(corr_id, None)
        return None

    with _pending_price_lookups_lock:
        _, response = _pending_price_lookups.pop(corr_id)
    return response


# ---------------------------------------------------------------------------
# Saga state-machine handlers
# ---------------------------------------------------------------------------

def _on_stock_response(r, saga, response):
    if response.get("status_code") == 200:
        payment_corr_id = f"{saga['id']}:payment:pay"
        try:
            order = get_order(r, saga["order_id"])
        except ValueError:
            _fire_rollback(r, saga)
            return
        advance_saga(r, saga["id"], SagaState.PAYMENT_REQUESTED)
        _publish_internal_request(
            INTERNAL_PAYMENT_STREAM, payment_corr_id,
            "POST", f"/pay/{order['user_id']}/{order['total_cost']}",
            headers={"Idempotency-Key": payment_corr_id},
        )
    else:
        error = (response.get("body") or {}).get("error", "Stock subtraction failed")
        advance_saga(r, saga["id"], SagaState.STOCK_FAILED)
        _log_saga_duration(saga["id"], SagaState.STOCK_FAILED)
        _publish_gateway_response(saga["original_correlation_id"], 400, {"error": error})


def _on_payment_response(r, saga, response):
    if response.get("status_code") == 200:
        try:
            mark_paid(r, saga["order_id"])
            save_idempotency_kafka(r, saga["idempotency_key"], 200, "Checkout successful")
            advance_saga(r, saga["id"], SagaState.COMPLETED)
        except Exception:
            logger.critical(
                "SAGA INCONSISTENCY: payment committed but order %s not marked paid",
                saga["order_id"])
            raise
        _log_saga_duration(saga["id"], SagaState.COMPLETED)
        _publish_gateway_response(saga["original_correlation_id"], 200, "Checkout successful")
    else:
        _fire_rollback(r, saga)


def _fire_rollback(r, saga):
    rollback_corr_id = f"{saga['id']}:stock:rollback"
    batch_items = [
        {"item_id": iid, "amount": qty}
        for iid, qty in saga["items_quantities"].items()
    ]
    advance_saga(r, saga["id"], SagaState.ROLLBACK_REQUESTED)
    _publish_internal_request(
        INTERNAL_STOCK_STREAM, rollback_corr_id,
        "POST", "/add_batch",
        body={"items": batch_items},
        headers={"Idempotency-Key": rollback_corr_id},
    )


def _on_rollback_response(r, saga, response):
    if response.get("status_code") == 200:
        advance_saga(r, saga["id"], SagaState.ROLLED_BACK)
        _log_saga_duration(saga["id"], SagaState.ROLLED_BACK)
        _publish_gateway_response(
            saga["original_correlation_id"], 400,
            {"error": "User has insufficient credit"},
        )
    else:
        logger.critical("SAGA INCONSISTENCY: stock rollback failed. saga_id=%s", saga["id"])
        advance_saga(r, saga["id"], SagaState.ROLLBACK_FAILED)
        _log_saga_duration(saga["id"], SagaState.ROLLBACK_FAILED)
        _publish_gateway_response(
            saga["original_correlation_id"], 400,
            {"error": "Checkout failed and rollback encountered an error."},
        )


_SAGA_HANDLERS = {
    ":stock:subtract_batch": (_on_stock_response,  SagaState.STOCK_REQUESTED),
    ":payment:pay":          (_on_payment_response, SagaState.PAYMENT_REQUESTED),
    ":stock:rollback":       (_on_rollback_response, SagaState.ROLLBACK_REQUESTED),
}


def handle_internal_response(payload):
    """Dispatch one message from the internal.responses stream."""
    correlation_id = payload.get("correlation_id", "")

    # price-lookup fast path
    with _pending_price_lookups_lock:
        if correlation_id in _pending_price_lookups:
            event, _ = _pending_price_lookups[correlation_id]
            _pending_price_lookups[correlation_id] = (event, payload)
            event.set()
            return

    # saga state-machine
    handler_fn, saga_id, expected_state = None, None, None
    for suffix, (fn, state) in _SAGA_HANDLERS.items():
        if correlation_id.endswith(suffix):
            saga_id = correlation_id[: -len(suffix)]
            handler_fn, expected_state = fn, state
            break

    if handler_fn is None:
        logger.warning("No handler for correlation_id: %s", correlation_id)
        return

    r = _get_r()
    try:
        saga = get_saga_for_update(r, saga_id)
        if saga is None:
            logger.error("Saga %s not found", saga_id)
            return
        if saga["state"] != expected_state:
            logger.warning("Saga %s: expected %s got %s — skipping",
                           saga_id, expected_state, saga["state"])
            return
        handler_fn(r, saga, payload)
    except Exception as exc:
        logger.error("Error advancing saga %s: %s", saga_id, exc, exc_info=True)


# ---------------------------------------------------------------------------
# Stream consumer loops
# ---------------------------------------------------------------------------

def start_gateway_consumer():
    """Read from gateway.orders stream, dispatch each message, publish response."""
    while True:
        try:
            bus = _get_bus()
            for msg_id, payload in read_pending_then_new(bus, GATEWAY_STREAM, GROUP_GATEWAY):
                correlation_id = payload.get("correlation_id")
                r = _get_r()
                result = None
                try:
                    result = route_gateway_message(payload, r)
                except Exception as exc:
                    logger.error("Error processing %s: %s", correlation_id, exc, exc_info=True)
                    result = (400, {"error": "Internal server error"})

                # checkout returns None — response sent async by saga handlers
                if result is not None:
                    _publish_gateway_response(correlation_id, result[0], result[1])

                ack(bus, GATEWAY_STREAM, GROUP_GATEWAY, msg_id)
        except Exception as exc:
            logger.error("Gateway consumer error, retrying in 1s: %s", exc)
            time.sleep(1)


def start_internal_consumer():
    """Read from internal.responses stream and advance saga state machine."""
    while True:
        try:
            bus = _get_bus()
            for msg_id, payload in read_pending_then_new(bus, INTERNAL_RESPONSE_STREAM, GROUP_INTERNAL):
                try:
                    handle_internal_response(payload)
                except Exception as exc:
                    logger.error("Error in internal consumer: %s", exc, exc_info=True)
                ack(bus, INTERNAL_RESPONSE_STREAM, GROUP_INTERNAL, msg_id)
        except Exception as exc:
            logger.error("Internal consumer error, retrying in 1s: %s", exc)
            time.sleep(1)


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------

def recovery_saga(redis_pool):
    """Re-publish Kafka messages for any non-terminal saga found on startup."""
    r = redis_lib.Redis(connection_pool=redis_pool)

    cursor = 0
    recovered = 0
    while True:
        cursor, keys = r.scan(cursor, match="saga:*", count=100)
        for key in keys:
            saga_id = key[len("saga:"):]
            saga = get_saga_for_update(r, saga_id)
            if saga is None or saga["state"] in TERMINAL_STATES:
                continue

            print(f"SAGA RECOVERY: saga={saga_id} state={saga['state']}", flush=True)
            recovered += 1

            batch_items = [
                {"item_id": iid, "amount": qty}
                for iid, qty in saga["items_quantities"].items()
            ]

            if saga["state"] == SagaState.STOCK_REQUESTED:
                corr_id = f"{saga_id}:stock:subtract_batch"
                _publish_internal_request(
                    INTERNAL_STOCK_STREAM, corr_id, "POST", "/subtract_batch",
                    body={"items": batch_items}, headers={"Idempotency-Key": corr_id},
                )
            elif saga["state"] == SagaState.PAYMENT_REQUESTED:
                try:
                    order = get_order(r, saga["order_id"])
                except ValueError:
                    continue
                corr_id = f"{saga_id}:payment:pay"
                _publish_internal_request(
                    INTERNAL_PAYMENT_STREAM, corr_id, "POST",
                    f"/pay/{order['user_id']}/{order['total_cost']}",
                    headers={"Idempotency-Key": corr_id},
                )
            elif saga["state"] == SagaState.ROLLBACK_REQUESTED:
                corr_id = f"{saga_id}:stock:rollback"
                _publish_internal_request(
                    INTERNAL_STOCK_STREAM, corr_id, "POST", "/add_batch",
                    body={"items": batch_items}, headers={"Idempotency-Key": corr_id},
                )

            print(f"SAGA RECOVERY: re-published for saga={saga_id}", flush=True)

        if cursor == 0:
            break

    if recovered == 0:
        print("SAGA RECOVERY: No incomplete sagas found", flush=True)
