import json
import uuid
import time
import random
import logging
import threading
import redis as redis_lib
from collections import defaultdict
from common.kafka_helpers import build_producer
from db import get_order, get_order_for_update, mark_paid
from db import create_saga, get_saga_for_update, advance_saga
from common.idempotency import check_idempotency_kafka, save_idempotency_kafka

logger = logging.getLogger(__name__)


class SagaState:
    STOCK_REQUESTED = "STOCK_REQUESTED"
    PAYMENT_REQUESTED = "PAYMENT_REQUESTED"
    ROLLBACK_REQUESTED = "ROLLBACK_REQUESTED"
    COMPLETED = "COMPLETED"
    STOCK_FAILED = "STOCK_FAILED"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    ROLLED_BACK = "ROLLED_BACK"
    ROLLBACK_FAILED = "ROLLBACK_FAILED"


TERMINAL_STATES = (
    SagaState.COMPLETED,
    SagaState.STOCK_FAILED,
    SagaState.PAYMENT_FAILED,
    SagaState.ROLLED_BACK,
    SagaState.ROLLBACK_FAILED,
)

ORDER_TOPIC = "gateway.orders"
GATEWAY_RESPONSE_TOPIC = "gateway.responses"
INTERNAL_STOCK_TOPIC = "internal.stock"
INTERNAL_PAYMENT_TOPIC = "internal.payment"
INTERNAL_RESPONSE_TOPIC = "internal.responses"

_gateway_producer = None
_internal_producer = None
_redis_pool = None  # renamed from _conn_pool — same purpose, Redis pool

_pending_price_lookups: dict[str, tuple[threading.Event, dict | None]] = {}
_pending_price_lookups_lock = threading.Lock()

_saga_start_times: dict[str, float] = {}


def _log_saga_duration(saga_id: str, final_state: str) -> None:
    start = _saga_start_times.pop(saga_id, None)
    if start is not None:
        print(
            f"[order] SAGA {saga_id} finished [{final_state}] "
            f"in {(time.perf_counter() - start) * 1000:.2f}ms",
            flush=True,
        )


def init(redis_pool, gateway_kafka: str, internal_kafka: str):
    global _gateway_producer, _internal_producer, _redis_pool
    _redis_pool = redis_pool
    _gateway_producer = build_producer(gateway_kafka)
    _internal_producer = build_producer(internal_kafka)


def _get_r():
    """Get a Redis client from the pool for use in background threads."""
    return redis_lib.Redis(connection_pool=_redis_pool)


def _publish_gateway_response(correlation_id, status_code, body):
    from kafka.errors import KafkaError

    payload = {
        "correlation_id": correlation_id,
        "status_code": status_code,
        "body": body,
    }
    try:
        _gateway_producer.send(
            GATEWAY_RESPONSE_TOPIC, key=correlation_id, value=payload
        )
        _gateway_producer.flush(timeout=5)
    except KafkaError as exc:
        logger.error(
            "Failed to publish gateway response for %s: %s", correlation_id, exc
        )


def _publish_internal_request(
    topic, correlation_id, method, path, body=None, headers=None
):
    from kafka.errors import KafkaError

    payload = {
        "correlation_id": correlation_id,
        "method": method.upper(),
        "path": path,
        "headers": headers or {},
        "body": body or {},
    }
    try:
        _internal_producer.send(topic, key=correlation_id, value=payload)
        _internal_producer.flush(timeout=5)
    except KafkaError as exc:
        logger.error("Failed to publish to %s: %s", topic, exc)
        raise


def handle_checkout_saga(r, order_id: str, headers: dict):
    """
    r       — redis.Redis client for the order service database
    The rest of the logic is unchanged from the PostgreSQL version.
    """
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")
    correlation_id = headers.get("X-Correlation-Id") or headers.get("x-correlation-id")

    cached = check_idempotency_kafka(r, idem_key)
    if cached:
        return cached

    try:
        order = get_order_for_update(r, order_id)
    except ValueError as exc:
        return 400, {"error": str(exc)}

    if order["paid"]:
        return 200, f"Order {order_id} is already paid for!"

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order["items"]:
        items_quantities[item_id] += quantity
    if not items_quantities:
        return 200, "Order has no items."

    saga_id = str(uuid.uuid4())
    stock_corr_id = f"{saga_id}:stock:subtract_batch"

    try:
        create_saga(
            r,
            saga_id,
            order_id,
            SagaState.STOCK_REQUESTED,
            dict(items_quantities),
            correlation_id,
            idem_key,
        )
    except Exception:
        raise

    _saga_start_times[saga_id] = time.perf_counter()

    batch_items = [
        {"item_id": iid, "amount": qty} for iid, qty in items_quantities.items()
    ]
    _publish_internal_request(
        INTERNAL_STOCK_TOPIC,
        stock_corr_id,
        "POST",
        "/subtract_batch",
        body={"items": batch_items},
        headers={"Idempotency-Key": stock_corr_id},
    )

    return None


def route_gateway_message(payload, r):
    # route a gateway message
    # r : redis.Redis client
    method = payload.get("method", "GET").upper()
    path = payload.get("path", "/")
    body = payload.get("body") or {}
    headers = payload.get("headers") or {}
    headers["X-Correlation-Id"] = payload.get("correlation_id", "")
    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    # POST /create/<user_id>
    if method == "POST" and len(segments) >= 2 and segments[0] == "create":
        order_id = str(uuid.uuid4())
        r.hset(
            f"order:{order_id}",
            mapping={
                "paid": "false",
                "items": json.dumps([]),
                "user_id": segments[1],
                "total_cost": "0",
            },
        )
        return 201, {"order_id": order_id}

    # POST /batch_init/<n>/<n_items>/<n_users>/<item_price>
    if method == "POST" and len(segments) >= 5 and segments[0] == "batch_init":
        n, n_items, n_users, item_price = (
            int(segments[1]),
            int(segments[2]),
            int(segments[3]),
            int(segments[4]),
        )
        pipe = r.pipeline(transaction=False)
        for i in range(n):
            uid = str(random.randint(0, n_users - 1))
            i1 = str(random.randint(0, n_items - 1))
            i2 = str(random.randint(0, n_items - 1))
            pipe.hset(
                f"order:{i}",
                mapping={
                    "paid": "false",
                    "items": json.dumps([[i1, 1], [i2, 1]]),
                    "user_id": uid,
                    "total_cost": str(2 * item_price),
                },
            )
        pipe.execute()
        return 200, {"msg": "Batch init for orders successful"}

    # GET /find/<order_id>
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
            return 503, {"error": "Stock service timeout fetching item price"}
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

        r.hset(
            f"order:{order_id}",
            mapping={
                "items": json.dumps(items_list),
                "total_cost": str(total_cost),
            },
        )
        body = f"Item: {item_id} added to: {order_id} price updated to: {total_cost}"
        save_idempotency_kafka(r, idem_key, 200, body)
        return 200, body

    # POST /checkout/<order_id>
    if method == "POST" and len(segments) >= 2 and segments[0] == "checkout":
        return handle_checkout_saga(r, segments[1], headers)

    return 404, {"error": f"No handler for {method} {path}"}


def _fetch_item_price(item_id):
    """Block until stock service responds with item price via internal Kafka."""
    corr_id = str(uuid.uuid4())
    event = threading.Event()

    with _pending_price_lookups_lock:
        _pending_price_lookups[corr_id] = (event, None)

    _publish_internal_request(INTERNAL_STOCK_TOPIC, corr_id, "GET", f"/find/{item_id}")

    if not event.wait(timeout=10):
        with _pending_price_lookups_lock:
            _pending_price_lookups.pop(corr_id, None)
        return None

    with _pending_price_lookups_lock:
        _, response = _pending_price_lookups.pop(corr_id)
    return response


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
            INTERNAL_PAYMENT_TOPIC,
            payment_corr_id,
            "POST",
            f"/pay/{order['user_id']}/{order['total_cost']}",
            headers={"Idempotency-Key": payment_corr_id},
        )
    else:
        error = (response.get("body") or {}).get("error", "Stock subtraction failed")
        advance_saga(r, saga["id"], SagaState.STOCK_FAILED)
        _log_saga_duration(saga["id"], SagaState.STOCK_FAILED)
        _publish_gateway_response(
            saga["original_correlation_id"], 400, {"error": error}
        )


def _on_payment_response(r, saga, response):
    if response.get("status_code") == 200:
        try:
            mark_paid(r, saga["order_id"])
            save_idempotency_kafka(
                r, saga["idempotency_key"], 200, "Checkout successful"
            )
            advance_saga(r, saga["id"], SagaState.COMPLETED)
        except Exception:
            logger.critical(
                "SAGA INCONSISTENCY: payment committed but order %s not marked paid",
                saga["order_id"],
            )
            raise
        _log_saga_duration(saga["id"], SagaState.COMPLETED)
        _publish_gateway_response(
            saga["original_correlation_id"], 200, "Checkout successful"
        )
    else:
        _fire_rollback(r, saga)


def _fire_rollback(r, saga):
    rollback_corr_id = f"{saga['id']}:stock:rollback"
    batch_items = [
        {"item_id": iid, "amount": qty} for iid, qty in saga["items_quantities"].items()
    ]
    advance_saga(r, saga["id"], SagaState.ROLLBACK_REQUESTED)
    _publish_internal_request(
        INTERNAL_STOCK_TOPIC,
        rollback_corr_id,
        "POST",
        "/add_batch",
        body={"items": batch_items},
        headers={"Idempotency-Key": rollback_corr_id},
    )


def _on_rollback_response(r, saga, response):
    if response.get("status_code") == 200:
        advance_saga(r, saga["id"], SagaState.ROLLED_BACK)
        _log_saga_duration(saga["id"], SagaState.ROLLED_BACK)
        _publish_gateway_response(
            saga["original_correlation_id"],
            400,
            {"error": "User has insufficient credit"},
        )
    else:
        logger.critical(
            "SAGA INCONSISTENCY: stock rollback failed. saga_id=%s", saga["id"]
        )
        advance_saga(r, saga["id"], SagaState.ROLLBACK_FAILED)
        _log_saga_duration(saga["id"], SagaState.ROLLBACK_FAILED)
        _publish_gateway_response(
            saga["original_correlation_id"],
            500,
            {"error": "Checkout failed and rollback encountered an error."},
        )


_SAGA_HANDLERS = {
    ":stock:subtract_batch": (_on_stock_response, SagaState.STOCK_REQUESTED),
    ":payment:pay": (_on_payment_response, SagaState.PAYMENT_REQUESTED),
    ":stock:rollback": (_on_rollback_response, SagaState.ROLLBACK_REQUESTED),
}


def handle_internal_response(payload):
    correlation_id = payload.get("correlation_id", "")

    with _pending_price_lookups_lock:
        if correlation_id in _pending_price_lookups:
            event, _ = _pending_price_lookups[correlation_id]
            _pending_price_lookups[correlation_id] = (event, payload)
            event.set()
            return

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
            logger.warning(
                "Saga %s: expected %s got %s — skipping",
                saga_id,
                expected_state,
                saga["state"],
            )
            return
        handler_fn(r, saga, payload)
    except Exception as exc:
        logger.error("Error advancing saga %s: %s", saga_id, exc, exc_info=True)


def start_gateway_consumer(gateway_kafka: str):
    import kafka

    while True:
        try:
            consumer = kafka.KafkaConsumer(
                ORDER_TOPIC,
                bootstrap_servers=gateway_kafka,
                group_id="order-service",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("Gateway consumer started on '%s'", ORDER_TOPIC)

            for message in consumer:
                payload = message.value
                correlation_id = payload.get("correlation_id")
                if not correlation_id:
                    consumer.commit()
                    continue

                r = _get_r()
                result = None
                try:
                    result = route_gateway_message(payload, r)
                except Exception as exc:
                    logger.error(
                        "Error processing %s: %s", correlation_id, exc, exc_info=True
                    )
                    result = (500, {"error": "Internal server error"})

                if result is not None:
                    _publish_gateway_response(correlation_id, result[0], result[1])

                try:
                    consumer.commit()
                except Exception:
                    pass

        except Exception as exc:
            logger.error("Gateway consumer crashed, reconnecting in 3s: %s", exc)
            time.sleep(3)


def start_internal_consumer(internal_kafka: str):
    import kafka

    while True:
        try:
            consumer = kafka.KafkaConsumer(
                INTERNAL_RESPONSE_TOPIC,
                bootstrap_servers=internal_kafka,
                group_id="order-service-internal",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("Internal consumer started on '%s'", INTERNAL_RESPONSE_TOPIC)

            for message in consumer:
                try:
                    handle_internal_response(message.value)
                except Exception as exc:
                    logger.error("Error in internal consumer: %s", exc, exc_info=True)
                try:
                    consumer.commit()
                except Exception:
                    pass

        except Exception as exc:
            logger.error("Internal consumer crashed, reconnecting in 3s: %s", exc)
            time.sleep(3)


def recovery_saga(redis_pool):
    """
    Scan all saga:* keys. For any non-terminal saga, re-publish the
    Kafka message that was in-flight when the crash happened.
    This is safe because all downstream consumers are idempotent.
    """
    r = redis_lib.Redis(connection_pool=redis_pool)

    cursor = 0
    recovered = 0
    while True:
        cursor, keys = r.scan(cursor, match="saga:*", count=100)
        for key in keys:
            saga_id = key[len("saga:") :]
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
                    INTERNAL_STOCK_TOPIC,
                    corr_id,
                    "POST",
                    "/subtract_batch",
                    body={"items": batch_items},
                    headers={"Idempotency-Key": corr_id},
                )

            elif saga["state"] == SagaState.PAYMENT_REQUESTED:
                try:
                    order = get_order(r, saga["order_id"])
                except ValueError:
                    print(
                        f"SAGA RECOVERY: Order {saga['order_id']} not found, "
                        f"skipping saga {saga_id}",
                        flush=True,
                    )
                    continue
                corr_id = f"{saga_id}:payment:pay"
                _publish_internal_request(
                    INTERNAL_PAYMENT_TOPIC,
                    corr_id,
                    "POST",
                    f"/pay/{order['user_id']}/{order['total_cost']}",
                    headers={"Idempotency-Key": corr_id},
                )

            elif saga["state"] == SagaState.ROLLBACK_REQUESTED:
                corr_id = f"{saga_id}:stock:rollback"
                _publish_internal_request(
                    INTERNAL_STOCK_TOPIC,
                    corr_id,
                    "POST",
                    "/add_batch",
                    body={"items": batch_items},
                    headers={"Idempotency-Key": corr_id},
                )

            print(f"SAGA RECOVERY: re-published for saga={saga_id}", flush=True)

        if cursor == 0:
            break

    if recovered == 0:
        print("SAGA RECOVERY: No incomplete sagas found", flush=True)