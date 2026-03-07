"""
order_service.py

Kafka-consuming order microservice — fully async saga-based checkout.

External topics (gateway Kafka):
    Consumed : gateway.orders
    Produced : gateway.responses

Internal topics (internal Kafka):
    Produced : internal.stock, internal.payment
    Consumed : internal.responses

Checkout is a non-blocking saga persisted to the DB. The consumer loop
never blocks waiting for another service — it fires requests and moves on.
Responses from stock/payment arrive on internal.responses and advance
the saga state machine.
"""

import atexit
import json
import logging
import os
import threading
import time
import uuid
from collections import defaultdict
from typing import Any

import kafka
import psycopg2
import psycopg2.pool
from flask import Flask, jsonify

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GATEWAY_KAFKA  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INTERNAL_KAFKA = os.environ.get("INTERNAL_KAFKA_BOOTSTRAP_SERVERS", "kafka-internal:9092")

# External topics
ORDER_TOPIC            = "gateway.orders"
GATEWAY_RESPONSE_TOPIC = "gateway.responses"

# Internal topics
INTERNAL_STOCK_TOPIC    = "internal.stock"
INTERNAL_PAYMENT_TOPIC  = "internal.payment"
INTERNAL_RESPONSE_TOPIC = "internal.responses"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Saga states
# ---------------------------------------------------------------------------

class SagaState:
    STOCK_REQUESTED    = "STOCK_REQUESTED"
    PAYMENT_REQUESTED  = "PAYMENT_REQUESTED"
    ROLLBACK_REQUESTED = "ROLLBACK_REQUESTED"
    COMPLETED          = "COMPLETED"
    STOCK_FAILED       = "STOCK_FAILED"
    PAYMENT_FAILED     = "PAYMENT_FAILED"
    ROLLED_BACK        = "ROLLED_BACK"
    # Terminal error state — for future saga log reconciliation
    ROLLBACK_FAILED    = "ROLLBACK_FAILED"

# ---------------------------------------------------------------------------
# DB pool
# ---------------------------------------------------------------------------

def _create_conn_pool(retries: int = 10, delay: int = 2) -> psycopg2.pool.ThreadedConnectionPool:
    for attempt in range(retries):
        try:
            return psycopg2.pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=20,
                host=os.environ["POSTGRES_HOST"],
                port=int(os.environ["POSTGRES_PORT"]),
                dbname=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
            )
        except psycopg2.OperationalError:
            if attempt < retries - 1:
                logger.warning("PostgreSQL not ready, retrying in %ds… (%d/%d)", delay, attempt + 1, retries)
                time.sleep(delay)
            else:
                raise


conn_pool = _create_conn_pool()
atexit.register(conn_pool.closeall)

# ---------------------------------------------------------------------------
# Domain exceptions
# ---------------------------------------------------------------------------

class NotFoundError(Exception):
    pass

class NotEnoughItemsInOrderError(Exception):
    pass

# ---------------------------------------------------------------------------
# DB helpers — orders
# ---------------------------------------------------------------------------

def db_get_order(conn, order_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT paid, items, user_id, total_cost FROM orders WHERE id = %s",
            (order_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise NotFoundError(f"Order {order_id} not found")
    return {"paid": row[0], "items": row[1], "user_id": row[2], "total_cost": row[3]}


def db_get_order_for_update(conn, order_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT paid, items, user_id, total_cost FROM orders WHERE id = %s FOR UPDATE",
            (order_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise NotFoundError(f"Order {order_id} not found")
    return {"paid": row[0], "items": row[1], "user_id": row[2], "total_cost": row[3]}


def db_create_order(conn, user_id: str) -> str:
    order_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO orders (id, paid, items, user_id, total_cost) VALUES (%s, %s, %s, %s, %s)",
            (order_id, False, json.dumps([]), user_id, 0),
        )
    conn.commit()
    return order_id


def db_batch_init(conn, n: int, n_items: int, n_users: int, item_price: int) -> None:
    import random
    with conn.cursor() as cur:
        for i in range(n):
            user_id  = str(random.randint(0, n_users - 1))
            item1_id = str(random.randint(0, n_items - 1))
            item2_id = str(random.randint(0, n_items - 1))
            items    = [[item1_id, 1], [item2_id, 1]]
            cur.execute(
                "INSERT INTO orders (id, paid, items, user_id, total_cost) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (id) DO UPDATE SET paid = EXCLUDED.paid, items = EXCLUDED.items, "
                "user_id = EXCLUDED.user_id, total_cost = EXCLUDED.total_cost",
                (str(i), False, json.dumps(items), user_id, 2 * item_price),
            )
    conn.commit()


def db_add_item(conn, order_id: str, item_id: str, quantity: int, item_price: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT items, total_cost FROM orders WHERE id = %s FOR UPDATE",
            (order_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise NotFoundError(f"Order {order_id} not found")
        items_list, total_cost = row

        # Merge into a dict to deduplicate, then serialize back to list
        items: dict[str, int] = {i[0]: i[1] for i in items_list}
        previous_quantity = items.get(item_id, 0)

        if quantity + previous_quantity < 0:
            raise NotEnoughItemsInOrderError(f"Can not have a negative quantity of items in order")           

        items[item_id] = previous_quantity + int(quantity)

        # Only charge for the newly added quantity, not the full new total
        total_cost += int(quantity) * item_price

        cur.execute(
            "UPDATE orders SET items = %s, total_cost = %s WHERE id = %s",
            (json.dumps(list(items.items())), total_cost, order_id),
        )
    return total_cost

def db_mark_paid(conn, order_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE orders SET paid = TRUE WHERE id = %s", (order_id,))

# ---------------------------------------------------------------------------
# DB helpers — sagas
# ---------------------------------------------------------------------------

def db_create_saga(
    conn,
    saga_id: str,
    order_id: str,
    state: str,
    items_quantities: dict[str, int],
    original_correlation_id: str,
    idempotency_key: str | None,
) -> None:
    """Insert a new saga row. Caller commits."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sagas
                (id, order_id, state, items_quantities, original_correlation_id, idempotency_key)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                saga_id,
                order_id,
                state,
                json.dumps(items_quantities),
                original_correlation_id,
                idempotency_key,
            ),
        )


def db_get_saga_for_update(conn, saga_id: str) -> dict | None:
    """Fetches and locks the saga row. Returns None if not found."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, order_id, state, items_quantities, original_correlation_id, idempotency_key
            FROM sagas WHERE id = %s FOR UPDATE
            """,
            (saga_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "id": row[0],
        "order_id": row[1],
        "state": row[2],
        "items_quantities": row[3],
        "original_correlation_id": row[4],
        "idempotency_key": row[5],
    }


def db_advance_saga(conn, saga_id: str, new_state: str) -> None:
    """Updates saga state. Caller commits."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE sagas SET state = %s, updated_at = NOW() WHERE id = %s",
            (new_state, saga_id),
        )

# ---------------------------------------------------------------------------
# DB helpers — idempotency
# ---------------------------------------------------------------------------

def check_idempotency(conn, idem_key: str | None) -> tuple[int, str] | None:
    if not idem_key:
        return None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status_code, body FROM idempotency_keys WHERE key = %s",
            (idem_key,),
        )
        row = cur.fetchone()
    return (row[0], row[1]) if row else None


def save_idempotency(conn, idem_key: str | None, status_code: int, body: str) -> None:
    """No commit — caller commits atomically with the business write."""
    if not idem_key:
        return
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO idempotency_keys (key, status_code, body) "
            "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (idem_key, status_code, body),
        )

# ---------------------------------------------------------------------------
# Kafka producers
# Separate producers for gateway and internal Kafka clusters.
# Initialised at startup — see entrypoint.
# ---------------------------------------------------------------------------

def _build_producer(bootstrap_servers: str) -> kafka.KafkaProducer:
    return kafka.KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=5,
        batch_size=32_768,
    )


gateway_producer: kafka.KafkaProducer | None  = None
internal_producer: kafka.KafkaProducer | None = None


def publish_gateway_response(correlation_id: str, status_code: int, body: Any) -> None:
    payload = {"correlation_id": correlation_id, "status_code": status_code, "body": body}
    try:
        gateway_producer.send(GATEWAY_RESPONSE_TOPIC, key=correlation_id, value=payload)
        gateway_producer.flush(timeout=5)
    except kafka.errors.KafkaError as exc:
        logger.error("Failed to publish gateway response for %s: %s", correlation_id, exc)


def publish_internal_request(
    topic: str,
    correlation_id: str,
    method: str,
    path: str,
    body: dict | None = None,
    headers: dict | None = None,
) -> None:
    """Fire-and-forget to an internal service topic."""
    payload = {
        "correlation_id": correlation_id,
        "method": method.upper(),
        "path": path,
        "headers": headers or {},
        "body": body or {},
    }
    try:
        internal_producer.send(topic, key=correlation_id, value=payload)
        internal_producer.flush(timeout=5)
    except kafka.errors.KafkaError as exc:
        logger.error("Failed to publish internal request to %s: %s", topic, exc)
        raise

# ---------------------------------------------------------------------------
# add_item price lookup — the one place we still need a synchronous internal
# call. Isolated here to keep the pattern explicit and easy to replace later.
# ---------------------------------------------------------------------------

# Keyed on correlation_id → (Event, response | None)
_pending_price_lookups: dict[str, tuple[threading.Event, dict | None]] = {}
_pending_price_lookups_lock = threading.Lock()


# *NOTE, there is no update price endpoint for item, so we could keep an inmemory cache
# Of prices for items, and only fetch prices for items we have not seen yet. 
# This is a possible optimization.
def _fetch_item_price(item_id: str) -> dict | None:
    """
    Synchronously fetches item price from the stock service via internal Kafka.
    Returns the full response dict or None on timeout.
    """
    corr_id = str(uuid.uuid4())
    event   = threading.Event()

    with _pending_price_lookups_lock:
        _pending_price_lookups[corr_id] = (event, None)

    publish_internal_request(
        topic=INTERNAL_STOCK_TOPIC,
        correlation_id=corr_id,
        method="GET",
        path=f"/stock/find/{item_id}",
    )

    if not event.wait(timeout=10):
        with _pending_price_lookups_lock:
            _pending_price_lookups.pop(corr_id, None)
        return None

    with _pending_price_lookups_lock:
        _, response = _pending_price_lookups.pop(corr_id)

    return response

# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

def handle_create_order(conn, path_params, _body, _headers) -> tuple[int, Any]:
    try:
        user_id = path_params[0]
    except IndexError:
        return 400, {"error": "Missing user_id"}
    order_id = db_create_order(conn, user_id)
    return 201, {"order_id": order_id}


def handle_batch_init(conn, path_params, _body, _headers) -> tuple[int, Any]:
    try:
        n          = int(path_params[0])
        n_items    = int(path_params[1])
        n_users    = int(path_params[2])
        item_price = int(path_params[3])
    except (IndexError, ValueError):
        return 400, {"error": "Expected /batch_init/<n>/<n_items>/<n_users>/<item_price>"}
    db_batch_init(conn, n, n_items, n_users, item_price)
    return 200, {"msg": "Batch init for orders successful"}


def handle_find_order(conn, path_params, _body, _headers) -> tuple[int, Any]:
    try:
        order_id = path_params[0]
    except IndexError:
        return 400, {"error": "Missing order_id"}
    try:
        order = db_get_order(conn, order_id)
    except NotFoundError as exc:
        return 400, {"error": str(exc)}
    return 200, {
        "order_id": order_id,
        "paid": order["paid"],
        "items": order["items"],
        "user_id": order["user_id"],
        "total_cost": order["total_cost"],
    }


def handle_add_item(conn, path_params, _body, _headers) -> tuple[int, Any]:
    try:
        order_id = path_params[0]
        item_id  = path_params[1]
        quantity = int(path_params[2])
    except (IndexError, ValueError):
        return 400, {"error": "Expected /addItem/<order_id>/<item_id>/<quantity>"}

    # add_item requires item price — one synchronous internal call is acceptable
    # here since this is not on the checkout hot path and involves no saga state.
    stock_response = _fetch_item_price(item_id)
    if stock_response is None:
        return 503, {"error": "Stock service timeout fetching item price"}
    if stock_response.get("status_code") != 200:
        return 400, {"error": f"Item {item_id} not found in stock service"}

    item_price = stock_response["body"]["price"]

    try:
        total_cost = db_add_item(conn, order_id, item_id, quantity, item_price)
        conn.commit()
    except NotFoundError as exc:
        conn.rollback()
        return 400, {"error": str(exc)}
    except Exception:
        conn.rollback()
        raise

    return 200, f"Item: {item_id} added to: {order_id} price updated to: {total_cost}"


def handle_checkout(conn, path_params, _body, headers) -> tuple[int, Any] | None:
    """
    Initiates the checkout saga and returns None immediately.
    The gateway response is sent asynchronously once the saga completes.
    Returning None signals the gateway consumer to skip the immediate reply.
    """
    try:
        order_id = path_params[0]
    except IndexError:
        return 400, {"error": "Missing order_id"}

    idem_key       = headers.get("Idempotency-Key") or headers.get("idempotency-key")
    correlation_id = headers.get("X-Correlation-Id") or headers.get("x-correlation-id")

    cached = check_idempotency(conn, idem_key)
    if cached:
        return cached

    try:
        order = db_get_order_for_update(conn, order_id)
    except NotFoundError as exc:
        conn.rollback()
        return 400, {"error": str(exc)}

    if order["paid"]:
        conn.rollback()
        return 409, {"error": f"Order {order_id} is already paid"}

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order["items"]:
        items_quantities[item_id] += quantity

    if not items_quantities:
        conn.rollback()
        return 400, {"error": "Order has no items"}

    # saga_id doubles as the transaction_id for deterministic idempotency keys
    saga_id       = idem_key or str(uuid.uuid4())
    stock_corr_id = f"{saga_id}:stock:subtract_batch"

    # Persist saga + fire stock request as close to atomic as possible.
    # If we crash after DB commit but before Kafka publish, a future recovery
    # process can find STOCK_REQUESTED sagas with no corresponding response
    # and re-publish. (Designed for future saga_events reconciliation.)
    try:
        db_create_saga(
            conn,
            saga_id=saga_id,
            order_id=order_id,
            state=SagaState.STOCK_REQUESTED,
            items_quantities=dict(items_quantities),
            original_correlation_id=correlation_id,
            idempotency_key=idem_key,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    batch_items = [{"item_id": iid, "amount": qty} for iid, qty in items_quantities.items()]
    publish_internal_request(
        topic=INTERNAL_STOCK_TOPIC,
        correlation_id=stock_corr_id,
        method="POST",
        path="/stock/subtract_batch",
        body={"items": batch_items},
        headers={"Idempotency-Key": stock_corr_id},
    )

    return None  # response sent asynchronously by saga handlers

# ---------------------------------------------------------------------------
# Routing table
# ---------------------------------------------------------------------------

ROUTES: list[tuple[str, str, callable]] = [
    ("POST", "/create/",     handle_create_order),
    ("POST", "/batch_init/", handle_batch_init),
    ("GET",  "/find/",       handle_find_order),
    ("POST", "/addItem/",    handle_add_item),
    ("POST", "/checkout/",   handle_checkout),
]


def route_gateway_message(payload: dict[str, Any], conn) -> tuple[int, Any] | None:
    method  = payload.get("method", "GET").upper()
    path    = payload.get("path", "/")
    body    = payload.get("body") or {}

    # Inject correlation_id into headers so checkout can store it in the saga
    headers = payload.get("headers") or {}
    headers["X-Correlation-Id"] = payload.get("correlation_id", "")

    segments    = [s for s in path.strip("/").split("/") if s]
    clean_path            = "/" + "/".join(segments) if segments else "/"
    clean_path_with_slash = clean_path if clean_path.endswith("/") else clean_path + "/"

    for route_method, prefix, handler in ROUTES:
        if method == route_method and clean_path_with_slash.startswith(prefix):
            return handler(conn, segments, body, headers)

    return 404, {"error": f"No handler for {method} {path}"}

# ---------------------------------------------------------------------------
# Saga advancement handlers
#
# Each handler receives a locked saga row and the incoming response.
# Contract:
#   - Must call db_advance_saga + conn.commit() before publishing to Kafka
#   - Must never leave the connection in a dirty state on return
# ---------------------------------------------------------------------------

def _on_stock_response(conn, saga: dict, response: dict) -> None:
    if response.get("status_code") == 200:
        payment_corr_id = f"{saga['id']}:payment:pay"

        try:
            order = db_get_order(conn, saga["order_id"])
        except NotFoundError:
            _fire_rollback(conn, saga)
            return

        db_advance_saga(conn, saga["id"], SagaState.PAYMENT_REQUESTED)
        conn.commit()

        publish_internal_request(
            topic=INTERNAL_PAYMENT_TOPIC,
            correlation_id=payment_corr_id,
            method="POST",
            path=f"/payment/pay/{order['user_id']}/{order['total_cost']}",
            headers={"Idempotency-Key": payment_corr_id},
        )
    else:
        # Batch is atomic — nothing was subtracted, no rollback needed
        error = (response.get("body") or {}).get("error", "Stock subtraction failed")
        db_advance_saga(conn, saga["id"], SagaState.STOCK_FAILED)
        conn.commit()
        publish_gateway_response(saga["original_correlation_id"], 400, {"error": error})


def _on_payment_response(conn, saga: dict, response: dict) -> None:
    if response.get("status_code") == 200:
        try:
            db_mark_paid(conn, saga["order_id"])
            save_idempotency(conn, saga["idempotency_key"], 200, "Checkout successful")
            db_advance_saga(conn, saga["id"], SagaState.COMPLETED)
            conn.commit()
        except Exception:
            logger.critical(
                "SAGA INCONSISTENCY: payment committed but order %s not marked paid. "
                "saga_id=%s", saga["order_id"], saga["id"],
            )
            conn.rollback()
            raise

        publish_gateway_response(saga["original_correlation_id"], 200, "Checkout successful")
    else:
        _fire_rollback(conn, saga)


def _fire_rollback(conn, saga: dict) -> None:
    rollback_corr_id = f"{saga['id']}:stock:rollback"
    items_quantities: dict[str, int] = saga["items_quantities"]
    batch_items = [{"item_id": iid, "amount": qty} for iid, qty in items_quantities.items()]

    db_advance_saga(conn, saga["id"], SagaState.ROLLBACK_REQUESTED)
    conn.commit()

    publish_internal_request(
        topic=INTERNAL_STOCK_TOPIC,
        correlation_id=rollback_corr_id,
        method="POST",
        path="/stock/add_batch",
        body={"items": batch_items},
        headers={"Idempotency-Key": rollback_corr_id},
    )


def _on_rollback_response(conn, saga: dict, response: dict) -> None:
    if response.get("status_code") == 200:
        db_advance_saga(conn, saga["id"], SagaState.ROLLED_BACK)
        conn.commit()
        publish_gateway_response(
            saga["original_correlation_id"], 400, {"error": "User has insufficient credit"},
        )
    else:
        # Rollback failed — mark for future reconciliation via saga_events
        logger.critical(
            "SAGA INCONSISTENCY: stock rollback failed. saga_id=%s order_id=%s — "
            "manual reconciliation required.", saga["id"], saga["order_id"],
        )
        db_advance_saga(conn, saga["id"], SagaState.ROLLBACK_FAILED)
        conn.commit()
        publish_gateway_response(
            saga["original_correlation_id"], 500,
            {"error": "Checkout failed and rollback encountered an error. Support has been notified."},
        )


# Maps correlation_id suffix → (handler, expected_state)
_SAGA_HANDLERS: dict[str, tuple[callable, str]] = {
    ":stock:subtract_batch": (_on_stock_response,   SagaState.STOCK_REQUESTED),
    ":payment:pay":          (_on_payment_response,  SagaState.PAYMENT_REQUESTED),
    ":stock:rollback":       (_on_rollback_response, SagaState.ROLLBACK_REQUESTED),
}


def handle_internal_response(payload: dict[str, Any]) -> None:
    """
    Routes an internal.responses message to the correct handler.

    Correlation ID structure: <saga_id>:<step>
    e.g. "abc-123:stock:subtract_batch"

    Price lookup responses (non-saga) are dispatched to _pending_price_lookups.
    """
    correlation_id = payload.get("correlation_id", "")

    # Price lookup responses for add_item
    with _pending_price_lookups_lock:
        if correlation_id in _pending_price_lookups:
            event, _ = _pending_price_lookups[correlation_id]
            _pending_price_lookups[correlation_id] = (event, payload)
            event.set()
            return

    # Saga responses
    handler_fn     = None
    saga_id        = None
    expected_state = None

    for suffix, (fn, state) in _SAGA_HANDLERS.items():
        if correlation_id.endswith(suffix):
            saga_id        = correlation_id[: -len(suffix)]
            handler_fn     = fn
            expected_state = state
            break

    if handler_fn is None:
        logger.warning("No handler found for internal correlation_id: %s", correlation_id)
        return

    conn = conn_pool.getconn()
    try:
        saga = db_get_saga_for_update(conn, saga_id)

        if saga is None:
            logger.error("Saga %s not found (correlation_id=%s)", saga_id, correlation_id)
            conn.rollback()
            return

        if saga["state"] != expected_state:
            # Duplicate delivery or out-of-order — idempotent ignore
            logger.warning(
                "Saga %s: expected state %s but got %s for %s — skipping",
                saga_id, expected_state, saga["state"], correlation_id,
            )
            conn.rollback()
            return

        handler_fn(conn, saga, payload)

    except Exception as exc:
        logger.error("Error advancing saga %s: %s", saga_id, exc, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn_pool.putconn(conn)

# ---------------------------------------------------------------------------
# Consumer loops
# ---------------------------------------------------------------------------

def start_gateway_consumer() -> None:
    consumer = kafka.KafkaConsumer(
        ORDER_TOPIC,
        bootstrap_servers=GATEWAY_KAFKA,
        group_id="order-service",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    logger.info("Gateway consumer started on '%s'", ORDER_TOPIC)

    for message in consumer:
        payload        = message.value
        correlation_id = payload.get("correlation_id")

        if not correlation_id:
            consumer.commit()
            continue

        conn = conn_pool.getconn()
        result = None
        try:
            result = route_gateway_message(payload, conn)
        except Exception as exc:
            logger.error("Unhandled error processing %s: %s", correlation_id, exc, exc_info=True)
            try:
                conn.rollback()
            except Exception:
                pass
            result = (500, {"error": "Internal server error"})
        finally:
            conn_pool.putconn(conn)

        # None = checkout saga initiated, response sent asynchronously
        if result is not None:
            status_code, body = result
            publish_gateway_response(correlation_id, status_code, body)

        consumer.commit()


def start_internal_consumer() -> None:
    consumer = kafka.KafkaConsumer(
        INTERNAL_RESPONSE_TOPIC,
        bootstrap_servers=INTERNAL_KAFKA,
        group_id="order-service-internal",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    logger.info("Internal response consumer started on '%s'", INTERNAL_RESPONSE_TOPIC)

    for message in consumer:
        try:
            handle_internal_response(message.value)
        except Exception as exc:
            logger.error("Unhandled error in internal consumer: %s", exc, exc_info=True)
        finally:
            consumer.commit()

# ---------------------------------------------------------------------------
# Health-check surface
# ---------------------------------------------------------------------------

health_app = Flask("order-service-health")

@health_app.route("/health")
def health():
    return jsonify({"status": "healthy"})

def start_health_server() -> None:
    health_app.run(host="0.0.0.0", port=8000, debug=False)

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

# Initialised at module level so startup is independent of how the module
# is loaded — whether via `python order_service.py` or imported by a
# process supervisor. Producers and consumer threads must be running before
# any work can be processed.
gateway_producer  = _build_producer(GATEWAY_KAFKA)
internal_producer = _build_producer(INTERNAL_KAFKA)

threading.Thread(target=start_internal_consumer, daemon=True, name="internal-consumer").start()
threading.Thread(target=start_health_server, daemon=True, name="health-server").start()

if __name__ == "__main__":
    start_gateway_consumer()  # blocks on main thread