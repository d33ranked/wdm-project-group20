"""
order_service.py

Kafka-consuming order microservice.
Communicates with stock and payment services exclusively via Kafka topics,
and publishes responses back to gateway.responses.

Topics consumed : gateway.orders
Topics produced : gateway.stock, gateway.payment, gateway.responses
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

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ORDER_TOPIC    = "gateway.orders"
STOCK_TOPIC    = "gateway.stock"
PAYMENT_TOPIC  = "gateway.payment"
RESPONSE_TOPIC = "gateway.responses"

# How long to wait for a response from stock/payment services (seconds)
INTERNAL_TIMEOUT_S = int(os.environ.get("INTERNAL_TIMEOUT_MS", "10000")) / 1000

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
                logger.warning(
                    "PostgreSQL not ready, retrying in %ds… (%d/%d)",
                    delay, attempt + 1, retries,
                )
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

class AlreadyPaidError(Exception):
    pass


# ---------------------------------------------------------------------------
# DB helpers
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
    """Locks the row for the duration of the transaction."""
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
    """Appends item to order and returns updated total_cost. Caller must commit."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT items, total_cost FROM orders WHERE id = %s FOR UPDATE",
            (order_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise NotFoundError(f"Order {order_id} not found")
        items, total_cost = row
        items.append([item_id, int(quantity)])
        total_cost += int(quantity) * item_price
        cur.execute(
            "UPDATE orders SET items = %s, total_cost = %s WHERE id = %s",
            (json.dumps(items), total_cost, order_id),
        )
    return total_cost


def db_mark_paid(conn, order_id: str) -> None:
    """Marks order as paid. Caller must commit."""
    with conn.cursor() as cur:
        cur.execute("UPDATE orders SET paid = TRUE WHERE id = %s", (order_id,))


# ---------------------------------------------------------------------------
# Idempotency
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
# Internal Kafka RPC client
#
# The order service needs synchronous responses from stock and payment during
# checkout. We reuse the same event-based pattern as the gateway's KafkaClient
# but scoped to this service's in-process needs.
# ---------------------------------------------------------------------------

class InternalKafkaClient:
    """
    Sends a request to a service topic and blocks until the correlated
    response arrives on gateway.responses.

    A single shared response consumer handles all in-flight requests.
    """

    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self._producer = _build_producer(bootstrap_servers)
        self._pending: dict[str, tuple[threading.Event, dict | None]] = {}
        self._pending_lock = threading.Lock()
        self._start_response_consumer()

    def call(
        self,
        topic: str,
        method: str,
        path: str,
        body: dict | None = None,
        headers: dict | None = None,
    ) -> dict[str, Any]:
        correlation_id = str(uuid.uuid4())
        event = threading.Event()

        with self._pending_lock:
            self._pending[correlation_id] = (event, None)

        payload = {
            "correlation_id": correlation_id,
            "method": method.upper(),
            "path": path,
            "headers": headers or {},
            "body": body or {},
        }

        try:
            self._producer.send(topic, key=correlation_id, value=payload).get(timeout=10)
        except kafka.errors.KafkaError as exc:
            self._remove_pending(correlation_id)
            raise RuntimeError(f"Failed to enqueue internal request: {exc}") from exc

        if not event.wait(timeout=INTERNAL_TIMEOUT_S):
            self._remove_pending(correlation_id)
            raise TimeoutError(f"Timeout waiting for response from topic '{topic}'")

        with self._pending_lock:
            _, response = self._pending.pop(correlation_id)

        return response

    def _remove_pending(self, correlation_id: str) -> None:
        with self._pending_lock:
            self._pending.pop(correlation_id, None)

    def _start_response_consumer(self) -> None:
        def consume() -> None:
            consumer = kafka.KafkaConsumer(
                RESPONSE_TOPIC,
                bootstrap_servers=self.bootstrap_servers,
                # Unique group so this consumer gets ALL responses, not just
                # a partition subset — the gateway has its own group.
                group_id=f"order-service-internal-{uuid.uuid4()}",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("Order service internal response consumer started")
            try:
                for message in consumer:
                    self._handle_response(message.value)
            except Exception as exc:
                logger.error("Internal response consumer crashed: %s", exc, exc_info=True)
            finally:
                consumer.close()

        thread = threading.Thread(target=consume, daemon=True, name="order-internal-response-consumer")
        thread.start()

    def _handle_response(self, payload: dict[str, Any]) -> None:
        correlation_id = payload.get("correlation_id")
        if not correlation_id:
            return
        with self._pending_lock:
            entry = self._pending.get(correlation_id)
            if entry is None:
                return   # not ours — belongs to the gateway consumer
            event, _ = entry
            self._pending[correlation_id] = (event, payload)
        event.set()


# ---------------------------------------------------------------------------
# Checkout saga helpers
# ---------------------------------------------------------------------------

def _subtract_stock(
    client: InternalKafkaClient,
    item_id: str,
    quantity: int,
    idempotency_key: str,
) -> bool:
    """Returns True on success, False if out of stock."""
    try:
        response = client.call(
            topic=STOCK_TOPIC,
            method="POST",
            path=f"/stock/subtract/{item_id}/{quantity}",
            headers={"Idempotency-Key": idempotency_key},
        )
        return response.get("status_code") == 200
    except (RuntimeError, TimeoutError) as exc:
        logger.error("Stock subtract failed for %s: %s", item_id, exc)
        return False


def _rollback_stock(
    client: InternalKafkaClient,
    removed_items: list[tuple[str, int]],
    transaction_id: str,
) -> None:
    """Best-effort rollback — idempotency keys make retries safe."""

    batch_items = [{"item_id": item_id, "amount": qty} for item_id, qty in removed_items.items()]
    try:
        stock_response = client.call(
            topic=STOCK_TOPIC,
            method="POST",
            path="/stock/add_batch",
            body={"items": batch_items},
            headers={"Idempotency-Key": f"{transaction_id}:stock:add_batch"},
        )
    except Exception as exc:
        logger.error("Stock rollback failed for transaction: %s: %s", transaction_id, exc)

def _deduct_payment(
    client: InternalKafkaClient,
    user_id: str,
    total_cost: int,
    idempotency_key: str,
) -> bool:
    """Returns True on success, False if insufficient funds."""
    try:
        response = client.call(
            topic=PAYMENT_TOPIC,
            method="POST",
            path=f"/payment/pay/{user_id}/{total_cost}",
            headers={"Idempotency-Key": idempotency_key},
        )
        return response.get("status_code") == 200
    except (RuntimeError, TimeoutError) as exc:
        logger.error("Payment failed for user %s: %s", user_id, exc)
        return False


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

def handle_create_order(conn, path_params, _body, _headers, _client) -> tuple[int, Any]:
    # path: /create/<user_id>
    try:
        user_id = path_params[0]
    except IndexError:
        return 400, {"error": "Missing user_id"}
    order_id = db_create_order(conn, user_id)
    return 201, {"order_id": order_id}


def handle_batch_init(conn, path_params, _body, _headers, _client) -> tuple[int, Any]:
    # path: /batch_init/<n>/<n_items>/<n_users>/<item_price>
    try:
        n          = int(path_params[0])
        n_items    = int(path_params[1])
        n_users    = int(path_params[2])
        item_price = int(path_params[3])
    except (IndexError, ValueError):
        return 400, {"error": "Expected /batch_init/<n>/<n_items>/<n_users>/<item_price>"}
    db_batch_init(conn, n, n_items, n_users, item_price)
    return 200, {"msg": "Batch init for orders successful"}


def handle_find_order(conn, path_params, _body, _headers, _client) -> tuple[int, Any]:
    # path: /find/<order_id>
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


def handle_add_item(conn, path_params, _body, _headers, client: InternalKafkaClient) -> tuple[int, Any]:
    # path: /addItem/<order_id>/<item_id>/<quantity>
    try:
        order_id = path_params[0]
        item_id  = path_params[1]
        quantity = int(path_params[2])
    except (IndexError, ValueError):
        return 400, {"error": "Expected /addItem/<order_id>/<item_id>/<quantity>"}

    # Fetch item price from stock service
    try:
        stock_response = client.call(
            topic=STOCK_TOPIC,
            method="GET",
            path=f"/stock/find/{item_id}",
        )
    except (RuntimeError, TimeoutError) as exc:
        return 503, {"error": f"Stock service unavailable: {exc}"}

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


def handle_checkout(conn, path_params, _body, headers, client: InternalKafkaClient) -> tuple[int, Any]:
    # path: /checkout/<order_id>
    try:
        order_id = path_params[0]
    except IndexError:
        return 400, {"error": "Missing order_id"}

    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")
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

    # Aggregate quantities so we subtract each item once
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order["items"]:
        items_quantities[item_id] += quantity

    transaction_id = idem_key or str(uuid.uuid4())

    # --- Saga step 1: subtract all stock in a single batch round-trip ---
    batch_items = [{"item_id": item_id, "amount": qty} for item_id, qty in items_quantities.items()]
    try:
        stock_response = client.call(
            topic=STOCK_TOPIC,
            method="POST",
            path="/stock/subtract_batch",
            body={"items": batch_items},
            headers={"Idempotency-Key": f"{transaction_id}:stock:subtract_batch"},
        )
    except (RuntimeError, TimeoutError) as exc:
        conn.rollback()
        return 503, {"error": f"Stock service unavailable: {exc}"}

    if stock_response.get("status_code") != 200:
        # Batch is atomic — if it failed, nothing was subtracted, no rollback needed
        conn.rollback()
        return 400, {"error": stock_response.get("body", {}).get("error", "Stock subtraction failed")}

    # --- Saga step 2: deduct payment ---
    payment_ok = _deduct_payment(
        client, order["user_id"], order["total_cost"],
        idempotency_key=f"{transaction_id}:payment:pay",
    )
    if not payment_ok:
        # Stock committed — we must compensate the entire batch
        _rollback_stock(client, list(items_quantities.items()), transaction_id)
        conn.rollback()
        return 400, {"error": "User has insufficient credit"}

    # --- Saga step 3: mark order paid + save idempotency atomically ---
    try:
        db_mark_paid(conn, order_id)
        save_idempotency(conn, idem_key, 200, "Checkout successful")
        conn.commit()
    except Exception:
        logger.critical(
            "SAGA INCONSISTENCY: payment and stock committed but order %s not marked paid. "
            "transaction_id=%s", order_id, transaction_id,
        )
        conn.rollback()
        raise

    return 200, "Checkout successful"
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


def route(payload: dict[str, Any], conn, client: InternalKafkaClient) -> tuple[int, Any]:
    method  = payload.get("method", "GET").upper()
    path    = payload.get("path", "/")
    body    = payload.get("body") or {}
    headers = payload.get("headers") or {}

    segments    = [s for s in path.strip("/").split("/") if s]
    path_params = segments[1:]  # drop the "orders" gateway prefix
    clean_path  = "/" + "/".join(segments[1:]) + "/" if len(segments) > 1 else "/"

    for route_method, prefix, handler in ROUTES:
        if method == route_method and clean_path.startswith(prefix):
            return handler(conn, path_params, body, headers, client)

    return 404, {"error": f"No handler for {method} {path}"}


# ---------------------------------------------------------------------------
# Kafka producer / consumer
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


def _publish_response(producer, correlation_id: str, status_code: int, body: Any) -> None:
    payload = {
        "correlation_id": correlation_id,
        "status_code": status_code,
        "body": body,
    }
    try:
        producer.send(RESPONSE_TOPIC, key=correlation_id, value=payload)
        producer.flush(timeout=5)
    except kafka.errors.KafkaError as exc:
        logger.error("Failed to publish response for %s: %s", correlation_id, exc)


def start_consumer(client: InternalKafkaClient) -> None:
    producer = _build_producer(KAFKA_BOOTSTRAP_SERVERS)
    consumer = kafka.KafkaConsumer(
        ORDER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="order-service",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    logger.info("Order service consuming from '%s'", ORDER_TOPIC)

    for message in consumer:
        payload        = message.value
        correlation_id = payload.get("correlation_id")

        if not correlation_id:
            consumer.commit()
            continue

        conn = conn_pool.getconn()
        try:
            status_code, body = route(payload, conn, client)
        except Exception as exc:
            logger.error("Unhandled error processing %s: %s", correlation_id, exc, exc_info=True)
            try:
                conn.rollback()
            except Exception:
                pass
            status_code, body = 500, {"error": "Internal server error"}
        finally:
            conn_pool.putconn(conn)

        _publish_response(producer, correlation_id, status_code, body)
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

if __name__ == "__main__":
    internal_client = InternalKafkaClient(KAFKA_BOOTSTRAP_SERVERS)

    health_thread = threading.Thread(
        target=start_health_server,
        daemon=True,
        name="health-server",
    )
    health_thread.start()

    start_consumer(internal_client)  # blocks on main thread