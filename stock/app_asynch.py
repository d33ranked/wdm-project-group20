"""
stock_service.py

Kafka-consuming stock microservice.
Handles traffic from two separate Kafka clusters:

  External (gateway Kafka):
    Consumed : gateway.stock
    Produced : gateway.responses

  Internal (internal Kafka):
    Consumed : internal.stock
    Produced : internal.responses

All business logic is shared — the only difference between the two
consumer loops is which topic they consume and which they reply to.
"""

import atexit
import json
import logging
import os
import threading
import time
import uuid
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

GATEWAY_STOCK_TOPIC    = "gateway.stock"
GATEWAY_RESPONSE_TOPIC = "gateway.responses"

INTERNAL_STOCK_TOPIC    = "internal.stock"
INTERNAL_RESPONSE_TOPIC = "internal.responses"

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

class InsufficientStockError(Exception):
    pass

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def db_get_item(conn, item_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT stock, price FROM items WHERE id = %s", (item_id,))
        row = cur.fetchone()
    if row is None:
        raise NotFoundError(f"Item {item_id} not found")
    return {"stock": row[0], "price": row[1]}


def db_create_item(conn, price: int) -> str:
    item_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO items (id, stock, price) VALUES (%s, %s, %s)",
            (item_id, 0, price),
        )
    conn.commit()
    return item_id


def db_batch_init(conn, n: int, starting_stock: int, item_price: int) -> None:
    with conn.cursor() as cur:
        for i in range(n):
            cur.execute(
                "INSERT INTO items (id, stock, price) VALUES (%s, %s, %s) "
                "ON CONFLICT (id) DO UPDATE SET stock = EXCLUDED.stock, price = EXCLUDED.price",
                (str(i), starting_stock, item_price),
            )
    conn.commit()


def db_add_stock(conn, item_id: str, amount: int) -> int:
    """Returns new stock level. Caller must commit/rollback."""
    with conn.cursor() as cur:
        cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
        if cur.fetchone() is None:
            raise NotFoundError(f"Item {item_id} not found")
        cur.execute(
            "UPDATE items SET stock = stock + %s WHERE id = %s RETURNING stock",
            (amount, item_id),
        )
        return cur.fetchone()[0]


def db_add_stock_batch(conn, items: list[tuple[str, int]]) -> dict[str, int]:
    """
    Adds stock for multiple items atomically.
    items: list of (item_id, amount) pairs
    Returns: dict of {item_id: new_stock}
    Raises NotFoundError — caller must rollback on exception.
    """
    item_ids = [item_id for item_id, _ in items]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, stock FROM items WHERE id = ANY(%s) ORDER BY id FOR UPDATE",
            (item_ids,),
        )
        rows = {row[0]: row[1] for row in cur.fetchall()}
        for item_id, _ in items:
            if item_id not in rows:
                raise NotFoundError(f"Item {item_id} not found")
        results = {}
        for item_id, amount in items:
            cur.execute(
                "UPDATE items SET stock = stock + %s WHERE id = %s RETURNING stock",
                (amount, item_id),
            )
            results[item_id] = cur.fetchone()[0]
    return results


def db_subtract_stock(conn, item_id: str, amount: int) -> int:
    """Returns new stock level. Caller must commit/rollback."""
    with conn.cursor() as cur:
        cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
        row = cur.fetchone()
        if row is None:
            raise NotFoundError(f"Item {item_id} not found")
        if row[0] - amount < 0:
            raise InsufficientStockError(f"Item {item_id} has insufficient stock")
        cur.execute(
            "UPDATE items SET stock = stock - %s WHERE id = %s RETURNING stock",
            (amount, item_id),
        )
        return cur.fetchone()[0]


def db_subtract_stock_batch(conn, items: list[tuple[str, int]]) -> dict[str, int]:
    """
    Subtracts stock for multiple items atomically.
    items: list of (item_id, amount) pairs
    Returns: dict of {item_id: new_stock}
    Raises NotFoundError or InsufficientStockError — caller must rollback on exception.
    """
    item_ids = [item_id for item_id, _ in items]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, stock FROM items WHERE id = ANY(%s) ORDER BY id FOR UPDATE",
            (item_ids,),
        )
        rows = {row[0]: row[1] for row in cur.fetchall()}
        for item_id, amount in items:
            if item_id not in rows:
                raise NotFoundError(f"Item {item_id} not found")
            if rows[item_id] - amount < 0:
                raise InsufficientStockError(f"Item {item_id} has insufficient stock")
        results = {}
        for item_id, amount in items:
            cur.execute(
                "UPDATE items SET stock = stock - %s WHERE id = %s RETURNING stock",
                (amount, item_id),
            )
            results[item_id] = cur.fetchone()[0]
    return results

# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

def check_idempotency(conn, idem_key: str | None) -> tuple[int, str] | None:
    """Returns (status_code, body) if already processed, else None."""
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
# Route handlers
# ---------------------------------------------------------------------------

def handle_create_item(conn, path_params, _body, _headers) -> tuple[int, Any]:
    # path: /item/create/<price>
    try:
        price = int(path_params[0])
    except (IndexError, ValueError):
        return 400, {"error": "Expected /item/create/<price>"}
    if price < 0:
        return 400, {"error": "Price cannot be negative"}
    item_id = db_create_item(conn, price)
    return 201, {"item_id": item_id}


def handle_batch_init(conn, path_params, _body, _headers) -> tuple[int, Any]:
    # path: /batch_init/<n>/<starting_stock>/<item_price>
    try:
        n              = int(path_params[0])
        starting_stock = int(path_params[1])
        item_price     = int(path_params[2])
    except (IndexError, ValueError):
        return 400, {"error": "Expected /batch_init/<n>/<starting_stock>/<item_price>"}

    if item_price < 0:
        return 400, {"error": "Price cannot be negative"}

    if starting_stock < 0:
        return 400, {"error": "Starting stock amount can't be negative"}

    db_batch_init(conn, n, starting_stock, item_price)
    return 200, {"msg": "Batch init for stock successful"}


def handle_find_item(conn, path_params, _body, _headers) -> tuple[int, Any]:
    # path: /find/<item_id>
    try:
        item_id = path_params[0]
    except IndexError:
        return 400, {"error": "Missing item_id"}
    try:
        item = db_get_item(conn, item_id)
    except NotFoundError as exc:
        return 400, {"error": str(exc)}
    return 200, {"stock": item["stock"], "price": item["price"]}


def handle_add_stock(conn, path_params, _body, headers) -> tuple[int, Any]:
    # path: /add/<item_id>/<amount>
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")
    cached = check_idempotency(conn, idem_key)
    if cached:
        # !WARN: new_stock in cached body is stale — but idempotency is guaranteed
        return cached
    try:
        item_id = path_params[0]
        amount  = int(path_params[1])
    except (IndexError, ValueError):
        return 400, {"error": "Expected /add/<item_id>/<amount>"}
    if amount < 0:
        return 400, {"error": "Adding negative stock is not allowed, use the subtract endpoint"}
    try:
        new_stock = db_add_stock(conn, item_id, amount)
        body = f"Item: {item_id} stock updated to: {new_stock}"
        save_idempotency(conn, idem_key, 200, body)
        conn.commit()
    except NotFoundError as exc:
        conn.rollback()
        return 400, {"error": str(exc)}
    except Exception:
        conn.rollback()
        raise
    return 200, body


def handle_add_batch_stock(conn, _path_params, body, headers) -> tuple[int, Any]:
    # path: /add_batch
    # body: {"items": [{"item_id": "abc", "amount": 3}, ...]}
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")
    cached = check_idempotency(conn, idem_key)
    if cached:
        # !WARN: new_stock in cached body is stale — but idempotency is guaranteed
        return cached
    try:
        raw_items = body["items"]
        items: list[tuple[str, int]] = [
            (entry["item_id"], int(entry["amount"]))
            for entry in raw_items
        ]
    except (KeyError, TypeError, ValueError):
        return 400, {"error": 'Expected body: {"items": [{"item_id": str, "amount": int}, ...]}'}
    if not items:
        return 400, {"error": "Items list cannot be empty"}
    if any(amount < 0 for _, amount in items):
        return 400, {"error": "Adding negative stock is not allowed, use the subtract endpoint"}
    try:
        results = db_add_stock_batch(conn, items)
        response_body = {"updated_stock": results}
        save_idempotency(conn, idem_key, 200, json.dumps(response_body))
        conn.commit()
    except NotFoundError as exc:
        conn.rollback()
        return 400, {"error": str(exc)}
    except Exception:
        conn.rollback()
        raise
    return 200, response_body


def handle_subtract_stock(conn, path_params, _body, headers) -> tuple[int, Any]:
    # path: /subtract/<item_id>/<amount>
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")
    cached = check_idempotency(conn, idem_key)
    if cached:
        # !WARN: new_stock in cached body is stale — but idempotency is guaranteed
        return cached
    try:
        item_id = path_params[0]
        amount  = int(path_params[1])
    except (IndexError, ValueError):
        return 400, {"error": "Expected /subtract/<item_id>/<amount>"}
    if amount < 0:
        return 400, {"error": "Subtracting negative stock is not allowed, use the add endpoint"}
    try:
        new_stock = db_subtract_stock(conn, item_id, amount)
        body = f"Item: {item_id} stock updated to: {new_stock}"
        save_idempotency(conn, idem_key, 200, body)
        conn.commit()
    except (NotFoundError, InsufficientStockError) as exc:
        conn.rollback()
        return 400, {"error": str(exc)}
    except Exception:
        conn.rollback()
        raise
    return 200, body


def handle_subtract_batch_stock(conn, _path_params, body, headers) -> tuple[int, Any]:
    # path: /subtract_batch
    # body: {"items": [{"item_id": "abc", "amount": 3}, ...]}
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")
    cached = check_idempotency(conn, idem_key)
    if cached:
        # !WARN: new_stock in cached body is stale — but idempotency is guaranteed
        return cached
    try:
        raw_items = body["items"]
        items: list[tuple[str, int]] = [
            (entry["item_id"], int(entry["amount"]))
            for entry in raw_items
        ]
    except (KeyError, TypeError, ValueError):
        return 400, {"error": 'Expected body: {"items": [{"item_id": str, "amount": int}, ...]}'}
    if not items:
        return 400, {"error": "Items list cannot be empty"}
    if any(amount < 0 for _, amount in items):
        return 400, {"error": "Subtracting negative stock is not allowed, use the add endpoint"}
    try:
        results = db_subtract_stock_batch(conn, items)
        response_body = {"updated_stock": results}
        save_idempotency(conn, idem_key, 200, json.dumps(response_body))
        conn.commit()
    except (NotFoundError, InsufficientStockError) as exc:
        conn.rollback()
        return 400, {"error": str(exc)}
    except Exception:
        conn.rollback()
        raise
    return 200, response_body

# ---------------------------------------------------------------------------
# Routing table
# ---------------------------------------------------------------------------

ROUTES: list[tuple[str, str, callable]] = [
    ("POST", "/item/create/",    handle_create_item),
    ("POST", "/batch_init/",     handle_batch_init),
    ("GET",  "/find/",           handle_find_item),
    ("POST", "/add/",            handle_add_stock),
    ("POST", "/add_batch/",      handle_add_batch_stock),
    ("POST", "/subtract/",       handle_subtract_stock),
    ("POST", "/subtract_batch/", handle_subtract_batch_stock),
]


def route(payload: dict[str, Any], conn) -> tuple[int, Any]:
    method  = payload.get("method", "GET").upper()
    path    = payload.get("path", "/")
    body    = payload.get("body") or {}
    headers = payload.get("headers") or {}

    segments            = [s for s in path.strip("/").split("/") if s]
    path_params         = segments[1:]  # drop "stock" service prefix
    clean_path          = "/" + "/".join(segments[1:]) if len(segments) > 1 else "/"
    clean_path_with_slash = clean_path if clean_path.endswith("/") else clean_path + "/"

    for route_method, prefix, handler in ROUTES:
        if method == route_method and clean_path_with_slash.startswith(prefix):
            # /item/create/<price> has an extra segment before the param
            if prefix.startswith("/item/"):
                return handler(conn, path_params[1:], body, headers)
            return handler(conn, path_params, body, headers)

    return 404, {"error": f"No handler for {method} {path}"}

# ---------------------------------------------------------------------------
# Kafka producers — one per cluster, built at startup
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


def _publish_response(
    producer: kafka.KafkaProducer,
    response_topic: str,
    correlation_id: str,
    status_code: int,
    body: Any,
) -> None:
    payload = {
        "correlation_id": correlation_id,
        "status_code": status_code,
        "body": body,
    }
    try:
        producer.send(response_topic, key=correlation_id, value=payload)
        producer.flush(timeout=5)
    except kafka.errors.KafkaError as exc:
        logger.error("Failed to publish response for %s: %s", correlation_id, exc)

# ---------------------------------------------------------------------------
# Shared consumer logic
# ---------------------------------------------------------------------------

def _run_consumer(
    consume_bootstrap: str,
    consume_topic: str,
    consume_group: str,
    producer: kafka.KafkaProducer,
    response_topic: str,
) -> None:
    consumer = kafka.KafkaConsumer(
        consume_topic,
        bootstrap_servers=consume_bootstrap,
        group_id=consume_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    logger.info("Stock consumer started on '%s' → replies to '%s'", consume_topic, response_topic)

    for message in consumer:
        payload        = message.value
        correlation_id = payload.get("correlation_id")

        if not correlation_id:
            consumer.commit()
            continue

        conn = conn_pool.getconn()
        try:
            status_code, body = route(payload, conn)
        except Exception as exc:
            logger.error("Unhandled error processing %s: %s", correlation_id, exc, exc_info=True)
            try:
                conn.rollback()
            except Exception:
                pass
            status_code, body = 500, {"error": "Internal server error"}
        finally:
            conn_pool.putconn(conn)

        _publish_response(producer, response_topic, correlation_id, status_code, body)

        # Commit offset only after response is published — on crash and redeliver,
        # idempotency keys guarantee the DB write is a safe no-op.
        consumer.commit()

# ---------------------------------------------------------------------------
# Health-check surface
# ---------------------------------------------------------------------------

health_app = Flask("stock-service-health")

@health_app.route("/health")
def health():
    return jsonify({"status": "healthy"})

def start_health_server() -> None:
    health_app.run(host="0.0.0.0", port=8000, debug=False)

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    gateway_producer  = _build_producer(GATEWAY_KAFKA)
    internal_producer = _build_producer(INTERNAL_KAFKA)

    threading.Thread(
        target=_run_consumer,
        args=(
            INTERNAL_KAFKA,
            INTERNAL_STOCK_TOPIC,
            "stock-service-internal",
            internal_producer,
            INTERNAL_RESPONSE_TOPIC,
        ),
        daemon=True,
        name="internal-consumer",
    ).start()

    threading.Thread(
        target=start_health_server,
        daemon=True,
        name="health-server",
    ).start()

    # Gateway consumer — main thread
    _run_consumer(
        GATEWAY_KAFKA,
        GATEWAY_STOCK_TOPIC,
        "stock-service-gateway",
        gateway_producer,
        GATEWAY_RESPONSE_TOPIC,
    )