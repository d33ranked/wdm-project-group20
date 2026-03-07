"""
payment_service.py

Kafka-consuming payment microservice.
Handles traffic from two separate Kafka clusters:

  External (gateway Kafka):
    Consumed : gateway.payment
    Produced : gateway.responses

  Internal (internal Kafka):
    Consumed : internal.payment
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

GATEWAY_PAYMENT_TOPIC  = "gateway.payment"
GATEWAY_RESPONSE_TOPIC = "gateway.responses"

INTERNAL_PAYMENT_TOPIC  = "internal.payment"
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

class InsufficientFundsError(Exception):
    pass

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def db_get_user(conn, user_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT credit FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
    if row is None:
        raise NotFoundError(f"User {user_id} not found")
    return {"credit": row[0]}


def db_create_user(conn) -> str:
    user_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users (id, credit) VALUES (%s, %s)", (user_id, 0))
    conn.commit()
    return user_id


def db_batch_init(conn, n: int, starting_money: int) -> None:
    with conn.cursor() as cur:
        for i in range(n):
            cur.execute(
                "INSERT INTO users (id, credit) VALUES (%s, %s) "
                "ON CONFLICT (id) DO UPDATE SET credit = EXCLUDED.credit",
                (str(i), starting_money),
            )
    conn.commit()


def db_add_funds(conn, user_id: str, amount: int) -> int:
    """Returns new credit. Caller must commit/rollback."""
    with conn.cursor() as cur:
        cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
        if cur.fetchone() is None:
            raise NotFoundError(f"User {user_id} not found")
        cur.execute(
            "UPDATE users SET credit = credit + %s WHERE id = %s RETURNING credit",
            (amount, user_id),
        )
        return cur.fetchone()[0]


def db_remove_credit(conn, user_id: str, amount: int) -> int:
    """Returns new credit. Caller must commit/rollback."""
    with conn.cursor() as cur:
        cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
        row = cur.fetchone()
        if row is None:
            raise NotFoundError(f"User {user_id} not found")
        if row[0] - amount < 0:
            raise InsufficientFundsError(f"User {user_id} has insufficient funds")
        cur.execute(
            "UPDATE users SET credit = credit - %s WHERE id = %s RETURNING credit",
            (amount, user_id),
        )
        return cur.fetchone()[0]

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

def handle_create_user(conn, _path_params, _body, _headers) -> tuple[int, Any]:
    user_id = db_create_user(conn)
    return 201, {"user_id": user_id}


def handle_batch_init(conn, path_params, _body, _headers) -> tuple[int, Any]:
    # path: /batch_init/<n>/<starting_money>
    try:
        n              = int(path_params[0])
        starting_money = int(path_params[1])
    except (IndexError, ValueError):
        return 400, {"error": "Expected /batch_init/<n>/<starting_money>"}

    if starting_money < 0:
        return 400, {"error", "Can not start with a negative amount of money"}

    db_batch_init(conn, n, starting_money)
    return 200, {"msg": "Batch init for users successful"}


def handle_find_user(conn, path_params, _body, _headers) -> tuple[int, Any]:
    # path: /find_user/<user_id>
    try:
        user_id = path_params[0]
    except IndexError:
        return 400, {"error": "Missing user_id"}
    try:
        user = db_get_user(conn, user_id)
    except NotFoundError as exc:
        return 400, {"error": str(exc)}
    return 200, {"user_id": user_id, "credit": user["credit"]}


def handle_add_funds(conn, path_params, _body, headers) -> tuple[int, Any]:
    # path: /add_funds/<user_id>/<amount>
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    cached = check_idempotency(conn, idem_key)
    if cached:
        # !WARN: new_credit in cached body is stale — but idempotency is guaranteed
        return cached

    try:
        user_id = path_params[0]
        amount  = int(path_params[1])
    except (IndexError, ValueError):
        return 400, {"error": "Expected /add_funds/<user_id>/<amount>"}

    if amount < 0:
        return 400, {"error": "Adding negative funds is not allowed, use the pay endpoint"}

    try:
        new_credit = db_add_funds(conn, user_id, amount)
        body = f"User: {user_id} credit updated to: {new_credit}"
        save_idempotency(conn, idem_key, 200, body)
        conn.commit()
    except NotFoundError as exc:
        conn.rollback()
        return 400, {"error": str(exc)}
    except Exception:
        conn.rollback()
        raise

    return 200, body


def handle_pay(conn, path_params, _body, headers) -> tuple[int, Any]:
    # path: /pay/<user_id>/<amount>
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    cached = check_idempotency(conn, idem_key)
    if cached:
        # !WARN: new_credit in cached body is stale — but idempotency is guaranteed
        return cached

    try:
        user_id = path_params[0]
        amount  = int(path_params[1])
    except (IndexError, ValueError):
        return 400, {"error": "Expected /pay/<user_id>/<amount>"}

    if amount < 0:
        return 400, {"error": "Paying negative money is not allowed, use the add_funds endpoint"}

    try:
        new_credit = db_remove_credit(conn, user_id, amount)
        body = f"User: {user_id} credit updated to: {new_credit}"
        save_idempotency(conn, idem_key, 200, body)
        conn.commit()
    except (NotFoundError, InsufficientFundsError) as exc:
        conn.rollback()
        return 400, {"error": str(exc)}
    except Exception:
        conn.rollback()
        raise

    return 200, body

# ---------------------------------------------------------------------------
# Routing table
# ---------------------------------------------------------------------------

ROUTES: list[tuple[str, str, callable]] = [
    ("POST", "/create_user", handle_create_user),
    ("POST", "/batch_init/", handle_batch_init),
    ("GET",  "/find_user/",  handle_find_user),
    ("POST", "/add_funds/",  handle_add_funds),
    ("POST", "/pay/",        handle_pay),
]


def route(payload: dict[str, Any], conn) -> tuple[int, Any]:
    method      = payload.get("method", "GET").upper()
    path        = payload.get("path", "/")
    body        = payload.get("body") or {}
    headers     = payload.get("headers") or {}
    path_params = [s for s in path.strip("/").split("/") if s][1:]  # drop service prefix

    for route_method, prefix, handler in ROUTES:
        if method == route_method and path.startswith(prefix):
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
#
# Both consumer loops do identical work — the only difference is which
# Kafka cluster they connect to and which response topic they publish to.
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
    logger.info("Payment consumer started on '%s' → replies to '%s'", consume_topic, response_topic)

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

health_app = Flask("payment-service-health")

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

    # Internal consumer — daemon thread (internal Kafka cluster)
    threading.Thread(
        target=_run_consumer,
        args=(
            INTERNAL_KAFKA,
            INTERNAL_PAYMENT_TOPIC,
            "payment-service-internal",
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
        GATEWAY_PAYMENT_TOPIC,
        "payment-service-gateway",
        gateway_producer,
        GATEWAY_RESPONSE_TOPIC,
    )