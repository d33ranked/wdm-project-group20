import gevent.monkey
gevent.monkey.patch_all()

import os
import json
import uuid
import time
import atexit
import hashlib
import logging
import threading
from typing import Any

import psycopg2
import psycopg2.pool

from time import perf_counter
from flask import Flask, jsonify, abort, request, Response, g

DB_ERROR_STR = "DB error"
TRANSACTION_MODE = os.environ.get("TRANSACTION_MODE", "TPC")

GATEWAY_KAFKA = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-external:9092")
INTERNAL_KAFKA = os.environ.get("INTERNAL_KAFKA_BOOTSTRAP_SERVERS", "kafka-internal:9092")

GATEWAY_PAYMENT_TOPIC = "gateway.payment"
GATEWAY_RESPONSE_TOPIC = "gateway.responses"
INTERNAL_PAYMENT_TOPIC = "internal.payment"
INTERNAL_RESPONSE_TOPIC = "internal.responses"

app = Flask("payment-service")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DB pool
# ---------------------------------------------------------------------------

def create_conn_pool(retries=10, delay=2):
    for attempt in range(retries):
        try:
            return psycopg2.pool.ThreadedConnectionPool(
                minconn=10,
                maxconn=100,
                host=os.environ["POSTGRES_HOST"],
                port=int(os.environ["POSTGRES_PORT"]),
                dbname=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
            )
        except psycopg2.OperationalError:
            if attempt < retries - 1:
                print(
                    f"PAYMENT: PostgreSQL not ready, retrying in {delay}s... (attempt {attempt+1}/{retries})"
                )
                time.sleep(delay)
            else:
                raise


conn_pool = create_conn_pool()


def close_db_connection():
    conn_pool.closeall()


atexit.register(close_db_connection)

# ---------------------------------------------------------------------------
# Flask request lifecycle
# ---------------------------------------------------------------------------

@app.before_request
def start_timer():
    g.start_time = perf_counter()
    g.conn = conn_pool.getconn()


@app.after_request
def log_response(response):
    duration = perf_counter() - g.start_time
    print(f"PAYMENT: Request took {duration:.7f} seconds")
    return response


@app.teardown_request
def teardown_request(exception):
    conn = g.pop("conn", None)
    if conn is not None:
        if exception:
            conn.rollback()
        else:
            conn.commit()
        conn_pool.putconn(conn)

# ---------------------------------------------------------------------------
# Idempotency helpers
# ---------------------------------------------------------------------------

def idempotency_token(key: str) -> int:
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2**31)


def check_idempotency():
    idem_key = request.headers.get("Idempotency-Key")
    if not idem_key:
        return None
    cur = g.conn.cursor()
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (idempotency_token(idem_key),))
    cur.execute(
        "SELECT status_code, body FROM idempotency_keys WHERE key = %s", (idem_key,)
    )
    row = cur.fetchone()
    cur.close()
    if row is not None:
        return Response(row[1], status=row[0])
    return None


def save_idempotency(status_code, body):
    idem_key = request.headers.get("Idempotency-Key")
    if not idem_key:
        return
    cur = g.conn.cursor()
    cur.execute(
        "INSERT INTO idempotency_keys (key, status_code, body) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
        (idem_key, status_code, body),
    )
    cur.close()


def check_idempotency_kafka(conn, idem_key):
    if not idem_key:
        return None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status_code, body FROM idempotency_keys WHERE key = %s",
            (idem_key,),
        )
        row = cur.fetchone()
    return (row[0], row[1]) if row else None


def save_idempotency_kafka(conn, idem_key, status_code, body):
    if not idem_key:
        return
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO idempotency_keys (key, status_code, body) "
            "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (idem_key, status_code, body),
        )

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_user_from_db(user_id: str):
    cur = g.conn.cursor()
    cur.execute("SELECT credit FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    if row is None:
        abort(400, f"User: {user_id} not found!")
    return {"credit": row[0]}

# ---------------------------------------------------------------------------
# Flask HTTP endpoints
# ---------------------------------------------------------------------------

@app.post("/create_user")
def create_user():
    key = str(uuid.uuid4())
    cur = g.conn.cursor()
    cur.execute("INSERT INTO users (id, credit) VALUES (%s, %s)", (key, 0))
    cur.close()
    return jsonify({"user_id": key})


@app.post("/batch_init/<n>/<starting_money>")
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    cur = g.conn.cursor()
    for i in range(n):
        cur.execute(
            "INSERT INTO users (id, credit) VALUES (%s, %s) "
            "ON CONFLICT (id) DO UPDATE SET credit = EXCLUDED.credit",
            (str(i), starting_money),
        )
    cur.close()
    return jsonify({"msg": "Batch init for users successful"})


@app.get("/find_user/<user_id>")
def find_user(user_id: str):
    user = get_user_from_db(user_id)
    return jsonify({"user_id": user_id, "credit": user["credit"]})


@app.post("/add_funds/<user_id>/<amount>")
def add_credit(user_id: str, amount: int):
    cached = check_idempotency()
    if cached is not None:
        return cached

    cur = g.conn.cursor()
    cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"User: {user_id} not found!")
    cur.execute(
        "UPDATE users SET credit = credit + %s WHERE id = %s RETURNING credit",
        (int(amount), user_id),
    )
    new_credit = cur.fetchone()[0]
    cur.close()

    body = f"User: {user_id} credit updated to: {new_credit}"
    save_idempotency(200, body)
    return Response(body, status=200)


@app.post("/pay/<user_id>/<amount>")
def remove_credit(user_id: str, amount: int):
    cached = check_idempotency()
    if cached is not None:
        return cached

    cur = g.conn.cursor()
    cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"User: {user_id} not found!")
    current_credit = row[0]
    if current_credit - int(amount) < 0:
        cur.close()
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    cur.execute(
        "UPDATE users SET credit = credit - %s WHERE id = %s RETURNING credit",
        (int(amount), user_id),
    )
    new_credit = cur.fetchone()[0]
    cur.close()

    body = f"User: {user_id} credit updated to: {new_credit}"
    save_idempotency(200, body)
    return Response(body, status=200)


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})

# ---------------------------------------------------------------------------
# 2PC endpoints (TPC mode)
# ---------------------------------------------------------------------------

@app.post("/prepare/<txn_id>/<user_id>/<amount>")
def prepare_transaction(txn_id: str, user_id: str, amount: int):
    amount = int(amount)
    cur = g.conn.cursor()

    cur.execute("SELECT 1 FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
    if cur.fetchone() is not None:
        cur.close()
        return Response("Transaction already prepared", status=200)

    cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"User: {user_id} not found!")
    current_credit = row[0]
    if current_credit < amount:
        cur.close()
        abort(400, f"User: {user_id} has insufficient credit!")

    cur.execute(
        "UPDATE users SET credit = credit - %s WHERE id = %s", (amount, user_id)
    )
    cur.execute(
        "INSERT INTO prepared_transactions (txn_id, user_id, amount) VALUES (%s, %s, %s)",
        (txn_id, user_id, amount),
    )
    cur.close()
    return Response("Transaction prepared", status=200)


@app.post("/commit/<txn_id>")
def commit_transaction(txn_id: str):
    cur = g.conn.cursor()
    cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
    cur.close()
    return Response("Transaction committed", status=200)


@app.post("/abort/<txn_id>")
def abort_transaction(txn_id: str):
    cur = g.conn.cursor()
    cur.execute(
        "SELECT user_id, amount FROM prepared_transactions WHERE txn_id = %s", (txn_id,)
    )
    row = cur.fetchone()
    if row is not None:
        user_id, amount = row
        cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
        if cur.fetchone() is not None:
            cur.execute(
                "UPDATE users SET credit = credit + %s WHERE id = %s", (amount, user_id)
            )
        cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
    cur.close()
    return Response("Transaction aborted", status=200)


def recovery_tpc():
    conn = conn_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT txn_id, user_id, amount FROM prepared_transactions "
            "WHERE created_at < NOW() - INTERVAL '5 minutes'"
        )
        rows = cur.fetchall()
        if not rows:
            cur.close()
            app.logger.info("RECOVERY: No stale prepared transactions found")
            return
        for txn_id, user_id, amount in rows:
            app.logger.warning(
                f"RECOVERY: Aborting stale prepared transaction txn={txn_id}, user={user_id}"
            )
            cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
            if cur.fetchone() is not None:
                cur.execute(
                    "UPDATE users SET credit = credit + %s WHERE id = %s",
                    (amount, user_id),
                )
            cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
            conn.commit()
        cur.close()
    finally:
        conn_pool.putconn(conn)

# ---------------------------------------------------------------------------
# Kafka consumer routing (SAGA mode)
# ---------------------------------------------------------------------------

def route_kafka_message(payload, conn):
    """Route a Kafka message to the correct handler, return (status_code, body)."""
    method = payload.get("method", "GET").upper()
    path = payload.get("path", "/")
    headers = payload.get("headers") or {}

    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    if method == "POST" and segments and segments[0] == "create_user":
        user_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (id, credit) VALUES (%s, %s)", (user_id, 0))
        conn.commit()
        return 201, {"user_id": user_id}

    if method == "POST" and len(segments) >= 3 and segments[0] == "batch_init":
        n, starting_money = int(segments[1]), int(segments[2])
        with conn.cursor() as cur:
            for i in range(n):
                cur.execute(
                    "INSERT INTO users (id, credit) VALUES (%s, %s) "
                    "ON CONFLICT (id) DO UPDATE SET credit = EXCLUDED.credit",
                    (str(i), starting_money),
                )
        conn.commit()
        return 200, {"msg": "Batch init for users successful"}

    if method == "GET" and len(segments) >= 2 and segments[0] == "find_user":
        user_id = segments[1]
        with conn.cursor() as cur:
            cur.execute("SELECT credit FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
        if row is None:
            return 400, {"error": f"User {user_id} not found"}
        return 200, {"user_id": user_id, "credit": row[0]}

    if method == "POST" and len(segments) >= 3 and segments[0] == "add_funds":
        user_id, amount = segments[1], int(segments[2])
        cached = check_idempotency_kafka(conn, idem_key)
        if cached:
            return cached
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
                if cur.fetchone() is None:
                    return 400, {"error": f"User {user_id} not found"}
                cur.execute("UPDATE users SET credit = credit + %s WHERE id = %s RETURNING credit", (amount, user_id))
                new_credit = cur.fetchone()[0]
            resp_body = f"User: {user_id} credit updated to: {new_credit}"
            save_idempotency_kafka(conn, idem_key, 200, resp_body)
            conn.commit()
            return 200, resp_body
        except Exception:
            conn.rollback()
            raise

    if method == "POST" and len(segments) >= 3 and segments[0] == "pay":
        user_id, amount = segments[1], int(segments[2])
        cached = check_idempotency_kafka(conn, idem_key)
        if cached:
            return cached
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
                row = cur.fetchone()
                if row is None:
                    return 400, {"error": f"User {user_id} not found"}
                if row[0] - amount < 0:
                    return 400, {"error": f"User {user_id} has insufficient funds"}
                cur.execute("UPDATE users SET credit = credit - %s WHERE id = %s RETURNING credit", (amount, user_id))
                new_credit = cur.fetchone()[0]
            resp_body = f"User: {user_id} credit updated to: {new_credit}"
            save_idempotency_kafka(conn, idem_key, 200, resp_body)
            conn.commit()
            return 200, resp_body
        except Exception:
            conn.rollback()
            raise

    return 404, {"error": f"No handler for {method} {path}"}

# ---------------------------------------------------------------------------
# Kafka consumer loops (SAGA mode)
# ---------------------------------------------------------------------------

def _build_kafka_producer(bootstrap_servers):
    import kafka
    return kafka.KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all", retries=3, linger_ms=5, batch_size=32_768,
    )


def _publish_response(producer, response_topic, correlation_id, status_code, body):
    import kafka as kafka_mod
    payload = {"correlation_id": correlation_id, "status_code": status_code, "body": body}
    try:
        producer.send(response_topic, key=correlation_id, value=payload)
        producer.flush(timeout=5)
    except kafka_mod.errors.KafkaError as exc:
        logger.error("Failed to publish response for %s: %s", correlation_id, exc)


def _run_consumer(consume_bootstrap, consume_topic, consume_group, producer, response_topic):
    import kafka
    while True:
        try:
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
                payload = message.value
                correlation_id = payload.get("correlation_id")
                if not correlation_id:
                    try:
                        consumer.commit()
                    except Exception:
                        pass
                    continue

                conn = conn_pool.getconn()
                try:
                    status_code, body = route_kafka_message(payload, conn)
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
                try:
                    consumer.commit()
                except Exception:
                    pass

        except Exception as exc:
            logger.error("Payment consumer on '%s' crashed, reconnecting in 3s: %s", consume_topic, exc)
            time.sleep(3)

# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

with app.app_context():
    if TRANSACTION_MODE == "TPC":
        try:
            recovery_tpc()
        except Exception as e:
            app.logger.warning(f"RECOVERY PAYMENT: Error during recovery: {e}")
    elif TRANSACTION_MODE == "SAGA":
        gateway_producer = _build_kafka_producer(GATEWAY_KAFKA)
        internal_producer = _build_kafka_producer(INTERNAL_KAFKA)

        threading.Thread(
            target=_run_consumer,
            args=(INTERNAL_KAFKA, INTERNAL_PAYMENT_TOPIC, "payment-service-internal",
                  internal_producer, INTERNAL_RESPONSE_TOPIC),
            daemon=True, name="internal-consumer",
        ).start()

        threading.Thread(
            target=_run_consumer,
            args=(GATEWAY_KAFKA, GATEWAY_PAYMENT_TOPIC, "payment-service-gateway",
                  gateway_producer, GATEWAY_RESPONSE_TOPIC),
            daemon=True, name="gateway-consumer",
        ).start()

        app.logger.info("SAGA mode: Kafka producers and consumers started")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
