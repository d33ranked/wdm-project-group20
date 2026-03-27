"""Stock SAGA participant — Redis storage, Kafka messaging (Phase 1).

What changed from the PostgreSQL version
-----------------------------------------
- db_subtract_stock_batch / db_add_stock_batch now use the deduct_stock_batch
  and restore_stock_batch Lua scripts instead of SELECT FOR UPDATE + UPDATE SQL.
- route_kafka_message receives r (redis.Redis) instead of conn (psycopg2 connection).
- Consumer loops are inlined here (start_gateway_consumer / start_internal_consumer)
  instead of delegating to the generic run_consumer_loop helper, because that
  helper uses psycopg2 connection pool semantics (getconn / putconn / rollback).

The Kafka producer/consumer and topic names are UNCHANGED.
Messaging migration (Kafka → Redis Streams) happens in Phase 2.
"""

import json
import uuid
import time
import logging

import redis as redis_lib

from common.idempotency import check_idempotency_kafka, save_idempotency_kafka
from common.kafka_helpers import build_producer, publish_response

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Topics (unchanged)
# ---------------------------------------------------------------------------

STOCK_GATEWAY_TOPIC     = "gateway.stock"
STOCK_INTERNAL_TOPIC    = "internal.stock"
GATEWAY_RESPONSE_TOPIC  = "gateway.responses"
INTERNAL_RESPONSE_TOPIC = "internal.responses"

# ---------------------------------------------------------------------------
# Module State
# ---------------------------------------------------------------------------

_redis_pool        = None
_scripts           = None  # LuaScripts instance shared from app.py startup
_gateway_producer  = None
_internal_producer = None


def init(redis_pool, scripts, gateway_kafka: str, internal_kafka: str):
    global _redis_pool, _scripts, _gateway_producer, _internal_producer
    _redis_pool        = redis_pool
    _scripts           = scripts
    _gateway_producer  = build_producer(gateway_kafka)
    _internal_producer = build_producer(internal_kafka)


def _get_r():
    """Get a Redis client from the pool — one per Kafka message."""
    return redis_lib.Redis(connection_pool=_redis_pool)


# ---------------------------------------------------------------------------
# Batch Operations (Atomic Multi-Item via Lua)
# ---------------------------------------------------------------------------

def db_subtract_stock_batch(r, items):
    # items = [(item_id, quantity), ...]
    # deduct_stock_batch lua: checks ALL items have sufficient stock before
    # deducting ANY — all-or-nothing, no partial deductions possible
    keys = [f"item:{item_id}" for item_id, _ in items]
    args = [qty for _, qty in items]
    # raises ResponseError with "NOT_FOUND:item:{id}" or "INSUFFICIENT:item:{id}"
    _scripts.deduct_stock_batch(keys=keys, args=args, client=r)

    # read new stock values in one pipeline round-trip
    pipe = r.pipeline(transaction=False)
    for item_id, _ in items:
        pipe.hget(f"item:{item_id}", "stock")
    stocks = pipe.execute()
    return {item_id: int(s) if s is not None else 0
            for (item_id, _), s in zip(items, stocks)}


def db_add_stock_batch(r, items):
    # items = [(item_id, quantity), ...]
    # restore_stock_batch lua: atomically adds stock back to each item
    # always succeeds — restoring cannot fail
    keys = [f"item:{item_id}" for item_id, _ in items]
    args = [qty for _, qty in items]
    _scripts.restore_stock_batch(keys=keys, args=args, client=r)

    # read new stock values in one pipeline round-trip
    pipe = r.pipeline(transaction=False)
    for item_id, _ in items:
        pipe.hget(f"item:{item_id}", "stock")
    stocks = pipe.execute()
    return {item_id: int(s) if s is not None else 0
            for (item_id, _), s in zip(items, stocks)}


# ---------------------------------------------------------------------------
# Kafka Message Routing
# ---------------------------------------------------------------------------

def route_kafka_message(payload, r):
    method   = payload.get("method", "GET").upper()
    path     = payload.get("path", "/")
    body     = payload.get("body") or {}
    headers  = payload.get("headers") or {}
    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    # POST /item/create/<price>
    if method == "POST" and len(segments) >= 2 and segments[0] == "item" and segments[1] == "create":
        price   = int(segments[2]) if len(segments) > 2 else 0
        item_id = str(uuid.uuid4())
        r.hset(f"item:{item_id}", mapping={"stock": "0", "price": str(price)})
        return 201, {"item_id": item_id}

    # POST /batch_init/<n>/<starting_stock>/<item_price>
    if method == "POST" and len(segments) >= 4 and segments[0] == "batch_init":
        n, starting_stock, item_price = int(segments[1]), int(segments[2]), int(segments[3])
        pipe = r.pipeline(transaction=False)
        for i in range(n):
            pipe.hset(f"item:{i}", mapping={"stock": str(starting_stock), "price": str(item_price)})
        pipe.execute()
        return 200, {"msg": "Batch init for stock successful"}

    # GET /find/<item_id>
    if method == "GET" and len(segments) >= 2 and segments[0] == "find":
        item_id = segments[1]
        data = r.hgetall(f"item:{item_id}")
        if not data:
            return 400, {"error": f"Item {item_id} not found"}
        return 200, {"stock": int(data["stock"]), "price": int(data["price"])}

    # POST /add/<item_id>/<amount>
    if method == "POST" and len(segments) >= 3 and segments[0] == "add":
        item_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive!"}
        cached = check_idempotency_kafka(r, idem_key)
        if cached:
            return cached
        if not r.hexists(f"item:{item_id}", "stock"):
            return 400, {"error": f"Item {item_id} not found"}
        new_stock = r.hincrby(f"item:{item_id}", "stock", amount)
        resp = f"Item: {item_id} stock updated to: {new_stock}"
        save_idempotency_kafka(r, idem_key, 200, resp)
        return 200, resp

    # POST /subtract/<item_id>/<amount>
    if method == "POST" and len(segments) >= 3 and segments[0] == "subtract":
        item_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive!"}
        cached = check_idempotency_kafka(r, idem_key)
        if cached:
            return cached
        try:
            _scripts.deduct_stock_batch(
                keys=[f"item:{item_id}"], args=[amount], client=r
            )
        except redis_lib.exceptions.ResponseError as exc:
            err = str(exc)
            if "NOT_FOUND" in err:
                return 400, {"error": f"Item {item_id} not found"}
            if "INSUFFICIENT" in err:
                return 400, {"error": f"Item {item_id} has insufficient stock"}
            raise
        new_stock = int(r.hget(f"item:{item_id}", "stock"))
        resp = f"Item: {item_id} stock updated to: {new_stock}"
        save_idempotency_kafka(r, idem_key, 200, resp)
        return 200, resp

    # POST /subtract_batch
    if method == "POST" and segments and segments[0] == "subtract_batch":
        cached = check_idempotency_kafka(r, idem_key)
        if cached:
            return cached
        try:
            items = [(e["item_id"], int(e["amount"])) for e in body["items"]]
        except (KeyError, TypeError, ValueError):
            return 400, {"error": 'Expected {"items": [{"item_id": str, "amount": int}, ...]}'}
        try:
            results = db_subtract_stock_batch(r, items)
            resp = {"updated_stock": results}
            save_idempotency_kafka(r, idem_key, 200, json.dumps(resp))
            return 200, resp
        except redis_lib.exceptions.ResponseError as exc:
            err = str(exc)
            if "NOT_FOUND" in err:
                item_id = err.split("item:")[-1]
                return 400, {"error": f"Item {item_id} not found"}
            if "INSUFFICIENT" in err:
                item_id = err.split("item:")[-1]
                return 400, {"error": f"Item {item_id} has insufficient stock"}
            return 400, {"error": str(exc)}

    # POST /add_batch
    if method == "POST" and segments and segments[0] == "add_batch":
        cached = check_idempotency_kafka(r, idem_key)
        if cached:
            return cached
        try:
            items = [(e["item_id"], int(e["amount"])) for e in body["items"]]
        except (KeyError, TypeError, ValueError):
            return 400, {"error": 'Expected {"items": [{"item_id": str, "amount": int}, ...]}'}
        try:
            results = db_add_stock_batch(r, items)
            resp = {"updated_stock": results}
            save_idempotency_kafka(r, idem_key, 200, json.dumps(resp))
            return 200, resp
        except Exception as exc:
            return 400, {"error": str(exc)}

    return 404, {"error": f"No handler for {method} {path}"}


# ---------------------------------------------------------------------------
# Kafka Consumer Loops
# ---------------------------------------------------------------------------

def start_gateway_consumer(gateway_kafka: str):
    import kafka
    while True:
        try:
            consumer = kafka.KafkaConsumer(
                STOCK_GATEWAY_TOPIC,
                bootstrap_servers=gateway_kafka,
                group_id="stock-service-gateway",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("Stock gateway consumer started on '%s'", STOCK_GATEWAY_TOPIC)

            for message in consumer:
                payload        = message.value
                correlation_id = payload.get("correlation_id")
                if not correlation_id:
                    try: consumer.commit()
                    except Exception: pass
                    continue

                r = _get_r()
                try:
                    status_code, body = route_kafka_message(payload, r)
                except Exception as exc:
                    logger.error("Error processing %s: %s", correlation_id, exc, exc_info=True)
                    status_code, body = 500, {"error": "Internal server error"}

                publish_response(_gateway_producer, GATEWAY_RESPONSE_TOPIC,
                                 correlation_id, status_code, body)
                try: consumer.commit()
                except Exception: pass

        except Exception as exc:
            logger.error("Stock gateway consumer crashed, reconnecting in 3s: %s", exc)
            time.sleep(3)


def start_internal_consumer(internal_kafka: str):
    import kafka
    while True:
        try:
            consumer = kafka.KafkaConsumer(
                STOCK_INTERNAL_TOPIC,
                bootstrap_servers=internal_kafka,
                group_id="stock-service-internal",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("Stock internal consumer started on '%s'", STOCK_INTERNAL_TOPIC)

            for message in consumer:
                payload        = message.value
                correlation_id = payload.get("correlation_id")
                if not correlation_id:
                    try: consumer.commit()
                    except Exception: pass
                    continue

                r = _get_r()
                try:
                    status_code, body = route_kafka_message(payload, r)
                except Exception as exc:
                    logger.error("Error processing %s: %s", correlation_id, exc, exc_info=True)
                    status_code, body = 500, {"error": "Internal server error"}

                publish_response(_internal_producer, INTERNAL_RESPONSE_TOPIC,
                                 correlation_id, status_code, body)
                try: consumer.commit()
                except Exception: pass

        except Exception as exc:
            logger.error("Stock internal consumer crashed, reconnecting in 3s: %s", exc)
            time.sleep(3)


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------

def recovery_saga(redis_pool):
    # stock is a SAGA participant, not an orchestrator — it holds no saga state
    # the order service (orchestrator) drives all recovery on restart
    print("SAGA RECOVERY STOCK: participant only — no saga state to recover", flush=True)
