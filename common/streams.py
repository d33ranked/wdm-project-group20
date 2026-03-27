# redis streams messaging layer — replaces kafka; provides at-least-once delivery via pending re-read

import json
import logging
import os

import redis as redis_lib

logger = logging.getLogger(__name__)

BLOCK_MS = 2_000  # blocking xreadgroup timeout per iteration
CONSUMER_NAME = (
    "worker"  # single worker per service; all pending entries owned by same name
)


def create_bus_pool() -> redis_lib.ConnectionPool:
    # connect to shared redis-bus (separate from storage redis so crashes don't drop messages)
    host = os.environ.get("REDIS_BUS_HOST", "redis-bus")
    port = int(os.environ.get("REDIS_BUS_PORT", 6379))
    pool = redis_lib.ConnectionPool(
        host=host,
        port=port,
        max_connections=20,
        decode_responses=True,
        socket_keepalive=True,
        socket_connect_timeout=2,
        socket_timeout=15,
    )
    logger.info("Redis bus pool → %s:%s", host, port)
    return pool


def get_bus(pool: redis_lib.ConnectionPool) -> redis_lib.Redis:
    return redis_lib.Redis(connection_pool=pool)


def ensure_groups(bus: redis_lib.Redis, stream_groups: list):
    # create consumer groups (and streams if missing); id="0" so no messages are silently skipped
    for stream, group in stream_groups:
        try:
            bus.xgroup_create(stream, group, id="0", mkstream=True)
        except redis_lib.exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise


def publish(bus: redis_lib.Redis, stream: str, payload: dict):
    # append one json-encoded message; maxlen ~50k keeps stream bounded
    bus.xadd(stream, {"data": json.dumps(payload)}, maxlen=50_000, approximate=True)


def read_pending_then_new(bus: redis_lib.Redis, stream: str, group: str) -> list:
    # phase 1: re-deliver unacked messages from before crash; phase 2: block for new messages
    pending = _xreadgroup(bus, stream, group, start_id="0", block=False)
    if pending:
        return pending
    return _xreadgroup(bus, stream, group, start_id=">", block=True)


def ack(bus: redis_lib.Redis, stream: str, group: str, message_id: str):
    # remove message from pending list; call only after processing and response are done
    bus.xack(stream, group, message_id)


def _xreadgroup(bus, stream, group, start_id, block):
    try:
        result = bus.xreadgroup(
            group,
            CONSUMER_NAME,
            {stream: start_id},
            count=10,
            block=BLOCK_MS if block else None,
        )
    except redis_lib.exceptions.ResponseError as exc:
        logger.warning("xreadgroup error on '%s': %s", stream, exc)
        return []

    if not result:
        return []

    messages = []
    for _stream_name, entries in result:
        for msg_id, fields in entries:
            try:
                payload = json.loads(fields["data"])
                messages.append((msg_id, payload))
            except (KeyError, json.JSONDecodeError) as exc:
                logger.error(
                    "malformed stream entry %s on '%s': %s", msg_id, stream, exc
                )
                bus.xack(
                    stream, group, msg_id
                )  # ack malformed entries to prevent infinite redelivery
    return messages