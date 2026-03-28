# redis streams messaging layer — replaces kafka; provides at-least-once delivery via pending re-read

import json
import logging
import os

import redis as redis_lib

logger = logging.getLogger(__name__)

BLOCK_MS = 2_000  # blocking xreadgroup timeout per iteration
import os as _os
CONSUMER_NAME = f"worker-{_os.getpid()}"  # unique per process so pending re-delivery is per-worker

_REDIS_MAX_CONNECTIONS = int(os.environ.get("REDIS_MAX_CONNECTIONS", "6000"))
# batch size: how many stream messages are pulled per xreadgroup call
# with concurrent processing (gevent.spawn per message), all messages in a batch run simultaneously
# so larger batches only reduce xreadgroup round-trip overhead — they no longer hurt tail latency
# 500 means 10 xreadgroup calls to drain a 5000-message queue instead of 500
_STREAM_BATCH_SIZE = int(os.environ.get("STREAM_BATCH_SIZE", "500"))


def create_bus_pool() -> redis_lib.ConnectionPool:
    # connect to shared redis-bus (separate from storage redis so crashes don't drop messages)
    master_name = os.environ.get("REDIS_BUS_HOST", "redis-bus")  # matches sentinel monitor name

    sentinel_hosts = os.environ.get("SENTINEL_HOSTS", "")
    if sentinel_hosts:
        from redis.sentinel import Sentinel
        sentinel_port = int(os.environ.get("SENTINEL_PORT", "26379"))
        addrs = [(h.strip(), sentinel_port) for h in sentinel_hosts.split(",")]
        s = Sentinel(addrs, socket_timeout=0.5, socket_connect_timeout=2)
        pool = s.master_for(
            master_name,
            decode_responses=True,
            socket_keepalive=True,
            socket_connect_timeout=2,
            socket_timeout=15,
            max_connections=_REDIS_MAX_CONNECTIONS,
        ).connection_pool
        logger.info("Redis bus Sentinel pool → master '%s' via %s", master_name, addrs)
        return pool

    # fallback: direct connection
    host = master_name
    port = int(os.environ.get("REDIS_BUS_PORT", 6379))
    pool = redis_lib.ConnectionPool(
        host=host,
        port=port,
        max_connections=_REDIS_MAX_CONNECTIONS,
        decode_responses=True,
        socket_keepalive=True,
        socket_connect_timeout=2,
        socket_timeout=15,
    )
    logger.info("Redis bus pool → %s:%s (direct, no sentinel)", host, port)
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
            count=_STREAM_BATCH_SIZE,
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