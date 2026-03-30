import json
import logging
import os
import time

import redis as redis_lib

logger = logging.getLogger(__name__)

BLOCK_MS = 2_000
import socket as _socket

CONSUMER_NAME = f"worker-{_socket.gethostname()}"

_REDIS_MAX_CONNECTIONS = int(os.environ.get("REDIS_MAX_CONNECTIONS", "6000"))
_STREAM_BATCH_SIZE = int(os.environ.get("STREAM_BATCH_SIZE", "100"))


def create_bus_pool() -> redis_lib.ConnectionPool:
    master_name = os.environ.get("REDIS_BUS_HOST", "redis-bus")

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
    for stream, group in stream_groups:
        try:
            bus.xgroup_create(stream, group, id="0", mkstream=True)
        except redis_lib.exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise


def publish(bus: redis_lib.Redis, stream: str, payload: dict):
    bus.xadd(stream, {"data": json.dumps(payload)}, maxlen=50_000, approximate=True)


def read_pending_then_new(bus: redis_lib.Redis, stream: str, group: str) -> list:
    pending = _xreadgroup(bus, stream, group, start_id="0", block=False)
    if pending:
        return pending
    return _xreadgroup(bus, stream, group, start_id=">", block=True)


def ack(bus: redis_lib.Redis, stream: str, group: str, message_id: str):
    bus.xack(stream, group, message_id)


def publish_response(bus, stream: str, correlation_id: str, status_code: int, body):
    """Publish a standard response envelope to a stream."""
    publish(bus, stream, {
        "correlation_id": correlation_id,
        "status_code": status_code,
        "body": body,
    })


def make_message_handler(get_bus_fn, get_r_fn, stream: str, group: str, response_stream: str, route_fn):
    """Return a greenlet-safe message handler for a stream consumer.

    The returned function follows the (msg_id, payload) -> None signature
    expected by run_gevent_consumer. It calls route_fn(payload, r) which must
    return (status_code, body), then publishes the response and acks the message.
    """
    def handle(msg_id: str, payload: dict):
        correlation_id = payload.get("correlation_id")
        bus = get_bus_fn()
        if not correlation_id:
            ack(bus, stream, group, msg_id)
            return
        r = get_r_fn()
        try:
            status_code, body = route_fn(payload, r)
        except Exception as exc:
            logger.error("Error processing %s: %s", correlation_id, exc, exc_info=True)
            status_code, body = 400, {"error": "Internal server error"}
        publish_response(bus, response_stream, correlation_id, status_code, body)
        ack(bus, stream, group, msg_id)
    return handle


def run_gevent_consumer(bus_pool, stream: str, group: str, handler_fn, name: str = ""):
    """Run a blocking gevent consumer loop for a stream group.

    Reads pending messages first (at-least-once delivery), then new ones.
    Spawns greenlets in small batches (size reduced to 20 from 100 to limit
    joinall fence duration). When one batch fence stalls on a slow message,
    only 20 messages are blocked instead of 100, minimizing tail latency impact.

    Uses gevent.joinall per batch to ensure all messages are ACKed before
    reading the next batch, maintaining correctness.
    """
    import gevent
    logger.info("%s consumer started on stream '%s'", name, stream)

    # Use smaller batch size to reduce joinall fence stall duration
    _SMALL_BATCH_SIZE = 20

    while True:
        try:
            msgs = read_pending_then_new(get_bus(bus_pool), stream, group)
            if msgs:
                # Process in smaller sub-batches to limit any single joinall stall
                for i in range(0, len(msgs), _SMALL_BATCH_SIZE):
                    batch = msgs[i:i+_SMALL_BATCH_SIZE]
                    gevent.joinall([gevent.spawn(handler_fn, mid, pl) for mid, pl in batch])
        except Exception as exc:
            logger.error("%s consumer error, retrying in 1s: %s", name, exc)
            time.sleep(1)


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
                bus.xack(stream, group, msg_id)
    return messages
