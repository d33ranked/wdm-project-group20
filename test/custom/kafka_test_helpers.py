"""
test_kafka.py
=============
Kafka helpers for the test suite.

Provides kafka_api() — a synchronous-feeling wrapper around the internal
Kafka broker that mirrors the api() HTTP helper used in all tests.

Design
------
Services publish all internal responses to the fixed topic "internal.responses"
(hardcoded in kafka_helpers.py via the response_topic passed to
run_consumer_loop).  The reply_to field in the request payload is ignored by
services — they always respond to their configured response_topic.

Therefore this helper:
  - Sends requests to the appropriate internal.{service}.tpc / saga topic
  - Listens on "internal.responses" with a unique consumer group per call
  - Filters messages by correlation_id to find its own reply

Each call uses its own consumer group (f"cg-{correlation_id}") so consumers
never compete and there is no cross-contamination between calls.

On timeout a FakeResponse(504) is returned — the check() pattern stays intact
and a missing reply shows as [FAIL] rather than a traceback.

Usage
-----
    from test_kafka import kafka_api, kafka_tpc, kafka_saga, wait_for_kafka

    wait_for_kafka()   # call once before your first kafka_tpc/saga call

    r = kafka_tpc("stock", {
        "type":     "stock.prepare",
        "txn_id":   "txn-abc",
        "item_id":  item_id,
        "quantity": 3,
    })
    check("Stock PREPARE votes YES", r.status_code == 200)
"""

import json
import os
import uuid
import time
from datetime import datetime
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
INTERNAL_KAFKA = os.environ.get(
    "INTERNAL_KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"
)
DEFAULT_TIMEOUT = 15   # seconds to wait for a reply

# Topic that all internal services publish their responses to
RESPONSE_TOPIC = "internal.responses"

_RUN_ID = datetime.now().strftime("%y%m%d-%H%M%S")


# ---------------------------------------------------------------------------
# Kafka readiness wait
# ---------------------------------------------------------------------------

def wait_for_kafka(timeout: int = 60) -> bool:
    """
    Block until the internal Kafka broker can complete a full produce->consume
    round-trip, mirroring the wait_for_service() pattern used for HTTP endpoints.

    A plain metadata probe (KafkaAdminClient.list_topics) succeeds as soon as
    the TCP connection is accepted, but Kafka brokers advertise an internal
    listener hostname (e.g. kafka-internal:9092) that may not be resolvable
    from the test-runner host.  The producer then fails with NodeNotReadyError
    when it tries to reconnect to that advertised address.

    By actually sending a message and reading it back we prove the full path
    works before any real test message is sent.
    """
    PROBE_TOPIC = f"test.probe.{_RUN_ID}"
    print(f"  [kafka] waiting for broker at {INTERNAL_KAFKA} ...")

    start = time.time()
    while time.time() - start < timeout:
        producer = None
        consumer = None
        try:
            # 1. Create the probe topic
            admin = KafkaAdminClient(
                bootstrap_servers=INTERNAL_KAFKA,
                request_timeout_ms=3000,
            )
            try:
                admin.create_topics([NewTopic(
                    name=PROBE_TOPIC,
                    num_partitions=1,
                    replication_factor=1,
                )])
            except TopicAlreadyExistsError:
                pass
            finally:
                admin.close()

            # 2. Produce a sentinel message
            producer = KafkaProducer(
                bootstrap_servers=INTERNAL_KAFKA,
                value_serializer=lambda v: json.dumps(v).encode(),
                request_timeout_ms=3000,
                max_block_ms=3000,
            )
            future = producer.send(PROBE_TOPIC, value={"probe": True})
            future.get(timeout=5)   # raises on delivery failure
            producer.flush()

            # 3. Consume it back — confirms the full path is live
            consumer = KafkaConsumer(
                PROBE_TOPIC,
                bootstrap_servers=INTERNAL_KAFKA,
                group_id=f"probe-{_RUN_ID}",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda b: json.loads(b.decode()),
                consumer_timeout_ms=5000,
            )
            for _ in consumer:
                print(f"  [kafka] broker is ready")
                return True

        except Exception:
            pass
        finally:
            if producer:
                try:
                    producer.close(timeout=1)
                except Exception:
                    pass
            if consumer:
                try:
                    consumer.close()
                except Exception:
                    pass

        time.sleep(2)

    print(f"  [kafka] timed out waiting for broker after {timeout}s")
    return False


# ---------------------------------------------------------------------------
# FakeResponse — drop-in replacement for requests.Response
# ---------------------------------------------------------------------------
class FakeResponse:
    """
    Wraps a Kafka reply so existing check() calls work unchanged.

        r.status_code   — taken from payload["status_code"] (default 200)
        r.json()        — returns the full payload dict
        r.text          — JSON-serialised payload string
    """

    def __init__(self, payload: dict):
        self._payload    = payload
        self.status_code = int(payload.get("status_code", 200))
        self.text        = json.dumps(payload)

    def json(self) -> dict:
        return self._payload

    def __repr__(self) -> str:
        return f"<FakeResponse status={self.status_code} payload={self._payload}>"


class TimeoutResponse(FakeResponse):
    """Returned when no reply arrives within the timeout window."""

    def __init__(self, correlation_id: str, topic: str, timeout: int):
        super().__init__({
            "status_code": 504,
            "type":        "test.timeout",
            "detail": (
                f"No reply received within {timeout}s "
                f"(sent to={topic}, response_topic={RESPONSE_TOPIC}, "
                f"correlation_id={correlation_id})"
            ),
        })


# ---------------------------------------------------------------------------
# Module-level producer — created once, reused across calls
# ---------------------------------------------------------------------------
_producer: KafkaProducer | None = None


def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=INTERNAL_KAFKA,
            value_serializer=lambda v: json.dumps(v).encode(),
        )
    return _producer


# ---------------------------------------------------------------------------
# Core helper
# ---------------------------------------------------------------------------
def kafka_api(
    topic: str,
    payload: dict[str, Any],
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> FakeResponse:
    """
    Send `payload` to `topic` on the internal Kafka broker and block until
    the matching reply arrives on "internal.responses" (or timeout).

    Services always publish responses to their fixed response_topic
    (internal.responses) — they do not honour a reply_to field.
    We filter by correlation_id to find our own reply.

    Each call uses its own consumer group so messages are never shared
    between concurrent or sequential calls.
    """
    correlation_id = str(uuid.uuid4())
    message = {
        **payload,
        "correlation_id": correlation_id,
    }

    # ---- Consume BEFORE producing to avoid missing a fast reply -----------
    # auto_offset_reset="latest" so we only see messages arriving after we
    # start listening — old responses from prior tests are ignored.
    group_id = f"cg-{correlation_id}"
    consumer = KafkaConsumer(
        RESPONSE_TOPIC,
        bootstrap_servers=INTERNAL_KAFKA,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode()),
    )
    # Force partition assignment before producing so we don't miss the reply
    consumer.poll(timeout_ms=1000)

    # ---- Produce ----------------------------------------------------------
    _get_producer().send(topic, value=message)
    _get_producer().flush()

    # ---- Poll until we see our correlation_id or time out -----------------
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            remaining_ms = int((deadline - time.time()) * 1000)
            if remaining_ms <= 0:
                print(f"timed out for msg: {message}")
                break
            records = consumer.poll(timeout_ms=min(remaining_ms, 500))
            for _, messages in records.items():
                for msg in messages:
                    if msg.value.get("correlation_id") == correlation_id:
                        return FakeResponse(msg.value)
    finally:
        consumer.close()

    return TimeoutResponse(correlation_id, topic, timeout)


# ---------------------------------------------------------------------------
# Convenience wrappers
# ---------------------------------------------------------------------------
def kafka_tpc(service: str, payload: dict[str, Any], **kwargs) -> FakeResponse:
    """Send a 2PC command to internal.{service}.tpc"""
    return kafka_api(f"internal.{service}.tpc", payload, **kwargs)


def kafka_saga(service: str, payload: dict[str, Any], **kwargs) -> FakeResponse:
    """Send a SAGA command to internal.{service}.saga"""
    return kafka_api(f"internal.{service}.saga", payload, **kwargs)