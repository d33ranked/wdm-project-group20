"""
Common Kafka helpers — producer, response publisher, and parallel consumer loop.

Consumer design
---------------
run_consumer_loop starts three permanent structures for the lifetime of the
calling thread:

  ThreadPoolExecutor  — workers that process messages concurrently
  commit thread       — periodically flushes safe offsets back to Kafka
  reconnect loop      — rebuilds only the KafkaConsumer socket on failure

Offset commit strategy
----------------------
Manual commits with per-partition tracking.  A min-heap per partition tracks
in-flight offsets and advances the commit cursor only up to the highest
*contiguous* completed offset, guaranteeing at-least-once delivery.

Async responses
---------------
If route_fn returns None the worker skips publishing a response.  This is
used by the order service checkout handler, which sends its response
asynchronously after the saga/TPC completes.

Rebalance handling
------------------
on_partitions_revoked flushes safe offsets before handing off partitions.
on_partitions_assigned creates fresh trackers for newly assigned partitions.
"""

import heapq
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from kafka import KafkaConsumer, KafkaProducer, OffsetAndMetadata, TopicPartition
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

CONSUMER_WORKERS  = int(os.environ.get("CONSUMER_WORKERS",   "12"))
COMMIT_INTERVAL_S = float(os.environ.get("COMMIT_INTERVAL_S", "1.0"))
RECONNECT_DELAY_S = float(os.environ.get("RECONNECT_DELAY_S", "3.0"))


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------

def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=5,
        batch_size=32_768,
    )


# ---------------------------------------------------------------------------
# Response publisher
# ---------------------------------------------------------------------------

def publish_response(
    producer: KafkaProducer,
    response_topic: str,
    correlation_id: str,
    status_code: int,
    body,
) -> None:
    payload = {
        "correlation_id": correlation_id,
        "status_code":    status_code,
        "body":           body,
    }
    try:
        producer.send(response_topic, key=correlation_id, value=payload)
        producer.flush(timeout=5)
    except KafkaError as exc:
        logger.error("Failed to publish response for %s: %s", correlation_id, exc)


# ---------------------------------------------------------------------------
# Per-partition offset tracker
# ---------------------------------------------------------------------------

class _PartitionOffsetTracker:
    """
    Tracks in-flight offsets for one partition using a min-heap.
    The commit cursor advances only over a contiguous completed prefix.
    """

    def __init__(self):
        self._lock                    = threading.Lock()
        self._heap: list              = []
        self._safe_offset: int | None = None

    def register(self, offset: int) -> list:
        entry = [offset, False]
        with self._lock:
            heapq.heappush(self._heap, entry)
        return entry

    def mark_done(self, entry: list) -> None:
        with self._lock:
            entry[1] = True
            while self._heap and self._heap[0][1]:
                offset, _ = heapq.heappop(self._heap)
                self._safe_offset = offset + 1

    def get_safe_offset(self) -> int | None:
        with self._lock:
            return self._safe_offset

    def flush_safe_offset(self) -> int | None:
        """Return safe offset and clear state — called on partition revocation."""
        with self._lock:
            offset = self._safe_offset
            self._safe_offset = None
            self._heap.clear()
            return offset


# ---------------------------------------------------------------------------
# Consumer loop
# ---------------------------------------------------------------------------

def run_consumer_loop(
    conn_pool,
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    producer: KafkaProducer,
    response_topic: str,
    route_fn,
    service_name: str,
) -> None:
    """
    Consume *topic* in parallel for the lifetime of the calling thread.

    route_fn(payload, conn) -> (status_code, body) | None
      - Must be idempotent (messages may be redelivered after a crash).
      - Must call conn.commit() before returning on the success path.
      - Must NOT call conn.rollback() — the worker does that on exception.
      - May return None to indicate the response will be sent asynchronously.
        In this case no response is published by the consumer loop.
    """
    trackers: dict[TopicPartition, _PartitionOffsetTracker] = {}
    trackers_lock  = threading.RLock()
    consumer_ref: list[KafkaConsumer | None] = [None]

    # -----------------------------------------------------------------------
    # Commit thread — permanent for the lifetime of this consumer loop
    # -----------------------------------------------------------------------

    def _commit_loop() -> None:
        while True:
            time.sleep(COMMIT_INTERVAL_S)
            consumer = consumer_ref[0]
            if consumer is None:
                continue
            with trackers_lock:
                snapshot = list(trackers.items())
            offsets = {}
            for tp, tracker in snapshot:
                offset = tracker.get_safe_offset()
                if offset is not None:
                    offsets[tp] = OffsetAndMetadata(offset, "")
            if offsets:
                try:
                    consumer.commit(offsets)
                except Exception as exc:
                    logger.warning("%s offset commit failed: %s", service_name, exc)

    threading.Thread(
        target=_commit_loop, daemon=True, name=f"{service_name}-commit",
    ).start()

    # -----------------------------------------------------------------------
    # Worker — executed inside the thread pool
    # -----------------------------------------------------------------------

    def _process(tp: TopicPartition, offset: int, payload: dict, entry: list) -> None:
        correlation_id = payload.get("correlation_id")
        conn = conn_pool.getconn()
        try:
            result = route_fn(payload, conn)
        except Exception as exc:
            logger.error(
                "%s error: correlation_id=%s partition=%s offset=%s — %s",
                service_name, correlation_id, tp.partition, offset, exc,
                exc_info=True,
            )
            try:
                conn.rollback()
            except Exception:
                pass
            result = (500, {"error": "Internal server error"})
        finally:
            conn_pool.putconn(conn)

        # Publish response only if route_fn returned a result (not async)
        if result is not None and correlation_id:
            status_code, body = result
            publish_response(producer, response_topic, correlation_id, status_code, body)

        with trackers_lock:
            tracker = trackers.get(tp)
        if tracker is not None:
            tracker.mark_done(entry)
        else:
            logger.debug(
                "%s partition %s revoked before offset %s marked done",
                service_name, tp.partition, offset,
            )

    # -----------------------------------------------------------------------
    # Rebalance callbacks
    # -----------------------------------------------------------------------

    def _on_partitions_revoked(revoked: list[TopicPartition]) -> None:
        if not revoked:
            return
        logger.info("%s partitions revoked: %s", service_name, revoked)
        consumer = consumer_ref[0]
        if consumer is None:
            return
        offsets = {}
        with trackers_lock:
            for tp in revoked:
                tracker = trackers.pop(tp, None)
                if tracker is None:
                    continue
                offset = tracker.flush_safe_offset()
                if offset is not None:
                    offsets[tp] = OffsetAndMetadata(offset, "")
        if offsets:
            try:
                consumer.commit(offsets)
            except Exception as exc:
                logger.warning("%s flush on revoke failed: %s", service_name, exc)

    def _on_partitions_assigned(assigned: list[TopicPartition]) -> None:
        if not assigned:
            return
        logger.info("%s partitions assigned: %s", service_name, assigned)
        with trackers_lock:
            for tp in assigned:
                trackers[tp] = _PartitionOffsetTracker()

    # -----------------------------------------------------------------------
    # Reconnect loop — only the KafkaConsumer socket is rebuilt on failure
    # -----------------------------------------------------------------------

    with ThreadPoolExecutor(
        max_workers=CONSUMER_WORKERS,
        thread_name_prefix=service_name,
    ) as pool:
        while True:
            consumer = None
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=bootstrap_servers,
                    group_id=group_id,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    on_partitions_revoked=_on_partitions_revoked,
                    on_partitions_assigned=_on_partitions_assigned,
                )
                consumer_ref[0] = consumer
                logger.info("%s consumer connected to '%s'", service_name, topic)

                for message in consumer:
                    tp      = TopicPartition(message.topic, message.partition)
                    offset  = message.offset
                    payload = message.value

                    if not payload.get("correlation_id"):
                        logger.warning(
                            "%s skipping message with no correlation_id at partition=%s offset=%s",
                            service_name, tp.partition, offset,
                        )
                        with trackers_lock:
                            tracker = trackers.get(tp)
                        if tracker is not None:
                            entry = tracker.register(offset)
                            tracker.mark_done(entry)
                        continue

                    with trackers_lock:
                        tracker = trackers.get(tp)
                        if tracker is None:
                            tracker = _PartitionOffsetTracker()
                            trackers[tp] = tracker

                    entry = tracker.register(offset)
                    pool.submit(_process, tp, offset, payload, entry)

            except Exception as exc:
                logger.error(
                    "%s consumer error, reconnecting in %.1fs: %s",
                    service_name, RECONNECT_DELAY_S, exc,
                    exc_info=True,
                )
            finally:
                consumer_ref[0] = None
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass

            time.sleep(RECONNECT_DELAY_S)