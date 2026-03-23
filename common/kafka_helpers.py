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
If route_fn returns (None, None) the worker skips publishing a response.
Used by the order service checkout handler, which sends its response
asynchronously after the saga/TPC protocol completes.

Rebalance handling
------------------
kafka-python rebalance callbacks are registered via ConsumerRebalanceListener
passed to consumer.subscribe(), not as constructor arguments.
on_revoke flushes safe offsets before handing off partitions.
on_assign creates fresh trackers for newly assigned partitions.
"""

import heapq
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from kafka import KafkaConsumer, KafkaProducer, OffsetAndMetadata, TopicPartition, ConsumerRebalanceListener
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
    The commit cursor advances only over a contiguous completed prefix,
    so a single slow message only holds back its own offset.
    """

    def __init__(self):
        self._lock                    = threading.Lock()
        self._heap: list              = []
        self._safe_offset: int | None = None

    def register(self, offset: int) -> list:
        """Record a newly fetched offset as in-flight. Must be called before
        dispatching to a worker to avoid a mark_done-before-register race."""
        entry = [offset, False]
        with self._lock:
            heapq.heappush(self._heap, entry)
        return entry

    def mark_done(self, entry: list) -> None:
        """Mark offset as processed and advance the commit cursor."""
        with self._lock:
            entry[1] = True
            while self._heap and self._heap[0][1]:
                offset, _ = heapq.heappop(self._heap)
                self._safe_offset = offset + 1  # Kafka expects next-to-fetch

    def get_safe_offset(self) -> int | None:
        with self._lock:
            return self._safe_offset

    def flush_safe_offset(self) -> int | None:
        """Return safe offset and reset state — called on partition revocation."""
        with self._lock:
            offset = self._safe_offset
            self._safe_offset = None
            self._heap.clear()
            return offset


# ---------------------------------------------------------------------------
# Rebalance listener — kafka-python style
# ---------------------------------------------------------------------------

class _RebalanceListener(ConsumerRebalanceListener):
    """
    kafka-python registers rebalance callbacks via ConsumerRebalanceListener
    passed to consumer.subscribe() — NOT as KafkaConsumer constructor args.

    on_partitions_revoked: flush safe offsets before handing off partitions
                           so the new owner starts from the right place.
    on_partitions_assigned: create fresh trackers for newly assigned partitions,
                            discarding any stale state from a prior assignment.
    """

    def __init__(self, trackers: dict, trackers_lock: threading.RLock,
                 consumer_ref: list, service_name: str):
        self._trackers      = trackers
        self._trackers_lock = trackers_lock
        self._consumer_ref  = consumer_ref
        self._service_name  = service_name

    def on_partitions_revoked(self, revoked):
        if not revoked:
            return
        logger.info("%s partitions revoked: %s", self._service_name, revoked)
        consumer = self._consumer_ref[0]
        if consumer is None:
            return
        offsets = {}
        with self._trackers_lock:
            for tp in revoked:
                tracker = self._trackers.pop(tp, None)
                if tracker is None:
                    continue
                offset = tracker.flush_safe_offset()
                if offset is not None:
                    offsets[tp] = OffsetAndMetadata(offset, "", -1)
        if offsets:
            try:
                consumer.commit(offsets)
                logger.info("%s flushed offsets on revoke: %s",
                            self._service_name, offsets)
            except Exception as exc:
                logger.warning("%s flush on revoke failed: %s",
                               self._service_name, exc)

    def on_partitions_assigned(self, assigned):
        if not assigned:
            return
        logger.info("%s partitions assigned: %s", self._service_name, assigned)
        with self._trackers_lock:
            for tp in assigned:
                self._trackers[tp] = _PartitionOffsetTracker()


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

    route_fn(payload, conn) -> (status_code, body) | (None, None)
      - Must be idempotent — messages may be redelivered after a crash.
      - Must call conn.commit() before returning on the success path.
      - Must NOT call conn.rollback() — the worker does that on exception.
      - May return (None, None) to indicate an async response — the consumer
        loop will not publish anything for that message.
    """
    trackers: dict[TopicPartition, _PartitionOffsetTracker] = {}
    trackers_lock  = threading.RLock()
    consumer_ref: list = [None]

    # -----------------------------------------------------------------------
    # Commit thread — permanent for the lifetime of this consumer loop
    # -----------------------------------------------------------------------

    def _commit_loop() -> None:
        """Periodically flush contiguous completed offsets to Kafka."""
        while True:
            time.sleep(COMMIT_INTERVAL_S)
            consumer = consumer_ref[0]
            if consumer is None:
                continue  # Between reconnects — skip this tick
            with trackers_lock:
                snapshot = list(trackers.items())
            offsets = {}
            for tp, tracker in snapshot:
                offset = tracker.get_safe_offset()
                if offset is not None:
                    offsets[tp] = OffsetAndMetadata(offset, "", -1)
            if offsets:
                try:
                    consumer.commit(offsets)
                    logger.debug("%s committed offsets: %s", service_name, offsets)
                except Exception as exc:
                    logger.warning("%s offset commit failed: %s", service_name, exc)

    threading.Thread(
        target=_commit_loop, daemon=True, name=f"{service_name}-commit",
    ).start()

    # -----------------------------------------------------------------------
    # Worker — executed inside the thread pool
    # -----------------------------------------------------------------------

    def _process(tp: TopicPartition, offset: int, payload: dict,
                 entry: list) -> None:
        """Process one message: call route_fn, publish response, mark done."""
        correlation_id = payload.get("correlation_id")
        conn = conn_pool.getconn()
        try:
            result = route_fn(payload, conn)
            logger.info(f"Got result: {result}")
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

        # Publish only if route_fn returned a real result (not async)
        if result is not None and result != (None, None) and correlation_id:
            status_code, body = result
            publish_response(producer, response_topic, correlation_id, status_code, body)

        # Advance commit cursor — must happen even if publish skipped
        with trackers_lock:
            tracker = trackers.get(tp)
        if tracker is not None:
            tracker.mark_done(entry)
        else:
            # Partition was revoked while this worker was running — the offset
            # was already flushed during revocation, nothing more to do
            logger.debug(
                "%s partition %s revoked before offset %s marked done",
                service_name, tp.partition, offset,
            )

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
                    bootstrap_servers=bootstrap_servers,
                    group_id=group_id,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                )
                # Rebalance listener registered via subscribe(), not constructor
                listener = _RebalanceListener(
                    trackers, trackers_lock, consumer_ref, service_name
                )
                consumer.subscribe([topic], listener=listener)

                consumer_ref[0] = consumer
                logger.info("%s consumer connected to '%s'", service_name, topic)

                for message in consumer:
                    tp      = TopicPartition(message.topic, message.partition)
                    offset  = message.offset
                    payload = message.value
                    logger.info(f"Got message: {message}")

                    # Skip malformed messages but advance their offset so they
                    # don't permanently stall the commit cursor
                    if not payload.get("correlation_id"):
                        logger.warning(
                            "%s skipping message with no correlation_id "
                            "at partition=%s offset=%s",
                            service_name, tp.partition, offset,
                        )
                        with trackers_lock:
                            tracker = trackers.get(tp)
                        if tracker is not None:
                            entry = tracker.register(offset)
                            tracker.mark_done(entry)
                        continue

                    # Register offset BEFORE submitting — prevents a race where
                    # the worker finishes and calls mark_done before register
                    with trackers_lock:
                        tracker = trackers.get(tp)
                        if tracker is None:
                            # Partition assigned between poll and here
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
                # Clear consumer ref so commit thread skips this interval
                consumer_ref[0] = None
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass

            time.sleep(RECONNECT_DELAY_S)