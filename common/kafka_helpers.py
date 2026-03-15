"""
Common Kafka helpers — producer, response publisher, and parallel consumer loop.

Consumer design
---------------
run_consumer_loop starts three permanent structures that live for the lifetime
of the calling thread:

  ThreadPoolExecutor  — workers that process messages concurrently
  commit thread       — periodically flushes safe offsets back to Kafka
  reconnect loop      — rebuilds only the KafkaConsumer socket on failure,
                        leaving the pool and commit thread untouched

Only the KafkaConsumer TCP connection can die.  Rebuilding the pool and commit
thread on every reconnect (the old design) caused zombie commit threads and
leaked tracker state.

Offset commit strategy
----------------------
Kafka's auto-commit advances the offset as soon as a message is *fetched*,
not when it is *processed*.  A crash between fetch and process would lose
those messages.  We use manual commits instead:

  - Each assigned partition has a _PartitionOffsetTracker (a min-heap of
    [offset, done] pairs).
  - The consumer thread registers each offset before dispatching to a worker.
  - Workers mark their offset done when they finish.
  - The commit thread advances the committed offset only up to the highest
    *contiguous* done offset — no gaps — so a crash always rewinds to the
    last fully processed message.

This gives at-least-once delivery.  Handlers must be idempotent.

Rebalance handling
------------------
When Kafka rebalances (a consumer joins or leaves the group), partitions are
reassigned.  Before giving up a partition we flush its safe committed offset so
the new owner starts from the right place.  After gaining a partition we
create a fresh tracker, discarding any stale state from a previous assignment.

Thread safety
-------------
_PartitionOffsetTracker is accessed from three threads simultaneously:
  - consumer thread  : register()
  - worker threads   : mark_done()
  - commit thread    : get_safe_offset()
A per-tracker lock serialises all three.

The trackers dict itself is written by the consumer thread (on rebalance) and
read by the commit thread.  A single RLock guards the dict.
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

# Workers per consumer loop.  >= num partitions keeps all partitions busy in
# parallel.  Stay well under the DB pool max (100) since each active worker
# holds one connection.
CONSUMER_WORKERS  = int(os.environ.get("CONSUMER_WORKERS",   "12"))

# How often the commit thread flushes contiguous completed offsets to Kafka.
COMMIT_INTERVAL_S = float(os.environ.get("COMMIT_INTERVAL_S", "1.0"))

# Seconds to wait before attempting to reconnect after a consumer error.
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
    Tracks in-flight offsets for one partition.

    Uses a min-heap of [offset, done] pairs.  The commit cursor advances only
    over a contiguous completed prefix, so a single slow message holds back
    only its own offset — later offsets can still complete and will be
    committed as soon as the gap is resolved.
    """

    def __init__(self):
        self._lock                    = threading.Lock()
        self._heap: list              = []    # min-heap of [offset, done_flag]
        self._safe_offset: int | None = None  # next offset to commit (last_done + 1)

    def register(self, offset: int) -> list:
        """
        Record a newly fetched offset as in-flight.
        Must be called by the consumer thread before dispatching to a worker.
        Returns the entry that the worker must pass back to mark_done().
        """
        entry = [offset, False]
        with self._lock:
            heapq.heappush(self._heap, entry)
        return entry

    def mark_done(self, entry: list) -> None:
        """
        Mark an offset as fully processed and advance the commit cursor over
        any newly completed contiguous prefix.
        Called by worker threads.
        """
        with self._lock:
            entry[1] = True
            # Pop all consecutive done entries from the front of the heap
            while self._heap and self._heap[0][1]:
                offset, _ = heapq.heappop(self._heap)
                self._safe_offset = offset + 1  # Kafka expects next-to-fetch

    def get_safe_offset(self) -> int | None:
        """Return the offset to pass to consumer.commit(), or None if nothing new."""
        with self._lock:
            return self._safe_offset

    def flush_safe_offset(self) -> int | None:
        """
        Return the current safe offset and reset internal state.
        Called during partition revocation so the commit thread does not
        re-commit a stale offset after the partition is reassigned elsewhere.
        """
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

    Spawns a permanent ThreadPoolExecutor and commit thread, then enters a
    reconnect loop that only rebuilds the KafkaConsumer socket on failure.

    route_fn(payload, conn) -> (status_code, body)
      - Must be idempotent (messages may be redelivered after a crash).
      - Must call conn.commit() before returning on success.
      - Must NOT call conn.rollback() — the caller does that on exception.
    """

    # Tracker registry — written by the consumer thread on rebalance,
    # read by the commit thread and worker threads.
    trackers: dict[TopicPartition, _PartitionOffsetTracker] = {}
    trackers_lock = threading.RLock()

    # One-element list so the commit thread always references the *current*
    # consumer after a reconnect without needing a re-closed-over variable.
    consumer_ref: list[KafkaConsumer | None] = [None]

    # -----------------------------------------------------------------------
    # Commit thread — permanent for the lifetime of this consumer loop
    # -----------------------------------------------------------------------

    def _commit_loop() -> None:
        """
        Periodically commit the highest safe offset for every tracked partition.
        Runs independently of worker threads to avoid per-message Kafka RTT.
        """
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
                    offsets[tp] = OffsetAndMetadata(offset, "")

            if offsets:
                try:
                    consumer.commit(offsets)
                    logger.debug("%s committed offsets: %s", service_name, offsets)
                except Exception as exc:
                    # Non-fatal — will retry on the next tick
                    logger.warning("%s offset commit failed: %s", service_name, exc)

    threading.Thread(
        target=_commit_loop,
        daemon=True,
        name=f"{service_name}-commit",
    ).start()

    # -----------------------------------------------------------------------
    # Worker function — executed inside the thread pool
    # -----------------------------------------------------------------------

    def _process(
        tp: TopicPartition,
        offset: int,
        payload: dict,
        entry: list,
    ) -> None:
        """
        Process one Kafka message.

        Borrows a DB connection, calls route_fn, publishes the response, then
        marks the offset done so the commit cursor can advance.
        """
        correlation_id = payload.get("correlation_id")
        conn = conn_pool.getconn()
        try:
            status_code, body = route_fn(payload, conn)
        except Exception as exc:
            logger.error(
                "%s unhandled error: correlation_id=%s partition=%s offset=%s — %s",
                service_name, correlation_id, tp.partition, offset, exc,
                exc_info=True,
            )
            try:
                conn.rollback()
            except Exception:
                pass
            status_code, body = 500, {"error": "Internal server error"}
        finally:
            conn_pool.putconn(conn)

        # Always publish a response so the caller does not time out waiting
        if correlation_id:
            publish_response(producer, response_topic, correlation_id, status_code, body)

        # Advance the commit cursor for this partition
        with trackers_lock:
            tracker = trackers.get(tp)
        if tracker is not None:
            tracker.mark_done(entry)
        else:
            # Partition was revoked while this worker was running — the offset
            # was already flushed during revocation, nothing more to do.
            logger.debug(
                "%s partition %s revoked before offset %s could be marked done",
                service_name, tp.partition, offset,
            )

    # -----------------------------------------------------------------------
    # Rebalance callbacks — called by the KafkaConsumer on the consumer thread
    # -----------------------------------------------------------------------

    def _on_partitions_revoked(revoked: list[TopicPartition]) -> None:
        """
        Flush the safe offset for each revoked partition before handing it off,
        so the new owner starts exactly where we finished rather than from the
        last periodic commit tick (which may be up to COMMIT_INTERVAL_S stale).
        """
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
                logger.info(
                    "%s flushed offsets on revoke: %s", service_name, offsets
                )
            except Exception as exc:
                logger.warning(
                    "%s failed to flush offsets on revoke: %s", service_name, exc
                )

    def _on_partitions_assigned(assigned: list[TopicPartition]) -> None:
        """
        Create a fresh tracker for each newly assigned partition, discarding
        any stale in-flight state from a previous assignment.
        """
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

                    # Skip malformed messages but still advance their offset
                    # so they do not permanently stall the commit cursor.
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

                    # Register offset *before* submitting to the pool.
                    # If we registered after, the worker could finish and call
                    # mark_done before register runs, corrupting the heap.
                    with trackers_lock:
                        tracker = trackers.get(tp)
                        if tracker is None:
                            # Partition assigned between poll and here — safe to create
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
                # Clear the consumer reference so the commit thread skips this
                # interval rather than trying to commit on a dead connection.
                consumer_ref[0] = None
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass

            time.sleep(RECONNECT_DELAY_S)