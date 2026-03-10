"""
Shared Kafka utilities for microservices running in SAGA mode.

Provides a standard producer configuration, a response-publishing helper,
and the generic consumer loop used by stock and payment services.
"""

import json
import time
import logging

logger = logging.getLogger(__name__)


def build_producer(bootstrap_servers: str):
    """Create a KafkaProducer with settings tuned for reliability over raw throughput.

    acks="all" waits for all in-sync replicas before acknowledging a write,
    preventing message loss at the cost of slightly higher latency. The small
    linger_ms batching window adds negligible latency but meaningful throughput
    gains under load.
    """
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=5,
        batch_size=32_768,
    )


def publish_response(producer, response_topic: str, correlation_id: str,
                     status_code: int, body):
    """Publish a response envelope so the requesting service can match it by correlation ID."""
    from kafka.errors import KafkaError

    payload = {
        "correlation_id": correlation_id,
        "status_code": status_code,
        "body": body,
    }
    try:
        producer.send(response_topic, key=correlation_id, value=payload)
        producer.flush(timeout=5)
    except KafkaError as exc:
        logger.error("Failed to publish response for %s: %s", correlation_id, exc)


def run_consumer_loop(conn_pool, bootstrap_servers: str, topic: str,
                      group_id: str, producer, response_topic: str,
                      route_fn, service_name: str):
    """Generic Kafka consumer loop used by stock and payment services.

    Polls messages from `topic`, dispatches each to `route_fn(payload, conn)`,
    publishes the result to `response_topic`, and commits the offset only after
    successful processing + DB commit. If the service crashes between DB commit
    and offset commit, the message is redelivered — idempotency prevents
    double-processing.

    The outer while-True loop reconnects with a 3-second backoff if the
    consumer crashes (e.g. Kafka broker restarts).
    """
    from kafka import KafkaConsumer

    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info(
                "%s consumer started on '%s' -> replies to '%s'",
                service_name, topic, response_topic,
            )

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
                    status_code, body = route_fn(payload, conn)
                except Exception as exc:
                    logger.error(
                        "Unhandled error processing %s: %s",
                        correlation_id, exc, exc_info=True,
                    )
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    status_code, body = 500, {"error": "Internal server error"}
                finally:
                    conn_pool.putconn(conn)

                publish_response(
                    producer, response_topic, correlation_id, status_code, body,
                )
                try:
                    consumer.commit()
                except Exception:
                    pass

        except Exception as exc:
            logger.error(
                "%s consumer on '%s' crashed, reconnecting in 3s: %s",
                service_name, topic, exc,
            )
            time.sleep(3)
