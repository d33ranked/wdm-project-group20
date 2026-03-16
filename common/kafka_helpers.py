import json
import time
import logging

logger = logging.getLogger(__name__)


def build_producer(bootstrap_servers):
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all", retries=3, linger_ms=5, batch_size=32_768,
    )


def publish_response(producer, response_topic, correlation_id, status_code, body):
    from kafka.errors import KafkaError
    payload = {"correlation_id": correlation_id, "status_code": status_code, "body": body}
    try:
        producer.send(response_topic, key=correlation_id, value=payload)
        producer.flush(timeout=5)
    except KafkaError as exc:
        logger.error("Failed to publish response for %s: %s", correlation_id, exc)


def run_consumer_loop(conn_pool, bootstrap_servers, topic, group_id,
                      producer, response_topic, route_fn, service_name):
    from kafka import KafkaConsumer
    while True:
        try:
            consumer = KafkaConsumer(
                topic, bootstrap_servers=bootstrap_servers,
                group_id=group_id, auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("%s consumer started on '%s'", service_name, topic)

            for message in consumer:
                payload = message.value
                correlation_id = payload.get("correlation_id")
                if not correlation_id:
                    try: consumer.commit()
                    except Exception: pass
                    continue

                conn = conn_pool.getconn()
                try:
                    status_code, body = route_fn(payload, conn)
                except Exception as exc:
                    logger.error("Error processing %s: %s", correlation_id, exc, exc_info=True)
                    status_code, body = 500, {"error": "Internal server error"}
                finally:
                    try: conn.rollback()
                    except Exception: pass
                    conn_pool.putconn(conn)

                publish_response(producer, response_topic, correlation_id, status_code, body)
                try: consumer.commit()
                except Exception: pass

        except Exception as exc:
            logger.error("%s consumer crashed, reconnecting in 3s: %s", service_name, exc)
            time.sleep(3)
