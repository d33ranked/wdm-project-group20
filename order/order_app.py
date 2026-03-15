"""
Order service — pure Kafka consumer process.

No HTTP server.  All requests arrive via Kafka, all responses published back.

Four threads:

  gateway-consumer      thread pool via run_consumer_loop
                        handles create, batch_init, find, addItem, checkout

  internal-consumer     single-threaded custom loop on internal.responses
                        routes TPC/SAGA outcomes to coordinators
                        single-threaded because state machine ordering matters

  price-lookup-consumer lightweight custom loop on gateway.responses
                        feeds item prices back to addItem workers

  recovery              daemon thread, re-drives stale transactions
"""

import json
import logging
import os
import threading
import time

from kafka import KafkaConsumer

from common.db import create_conn_pool
from common.kafka_helpers import build_producer, run_consumer_loop

import kafka_handler
import tpc as tpc_coordinator
import saga as saga_coordinator
import recovery

GATEWAY_KAFKA  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS",          "kafka-external:9092")
INTERNAL_KAFKA = os.environ.get("INTERNAL_KAFKA_BOOTSTRAP_SERVERS", "kafka-internal:9092")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _run_internal_response_consumer(conn_pool, bootstrap: str) -> None:
    """Single-threaded consumer — routes responses to TPC/SAGA state machines."""
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                "internal.responses",
                bootstrap_servers=bootstrap,
                group_id="order-service-internal",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("Internal response consumer connected")

            for message in consumer:
                payload  = message.value
                corr_id  = payload.get("correlation_id", "")

                if not corr_id:
                    try:
                        consumer.commit()
                    except Exception:
                        pass
                    continue

                conn = conn_pool.getconn()
                try:
                    if tpc_coordinator.is_tpc_message(corr_id):
                        tpc_coordinator.handle_response(corr_id, payload, conn)
                    elif saga_coordinator.is_saga_message(corr_id):
                        saga_coordinator.handle_response(corr_id, payload, conn)
                    else:
                        logger.debug("No handler for corr_id=%s", corr_id)
                        conn.rollback()
                except Exception as exc:
                    logger.error("Error processing corr_id=%s: %s",
                                 corr_id, exc, exc_info=True)
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                finally:
                    conn_pool.putconn(conn)

                try:
                    consumer.commit()
                except Exception:
                    pass

        except Exception as exc:
            logger.error("Internal consumer error, reconnecting in 3s: %s", exc)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass
        time.sleep(3)


def _run_price_lookup_consumer(bootstrap: str) -> None:
    """Delivers gateway.responses to addItem workers waiting on item prices."""
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                "gateway.responses",
                bootstrap_servers=bootstrap,
                group_id="order-service-price-lookup",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("Price lookup consumer connected")

            for message in consumer:
                corr_id = message.value.get("correlation_id", "")
                kafka_handler.register_price_lookup_response(
                    corr_id, message.value
                )

        except Exception as exc:
            logger.error("Price lookup consumer error, reconnecting in 3s: %s", exc)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass
        time.sleep(3)


def main() -> None:
    conn_pool = create_conn_pool("ORDER")

    gateway_producer  = build_producer(GATEWAY_KAFKA)
    internal_producer = build_producer(INTERNAL_KAFKA)

    # Inject producers into modules that need them
    kafka_handler._gateway_producer = gateway_producer
    tpc_coordinator.init(conn_pool, internal_producer, gateway_producer)
    saga_coordinator.init(conn_pool, internal_producer, gateway_producer)

    # Gateway consumer — thread pool for all inbound requests
    threading.Thread(
        target=run_consumer_loop,
        args=(
            conn_pool, GATEWAY_KAFKA,
            "gateway.orders", "order-service-gateway",
            gateway_producer, "gateway.responses",
            kafka_handler.handle_gateway_message, "Order-Gateway",
        ),
        daemon=True, name="gateway-consumer",
    ).start()

    # Internal response consumer — single-threaded state machine driver
    threading.Thread(
        target=_run_internal_response_consumer,
        args=(conn_pool, INTERNAL_KAFKA),
        daemon=True, name="internal-consumer",
    ).start()

    # Price lookup consumer — feeds item prices back to addItem workers
    threading.Thread(
        target=_run_price_lookup_consumer,
        args=(GATEWAY_KAFKA,),
        daemon=True, name="price-lookup-consumer",
    ).start()

    # Recovery thread — re-drives stale in-flight transactions
    threading.Thread(
        target=recovery.run_recovery_loop,
        args=(conn_pool, internal_producer, gateway_producer),
        daemon=True, name="recovery",
    ).start()

    logger.info("Order service started (checkout mode: %s)",
                os.environ.get("CHECKOUT_MODE", "SAGA"))

    threading.Event().wait()


if __name__ == "__main__":
    main()