"""
Stock service — pure Kafka consumer process.

Three consumer loops run as threads:

  gateway-consumer  — HTTP-proxy requests from the api-gateway
                      (create_item, batch_init, find, add, subtract, add_batch, subtract_batch)
  tpc-consumer      — 2PC commands from the order service
                      (stock.prepare / stock.commit / stock.rollback)
  saga-consumer     — SAGA commands from the order service
                      (stock.execute / stock.rollback)

A background recovery thread periodically rolls back stale 2PC prepared
transactions.  SAGA transactions have no stale state — silence means success.
"""

import logging
import os
import threading

from common.db import create_conn_pool
from common.kafka_helpers import build_producer, run_consumer_loop

import kafka_handler
import recovery

from datetime import datetime

GATEWAY_KAFKA  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS",          "kafka-external:9092")
INTERNAL_KAFKA = os.environ.get("INTERNAL_KAFKA_BOOTSTRAP_SERVERS", "kafka-internal:9092")

# Create logs dir and timestamped file
os.makedirs("/logs", exist_ok=True)
_log_filename = "stock-" + datetime.now().strftime("%y%m%d-%H%M%S") + ".log"
_log_path = os.path.join("/logs", _log_filename)

# Root config: write to both stdout and file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(),           # keeps docker compose logs -f working
        logging.FileHandler(_log_path),    # writes to /logs/YYMMDD-HHMMSS.log
    ]
)

# Silence noisy kafka loggers
for _noisy in ("kafka", "kafka.conn", "kafka.client",
               "kafka.consumer", "kafka.producer"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

def _start_consumer(conn_pool, bootstrap, topic, group_id, producer, response_topic, handler, name):
    """Convenience wrapper — starts a run_consumer_loop daemon thread."""
    threading.Thread(
        target=run_consumer_loop,
        args=(conn_pool, bootstrap, topic, group_id, producer, response_topic, handler, name),
        daemon=True,
        name=name,
    ).start()


def main():
    conn_pool = create_conn_pool("STOCK")

    gateway_producer  = build_producer(GATEWAY_KAFKA)
    internal_producer = build_producer(INTERNAL_KAFKA)

    # Gateway — HTTP-proxy requests from the api-gateway
    _start_consumer(
        conn_pool, GATEWAY_KAFKA,
        "gateway.stock", "stock-service-gateway",
        gateway_producer, "gateway.responses",
        kafka_handler.handle_gateway_message, "Stock-Gateway",
    )

    # 2PC — pessimistic coordination (prepare / commit / rollback)
    _start_consumer(
        conn_pool, INTERNAL_KAFKA,
        "internal.stock.tpc", "stock-service-tpc",
        internal_producer, "internal.responses",
        kafka_handler.handle_tpc_message, "Stock-TPC",
    )

    # SAGA — optimistic coordination (execute / rollback, silence = success)
    _start_consumer(
        conn_pool, INTERNAL_KAFKA,
        "internal.stock.saga", "stock-service-saga",
        internal_producer, "internal.responses",
        kafka_handler.handle_saga_message, "Stock-SAGA",
    )

    # Recovery — periodically rolls back stale 2PC prepared transactions
    threading.Thread(
        target=recovery.run_recovery_loop,
        args=(conn_pool, internal_producer),
        daemon=True,
        name="recovery",
    ).start()

    logger.info("Stock service started")
    threading.Event().wait()


if __name__ == "__main__":
    main()