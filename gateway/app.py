"""API Gateway — HTTP-to-Kafka bridge. Only active in SAGA mode."""

import json
import logging
import os
import threading
import time
import uuid

import kafka
from flask import Flask, Response, abort, jsonify, request
from kafka.admin import NewTopic
from kafka.errors import KafkaError

from common.kafka_helpers import build_producer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-external:9092")
REQUEST_TIMEOUT_S = int(os.environ.get("REQUEST_TIMEOUT_MS", "30000")) / 1000
GATEWAY_TOPICS = ["gateway.orders", "gateway.stock", "gateway.payment"]
RESPONSE_TOPIC = "gateway.responses"

_STRIP_HEADERS = {"host", "connection", "transfer-encoding", "content-length"}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask("gateway-service")

# ---------------------------------------------------------------------------
# Kafka Helpers
# ---------------------------------------------------------------------------

def ensure_topics(bootstrap_servers, topics):
    try:
        admin = kafka.KafkaAdminClient(bootstrap_servers=bootstrap_servers, request_timeout_ms=10_000)
        existing = set(admin.list_topics())
        missing = [t for t in topics if t not in existing]
        if missing:
            admin.create_topics(
                [NewTopic(name=t, num_partitions=3, replication_factor=1) for t in missing],
                validate_only=False,
            )
            logger.info("Created Kafka topics: %s", missing)
        admin.close()
    except KafkaError as exc:
        logger.warning("Could not ensure topics exist: %s", exc)


# ---------------------------------------------------------------------------
# KafkaClient — Publish Request, Block For Correlated Response
# ---------------------------------------------------------------------------

class KafkaClient:

    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self._producer = build_producer(bootstrap_servers)
        self._pending: dict[str, tuple[threading.Event, dict | None]] = {}
        self._pending_lock = threading.Lock()
        self._start_response_consumer()

    def send_request(self, topic, payload, correlation_id):
        event = threading.Event()

        # Register Before Publishing To Prevent Race
        with self._pending_lock:
            self._pending[correlation_id] = (event, None)

        try:
            self._producer.send(topic, key=correlation_id, value=payload).get(timeout=10)
        except KafkaError as exc:
            self._remove_pending(correlation_id)
            raise RuntimeError(f"Failed to enqueue message: {exc}") from exc

        if not event.wait(timeout=REQUEST_TIMEOUT_S):
            self._remove_pending(correlation_id)
            abort(504, description="Gateway timeout waiting for service response")

        with self._pending_lock:
            _, response = self._pending.pop(correlation_id)

        return response

    def _remove_pending(self, correlation_id):
        with self._pending_lock:
            self._pending.pop(correlation_id, None)

    def _start_response_consumer(self):
        def consume():
            while True:
                try:
                    consumer = kafka.KafkaConsumer(
                        RESPONSE_TOPIC,
                        bootstrap_servers=self.bootstrap_servers,
                        group_id="gateway-response-listener",
                        auto_offset_reset="latest",
                        enable_auto_commit=True,
                        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    )
                    logger.info("Response consumer started on '%s'", RESPONSE_TOPIC)
                    for message in consumer:
                        self._handle_response(message.value)
                except Exception as exc:
                    logger.error("Response consumer crashed, reconnecting in 3s: %s", exc)
                    time.sleep(3)

        threading.Thread(target=consume, daemon=True, name="kafka-response-consumer").start()

    def _handle_response(self, payload):
        correlation_id = payload.get("correlation_id")
        if not correlation_id:
            return
        with self._pending_lock:
            entry = self._pending.get(correlation_id)
            if entry is None:
                return
            event, _ = entry
            self._pending[correlation_id] = (event, payload)
        event.set()

# ---------------------------------------------------------------------------
# Flask Proxy Routes
# ---------------------------------------------------------------------------

def _proxy(service_topic, subpath, client):
    full_path = f"/{subpath}" if subpath else "/"
    correlation_id = request.correlation_id

    forwarded_headers = {k: v for k, v in request.headers if k.lower() not in _STRIP_HEADERS}

    payload = {
        "method": request.method, "path": full_path,
        "correlation_id": correlation_id,
        "query_params": dict(request.args),
        "headers": forwarded_headers,
        "body": request.get_json(silent=True) or dict(request.form) or None,
    }

    try:
        response = client.send_request(topic=service_topic, payload=payload, correlation_id=correlation_id)
    except RuntimeError as exc:
        logger.error("Kafka send error for topic '%s': %s", service_topic, exc)
        abort(502, description="Service temporarily unavailable")

    return _build_response(response)


def _build_response(response):
    status_code = response.get("status_code", 200)
    body = response.get("body", "")
    headers = response.get("headers") or {}
    if isinstance(body, (dict, list)):
        return jsonify(body), status_code, headers
    return Response(str(body), status=status_code, headers=headers)


@app.before_request
def _attach_correlation_id():
    request.correlation_id = str(uuid.uuid4())


METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]

@app.route("/orders/", defaults={"subpath": ""}, methods=METHODS)
@app.route("/orders/<path:subpath>", methods=METHODS)
def orders_proxy(subpath):
    return _proxy("gateway.orders", subpath, kafka_client)

@app.route("/stock/", defaults={"subpath": ""}, methods=METHODS)
@app.route("/stock/<path:subpath>", methods=METHODS)
def stock_proxy(subpath):
    return _proxy("gateway.stock", subpath, kafka_client)

@app.route("/payment/", defaults={"subpath": ""}, methods=METHODS)
@app.route("/payment/<path:subpath>", methods=METHODS)
def payment_proxy(subpath):
    return _proxy("gateway.payment", subpath, kafka_client)

@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


ensure_topics(KAFKA_BOOTSTRAP_SERVERS, GATEWAY_TOPICS + [RESPONSE_TOPIC])
kafka_client = KafkaClient(KAFKA_BOOTSTRAP_SERVERS)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
