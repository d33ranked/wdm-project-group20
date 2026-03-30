import gevent.monkey

gevent.monkey.patch_all()

import json
import logging
import os
import threading
import time
import uuid

import redis as redis_lib
from flask import Flask, Response, abort, jsonify, request

from common.stream_rpc import StreamRpc
from common.streams import create_bus_pool, get_bus, ensure_groups, publish

GATEWAY_STREAMS = ["gateway.orders", "gateway.stock", "gateway.payment"]
RESPONSE_STREAM = "gateway.responses"
REQUEST_TIMEOUT_S = int(os.environ.get("REQUEST_TIMEOUT_MS", "30000")) / 1000
_RESPONSE_BATCH_SIZE = int(os.environ.get("STREAM_BATCH_SIZE", "100"))

_STRIP_HEADERS = {"host", "connection", "transfer-encoding", "content-length"}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask("gateway-service")


class StreamClient:

    def __init__(self, bus_pool):
        self._pool = bus_pool
        self._rpc = StreamRpc(default_timeout=REQUEST_TIMEOUT_S)
        self._start_response_consumer()

    def send_request(self, stream: str, payload: dict, correlation_id: str) -> dict:
        response = self._rpc.send(self._pool, stream, payload, correlation_id)
        if response.get("status_code") == 400 and "timed out" in str(response.get("body", "")):
            abort(504, description="Gateway timeout waiting for service response")
        return response

    def _start_response_consumer(self):
        def consume():
            bus = get_bus(self._pool)
            last_id = "$"
            while True:
                try:
                    result = bus.xread(
                        {RESPONSE_STREAM: last_id},
                        count=_RESPONSE_BATCH_SIZE,
                        block=2000,
                    )
                    if not result:
                        continue
                    for _stream, entries in result:
                        for msg_id, fields in entries:
                            last_id = msg_id
                            try:
                                payload = json.loads(fields["data"])
                                corr_id = payload.get("correlation_id", "")
                                if corr_id:
                                    self._rpc.handle_response(corr_id, payload)
                            except (KeyError, json.JSONDecodeError) as exc:
                                logger.error(
                                    "Malformed response entry %s: %s", msg_id, exc
                                )
                except Exception as exc:
                    logger.error("Response consumer error, retrying in 1s: %s", exc)
                    time.sleep(1)

        threading.Thread(
            target=consume, daemon=True, name="stream-response-consumer"
        ).start()


def _proxy(service_stream: str, subpath: str, client: StreamClient):
    full_path = f"/{subpath}" if subpath else "/"
    correlation_id = request.correlation_id

    forwarded_headers = {
        k: v for k, v in request.headers if k.lower() not in _STRIP_HEADERS
    }

    payload = {
        "method": request.method,
        "path": full_path,
        "correlation_id": correlation_id,
        "query_params": dict(request.args),
        "headers": forwarded_headers,
        "body": request.get_json(silent=True) or dict(request.form) or None,
    }

    response = client.send_request(
        stream=service_stream,
        payload=payload,
        correlation_id=correlation_id,
    )

    return _build_response(response)


def _build_response(response: dict):
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
    return _proxy("gateway.orders", subpath, stream_client)


@app.route("/stock/", defaults={"subpath": ""}, methods=METHODS)
@app.route("/stock/<path:subpath>", methods=METHODS)
def stock_proxy(subpath):
    return _proxy("gateway.stock", subpath, stream_client)


@app.route("/payment/", defaults={"subpath": ""}, methods=METHODS)
@app.route("/payment/<path:subpath>", methods=METHODS)
def payment_proxy(subpath):
    return _proxy("gateway.payment", subpath, stream_client)


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


bus_pool = create_bus_pool()

_startup_bus = get_bus(bus_pool)
ensure_groups(_startup_bus, [(RESPONSE_STREAM, "gateway-init")])

stream_client = StreamClient(bus_pool)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
