import threading
import logging

from common.streams import get_bus, publish

logger = logging.getLogger(__name__)


class StreamRpc:
    """Synchronous request–response over a Redis Stream.

    send() publishes a message and blocks until handle_response() is called
    with the matching correlation_id by whichever consumer reads the reply stream.
    """

    def __init__(self, default_timeout: float = 10.0):
        self._default_timeout = default_timeout
        self._pending: dict = {}
        self._lock = threading.Lock()

    def send(
        self,
        bus_pool,
        stream: str,
        payload: dict,
        correlation_id: str,
        timeout: float = None,
    ) -> dict:
        timeout = timeout if timeout is not None else self._default_timeout
        # register before publishing to avoid a race where the response arrives first
        event = threading.Event()
        with self._lock:
            self._pending[correlation_id] = (event, None)

        try:
            publish(get_bus(bus_pool), stream, payload)
        except Exception as exc:
            self._discard(correlation_id)
            logger.error("StreamRpc publish failed on '%s': %s", stream, exc)
            return {"status_code": 400, "body": f"Bus publish error: {exc}"}

        if not event.wait(timeout=timeout):
            self._discard(correlation_id)
            logger.warning("StreamRpc timeout for %s", correlation_id)
            return {"status_code": 400, "body": "Request timed out"}

        with self._lock:
            _, response = self._pending.pop(correlation_id)
        return response

    def handle_response(self, correlation_id: str, payload: dict) -> bool:
        """Deliver a response to a waiting send() call.

        Returns True if a waiter was found and signalled, False if the
        correlation_id is unknown (response for a timed-out request).
        """
        with self._lock:
            entry = self._pending.get(correlation_id)
            if entry is None:
                return False
            event, _ = entry
            self._pending[correlation_id] = (event, payload)
        event.set()
        return True

    def _discard(self, correlation_id: str):
        with self._lock:
            self._pending.pop(correlation_id, None)
