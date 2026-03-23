"""
Order service — gateway message handler.

Routes HTTP-proxy envelopes from gateway.orders.  Follows the same pattern
as payment and stock: a single route_fn called by run_consumer_loop.

Synchronous operations (create, batch_init, find, addItem) return
(status_code, body) directly.

Checkout delegates to the TPC or SAGA coordinator and returns (None, None)
— the gateway response is published asynchronously by the coordinator once
the protocol completes.

Price lookup
------------
addItem needs the item's current price from the stock service.  We publish
a GET /find/<item_id> to gateway.stock and block until the price-lookup
consumer thread delivers the response.  The 10s timeout means a stalled
stock service causes addItem to fail gracefully rather than hang forever.
"""

import json
import logging
import os
import threading
import uuid
from collections import defaultdict

from common.idempotency import check_idempotency, save_idempotency
from psycopg2.extras import execute_batch
from db import get_order

import tpc as tpc_coordinator
import saga as saga_coordinator

logger = logging.getLogger(__name__)

CHECKOUT_MODE = os.environ.get("CHECKOUT_MODE", "SAGA").upper()

# ---------------------------------------------------------------------------
# Price lookup — shared state between handler workers and lookup consumer
# ---------------------------------------------------------------------------

_pending_lookups: dict[str, tuple[threading.Event, dict | None]] = {}
_pending_lookups_lock = threading.Lock()

# Injected by app.py after the producer is built
_gateway_producer    = None
_GATEWAY_STOCK_TOPIC = "gateway.stock"


def register_price_lookup_response(corr_id: str, payload: dict) -> bool:
    """
    Called by the price-lookup consumer for every gateway.responses message.
    Returns True if this corr_id belonged to a pending price lookup.
    """
    with _pending_lookups_lock:
        if corr_id not in _pending_lookups:
            return False
        event, _ = _pending_lookups[corr_id]
        _pending_lookups[corr_id] = (event, payload)
        event.set()
        return True


def _fetch_item_price(item_id: str) -> dict | None:
    """
    Send GET /find/<item_id> to gateway.stock and block for the response.
    Returns the full response payload or None on timeout.
    """
    corr_id = str(uuid.uuid4())
    event   = threading.Event()

    with _pending_lookups_lock:
        _pending_lookups[corr_id] = (event, None)

    try:
        from kafka.errors import KafkaError
        _gateway_producer.send(
            _GATEWAY_STOCK_TOPIC,
            key=corr_id,
            value={
                "correlation_id": corr_id,
                "method":  "GET",
                "path":    f"/find/{item_id}",
                "headers": {},
                "body":    None,
            },
        )
        _gateway_producer.flush(timeout=5)
    except Exception as exc:
        with _pending_lookups_lock:
            _pending_lookups.pop(corr_id, None)
        logger.error("Failed to send price lookup for item=%s: %s", item_id, exc)
        return None

    if not event.wait(timeout=10):
        with _pending_lookups_lock:
            _pending_lookups.pop(corr_id, None)
        logger.warning("Price lookup timeout for item=%s", item_id)
        return None

    with _pending_lookups_lock:
        _, response = _pending_lookups.pop(corr_id)
    return response


# ---------------------------------------------------------------------------
# Gateway message routing
# ---------------------------------------------------------------------------

def handle_gateway_message(payload: dict, conn) -> tuple:
    """
    Route an HTTP-proxy envelope from gateway.orders.
    Returns (status_code, body) for sync ops, (None, None) for checkout.
    """
    method   = payload.get("method", "GET").upper()
    path     = payload.get("path", "/")
    body     = payload.get("body") or {}
    headers  = payload.get("headers") or {}
    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")
    corr_id  = payload.get("correlation_id", "")

    logger.info(f"Handeling message with payload: {payload}")

    # POST /create/<user_id>
    if method == "POST" and len(segments) >= 2 and segments[0] == "create":
        order_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO orders (id, paid, items, user_id, total_cost) "
                "VALUES (%s, %s, %s, %s, %s)",
                (order_id, False, json.dumps([]), segments[1], 0),
            )
        conn.commit()
        return 201, {"order_id": order_id}

    # POST /batch_init/<n>/<n_items>/<n_users>/<item_price>
    elif method == "POST" and len(segments) >= 5 and segments[0] == "batch_init":
        import random
        n, n_items, n_users, item_price = (
            int(segments[1]), int(segments[2]),
            int(segments[3]), int(segments[4]),
        )
        if any(v < 0 for v in (n, n_items, n_users, item_price)):
            return 400, {"error": "All batch_init parameters must be non-negative"}
        with conn.cursor() as cur:
            execute_batch(
                cur,
                "INSERT INTO orders (id, paid, items, user_id, total_cost) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO UPDATE "
                "SET paid = EXCLUDED.paid, items = EXCLUDED.items, "
                "    user_id = EXCLUDED.user_id, total_cost = EXCLUDED.total_cost",
                [
                    (
                        str(i), False,
                        json.dumps([
                            [str(random.randint(0, max(n_items - 1, 0))), 1],
                            [str(random.randint(0, max(n_items - 1, 0))), 1],
                        ]),
                        str(random.randint(0, max(n_users - 1, 0))),
                        2 * item_price,
                    )
                    for i in range(n)
                ],
            )
        conn.commit()
        return 200, {"msg": "Batch init for orders successful"}

    # GET /find/<order_id>
    elif method == "GET" and len(segments) >= 2 and segments[0] == "find":
        try:
            order = get_order(conn, segments[1])
        except ValueError as exc:
            conn.commit()
            return 400, {"error": str(exc)}
        conn.commit()
        return 200, {
            "order_id":   segments[1],
            "paid":       order["paid"],
            "items":      order["items"],
            "user_id":    order["user_id"],
            "total_cost": order["total_cost"],
        }

    # POST /addItem/<order_id>/<item_id>/<quantity>
    elif method == "POST" and len(segments) >= 4 and segments[0] == "addItem":
        order_id = segments[1]
        item_id  = segments[2]
        quantity = int(segments[3])

        if quantity <= 0:
            return 400, {"error": "Quantity must be positive"}

        with conn.cursor() as cur:
            cached = check_idempotency(cur, idem_key)
            if cached:
                conn.commit()
                return cached

            price_resp = _fetch_item_price(item_id)
            if price_resp is None:
                conn.rollback()
                return 503, {"error": f"Could not fetch price for item {item_id}"}
            if price_resp.get("status_code") != 200:
                conn.rollback()
                return 400, {"error": f"Item {item_id} not found in stock service"}

            item_price = price_resp["body"]["price"]

            cur.execute(
                "SELECT items, total_cost FROM orders WHERE id = %s FOR UPDATE",
                (order_id,),
            )
            row = cur.fetchone()
            if row is None:
                conn.rollback()
                return 400, {"error": f"Order {order_id} not found"}

            items_list, total_cost = row
            merged = False
            for entry in items_list:
                if entry[0] == item_id:
                    if entry[1] + quantity < 0:
                        conn.rollback()
                        return (400, {"error": f"Can't have negative amount of items in order"})
                    entry[1] += quantity
                    merged = True
                    break
            if not merged:
                items_list.append([item_id, quantity])
            total_cost += quantity * item_price

            cur.execute(
                "UPDATE orders SET items = %s, total_cost = %s WHERE id = %s",
                (json.dumps(items_list), total_cost, order_id),
            )
            resp = f"Item: {item_id} added to order: {order_id}, total: {total_cost}"
            save_idempotency(cur, idem_key, 200, resp)
        conn.commit()
        return 200, resp

    # POST /checkout/<order_id>
    elif method == "POST" and len(segments) >= 2 and segments[0] == "checkout":
        order_id = segments[1]
        if CHECKOUT_MODE == "TPC":
            return tpc_coordinator.checkout_tpc(conn, order_id, corr_id, idem_key)
        else:
            return saga_coordinator.checkout_saga(conn, order_id, corr_id, idem_key)

    return 404, {"error": f"No handler for {method} {path}"}