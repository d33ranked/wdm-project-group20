"""
Stock service — unified Kafka message handler.

Three consumer topics, three handlers:

  gateway.stock         — HTTP-proxy requests from the api-gateway
  internal.stock.tpc    — 2PC commands from the order service (pessimistic)
  internal.stock.saga   — SAGA commands from the order service (optimistic)

Gateway message contract (HTTP-proxy envelope):
  { "method": "POST", "path": "/subtract_batch", "body": {"items": [...]}, ... }

2PC message contract (internal.stock.tpc):
  Incoming:
    { "type": "stock.prepare",  "txn_id": "...", "items": [{"item_id": "...", "quantity": 1}] }
    { "type": "stock.commit",   "txn_id": "..." }
    { "type": "stock.rollback", "txn_id": "..." }
  Outgoing:
    { "type": "stock.prepared",   "txn_id": "..." }
    { "type": "stock.committed",  "txn_id": "..." }
    { "type": "stock.rolledback", "txn_id": "..." }
    { "type": "stock.failed",     "txn_id": "...", "reason": "..." }

SAGA message contract (internal.stock.saga):
  Incoming:
    { "type": "stock.execute",  "txn_id": "...", "items": [{"item_id": "...", "quantity": 1}] }
    { "type": "stock.rollback", "txn_id": "..." }
  Outgoing:
    { "type": "stock.executed",   "txn_id": "..." }
    { "type": "stock.rolledback", "txn_id": "..." }
    { "type": "stock.failed",     "txn_id": "...", "reason": "..." }

Multi-item locking
------------------
Stock operations may touch multiple item rows simultaneously.  To prevent
deadlocks between concurrent transactions, all multi-row locks are always
acquired in ascending item_id order.  This is enforced in _lock_items_sorted.

Consistency guarantees
----------------------
  - 2PC:  stock is deducted at prepare-time and held in prepared_transactions
          (composite PK txn_id+item_id) until commit (delete entries) or
          rollback (restore stock + delete entries).
  - SAGA: stock is deducted immediately at execute-time and recorded in
          compensating_transactions so a rollback can restore the exact
          quantities.  Silence after execute means success.
  - Both paths are fully idempotent by txn_id via advisory_lock.
"""

import logging
import uuid

from common.idempotency import get_advisory_lock, check_idempotency, save_idempotency
from psycopg2.extras import execute_batch

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared DB helpers
# ---------------------------------------------------------------------------

def _lock_items_sorted(cur, item_ids: list) -> dict:
    """
    Lock all requested item rows in ascending id order and return a
    {item_id: stock} map.  Sorted order prevents deadlocks when concurrent
    transactions compete for overlapping sets of items.
    """
    sorted_ids = sorted(set(item_ids))
    cur.execute(
        "SELECT id, stock FROM items WHERE id = ANY(%s) ORDER BY id FOR UPDATE",
        (sorted_ids,),
    )
    return {row[0]: row[1] for row in cur.fetchall()}


def _validate_items(raw_items: list):
    """
    Parse and validate the items list from a message payload.
    Returns a list of (item_id, quantity) tuples, or an error string.
    """
    if not raw_items:
        return "No items provided"
    try:
        parsed = [(entry["item_id"], int(entry["quantity"])) for entry in raw_items]
    except (KeyError, TypeError, ValueError) as e:
        return f'Expected items: [{{"item_id": str, "quantity": int}}], got error: {e}'
    for item_id, qty in parsed:
        if qty <= 0:
            return f"Item {item_id} quantity must be positive, got {qty}"
    return parsed


# ---------------------------------------------------------------------------
# Gateway handler
# ---------------------------------------------------------------------------

def handle_gateway_message(payload, conn):
    """
    Route an HTTP-proxy envelope arriving on gateway.stock.
    Returns (status_code, body) — published to gateway.responses by the consumer loop.
    """
    method   = payload.get("method", "GET").upper()
    path     = payload.get("path", "/")
    body     = payload.get("body") or {}
    headers  = payload.get("headers") or {}
    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    logger.info(f"Handeling message with payload: {payload}")

    # POST /item/create/<price>
    if method == "POST" and len(segments) >= 3 and segments[0] == "item" and segments[1] == "create":
        logger.info(f"Handeling create message: {payload}")
        price = int(segments[2])
        if price < 0:
            return 400, {"error": "Price must be non-negative!"}
        item_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO items (id, stock, price) VALUES (%s, %s, %s)",
                (item_id, 0, price),
            )
        conn.commit()
        logger.info("Finished handeling create")
        return 201, {"item_id": item_id}

    # POST /batch_init/<n>/<starting_stock>/<item_price>
    elif method == "POST" and len(segments) >= 4 and segments[0] == "batch_init":
        n, starting_stock, item_price = int(segments[1]), int(segments[2]), int(segments[3])
        if n < 0:
            return 400, {"error": f"n must be non-negative, got: {n}"}
        elif starting_stock < 0:
            return 400, {"error": f"starting_stock must be non-negative, got: {starting_stock}"}
        elif item_price < 0:
            return 400, {"error": f"item_price must be non-negative, got: {item_price}"}
        with conn.cursor() as cur:
            execute_batch(
                cur,
                "INSERT INTO items (id, stock, price) VALUES (%s, %s, %s) "
                "ON CONFLICT (id) DO UPDATE SET stock = EXCLUDED.stock, price = EXCLUDED.price",
                [(str(i), starting_stock, item_price) for i in range(n)],
            )
        conn.commit()
        return 200, {"msg": "Batch init for stock successful"}

    # GET /find/<item_id>
    elif method == "GET" and len(segments) >= 2 and segments[0] == "find":
        item_id = segments[1]
        logger.info(f"Handling find for: {item_id}")
        with conn.cursor() as cur:
            cur.execute("SELECT stock, price FROM items WHERE id = %s", (item_id,))
            row = cur.fetchone()
        conn.commit()
        logger.info("Finshed handeling find")
        if row is None:
            return 400, {"error": f"Item {item_id} not found"}
        return 200, {"item_id": item_id, "stock": row[0], "price": row[1]}

    # POST /add/<item_id>/<amount>
    elif method == "POST" and len(segments) >= 3 and segments[0] == "add":
        item_id, amount = segments[1], int(segments[2])
        logger.info(f"Handling add for: {item_id} with {amount}")
        if amount <= 0:
            return 400, {"error": "Amount must be positive! use the subtract endpoint."}
        with conn.cursor() as cur:
            cached = check_idempotency(cur, idem_key)
            if cached:
                conn.commit()
                logger.warning(f"Idempotency hit on add stock, item_id: {item_id}, cached result is: {cached}")
                return cached
            cur.execute("SELECT 1 FROM items WHERE id = %s FOR UPDATE", (item_id,))
            if cur.fetchone() is None:
                conn.rollback()
                return 400, {"error": f"Item {item_id} not found"}
            cur.execute(
                "UPDATE items SET stock = stock + %s WHERE id = %s RETURNING stock",
                (amount, item_id),
            )
            new_stock = cur.fetchone()[0]
            resp = f"Item: {item_id} stock updated, added {amount}"
            save_idempotency(cur, idem_key, 200, resp)
        conn.commit()
        logger.info("successfully finished add item")
        return 200, resp

    # POST /subtract/<item_id>/<amount>
    elif method == "POST" and len(segments) >= 3 and segments[0] == "subtract":
        item_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive! Use the add enpoint."}
        with conn.cursor() as cur:
            cached = check_idempotency(cur, idem_key)
            if cached:
                conn.commit()
                logger.warning(f"Idempotency hit on subtract stock, item_id: {item_id}, cached result is: {cached}")
                return cached
            cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
            row = cur.fetchone()
            if row is None:
                conn.rollback()
                return 400, {"error": f"Item {item_id} not found"}
            elif row[0] - amount < 0:
                conn.rollback()
                return 400, {"error": f"Item {item_id} has insufficient stock"}
            cur.execute(
                "UPDATE items SET stock = stock - %s WHERE id = %s RETURNING stock",
                (amount, item_id),
            )
            resp = f"Item: {item_id} stock updated subtraced {amount}"
            save_idempotency(cur, idem_key, 200, resp)
        conn.commit()
        return 200, resp

    return 404, {"error": f"No handler for {method} {path}"}


# ---------------------------------------------------------------------------
# 2PC handler (internal.stock.tpc)
# ---------------------------------------------------------------------------

def handle_tpc_message(payload, conn):
    """
    Route a 2PC command arriving on internal.stock.tpc.

    Pessimistic path: stock is deducted at prepare-time and held in
    prepared_transactions until commit or rollback.
    """
    msg_type = payload.get("type", "")
    txn_id   = payload.get("txn_id")

    if not txn_id:
        logger.warning("TPC message missing txn_id: %s", payload)
        return 400, {"type": "stock.failed", "txn_id": None, "reason": "missing txn_id"}

    elif msg_type == "stock.prepare":
        return _tpc_prepare(payload, conn, txn_id)
    elif msg_type == "stock.commit":
        return _tpc_commit(conn, txn_id)
    elif msg_type == "stock.rollback":
        return _tpc_rollback(conn, txn_id)

    logger.warning("Unknown TPC message type: %s", msg_type)
    return 400, {"type": "stock.failed", "txn_id": txn_id, "reason": f"unknown type: {msg_type}"}


def _tpc_prepare(payload, conn, txn_id):
    """
    Deduct stock for all items and write prepared_transactions undo-log entries.

    Items locked in sorted order to prevent deadlocks.  Idempotent by txn_id.
    """
    items = _validate_items(payload.get("items", []))
    if isinstance(items, str):
        return 400, {"type": "stock.failed", "txn_id": txn_id, "reason": items}

    with conn.cursor() as cur:
        # Advisory lock serialises concurrent prepares for the same txn_id
        get_advisory_lock(cur, txn_id)
        cur.execute(
            "SELECT 1 FROM prepared_transactions WHERE txn_id = %s LIMIT 1",
            (txn_id,),
        )
        if cur.fetchone() is not None:
            logger.info("TPC prepare idempotent hit txn=%s", txn_id)
            conn.commit()
            return 200, {"type": "stock.prepared", "txn_id": txn_id}

        # Lock all rows in sorted order before validating
        stock_map = _lock_items_sorted(cur, [item_id for item_id, _ in items])
        for item_id, quantity in items:
            if item_id not in stock_map:
                conn.rollback()
                return 400, {"type": "stock.failed", "txn_id": txn_id, "reason": f"item {item_id} not found"}
            elif stock_map[item_id] < quantity:
                conn.rollback()
                return 400, {"type": "stock.failed", "txn_id": txn_id, "reason": f"item {item_id} has insufficient stock"}

        # Deduct and write undo-log entries for all items atomically
        for item_id, quantity in items:
            cur.execute("UPDATE items SET stock = stock - %s WHERE id = %s", (quantity, item_id))
            cur.execute(
                "INSERT INTO prepared_transactions (txn_id, item_id, quantity) VALUES (%s, %s, %s)",
                (txn_id, item_id, quantity),
            )

    conn.commit()
    logger.info("TPC prepared txn=%s items=%s", txn_id, items)
    return 200, {"type": "stock.prepared", "txn_id": txn_id}


def _tpc_commit(conn, txn_id):
    """
    Finalise a prepared transaction by deleting all undo-log entries.
    Stock already deducted at prepare-time.  Idempotent: no rows = success.
    """
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM prepared_transactions WHERE txn_id = %s RETURNING item_id",
            (txn_id,),
        )
        deleted = cur.fetchall()
    conn.commit()

    if not deleted:
        logger.info("TPC commit idempotent hit (no rows) txn=%s", txn_id)
    else:
        logger.info("TPC committed txn=%s (%d items)", txn_id, len(deleted))

    return 200, {"type": "stock.committed", "txn_id": txn_id}


def _tpc_rollback(conn, txn_id):
    """
    Abort a prepared transaction by restoring stock for all reserved items.

    Locks item rows in sorted order before restoring.  Idempotent: no rows = success.
    """
    with conn.cursor() as cur:
        # Lock log rows to prevent concurrent double-rollback
        cur.execute(
            "DELETE FROM prepared_transactions WHERE txn_id = %s RETURNING item_id, quantity",
            (txn_id,)
        )
        rows = cur.fetchall()

        if not rows:
            logger.info("TPC rollback idempotent hit (no rows) txn=%s", txn_id)
            conn.commit()
            return 200, {"type": "stock.rolledback", "txn_id": txn_id}

        # Lock item rows in sorted order before restoring
        item_ids = sorted(item_id for item_id, _ in rows)
        cur.execute(
            "SELECT id FROM items WHERE id = ANY(%s) ORDER BY id FOR UPDATE",
            (item_ids,),
        )
        execute_batch(
            cur,
            "UPDATE items SET stock = stock + %s WHERE id = %s",
            [(quantity, item_id) for item_id, quantity in rows]
        )
    conn.commit()
    logger.info("TPC rolled back txn=%s (%d items)", txn_id, len(rows))
    logger.info(f"Rolled bac txn=%s, added: {[(quantity, item_id) for item_id, quantity in rows]}")
    return 200, {"type": "stock.rolledback", "txn_id": txn_id}


# ---------------------------------------------------------------------------
# SAGA handler (internal.stock.saga)
# ---------------------------------------------------------------------------

def handle_saga_message(payload, conn):
    """
    Route a SAGA command arriving on internal.stock.saga.

    Optimistic path: stock is deducted immediately and the operation is
    considered committed unless a rollback arrives later.  Silence = success.
    """
    msg_type = payload.get("type", "")
    txn_id   = payload.get("txn_id")

    if not txn_id:
        logger.warning("SAGA message missing txn_id: %s", payload)
        return 400, {"type": "stock.failed", "txn_id": None, "reason": "missing txn_id"}

    elif msg_type == "stock.execute":
        return _saga_execute(payload, conn, txn_id)
    elif msg_type == "stock.rollback":
        return _saga_rollback(conn, txn_id)

    logger.warning("Unknown SAGA message type: %s", msg_type)
    return 400, {"type": "stock.failed", "txn_id": txn_id, "reason": f"unknown type: {msg_type}"}


def _saga_execute(payload, conn, txn_id):
    """
    Deduct stock immediately for all items and record compensating entries.

    Items locked in sorted order to prevent deadlocks.  Idempotent by txn_id.
    """
    items = _validate_items(payload.get("items", []))
    if isinstance(items, str):
        return 400, {"type": "stock.failed", "txn_id": txn_id, "reason": items}

    with conn.cursor() as cur:
        # Advisory lock serialises concurrent executes for the same txn_id
        get_advisory_lock(cur, txn_id)
        cur.execute(
            "SELECT 1 FROM compensating_transactions WHERE txn_id = %s LIMIT 1",
            (txn_id,),
        )
        if cur.fetchone() is not None:
            logger.info("SAGA execute idempotent hit txn=%s", txn_id)
            conn.commit()
            return 200, {"type": "stock.executed", "txn_id": txn_id}

        # Lock all rows in sorted order before validating
        stock_map = _lock_items_sorted(cur, [item_id for item_id, _ in items])

        for item_id, quantity in items:
            if item_id not in stock_map:
                conn.rollback()
                return 400, {"type": "stock.failed", "txn_id": txn_id, "reason": f"item {item_id} not found"}
            elif stock_map[item_id] < quantity:
                conn.rollback()
                return 400, {"type": "stock.failed", "txn_id": txn_id, "reason": f"item {item_id} has insufficient stock"}

        # Deduct and write compensating entries for all items atomically
        for item_id, quantity in items:
            cur.execute("UPDATE items SET stock = stock - %s WHERE id = %s", (quantity, item_id))
            cur.execute(
                "INSERT INTO compensating_transactions (txn_id, item_id, quantity) VALUES (%s, %s, %s)",
                (txn_id, item_id, quantity),
            )

    conn.commit()
    logger.info("SAGA executed txn=%s items=%s", txn_id, items)
    return 200, {"type": "stock.executed", "txn_id": txn_id}


def _saga_rollback(conn, txn_id):
    """
    Compensate a SAGA stock deduction by restoring all item quantities.

    Locks item rows in sorted order before restoring.  Idempotent: no rows = success.
    """
    with conn.cursor() as cur:
        # Lock log rows to prevent concurrent double-rollback
        cur.execute(
            "DELETE FROM compensating_transactions WHERE txn_id = %s RETURNING item_id, quantity",
            (txn_id,),
        )
        rows = cur.fetchall()

        if not rows:
            logger.info("SAGA rollback idempotent hit (no rows) txn=%s", txn_id)
            conn.commit()
            return 200, {"type": "stock.rolledback", "txn_id": txn_id}

        # Lock item rows in sorted order before restoring
        _lock_items_sorted(cur, item_ids=[item_id for item_id, _ in rows] )
        execute_batch(
            cur,
            "UPDATE items SET stock = stock + %s WHERE id = %s",
            [(quantity, item_id) for item_id, quantity in rows]
        )

    conn.commit()
    logger.info("SAGA rolled back txn=%s (%d items)", txn_id, len(rows))
    return 200, {"type": "stock.rolledback", "txn_id": txn_id}