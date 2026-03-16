"""Stock SAGA participant — batch operations + Kafka message routing."""

import json
import uuid

from common.idempotency import check_idempotency_kafka, save_idempotency_kafka


# ---------------------------------------------------------------------------
# Batch Operations (Atomic Multi-Item, Sorted Lock Order To Prevent Deadlocks)
# ---------------------------------------------------------------------------

def db_subtract_stock_batch(conn, items):
    item_ids = [item_id for item_id, _ in items]
    with conn.cursor() as cur:
        cur.execute("SELECT id, stock FROM items WHERE id = ANY(%s) ORDER BY id FOR UPDATE", (item_ids,))
        rows = {r[0]: r[1] for r in cur.fetchall()}
        for item_id, amount in items:
            if amount <= 0:
                raise ValueError(f"Item {item_id} has invalid amount {amount}: must be positive")
            if item_id not in rows:
                raise ValueError(f"Item {item_id} not found")
            if rows[item_id] - amount < 0:
                raise ValueError(f"Item {item_id} has insufficient stock")
        results = {}
        for item_id, amount in items:
            cur.execute("UPDATE items SET stock = stock - %s WHERE id = %s RETURNING stock", (amount, item_id))
            results[item_id] = cur.fetchone()[0]
    return results


def db_add_stock_batch(conn, items):
    item_ids = [item_id for item_id, _ in items]
    with conn.cursor() as cur:
        cur.execute("SELECT id, stock FROM items WHERE id = ANY(%s) ORDER BY id FOR UPDATE", (item_ids,))
        rows = {r[0]: r[1] for r in cur.fetchall()}
        for item_id, amount in items:
            if amount <= 0:
                raise ValueError(f"Item {item_id} has invalid amount {amount}: must be positive")
            if item_id not in rows:
                raise ValueError(f"Item {item_id} not found")
        results = {}
        for item_id, amount in items:
            cur.execute("UPDATE items SET stock = stock + %s WHERE id = %s RETURNING stock", (amount, item_id))
            results[item_id] = cur.fetchone()[0]
    return results


# ---------------------------------------------------------------------------
# Kafka Message Routing (Mirrors Flask Endpoints For Kafka-Delivered Messages)
# ---------------------------------------------------------------------------

def route_kafka_message(payload, conn):
    method = payload.get("method", "GET").upper()
    path = payload.get("path", "/")
    body = payload.get("body") or {}
    headers = payload.get("headers") or {}
    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    # POST /item/create/<price>
    if method == "POST" and len(segments) >= 2 and segments[0] == "item" and segments[1] == "create":
        price = int(segments[2]) if len(segments) > 2 else 0
        item_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute("INSERT INTO items (id, stock, price) VALUES (%s, %s, %s)", (item_id, 0, price))
        conn.commit()
        return 201, {"item_id": item_id}

    # POST /batch_init/<n>/<starting_stock>/<item_price>
    if method == "POST" and len(segments) >= 4 and segments[0] == "batch_init":
        n, starting_stock, item_price = int(segments[1]), int(segments[2]), int(segments[3])
        with conn.cursor() as cur:
            for i in range(n):
                cur.execute(
                    "INSERT INTO items (id, stock, price) VALUES (%s, %s, %s) "
                    "ON CONFLICT (id) DO UPDATE SET stock = EXCLUDED.stock, price = EXCLUDED.price",
                    (str(i), starting_stock, item_price),
                )
        conn.commit()
        return 200, {"msg": "Batch init for stock successful"}

    # GET /find/<item_id>
    if method == "GET" and len(segments) >= 2 and segments[0] == "find":
        item_id = segments[1]
        with conn.cursor() as cur:
            cur.execute("SELECT stock, price FROM items WHERE id = %s", (item_id,))
            row = cur.fetchone()
        if row is None:
            return 400, {"error": f"Item {item_id} not found"}
        return 200, {"stock": row[0], "price": row[1]}

    # POST /add/<item_id>/<amount>
    if method == "POST" and len(segments) >= 3 and segments[0] == "add":
        item_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive!"}
        cached = check_idempotency_kafka(conn, idem_key)
        if cached:
            return cached
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
                if cur.fetchone() is None:
                    return 400, {"error": f"Item {item_id} not found"}
                cur.execute("UPDATE items SET stock = stock + %s WHERE id = %s RETURNING stock", (amount, item_id))
                new_stock = cur.fetchone()[0]
            resp = f"Item: {item_id} stock updated to: {new_stock}"
            save_idempotency_kafka(conn, idem_key, 200, resp)
            conn.commit()
            return 200, resp
        except Exception:
            conn.rollback()
            raise

    # POST /subtract/<item_id>/<amount>
    if method == "POST" and len(segments) >= 3 and segments[0] == "subtract":
        item_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive!"}
        cached = check_idempotency_kafka(conn, idem_key)
        if cached:
            return cached
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
                row = cur.fetchone()
                if row is None:
                    return 400, {"error": f"Item {item_id} not found"}
                if row[0] - amount < 0:
                    return 400, {"error": f"Item {item_id} has insufficient stock"}
                cur.execute("UPDATE items SET stock = stock - %s WHERE id = %s RETURNING stock", (amount, item_id))
                new_stock = cur.fetchone()[0]
            resp = f"Item: {item_id} stock updated to: {new_stock}"
            save_idempotency_kafka(conn, idem_key, 200, resp)
            conn.commit()
            return 200, resp
        except Exception:
            conn.rollback()
            raise

    # POST /subtract_batch
    if method == "POST" and segments and segments[0] == "subtract_batch":
        cached = check_idempotency_kafka(conn, idem_key)
        if cached:
            return cached
        try:
            items = [(e["item_id"], int(e["amount"])) for e in body["items"]]
        except (KeyError, TypeError, ValueError):
            return 400, {"error": 'Expected {"items": [{"item_id": str, "amount": int}, ...]}'}
        try:
            results = db_subtract_stock_batch(conn, items)
            resp = {"updated_stock": results}
            save_idempotency_kafka(conn, idem_key, 200, json.dumps(resp))
            conn.commit()
            return 200, resp
        except ValueError as exc:
            conn.rollback()
            return 400, {"error": str(exc)}

    # POST /add_batch
    if method == "POST" and segments and segments[0] == "add_batch":
        cached = check_idempotency_kafka(conn, idem_key)
        if cached:
            return cached
        try:
            items = [(e["item_id"], int(e["amount"])) for e in body["items"]]
        except (KeyError, TypeError, ValueError):
            return 400, {"error": 'Expected {"items": [{"item_id": str, "amount": int}, ...]}'}
        try:
            results = db_add_stock_batch(conn, items)
            resp = {"updated_stock": results}
            save_idempotency_kafka(conn, idem_key, 200, json.dumps(resp))
            conn.commit()
            return 200, resp
        except ValueError as exc:
            conn.rollback()
            return 400, {"error": str(exc)}

    return 404, {"error": f"No handler for {method} {path}"}
