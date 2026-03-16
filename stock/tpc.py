"""Stock 2PC participant — prepare/commit/abort + stale-transaction recovery."""

from flask import g, abort, Response, request, jsonify


def init_routes(app):

    # PREPARE BATCH: Deduct All Items Atomically, Record All Reservations
    @app.post("/prepare_batch/<txn_id>")
    def prepare_batch(txn_id: str):
        body = request.get_json(silent=True) or {}
        items = body.get("items", [])
        if not items:
            abort(400, "No items provided for prepare_batch")

        cur = g.conn.cursor()

        # Already Prepared — Idempotent (check any item from this txn)
        first_item_id = items[0]["item_id"]
        cur.execute("SELECT 1 FROM prepared_transactions WHERE txn_id = %s AND item_id = %s",
                    (txn_id, first_item_id))
        if cur.fetchone() is not None:
            cur.close()
            return Response("Transaction already prepared", status=200)

        # Lock All Items In Sorted Order To Prevent Deadlocks, Check Stock
        item_ids = sorted(e["item_id"] for e in items)
        cur.execute("SELECT id, stock FROM items WHERE id = ANY(%s) ORDER BY id FOR UPDATE", (item_ids,))
        stock_map = {row[0]: row[1] for row in cur.fetchall()}

        for entry in items:
            item_id, quantity = entry["item_id"], int(entry["quantity"])
            if item_id not in stock_map:
                cur.close()
                abort(400, f"Item: {item_id} not found!")
            if stock_map[item_id] < quantity:
                cur.close()
                abort(400, f"Item: {item_id} has insufficient stock!")

        # Deduct And Record All — Atomic
        for entry in items:
            item_id, quantity = entry["item_id"], int(entry["quantity"])
            cur.execute("UPDATE items SET stock = stock - %s WHERE id = %s", (quantity, item_id))
            cur.execute("INSERT INTO prepared_transactions (txn_id, item_id, quantity) VALUES (%s, %s, %s)",
                        (txn_id, item_id, quantity))
        cur.close()
        return Response("Transaction prepared", status=200)

    # PREPARE (single item — kept for backward compatibility and direct tests)
    @app.post("/prepare/<txn_id>/<item_id>/<quantity>")
    def prepare_transaction(txn_id: str, item_id: str, quantity: int):
        quantity = int(quantity)
        cur = g.conn.cursor()

        # Already Prepared — Idempotent
        cur.execute("SELECT 1 FROM prepared_transactions WHERE txn_id = %s AND item_id = %s", (txn_id, item_id))
        if cur.fetchone() is not None:
            cur.close()
            return Response("Transaction already prepared", status=200)

        # Check Sufficient Stock
        cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
        row = cur.fetchone()
        if row is None:
            cur.close()
            abort(400, f"Item: {item_id} not found!")
        if row[0] < quantity:
            cur.close()
            abort(400, f"Item: {item_id} has insufficient stock!")

        # Deduct And Record
        cur.execute("UPDATE items SET stock = stock - %s WHERE id = %s", (quantity, item_id))
        cur.execute("INSERT INTO prepared_transactions (txn_id, item_id, quantity) VALUES (%s, %s, %s)",
                    (txn_id, item_id, quantity))
        cur.close()
        return Response("Transaction prepared", status=200)

    # COMMIT: Delete Reservation (Deduction Already Applied)
    @app.post("/commit/<txn_id>")
    def commit_transaction(txn_id: str):
        cur = g.conn.cursor()
        cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
        cur.close()
        return Response("Transaction committed", status=200)

    # ABORT: Restore Stock, Delete Reservation
    @app.post("/abort/<txn_id>")
    def abort_transaction(txn_id: str):
        cur = g.conn.cursor()
        cur.execute("SELECT item_id, quantity FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
        for item_id, quantity in cur.fetchall():
            cur.execute("UPDATE items SET stock = stock + %s WHERE id = %s", (quantity, item_id))
        cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
        cur.close()
        return Response("Transaction aborted", status=200)


def recovery(conn_pool):
    """Auto-Abort Prepared Transactions Older Than 5 Minutes."""
    conn = conn_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT txn_id, item_id, quantity FROM prepared_transactions "
                "WHERE created_at < NOW() - INTERVAL '5 minutes'"
            )
            rows = cur.fetchall()
            if not rows:
                print("RECOVERY: No stale prepared transactions found", flush=True)
                return
            for txn_id, item_id, quantity in rows:
                print(f"RECOVERY: Aborting stale txn={txn_id}, item={item_id}", flush=True)
                cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
                cur.execute("UPDATE items SET stock = stock + %s WHERE id = %s", (quantity, item_id))
                cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s AND item_id = %s", (txn_id, item_id))
                conn.commit()
    finally:
        conn_pool.putconn(conn)
