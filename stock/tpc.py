"""Stock 2PC participant — prepare/commit/abort + stale-transaction recovery."""

from flask import g, abort, Response


def init_routes(app):

    # PREPARE: Deduct Stock Immediately, Record Reservation
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


def recovery(conn_pool, logger):
    """Auto-Abort Prepared Transactions Older Than 5 Minutes."""
    conn = conn_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT txn_id, item_id, quantity FROM prepared_transactions "
            "WHERE created_at < NOW() - INTERVAL '5 minutes'"
        )
        rows = cur.fetchall()
        if not rows:
            cur.close()
            logger.info("RECOVERY: No stale prepared transactions found")
            return
        for txn_id, item_id, quantity in rows:
            logger.warning(f"RECOVERY: Aborting stale txn={txn_id}, item={item_id}")
            cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
            cur.execute("UPDATE items SET stock = stock + %s WHERE id = %s", (quantity, item_id))
            cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s AND item_id = %s", (txn_id, item_id))
            conn.commit()
        cur.close()
    finally:
        conn_pool.putconn(conn)
