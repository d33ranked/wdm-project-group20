"""Payment 2PC participant — prepare/commit/abort + stale-transaction recovery."""

from flask import g, abort, Response


def init_routes(app):

    # PREPARE: Deduct Credit Immediately, Record Reservation
    @app.post("/prepare/<txn_id>/<user_id>/<amount>")
    def prepare_transaction(txn_id: str, user_id: str, amount: int):
        amount = int(amount)
        cur = g.conn.cursor()

        # Already Prepared — Idempotent
        cur.execute("SELECT 1 FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
        if cur.fetchone() is not None:
            cur.close()
            return Response("Transaction already prepared", status=200)

        # Check Sufficient Credit
        cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
        row = cur.fetchone()
        if row is None:
            cur.close()
            abort(400, f"User: {user_id} not found!")
        if row[0] < amount:
            cur.close()
            abort(400, f"User: {user_id} has insufficient credit!")

        # Deduct And Record
        cur.execute("UPDATE users SET credit = credit - %s WHERE id = %s", (amount, user_id))
        cur.execute("INSERT INTO prepared_transactions (txn_id, user_id, amount) VALUES (%s, %s, %s)",
                    (txn_id, user_id, amount))
        cur.close()
        return Response("Transaction prepared", status=200)

    # COMMIT: Delete Reservation (Deduction Already Applied)
    @app.post("/commit/<txn_id>")
    def commit_transaction(txn_id: str):
        cur = g.conn.cursor()
        cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
        cur.close()
        return Response("Transaction committed", status=200)

    # ABORT: Restore Credit, Delete Reservation
    @app.post("/abort/<txn_id>")
    def abort_transaction(txn_id: str):
        cur = g.conn.cursor()
        cur.execute("SELECT user_id, amount FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
        row = cur.fetchone()
        if row is not None:
            user_id, amount = row
            cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
            if cur.fetchone() is not None:
                cur.execute("UPDATE users SET credit = credit + %s WHERE id = %s", (amount, user_id))
            cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
        cur.close()
        return Response("Transaction aborted", status=200)


def recovery(conn_pool):
    """Auto-Abort Prepared Transactions Older Than 5 Minutes."""
    conn = conn_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT txn_id, user_id, amount FROM prepared_transactions "
                "WHERE created_at < NOW() - INTERVAL '5 minutes'"
            )
            rows = cur.fetchall()
            if not rows:
                print("RECOVERY: No stale prepared transactions found", flush=True)
                return
            for txn_id, user_id, amount in rows:
                print(f"RECOVERY: Aborting stale txn={txn_id}, user={user_id}", flush=True)
                cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
                if cur.fetchone() is not None:
                    cur.execute("UPDATE users SET credit = credit + %s WHERE id = %s", (amount, user_id))
                cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
                conn.commit()
    finally:
        conn_pool.putconn(conn)
