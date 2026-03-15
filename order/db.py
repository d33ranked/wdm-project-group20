"""Order service data access — orders, transaction_log, and sagas tables."""

import json


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def get_order(conn, order_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT paid, items, user_id, total_cost FROM orders WHERE id = %s",
            (order_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"Order {order_id} not found")
    return {"paid": row[0], "items": row[1], "user_id": row[2], "total_cost": row[3]}


def get_order_for_update(conn, order_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT paid, items, user_id, total_cost "
            "FROM orders WHERE id = %s FOR UPDATE",
            (order_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"Order {order_id} not found")
    return {"paid": row[0], "items": row[1], "user_id": row[2], "total_cost": row[3]}


def mark_order_paid(conn, order_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE orders SET paid = TRUE WHERE id = %s", (order_id,))


# ---------------------------------------------------------------------------
# 2PC transaction log
# ---------------------------------------------------------------------------

def create_transaction(conn, txn_id: str, order_id: str, status: str,
                       items: list, user_id: str, total_cost: int,
                       original_correlation_id: str,
                       idempotency_key: str | None) -> None:
    """
    Write full transaction record before any external call.
    Everything needed to re-drive recovery is self-contained in this row.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO transaction_log "
            "(txn_id, order_id, status, items, user_id, total_cost, "
            " original_correlation_id, idempotency_key) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (txn_id, order_id, status, json.dumps(items), user_id,
             total_cost, original_correlation_id, idempotency_key),
        )


def get_txn_for_update(conn, txn_id: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT txn_id, order_id, status, items, user_id, total_cost, "
            "       original_correlation_id, idempotency_key, "
            "       stock_vote, payment_vote "
            "FROM transaction_log WHERE txn_id = %s FOR UPDATE",
            (txn_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "txn_id":                  row[0],
        "order_id":                row[1],
        "status":                  row[2],
        "items":                   row[3],
        "user_id":                 row[4],
        "total_cost":              row[5],
        "original_correlation_id": row[6],
        "idempotency_key":         row[7],
        "stock_vote":              row[8],
        "payment_vote":            row[9],
    }


def advance_txn(conn, txn_id: str, new_status: str,
                stock_vote: str | None = None,
                payment_vote: str | None = None) -> None:
    """Advance status, optionally recording a participant vote."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE transaction_log SET
                status       = %s,
                stock_vote   = COALESCE(%s, stock_vote),
                payment_vote = COALESCE(%s, payment_vote),
                updated_at   = NOW()
            WHERE txn_id = %s
            """,
            (new_status, stock_vote, payment_vote, txn_id),
        )


# ---------------------------------------------------------------------------
# SAGA log
# ---------------------------------------------------------------------------

def create_saga(conn, saga_id: str, order_id: str, state: str,
                items_quantities: dict, user_id: str, total_cost: int,
                original_correlation_id: str,
                idempotency_key: str | None) -> None:
    """
    Write full saga record before firing any messages.
    user_id and total_cost are denormalized so recovery can reconstruct
    payment messages without querying the orders table.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO sagas "
            "(id, order_id, state, items_quantities, user_id, total_cost, "
            " original_correlation_id, idempotency_key) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (saga_id, order_id, state, json.dumps(items_quantities),
             user_id, total_cost, original_correlation_id, idempotency_key),
        )


def get_saga_for_update(conn, saga_id: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, order_id, state, items_quantities, user_id, total_cost, "
            "       original_correlation_id, idempotency_key, "
            "       stock_ok, payment_ok, failure_reason "
            "FROM sagas WHERE id = %s FOR UPDATE",
            (saga_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "id":                      row[0],
        "order_id":                row[1],
        "state":                   row[2],
        "items_quantities":        row[3],
        "user_id":                 row[4],
        "total_cost":              row[5],
        "original_correlation_id": row[6],
        "idempotency_key":         row[7],
        "stock_ok":                row[8],
        "payment_ok":              row[9],
        "failure_reason":          row[10],
    }


def advance_saga(conn, saga_id: str, new_state: str,
                 stock_ok: bool | None = None,
                 payment_ok: bool | None = None,
                 failure_reason: str | None = None) -> None:
    """Advance state, optionally updating partial completion flags."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sagas SET
                state          = %s,
                stock_ok       = COALESCE(%s, stock_ok),
                payment_ok     = COALESCE(%s, payment_ok),
                failure_reason = COALESCE(%s, failure_reason),
                updated_at     = NOW()
            WHERE id = %s
            """,
            (new_state, stock_ok, payment_ok, failure_reason, saga_id),
        )