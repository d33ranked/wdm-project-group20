"""Order service data access — orders and sagas tables."""

import json


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def get_order(conn, order_id):
    with conn.cursor() as cur:
        cur.execute("SELECT paid, items, user_id, total_cost FROM orders WHERE id = %s", (order_id,))
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"Order {order_id} not found")
    return {"paid": row[0], "items": row[1], "user_id": row[2], "total_cost": row[3]}


def get_order_for_update(conn, order_id):
    with conn.cursor() as cur:
        cur.execute("SELECT paid, items, user_id, total_cost FROM orders WHERE id = %s FOR UPDATE", (order_id,))
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"Order {order_id} not found")
    return {"paid": row[0], "items": row[1], "user_id": row[2], "total_cost": row[3]}


def mark_paid(conn, order_id):
    with conn.cursor() as cur:
        cur.execute("UPDATE orders SET paid = TRUE WHERE id = %s", (order_id,))


# ---------------------------------------------------------------------------
# Sagas
# ---------------------------------------------------------------------------

def create_saga(conn, saga_id, order_id, state, items_quantities,
                original_correlation_id, idempotency_key):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO sagas (id, order_id, state, items_quantities, "
            "original_correlation_id, idempotency_key) VALUES (%s, %s, %s, %s, %s, %s)",
            (saga_id, order_id, state, json.dumps(items_quantities),
             original_correlation_id, idempotency_key),
        )


def get_saga_for_update(conn, saga_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, order_id, state, items_quantities, "
            "original_correlation_id, idempotency_key "
            "FROM sagas WHERE id = %s FOR UPDATE", (saga_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "id": row[0], "order_id": row[1], "state": row[2],
        "items_quantities": row[3], "original_correlation_id": row[4],
        "idempotency_key": row[5],
    }


def advance_saga(conn, saga_id, new_state):
    with conn.cursor() as cur:
        cur.execute("UPDATE sagas SET state = %s, updated_at = NOW() WHERE id = %s", (new_state, saga_id))
