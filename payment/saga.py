"""Payment SAGA participant — Kafka message routing."""

import uuid

from common.idempotency import check_idempotency_kafka, save_idempotency_kafka


def route_kafka_message(payload, conn):
    method = payload.get("method", "GET").upper()
    path = payload.get("path", "/")
    headers = payload.get("headers") or {}
    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    # POST /create_user
    if method == "POST" and segments and segments[0] == "create_user":
        user_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (id, credit) VALUES (%s, %s)", (user_id, 0))
        conn.commit()
        return 201, {"user_id": user_id}

    # POST /batch_init/<n>/<starting_money>
    if method == "POST" and len(segments) >= 3 and segments[0] == "batch_init":
        n, starting_money = int(segments[1]), int(segments[2])
        with conn.cursor() as cur:
            for i in range(n):
                cur.execute(
                    "INSERT INTO users (id, credit) VALUES (%s, %s) "
                    "ON CONFLICT (id) DO UPDATE SET credit = EXCLUDED.credit",
                    (str(i), starting_money),
                )
        conn.commit()
        return 200, {"msg": "Batch init for users successful"}

    # GET /find_user/<user_id>
    if method == "GET" and len(segments) >= 2 and segments[0] == "find_user":
        user_id = segments[1]
        with conn.cursor() as cur:
            cur.execute("SELECT credit FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
        if row is None:
            return 400, {"error": f"User {user_id} not found"}
        return 200, {"user_id": user_id, "credit": row[0]}

    # POST /add_funds/<user_id>/<amount>
    if method == "POST" and len(segments) >= 3 and segments[0] == "add_funds":
        user_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive!"}
        cached = check_idempotency_kafka(conn, idem_key)
        if cached:
            return cached
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
                if cur.fetchone() is None:
                    return 400, {"error": f"User {user_id} not found"}
                cur.execute("UPDATE users SET credit = credit + %s WHERE id = %s RETURNING credit", (amount, user_id))
                new_credit = cur.fetchone()[0]
            resp = f"User: {user_id} credit updated to: {new_credit}"
            save_idempotency_kafka(conn, idem_key, 200, resp)
            conn.commit()
            return 200, resp
        except Exception:
            conn.rollback()
            raise

    # POST /pay/<user_id>/<amount>
    if method == "POST" and len(segments) >= 3 and segments[0] == "pay":
        user_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive!"}
        cached = check_idempotency_kafka(conn, idem_key)
        if cached:
            return cached
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
                row = cur.fetchone()
                if row is None:
                    return 400, {"error": f"User {user_id} not found"}
                if row[0] - amount < 0:
                    return 400, {"error": f"User {user_id} has insufficient funds"}
                cur.execute("UPDATE users SET credit = credit - %s WHERE id = %s RETURNING credit", (amount, user_id))
                new_credit = cur.fetchone()[0]
            resp = f"User: {user_id} credit updated to: {new_credit}"
            save_idempotency_kafka(conn, idem_key, 200, resp)
            conn.commit()
            return 200, resp
        except Exception:
            conn.rollback()
            raise

    return 404, {"error": f"No handler for {method} {path}"}
