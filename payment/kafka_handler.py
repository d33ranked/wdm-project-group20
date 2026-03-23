"""
Payment service — unified Kafka message handler.

Three consumer topics, three handlers:

  gateway.payment        — HTTP-proxy requests from the api-gateway
  internal.payment.tpc   — 2PC commands from the order service (pessimistic)
  internal.payment.saga  — SAGA commands from the order service (optimistic)

Gateway message contract (HTTP-proxy envelope):
  { "method": "POST", "path": "/pay/u1/100", "headers": {...}, ... }

2PC message contract (internal.payment.tpc):
  Incoming:
    { "type": "payment.prepare",  "txn_id": "...", "user_id": "...", "amount": 123 }
    { "type": "payment.commit",   "txn_id": "..." }
    { "type": "payment.rollback", "txn_id": "..." }
  Outgoing:
    { "type": "payment.prepared",   "txn_id": "..." }
    { "type": "payment.committed",  "txn_id": "..." }
    { "type": "payment.rolledback", "txn_id": "..." }
    { "type": "payment.failed",     "txn_id": "...", "reason": "..." }

SAGA message contract (internal.payment.saga):
  Incoming:
    { "type": "payment.execute",  "txn_id": "...", "user_id": "...", "amount": 123 }
    { "type": "payment.rollback", "txn_id": "..." }
  Outgoing:
    { "type": "payment.executed",   "txn_id": "..." }
    { "type": "payment.rolledback", "txn_id": "..." }
    { "type": "payment.failed",     "txn_id": "...", "reason": "..." }

Consistency guarantees:
  - 2PC:  credit is deducted at prepare-time and held in prepared_transactions
          until commit (delete entry) or rollback (refund + delete entry).
  - SAGA: credit is deducted immediately at execute-time and recorded in
          compensating_transactions so a rollback can refund the exact amount.
          Silence after execute means success — no commit step required.
  - Both paths are fully idempotent by txn_id.
"""

import logging
import uuid

from common.idempotency import check_idempotency, save_idempotency, get_advisory_lock 
from psycopg2.extras import execute_batch

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Gateway handler
# ---------------------------------------------------------------------------

def handle_gateway_message(payload, conn):
    """
    Route an HTTP-proxy envelope arriving on gateway.payment.
    Returns (status_code, body) — published to gateway.responses by the consumer loop.
    """
    method   = payload.get("method", "GET").upper()
    path     = payload.get("path", "/")
    headers  = payload.get("headers") or {}
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
    elif method == "POST" and len(segments) >= 3 and segments[0] == "batch_init":
        n, starting_money = int(segments[1]), int(segments[2])
        if n < 0:
            return 400, {"error": f"n must be non-negative, got: {n}"}
        if starting_money < 0:
            return 400, {"error": f"starting_money must be non-negative, got: {starting_money}"}
        with conn.cursor() as cur:
            execute_batch(
                cur,
                "INSERT INTO users (id, credit) VALUES (%s, %s) "
                "ON CONFLICT (id) DO UPDATE SET credit = EXCLUDED.credit",
                [(str(i), starting_money) for i in range(n)],
            )
        conn.commit()
        return 200, {"msg": "Batch init for users successful"}

    # GET /find_user/<user_id>
    elif method == "GET" and len(segments) >= 2 and segments[0] == "find_user":
        user_id = segments[1]
        with conn.cursor() as cur:
            cur.execute("SELECT credit FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
        conn.commit()
        if row is None:
            return 400, {"error": f"User {user_id} not found"}
        return 200, {"user_id": user_id, "credit": row[0]}

    # POST /add_funds/<user_id>/<amount>
    elif method == "POST" and len(segments) >= 3 and segments[0] == "add_funds":
        user_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive!"}
        with conn.cursor() as cur:
            cached = check_idempotency(cur, idem_key)
            if cached:
                conn.commit()
                logger.warning(f"Idempotency hit on add_funds, user: {user_id}, cached result is: {cached}")
                return cached
            cur.execute("SELECT 1 FROM users WHERE id = %s FOR UPDATE", (user_id,))
            if cur.fetchone() is None:
                conn.rollback()
                return 400, {"error": f"User {user_id} not found"}
            cur.execute(
                "UPDATE users SET credit = credit + %s WHERE id = %s RETURNING credit",
                (amount, user_id),
            )
            new_credit = cur.fetchone()[0]
            resp = f"User: {user_id} credit updated to: {new_credit}"
            save_idempotency(cur, idem_key, 200, resp)
        conn.commit()
        return 200, resp

    # POST /pay/<user_id>/<amount>
    elif method == "POST" and len(segments) >= 3 and segments[0] == "pay":
        user_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive!"}
        with conn.cursor() as cur:
            cached = check_idempotency(cur, idem_key)
            if cached:
                conn.commit()
                logger.warning(f"Idempotency hit on pay, user: {user_id}, cached result is: {cached}")
                return cached
            cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
            row = cur.fetchone()
            if row is None:
                conn.rollback()
                return 400, {"error": f"User {user_id} not found"}
            if row[0] - amount < 0:
                conn.rollback()
                return 400, {"error": f"User {user_id} has insufficient funds"}
            cur.execute(
                "UPDATE users SET credit = credit - %s WHERE id = %s RETURNING credit",
                (amount, user_id),
            )
            new_credit = cur.fetchone()[0]
            resp = f"User: {user_id} credit updated to: {new_credit}"
            save_idempotency(cur, idem_key, 200, resp)
        conn.commit()
        return 200, resp

    return 404, {"error": f"No handler for {method} {path}"}


# ---------------------------------------------------------------------------
# 2PC handler (internal.payment.tpc)
# ---------------------------------------------------------------------------

def handle_tpc_message(payload, conn):
    """
    Route a 2PC command arriving on internal.payment.tpc.

    Pessimistic path: credit is held (deducted) at prepare-time and only
    finalised on commit or refunded on rollback.
    """
    msg_type = payload.get("type", "")
    txn_id   = payload.get("txn_id")

    if not txn_id:
        logger.warning("TPC message missing txn_id: %s", payload)
        return 400, {"type": "payment.failed", "txn_id": None, "reason": "missing txn_id"}

    elif msg_type == "payment.prepare":
        return _tpc_prepare(payload, conn, txn_id)

    elif msg_type == "payment.commit":
        return _tpc_commit(conn, txn_id)

    elif msg_type == "payment.rollback":
        return _tpc_rollback(conn, txn_id)

    logger.warning("Unknown TPC message type: %s", msg_type)
    return 400, {"type": "payment.failed", "txn_id": txn_id, "reason": f"unknown type: {msg_type}"}


def _tpc_prepare(payload, conn, txn_id):
    """
    Deduct credit and write a prepared_transactions row as an undo-log entry.

    Idempotent: a duplicate prepare for the same txn_id returns success
    without double-deducting.
    """
    user_id = payload.get("user_id")
    amount  = payload.get("amount")

    if not user_id or amount is None:
        return 400, {"type": "payment.failed", "txn_id": txn_id, "reason": "missing user_id or amount"}

    amount = int(amount)

    with conn.cursor() as cur:
        # Idempotency — already prepared, do not deduct again
        cur.execute("SELECT 1 FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
        if cur.fetchone() is not None:
            logger.info("TPC prepare idempotent hit txn=%s", txn_id)
            conn.commit()
            return 200, {"type": "payment.prepared", "txn_id": txn_id}

        # Row lock prevents concurrent over-spending on the same user
        cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            return 400, {"type": "payment.failed", "txn_id": txn_id, "reason": f"user {user_id} not found"}
        elif row[0] < amount:
            conn.rollback()
            return 400, {"type": "payment.failed", "txn_id": txn_id, "reason": f"user {user_id} has insufficient funds"}

        # Deduct and record undo-log entry atomically — both or neither
        cur.execute("UPDATE users SET credit = credit - %s WHERE id = %s", (amount, user_id))
        cur.execute(
            "INSERT INTO prepared_transactions (txn_id, user_id, amount) VALUES (%s, %s, %s)",
            (txn_id, user_id, amount),
        )

    conn.commit()
    logger.info("TPC prepared txn=%s user=%s amount=%s", txn_id, user_id, amount)
    return 200, {"type": "payment.prepared", "txn_id": txn_id}


def _tpc_commit(conn, txn_id):
    """
    Finalise a prepared transaction by deleting the undo-log entry.

    Credit was already deducted at prepare-time so no credit change is needed.
    Idempotent: if the row is already gone treat as success.
    """
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM prepared_transactions WHERE txn_id = %s RETURNING txn_id",
            (txn_id,),
        )
        deleted = cur.fetchone()
    conn.commit()

    if deleted is None:
        logger.info("TPC commit idempotent hit (no row) txn=%s", txn_id)
    else:
        logger.info("TPC committed txn=%s", txn_id)

    return 200, {"type": "payment.committed", "txn_id": txn_id}


def _tpc_rollback(conn, txn_id):
    """
    Abort a prepared transaction by refunding the reserved credit.

    Uses the undo-log entry to know how much to refund.  Idempotent: if the
    row is already gone (already rolled back or committed), treat as success.
    """
    with conn.cursor() as cur:
        # FOR UPDATE prevents two concurrent rollbacks from both refunding
        cur.execute(
            "DELETE FROM prepared_transactions WHERE txn_id = %s RETURNING user_id, amount",
            (txn_id,),
        )
        row = cur.fetchone()

        if row is None:
            logger.info("TPC rollback idempotent hit (no row) txn=%s", txn_id)
            conn.commit()
            return 200, {"type": "payment.rolledback", "txn_id": txn_id}

        user_id, amount = row

        # Restore credit and remove undo-log entry atomically
        cur.execute("UPDATE users SET credit = credit + %s WHERE id = %s", (amount, user_id))

    conn.commit()
    logger.info("TPC rolled back txn=%s user=%s refunded=%s", txn_id, user_id, amount)
    return 200, {"type": "payment.rolledback", "txn_id": txn_id}


# ---------------------------------------------------------------------------
# SAGA handler (internal.payment.saga)
# ---------------------------------------------------------------------------

def handle_saga_message(payload, conn):
    """
    Route a SAGA command arriving on internal.payment.saga.

    Optimistic path: credit is deducted immediately and the operation is
    considered committed unless a rollback arrives later.  Silence = success.
    """
    msg_type = payload.get("type", "")
    txn_id   = payload.get("txn_id")

    if not txn_id:
        logger.warning("SAGA message missing txn_id: %s", payload)
        return 400, {"type": "payment.failed", "txn_id": None, "reason": "missing txn_id"}

    elif msg_type == "payment.execute":
        return _saga_execute(payload, conn, txn_id)

    elif msg_type == "payment.rollback":
        return _saga_rollback(conn, txn_id)

    logger.warning("Unknown SAGA message type: %s", msg_type)
    return 400, {"type": "payment.failed", "txn_id": txn_id, "reason": f"unknown type: {msg_type}"}


def _saga_execute(payload, conn, txn_id):
    """
    Deduct credit immediately and write a compensating_transactions entry so
    a rollback can refund the exact amount if the order fails later.

    Idempotent: a duplicate execute for the same txn_id returns success
    without double-deducting.
    """
    user_id = payload.get("user_id")
    amount  = payload.get("amount")

    if not user_id or amount is None:
        return 400, {"type": "payment.failed", "txn_id": txn_id, "reason": "missing user_id or amount"}

    amount = int(amount)

    with conn.cursor() as cur:
        # Idempotency — already executed, do not deduct again
        get_advisory_lock(cur, txn_id)
        cur.execute("SELECT 1 FROM compensating_transactions WHERE txn_id = %s", (txn_id,))
        if cur.fetchone() is not None:
            logger.info("SAGA execute idempotent hit txn=%s", txn_id)
            conn.commit()
            return 200, {"type": "payment.executed", "txn_id": txn_id}

        # Row lock prevents concurrent over-spending on the same user
        cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            return 400, {"type": "payment.failed", "txn_id": txn_id, "reason": f"user {user_id} not found"}
        elif row[0] < amount:
            conn.rollback()
            return 400, {"type": "payment.failed", "txn_id": txn_id, "reason": f"user {user_id} has insufficient funds"}

        # Deduct and record compensating entry atomically — both or neither
        cur.execute("UPDATE users SET credit = credit - %s WHERE id = %s", (amount, user_id))
        cur.execute(
            "INSERT INTO compensating_transactions (txn_id, user_id, amount) VALUES (%s, %s, %s)",
            (txn_id, user_id, amount),
        )

    conn.commit()
    logger.info("SAGA executed txn=%s user=%s amount=%s", txn_id, user_id, amount)
    return 200, {"type": "payment.executed", "txn_id": txn_id}


def _saga_rollback(conn, txn_id):
    """
    Compensate a previously executed SAGA payment by refunding the credit.

    Uses the compensating_transactions entry to know how much to refund.
    Idempotent: if the entry is already gone the compensation was already
    applied — treat as success.
    """
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM compensating_transactions WHERE txn_id = %s RETURNING user_id, amount",
            (txn_id,),
        )
        row = cur.fetchone()

        if row is None:
            logger.info("SAGA rollback idempotent hit (no row) txn=%s", txn_id)
            conn.commit()
            return 200, {"type": "payment.rolledback", "txn_id": txn_id}

        user_id, amount = row

        # Restore credit and remove compensating entry atomically
        cur.execute("UPDATE users SET credit = credit + %s WHERE id = %s", (amount, user_id))

    conn.commit()
    logger.info("SAGA rolled back txn=%s user=%s refunded=%s", txn_id, user_id, amount)
    return 200, {"type": "payment.rolledback", "txn_id": txn_id}