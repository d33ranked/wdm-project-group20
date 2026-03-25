import logging
import uuid

from common.idempotency import check_idempotency, save_idempotency, get_advisory_lock
from psycopg2.extras import execute_batch

logger = logging.getLogger(__name__)

# --- Registry for Gateway Routing ---
# maps (method, path_prefix) -> function
GATEWAY_ROUTES = {}

def route(method, path_prefix):
    def decorator(f):
        GATEWAY_ROUTES[(method.upper(), path_prefix)] = f
        return f
    return decorator

# ---------------------------------------------------------------------------
# Gateway Handlers (HTTP-Proxy)
# ---------------------------------------------------------------------------

@route("POST", "create_user")
def handle_create_user(segments, payload, conn, idem_key):
    user_id = str(uuid.uuid4())
    with conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (id, credit) VALUES (%s, %s)", (user_id, 0))
        conn.commit()
    logger.debug(f"Created new user, user_id is: {user_id}")
    return 201, {"user_id": user_id}

@route("POST", "batch_init")
def handle_batch_init(segments, payload, conn, idem_key):
    if len(segments) < 3:
        return 400, {"error": "Missing params for batch_init"}
    n, starting_money = int(segments[1]), int(segments[2])

    if n < 0:
        return 400, {"error": f"n must be non-negative, got: {n}"}
    elif starting_money < 0:
        return 400, {"error": f"starting_money must be non-negative, got: {starting_money}"}
    
    with conn:
        with conn.cursor() as cur:
            execute_batch(
                cur,
                "INSERT INTO users (id, credit) VALUES (%s, %s) "
                "ON CONFLICT (id) DO UPDATE SET credit = EXCLUDED.credit",
                [(str(i), starting_money) for i in range(n)],
            )
        conn.commit()
    logger.debug(f"Handled batch init request. Created n={n} users with starting amount: {starting_money}")
    return 200, {"msg": "Batch init successful"}

@route("GET", "find_user")
def handle_find_user(segments, payload, conn, idem_key):
    user_id = segments[1]
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT credit FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
    if not row:
        logger.debug(f"Handled find user request. user was not found. user_id: {user_id}")
        return 400, {"error": f"User {user_id} not found"}
    
    logger.debug(f"Handled find user request, found: {user_id} with credit: {row[0]}")
    return 200, {"user_id": user_id, "credit": row[0]}

@route("POST", "add_funds")
def handle_add_funds(segments, payload, conn, idem_key):
    user_id, amount = segments[1], int(segments[2])

    if amount <= 0:
        return 400, {"error": "Amount must be positive! Use the 'pay' endpoint to subtract money."}

    with conn:
        with conn.cursor() as cur:
            cached = check_idempotency(cur, idem_key)
            if cached: 
                conn.rollback()
                logger.info(f"Idempotency hit on add_funds, user: {user_id}, cached result is: {cached}")
                return cached
            
            cur.execute("SELECT 1 FROM users WHERE id = %s FOR UPDATE", (user_id,))
            if not cur.fetchone(): 
                conn.rollback()
                logger.warning(f"Request for add_funds of user: {user_id}, with amount: {amount}, but user was not found")
                return 400, {"error": "User not found"}
            
            cur.execute("UPDATE users SET credit = credit + %s WHERE id = %s RETURNING credit", (amount, user_id))
            new_credit = cur.fetchone()[0]
            resp = {"user_id": user_id, "added": amount, "new_amount": new_credit}
            save_idempotency(cur, idem_key, 200, resp)
        conn.commit()

    logger.debug(f"Gateway request, handled add funds for user: {user_id}, with amount: {amount}, new credit value is: {new_credit}")
    return 200, resp

@route("POST", "pay")
def handle_pay(segments, payload, conn, idem_key):
    user_id, amount = segments[1], int(segments[2])

    if amount <= 0:
        return 400, {"error": "Amount must be positive! Use the 'add_funds' endpoint to add money."}

    with conn:
        with conn.cursor() as cur:
            cached = check_idempotency(cur, idem_key)
            if cached: 
                conn.rollback()
                logger.info(f"Idempotency hit on pay, user: {user_id}, cached result is: {cached}")
                return cached
            
            cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
            row = cur.fetchone()
            if not row: 
                conn.rollback()
                logger.warning(f"Request for pay of user: {user_id}, with amount: {amount}, but user was not found")
                return 400, {"error": "User not found"}

            if row[0] < amount: 
                conn.rollback()
                logger.info(f"Request for pay of user: {user_id}, with amount: {amount}, but has insufficient funds. current funds: {row[0]}")
                return 400, {"error": "Insufficient funds"}
            
            cur.execute("UPDATE users SET credit = credit - %s WHERE id = %s RETURNING credit", (amount, user_id))
            new_credit = cur.fetchone()[0]
            resp = {"user_id": user_id, "new_credit": new_credit}
            save_idempotency(cur, idem_key, 200, resp)
        conn.commit()
    logger.debug(f"Gateway request, handled paymetn for user: {user_id}, with amount: {amount}, new credit value is: {new_credit}")
    return 200, resp

def handle_gateway_logic(payload, pool, correlation_id):
    """Entry point for gateway tasks."""
    method = payload.get("method", "GET").upper()
    path = payload.get("path", "/")
    headers = payload.get("headers") or {}
    segments = [s for s in path.strip("/").split("/") if s]
    
    # Use Gateway Correlation ID as fallback for idempotency
    idem_key = headers.get("Idempotency-Key") or correlation_id
    
    prefix = segments[0] if segments else ""
    handler = GATEWAY_ROUTES.get((method, prefix))
    
    if not handler:
        return 404, {"error": f"No route for {method} {path}"}
        
    conn = pool.getconn()
    try:
        logger.debug(f"Handeling request for: {method} {path} ")
        return handler(segments, payload, conn, idem_key)
    finally:
        pool.putconn(conn)

# ---------------------------------------------------------------------------
# 2PC Logic 
# ---------------------------------------------------------------------------

def handle_tpc_logic(payload, pool):
    msg_type = payload.get("type")
    txn_id = payload.get("txn_id")
    conn = pool.getconn()
    try:
        logger.debug(f"Handeling TPC request, with txn_id: {txn_id}, and msg_type: {msg_type}")
        with conn:
            if msg_type == "payment.prepare":
                return _tpc_prepare(payload, conn, txn_id)
            elif msg_type == "payment.commit":
                return _tpc_commit(conn, txn_id)
            elif msg_type == "payment.rollback":
                return _tpc_rollback(conn, txn_id)
    finally:
        pool.putconn(conn)

def _tpc_prepare(payload, conn, txn_id):
    user_id = payload.get("user_id")
    amount  = payload.get("amount")

    if not user_id or amount is None:
        return 400, {"type": "payment.failed", "txn_id": txn_id, "reason": "missing user_id or amount"}

    amount = int(amount)
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
        if cur.fetchone():
            conn.rollback()
            logger.info("TPC prepare idempotent hit txn=%s", txn_id)
            return {"type": "payment.prepared", "txn_id": txn_id}
        
        cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            return {"type": "payment.failed", "txn_id": txn_id, "reason": "user not found"}
        elif row[0] < amount:
            conn.rollback()
            return {"type": "payment.failed", "txn_id": txn_id, "reason": "insufficient funds"}
            
        cur.execute("UPDATE users SET credit = credit - %s WHERE id = %s", (amount, user_id))
        cur.execute("INSERT INTO prepared_transactions (txn_id, user_id, amount) VALUES (%s, %s, %s)", (txn_id, user_id, amount))

    conn.commit()
    logger.debug("TPC prepared txn=%s user=%s amount=%s", txn_id, user_id, amount)
    return {"type": "payment.prepared", "txn_id": txn_id}

def _tpc_commit(conn, txn_id):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
    conn.commit()
    logger.debug(f"Handled tpc commit request for txn_id: {txn_id}")
    return {"type": "payment.committed", "txn_id": txn_id}

def _tpc_rollback(conn, txn_id):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s RETURNING user_id, amount", (txn_id,))
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE users SET credit = credit + %s WHERE id = %s", (row[1], row[0]))
            logger.info("TPC rolled back txn=%s user=%s refunded=%s", txn_id, row[0], row[1])
        
    conn.commit()
    logger.debug(f"Handled tpc rollback for txn_id: {txn_id}")
    return {"type": "payment.rolledback", "txn_id": txn_id}

# ---------------------------------------------------------------------------
# SAGA Logic (Optimistic)
# ---------------------------------------------------------------------------

def handle_saga_logic(payload, pool):
    msg_type = payload.get("type")
    txn_id = payload.get("txn_id")
    conn = pool.getconn()

    if not txn_id:
        logger.warning("SAGA message missing txn_id: %s", payload)
        return {"type": "payment.failed", "txn_id": None, "reason": "missing txn_id"}

    try:
        logger.debug(f"Handeling saga msg, txn_id: {txn_id}, msg_type: {msg_type}")
        with conn:
            if msg_type == "payment.execute":
                return _saga_execute(payload, conn, txn_id)
            elif msg_type == "payment.rollback":
                return _saga_rollback(conn, txn_id)
    finally:
        pool.putconn(conn)

def _saga_execute(payload, conn, txn_id):
    user_id = payload.get("user_id")
    amount  = payload.get("amount")

    if not user_id or amount is None:
        return {"type": "payment.failed", "txn_id": txn_id, "reason": "missing user_id or amount"}

    amount = int(amount)
    with conn.cursor() as cur:
        get_advisory_lock(cur, txn_id)
        cur.execute("SELECT 1 FROM compensating_transactions WHERE txn_id = %s", (txn_id,))
        if cur.fetchone():
            conn.rollback()
            logger.info("SAGA execute idempotent hit txn=%s, already has a compensating transactions entry", txn_id)
            return {"type": "payment.executed", "txn_id": txn_id}
        
        cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            logger.warning(f"SAGA execute request with txn_id: {txn_id}, but user {user_id} not found")
            return {"type": "payment.failed", "txn_id": txn_id, "reason": f"user {user_id} not found"}
        elif row[0] < amount:
            conn.rollback()
            logger.info(f"SAGA execute request with txn_id: {txn_id}, but user {user_id} has insufficient funds")
            return {"type": "payment.failed", "txn_id": txn_id, "reason": f"user {user_id} has insufficient funds"}
            
        cur.execute("UPDATE users SET credit = credit - %s WHERE id = %s", (amount, user_id))
        cur.execute("INSERT INTO compensating_transactions (txn_id, user_id, amount) VALUES (%s, %s, %s)", (txn_id, user_id, amount))
    conn.commit()
    logger.debug(f"Handled SAGA execute request with txn_id: {txn_id}")
    return {"type": "payment.executed", "txn_id": txn_id}

def _saga_rollback(conn, txn_id):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM compensating_transactions WHERE txn_id = %s RETURNING user_id, amount", (txn_id,))
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE users SET credit = credit + %s WHERE id = %s", (row[1], row[0]))
            logger.info("SAGA rolled back txn=%s user=%s refunded=%s", txn_id, row[0], row[1])
    conn.commit()
    logger.debug(f"Handled SAGA rollback for txn_id: {txn_id}")
    return {"type": "payment.rolledback", "txn_id": txn_id}