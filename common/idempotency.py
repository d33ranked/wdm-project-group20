"""
Shared idempotency utilities for all microservices.

Two variants exist because the concurrency model differs between modes:

  HTTP mode (TPC):  Multiple gunicorn workers handle concurrent requests.
                    A pg_advisory_xact_lock serialises duplicate requests at
                    the DB level, preventing the race between "check if key
                    exists" and "insert new key".

  Kafka mode (SAGA): A single-threaded consumer poll loop processes messages
                     sequentially. No advisory lock is needed because there
                     is no concurrent-request race.

Both variants share the same idempotency_keys table schema:
  (key TEXT PK, status_code INT, body TEXT, created_at TIMESTAMP).
"""

import hashlib


def idempotency_token(key: str) -> int:
    """Derive a 31-bit integer from a key for use as a pg_advisory_xact_lock token."""
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2**31)


# ---------------------------------------------------------------------------
# HTTP mode — used by stock and payment Flask endpoints in TPC mode
# ---------------------------------------------------------------------------

def check_idempotency_http(conn, idem_key: str):
    """Check for a cached response under idem_key, acquiring an advisory lock first.

    Returns a (status_code, body) tuple if a cached response exists, else None.
    The advisory lock is transaction-scoped: it is released when the caller
    commits or rolls back.
    """
    if not idem_key:
        return None
    cur = conn.cursor()
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (idempotency_token(idem_key),))
    cur.execute(
        "SELECT status_code, body FROM idempotency_keys WHERE key = %s", (idem_key,)
    )
    row = cur.fetchone()
    cur.close()
    if row is not None:
        return (row[0], row[1])
    return None


def save_idempotency_http(conn, idem_key: str, status_code: int, body: str):
    """Persist a response so future duplicate requests return the cached result."""
    if not idem_key:
        return
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO idempotency_keys (key, status_code, body) "
        "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
        (idem_key, status_code, body),
    )
    cur.close()


# ---------------------------------------------------------------------------
# Kafka mode — used by stock, payment, and order Kafka consumers in SAGA mode
# ---------------------------------------------------------------------------

def check_idempotency_kafka(conn, idem_key: str):
    """Check for a cached response (no advisory lock — single-threaded consumer).

    Returns a (status_code, body) tuple if a cached response exists, else None.
    """
    if not idem_key:
        return None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status_code, body FROM idempotency_keys WHERE key = %s",
            (idem_key,),
        )
        row = cur.fetchone()
    return (row[0], row[1]) if row else None


def save_idempotency_kafka(conn, idem_key: str, status_code: int, body: str):
    """Persist a response (no advisory lock — single-threaded consumer)."""
    if not idem_key:
        return
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO idempotency_keys (key, status_code, body) "
            "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (idem_key, status_code, body),
        )
