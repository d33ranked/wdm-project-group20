"""
Idempotency helpers — used by all Kafka consumer paths.

Uses a PostgreSQL advisory transaction lock to serialize concurrent worker
threads that share the same idempotency key.  Without this, two threads that
receive the same key (e.g. due to Kafka redelivery after a crash) could both
pass the check before either has written the cached result, causing the
underlying operation to execute twice.

pg_advisory_xact_lock(token) is automatically released at transaction end so
no explicit unlock is needed.  Only requests sharing the same derived token
contend — different keys remain fully parallel.
"""

import hashlib

def get_advisory_lock(cur, key: str) -> None:
    """Acquire a transaction-scoped advisory lock keyed on an arbitrary string."""
    # Derive a stable 32-bit integer lock token from an idempotency key string.
    token = int(hashlib.md5(key.encode()).hexdigest(), 16) % (2**31)
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (token,))


def check_idempotency(cur, idem_key: str | None):
    """
    Acquire an advisory lock then check for a cached response.

    Returns (status_code, body) if a cached response exists, else None.
    The lock is held for the duration of the transaction, so a concurrent
    worker with the same key blocks here until we either find a cached result
    or finish writing one via save_idempotency().
    """
    if not idem_key:
        return None

    get_advisory_lock(cur, idem_key)
    cur.execute(
        "SELECT status_code, body FROM idempotency_keys WHERE key = %s",
        (idem_key,),
    )
    row = cur.fetchone()
    return (row[0], row[1]) if row else None


def save_idempotency(cur, idem_key: str | None, status_code: int, body: str):
    """Persist a response so subsequent requests with the same key get a cache hit."""
    if not idem_key:
        return

    cur.execute(
        "INSERT INTO idempotency_keys (key, status_code, body) "
        "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
        (idem_key, status_code, body),
    )