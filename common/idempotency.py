import hashlib


def _token(key):
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2**31)


# --- HTTP Mode (Advisory Lock For Concurrent Requests) ---

def check_idempotency_http(conn, idem_key):
    if not idem_key:
        return None
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (_token(idem_key),))
        cur.execute("SELECT status_code, body FROM idempotency_keys WHERE key = %s", (idem_key,))
        row = cur.fetchone()
    return (row[0], row[1]) if row else None


def save_idempotency_http(conn, idem_key, status_code, body):
    if not idem_key:
        return
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO idempotency_keys (key, status_code, body) "
            "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (idem_key, status_code, body),
        )


# --- Kafka Mode (No Lock — Single-Threaded Consumer) ---

def check_idempotency_kafka(conn, idem_key):
    if not idem_key:
        return None
    with conn.cursor() as cur:
        cur.execute("SELECT status_code, body FROM idempotency_keys WHERE key = %s", (idem_key,))
        row = cur.fetchone()
    return (row[0], row[1]) if row else None


def save_idempotency_kafka(conn, idem_key, status_code, body):
    if not idem_key:
        return
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO idempotency_keys (key, status_code, body) "
            "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (idem_key, status_code, body),
        )
