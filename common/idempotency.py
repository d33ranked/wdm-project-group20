import logging

logger = logging.getLogger(__name__)


def check_idempotency_http(r, idem_key: str):
    # check if key is present in database
    #     yes : return the cached response
    #     no : process and save the result
    # TTL is 1 hour after which the key expires and the operation can run again
    if not idem_key:
        return None
    result = r.hmget(f"idem:{idem_key}", "status_code", "body")
    # hmget returns [None, None] when the key does not exist
    if result[0] is None:
        return None
    return int(result[0]), result[1]


def save_idempotency_http(r, idem_key: str, status_code: int, body: str):
    # persist the result of a completed operation
    # uses a pipeline (two commands sent in one round-trip) for efficiency
    # the key expires after 1 hour
    if not idem_key:
        return
    pipe = r.pipeline(transaction=False)
    pipe.hset(
        f"idem:{idem_key}", mapping={"status_code": str(status_code), "body": str(body)}
    )
    pipe.expire(f"idem:{idem_key}", 3600)
    pipe.execute()


def check_idempotency_kafka(r, idem_key: str):
    # identical logic to HTTP mode — just a clearer name for stream consumers
    return check_idempotency_http(r, idem_key)


def save_idempotency_kafka(r, idem_key: str, status_code: int, body: str):
    # identical logic to HTTP mode — just a clearer name for stream consumers
    save_idempotency_http(r, idem_key, status_code, body)