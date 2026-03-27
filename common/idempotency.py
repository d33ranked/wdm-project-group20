import logging

logger = logging.getLogger(__name__)


def check_idempotency(r, idem_key: str):
    # return cached (status_code, body) if key exists, else None
    if not idem_key:
        return None
    result = r.hmget(f"idem:{idem_key}", "status_code", "body")
    if result[0] is None:
        return None
    return int(result[0]), result[1]


def save_idempotency(r, idem_key: str, status_code: int, body: str):
    # persist result under idem key with 1-hour ttl
    if not idem_key:
        return
    pipe = r.pipeline(transaction=False)
    pipe.hset(
        f"idem:{idem_key}", mapping={"status_code": str(status_code), "body": str(body)}
    )
    pipe.expire(f"idem:{idem_key}", 3600)
    pipe.execute()