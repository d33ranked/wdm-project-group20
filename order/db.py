# order service database schema
# order:{order_id} – hash { paid, items (JSON), user_id, total_cost }
# saga:{saga_id} – hash { order_id, state, items_quantities (JSON), original_correlation_id, idempotency_key }

# tpc acquires a short-lived checkout lock key before reading the order so concurrent requests serialize
# stream consumer is single-threaded so two checkout messages for the same order are processed one at a time
# final "mark paid" step uses the mark_order_paid Lua script which atomically checks-and-sets, guaranteeing no double charge even if the lock somehow fails

import json


# get an order
def get_order(r, order_id: str) -> dict:
    # fetch an order by id
    # r : redis.Redis client
    # raises ValueError if the order does not exist
    data = r.hgetall(f"order:{order_id}")
    if not data:
        raise ValueError(f"Order {order_id} not found")
    return {
        "paid": data["paid"] == "true",
        "items": json.loads(data.get("items", "[]")),
        "user_id": data["user_id"],
        "total_cost": int(data.get("total_cost", 0)),
    }


# get an order for update
def get_order_for_update(r, order_id: str) -> dict:
    # alias for get_order — the 'for_update' distinction no longer applies
    return get_order(r, order_id)


# mark an order as paid
def mark_paid(r, order_id: str):
    # mark an order as paid
    # r : redis.Redis client
    # order_id : the id of the order to mark as paid
    r.hset(f"order:{order_id}", "paid", "true")


# create a new saga
def create_saga(
    r,
    saga_id: str,
    order_id: str,
    state: str,
    items_quantities: dict,
    original_correlation_id: str,
    idempotency_key: str,
):
    # persist a new saga record
    # items_quantities is a {item_id: quantity} dict stored as a JSON string
    r.hset(
        f"saga:{saga_id}",
        mapping={
            "order_id": order_id,
            "state": state,
            "items_quantities": json.dumps(items_quantities),
            "original_correlation_id": original_correlation_id or "",
            "idempotency_key": idempotency_key or "",
        },
    )


# get a saga for update
def get_saga_for_update(r, saga_id: str) -> dict | None:
    # fetch a saga by id
    # returns None if not found
    # the 'for_update' name is kept for API compatibility with saga.py
    data = r.hgetall(f"saga:{saga_id}")
    if not data:
        return None
    return {
        "id": saga_id,
        "order_id": data["order_id"],
        "state": data["state"],
        "items_quantities": json.loads(data["items_quantities"]),
        "original_correlation_id": data.get("original_correlation_id", ""),
        "idempotency_key": data.get("idempotency_key") or None,
    }

# advance a saga
def advance_saga(r, saga_id: str, new_state: str):
    # update the saga state
    # each hset is immediately durable via AOF
    r.hset(f"saga:{saga_id}", "state", new_state)