# order service database schema
# order:{order_id} – hash { paid, items (JSON), user_id, total_cost }
# wf:{wf_id}       – hash { name, step, status, comp_step, context (JSON), error }  (owned by common/orchestrator.py)

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
