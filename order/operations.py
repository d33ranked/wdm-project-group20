import json
import uuid
import random


def create_order(r, user_id: str) -> str:
    order_id = str(uuid.uuid4())
    r.hset(
        f"order:{order_id}",
        mapping={
            "paid": "false",
            "items": json.dumps([]),
            "user_id": user_id,
            "total_cost": "0",
        },
    )
    return order_id


def batch_init_orders(r, n: int, n_items: int, n_users: int, item_price: int):
    pipe = r.pipeline(transaction=False)
    for i in range(n):
        uid = str(random.randint(0, n_users - 1))
        i1 = str(random.randint(0, n_items - 1))
        i2 = str(random.randint(0, n_items - 1))
        pipe.hset(
            f"order:{i}",
            mapping={
                "paid": "false",
                "items": json.dumps([[i1, 1], [i2, 1]]),
                "user_id": uid,
                "total_cost": str(2 * item_price),
            },
        )
    pipe.execute()


def add_item_to_order(r, order_id: str, item_id: str, quantity: int, item_price: int):
    """Merge item into order and update total_cost.

    Returns (total_cost, None) on success, (None, error_message) if the order
    is not found.
    """
    order_data = r.hgetall(f"order:{order_id}")
    if not order_data:
        return None, f"Order {order_id} not found"

    items_list = json.loads(order_data.get("items", "[]"))
    total_cost = int(order_data.get("total_cost", 0))

    merged = False
    for entry in items_list:
        if entry[0] == item_id:
            entry[1] += quantity
            merged = True
            break
    if not merged:
        items_list.append([item_id, quantity])
    total_cost += quantity * item_price

    r.hset(
        f"order:{order_id}",
        mapping={
            "items": json.dumps(items_list),
            "total_cost": str(total_cost),
        },
    )
    return total_cost, None
