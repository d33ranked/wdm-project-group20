import os
import json
import uuid
import time
import atexit
import random
import logging
from collections import defaultdict

import psycopg2
import requests
import psycopg2.pool

from time import perf_counter
from flask import Flask, jsonify, abort, Response, g


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ["GATEWAY_URL"]

app = Flask("order-service")


def create_conn_pool(retries=10, delay=2):
    for attempt in range(retries):
        try:
            return psycopg2.pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=10,
                host=os.environ["POSTGRES_HOST"],
                port=int(os.environ["POSTGRES_PORT"]),
                dbname=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
            )
        except psycopg2.OperationalError:
            if attempt < retries - 1:
                print(
                    f"ORDER: PostgreSQL not ready, retrying in {delay}s... (attempt {attempt+1}/{retries})"
                )
                time.sleep(delay)
            else:
                raise


conn_pool = create_conn_pool()


@app.before_request
def before_req():
    g.start_time = perf_counter()
    g.conn = conn_pool.getconn()


@app.after_request
def log_response(response):
    duration = perf_counter() - g.start_time
    print(f"ORDER: Request took {duration:.7f} seconds")
    return response


@app.teardown_request
def teardown_req(exception):
    conn = g.pop("conn", None)
    if conn is not None:
        if exception:
            conn.rollback()
        else:
            conn.commit()
        conn_pool.putconn(conn)


def close_db_connection():
    conn_pool.closeall()


atexit.register(close_db_connection)


def get_order_from_db(order_id: str):
    cur = g.conn.cursor()
    cur.execute(
        "SELECT paid, items, user_id, total_cost FROM orders WHERE id = %s", (order_id,)
    )
    row = cur.fetchone()
    cur.close()
    if row is None:
        abort(400, f"Order: {order_id} not found!")
    return {"paid": row[0], "items": row[1], "user_id": row[2], "total_cost": row[3]}


@app.post("/create/<user_id>")
def create_order(user_id: str):
    key = str(uuid.uuid4())
    cur = g.conn.cursor()
    cur.execute(
        "INSERT INTO orders (id, paid, items, user_id, total_cost) VALUES (%s, %s, %s, %s, %s)",
        (key, False, json.dumps([]), user_id, 0),
    )
    cur.close()
    return jsonify({"order_id": key})


@app.post("/batch_init/<n>/<n_items>/<n_users>/<item_price>")
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)
    cur = g.conn.cursor()
    for i in range(n):
        user_id = str(random.randint(0, n_users - 1))
        item1_id = str(random.randint(0, n_items - 1))
        item2_id = str(random.randint(0, n_items - 1))
        items = [[item1_id, 1], [item2_id, 1]]
        cur.execute(
            "INSERT INTO orders (id, paid, items, user_id, total_cost) VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (id) DO UPDATE SET paid = EXCLUDED.paid, items = EXCLUDED.items, "
            "user_id = EXCLUDED.user_id, total_cost = EXCLUDED.total_cost",
            (str(i), False, json.dumps(items), user_id, 2 * item_price),
        )
    cur.close()
    return jsonify({"msg": "Batch init for orders successful"})


@app.get("/find/<order_id>")
def find_order(order_id: str):
    order = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order["paid"],
            "items": order["items"],
            "user_id": order["user_id"],
            "total_cost": order["total_cost"],
        }
    )


def send_post_request(url: str, idempotency_key: str = None):
    headers = {}

    # set idempotency key in headers
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
    try:
        start_time = perf_counter()
        response = requests.post(url, headers=headers)
        duration = perf_counter() - start_time
        print(f"ORDER: POST request took {duration:.7f} seconds")
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        start_time = perf_counter()
        response = requests.get(url)
        duration = perf_counter() - start_time
        print(f"ORDER: GET request took {duration:.7f} seconds")
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post("/addItem/<order_id>/<item_id>/<quantity>")
def add_item(order_id: str, item_id: str, quantity: int):
    cur = g.conn.cursor()
    cur.execute(
        "SELECT items, total_cost FROM orders WHERE id = %s FOR UPDATE", (order_id,)
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"Order: {order_id} not found!")
    items, total_cost = row

    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        cur.close()
        abort(400, f"Item: {item_id} does not exist!")

    item_json = item_reply.json()
    items.append([item_id, int(quantity)])
    total_cost += int(quantity) * item_json["price"]

    cur.execute(
        "UPDATE orders SET items = %s, total_cost = %s WHERE id = %s",
        (json.dumps(items), total_cost, order_id),
    )
    cur.close()
    return Response(
        f"Item: {item_id} added to: {order_id} price updated to: {total_cost}",
        status=200,
    )


def rollback_stock(removed_items: list[tuple[str, int]], transaction_id: str):
    for item_id, quantity in removed_items:
        send_post_request(
            f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}",
            idempotency_key=f"{transaction_id}:stock:rollback:{item_id}",
        )


@app.post("/checkout/<order_id>")
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")

    # generate idempotency key (transaction id)
    transaction_id = str(uuid.uuid4())

    # select and lock order
    cur = g.conn.cursor()
    cur.execute(
        "SELECT paid, items, user_id, total_cost FROM orders WHERE id = %s FOR UPDATE",
        (order_id,),
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"Order: {order_id} not found!")
    paid, items, user_id, total_cost = row

    # aggregate item quantities
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in items:
        items_quantities[item_id] += quantity

    # track successfully subtracted items
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        stock_reply = send_post_request(
            f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}",
            idempotency_key=f"{transaction_id}:stock:subtract:{item_id}",
        )
        if stock_reply.status_code != 200:
            rollback_stock(removed_items, transaction_id)
            cur.close()
            abort(400, f"Out of stock on item_id: {item_id}")
        removed_items.append((item_id, quantity))

    # pay for the order
    user_reply = send_post_request(
        f"{GATEWAY_URL}/payment/pay/{user_id}/{total_cost}",
        idempotency_key=f"{transaction_id}:payment:pay",
    )
    if user_reply.status_code != 200:
        rollback_stock(removed_items, transaction_id)
        cur.close()
        abort(400, "User out of credit")

    # update order status
    cur.execute("UPDATE orders SET paid = TRUE WHERE id = %s", (order_id,))
    cur.close()
    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
