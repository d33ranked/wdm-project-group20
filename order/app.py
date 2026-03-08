import gevent.monkey
gevent.monkey.patch_all()

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
TRANSACTION_MODE = os.environ.get("TRANSACTION_MODE", "TPC")

app = Flask("order-service")


def create_conn_pool(retries=10, delay=2):
    for attempt in range(retries):
        try:
            return psycopg2.pool.ThreadedConnectionPool(
                minconn=10,
                maxconn=100,
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


def send_post_request(url: str, idempotency_key: str = None, max_retries: int = 3):
    headers = {}

    # set idempotency key in headers
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key

    # retry and backoff
    start_time = perf_counter()
    for attempt in range(max_retries + 1):
        try:
            response = requests.post(url, headers=headers, timeout=5)
            # 2xx or 4xx is valid response, don't retry
            if response.status_code < 500:
                duration = perf_counter() - start_time
                print(f"ORDER: POST request took {duration:.7f} seconds")
                return response
            # 5xx is retryable error, retry
            app.logger.warning(
                f"Retry {attempt + 1}/{max_retries} for {url} returned {response.status_code}"
            )
        except requests.exceptions.RequestException as e:
            app.logger.warning(
                f"Retry {attempt + 1}/{max_retries} for {url} failed: {e}"
            )
            if attempt == max_retries:
                abort(400, REQ_ERROR_STR)
        # exponential backoff
        if attempt < max_retries:
            wait = 0.1 * (2**attempt)
            time.sleep(wait)
    abort(400, REQ_ERROR_STR)


def send_get_request(url: str):
    try:
        start_time = perf_counter()
        response = requests.get(url, timeout=5)
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


def commit_tpc(txn_id: str, prepared_stock: list, prepared_payment: bool):
    # send commit to all prepared participants
    if prepared_stock:
        send_post_request(
            f"{GATEWAY_URL}/stock/commit/{txn_id}",
            idempotency_key=f"{txn_id}:stock:commit",
        )
    if prepared_payment:
        send_post_request(
            f"{GATEWAY_URL}/payment/commit/{txn_id}",
            idempotency_key=f"{txn_id}:payment:commit",
        )


def abort_tpc(txn_id: str, prepared_stock: list, prepared_payment: bool):
    # send abort to all prepared participants
    if prepared_stock:
        send_post_request(
            f"{GATEWAY_URL}/stock/abort/{txn_id}",
            idempotency_key=f"{txn_id}:stock:abort",
        )
    if prepared_payment:
        send_post_request(
            f"{GATEWAY_URL}/payment/abort/{txn_id}",
            idempotency_key=f"{txn_id}:payment:abort",
        )


@app.post("/checkout/<order_id>")
def checkout(order_id: str):
    if TRANSACTION_MODE == "TPC":
        return checkout_tpc(order_id)
    else:
        return checkout_saga(order_id)


def checkout_saga(order_id: str):
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


def checkout_tpc(order_id: str):
    conn = g.conn
    cur = conn.cursor()

    # generate transaction id
    txn_id = str(uuid.uuid4())

    # read and lock order row for update
    cur.execute(
        "SELECT paid, items, user_id, total_cost FROM orders WHERE id = %s FOR UPDATE",
        (order_id,),
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"Order: {order_id} not found!")
    paid, items, user_id, total_cost = row

    # return if order is already paid
    if paid:
        cur.close()
        abort(400, "Order is already paid for!")

    # aggregate each item quantity
    items_quantities = defaultdict(int)
    for item_id, qty in items:
        items_quantities[item_id] += qty

    # return if the order has no items
    if not items_quantities:
        cur.close()
        abort(400, "Order has no items!")

    # create transaction log record for this transaction
    cur.execute(
        "INSERT INTO transaction_log (txn_id, order_id, status, prepared_stock, prepared_payment, user_id, total_cost) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (txn_id, order_id, "started", json.dumps([]), False, user_id, total_cost),
    )
    conn.commit()

    # update transaction status = "preparing_stock" before sending PREPARE request
    cur.execute(
        "UPDATE transaction_log SET status = 'preparing_stock' WHERE txn_id = %s",
        (txn_id,),
    )
    conn.commit()

    # prepare stock for each item
    prepared_stock = []
    for item_id, qty in sorted(items_quantities.items()):
        reply = send_post_request(
            f"{GATEWAY_URL}/stock/prepare/{txn_id}/{item_id}/{qty}",
            idempotency_key=f"{txn_id}:stock:prepare:{item_id}",
        )
        if reply.status_code != 200:
            # set transaction status = "aborting"
            cur.execute(
                "UPDATE transaction_log SET status = 'aborting' WHERE txn_id = %s",
                (txn_id,),
            )
            conn.commit()

            # send ABORT request to all participants
            abort_tpc(txn_id, prepared_stock, False)

            # set transaction status = "aborted"
            cur.execute(
                "UPDATE transaction_log SET status = 'aborted' WHERE txn_id = %s",
                (txn_id,),
            )
            conn.commit()

            cur.close()
            abort(400, "Failed to PREPARE stock")

        # append to prepared stock list
        prepared_stock.append([item_id, qty])
        cur.execute(
            "UPDATE transaction_log SET prepared_stock = %s WHERE txn_id = %s",
            (json.dumps(prepared_stock), txn_id),
        )
        conn.commit()

    # prepare payment service
    cur.execute(
        "UPDATE transaction_log SET status = 'preparing_payment' WHERE txn_id = %s",
        (txn_id,),
    )
    conn.commit()

    payment_reply = send_post_request(
        f"{GATEWAY_URL}/payment/prepare/{txn_id}/{user_id}/{total_cost}",
        idempotency_key=f"{txn_id}:payment:prepare",
    )
    if payment_reply.status_code != 200:
        # set transaction status = "aborting"
        cur.execute(
            "UPDATE transaction_log SET status = 'aborting' WHERE txn_id = %s",
            (txn_id,),
        )
        conn.commit()

        # send ABORT request to all participants
        abort_tpc(txn_id, prepared_stock, False)

        # set transaction status = "aborted"
        cur.execute(
            "UPDATE transaction_log SET status = 'aborted' WHERE txn_id = %s", (txn_id,)
        )
        conn.commit()

        cur.close()
        abort(400, "Failed to PREPARE payment")

    # set transaction payment status = TRUE
    cur.execute(
        "UPDATE transaction_log SET prepared_payment = TRUE WHERE txn_id = %s",
        (txn_id,),
    )
    conn.commit()

    # all voted YES - send COMMIT request to all participants
    cur.execute(
        "UPDATE transaction_log SET status = 'committing' WHERE txn_id = %s", (txn_id,)
    )
    conn.commit()

    commit_tpc(txn_id, prepared_stock, True)

    # mark order as paid
    cur.execute("UPDATE orders SET paid = TRUE WHERE id = %s", (order_id,))

    # clean up transaction log
    cur.execute(
        "UPDATE transaction_log SET status = 'committed' WHERE txn_id = %s", (txn_id,)
    )
    conn.commit()
    cur.close()
    return Response("Checkout successful", status=200)


def recovery_tpc():
    conn = conn_pool.getconn()

    # on startup, check for incomplete transactions
    try:
        cur = conn.cursor()

        # get all incomplete transactions
        cur.execute(
            "SELECT txn_id, order_id, status, prepared_stock, prepared_payment, user_id, total_cost FROM transaction_log WHERE status NOT IN ('committed', 'aborted')"
        )
        rows = cur.fetchall()

        # return if none found
        if not rows:
            cur.close()
            app.logger.info("RECOVERY: No incomplete transactions found")
            return

        # process each incomplete transaction
        for (
            txn_id,
            order_id,
            status,
            prepared_stock,
            prepared_payment,
            user_id,
            total_cost,
        ) in rows:
            # ensure prepared_stock is a list (JSONB may come as list or str)
            if prepared_stock is None:
                prepared_stock = []
            elif isinstance(prepared_stock, str):
                try:
                    prepared_stock = json.loads(prepared_stock) if prepared_stock else []
                except (TypeError, ValueError):
                    prepared_stock = []

            app.logger.warning(
                f"RECOVERY: Found incomplete transaction txn={txn_id}, status={status}"
            )

            # handle uncommitted transactions - safe to abort
            if status in ("started", "preparing_stock", "preparing_payment"):
                # have not committed so safe to abort
                abort_tpc(txn_id, prepared_stock, prepared_payment)
                cur.execute(
                    "UPDATE transaction_log SET status = 'aborted' WHERE txn_id = %s",
                    (txn_id,),
                )
                conn.commit()

            # handle committed transactions - need to commit
            elif status == "committing":
                # decision was COMMIT, but not all participants got the message
                # resume sending the commits
                commit_tpc(txn_id, prepared_stock, prepared_payment)
                cur.execute("UPDATE orders SET paid = TRUE WHERE id = %s", (order_id,))
                cur.execute(
                    "UPDATE transaction_log SET status = 'committed' WHERE txn_id = %s",
                    (txn_id,),
                )
                conn.commit()
            # handle aborted transactions - need to abort
            elif status == "aborting":
                # was aborting, but crashed in flight
                # resume sending the aborts
                abort_tpc(txn_id, prepared_stock, prepared_payment)
                cur.execute(
                    "UPDATE transaction_log SET status = 'aborted' WHERE txn_id = %s",
                    (txn_id,),
                )
                conn.commit()
            # handle unknown transactions - log warning
            else:
                app.logger.warning(
                    f"RECOVERY: Found unknown transaction status txn={txn_id}, status={status}"
                )
        cur.close()
    # close connection and return
    finally:
        conn_pool.putconn(conn)


with app.app_context():
    try:
        recovery_tpc()
    except Exception as e:
        app.logger.warning(f"RECOVERY: Error during recovery: {e}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
