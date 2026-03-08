import gevent.monkey
gevent.monkey.patch_all()

import os
import time
import uuid
import atexit
import hashlib
import logging

import psycopg2
import psycopg2.pool

from time import perf_counter
from flask import Flask, jsonify, abort, request, Response, g

DB_ERROR_STR = "DB error"
app = Flask("stock-service")


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
                    f"STOCK: PostgreSQL not ready, retrying in {delay}s... (attempt {attempt+1}/{retries})"
                )
                time.sleep(delay)
            else:
                raise


conn_pool = create_conn_pool()


@app.before_request
def start_timer():
    g.start_time = perf_counter()
    g.conn = conn_pool.getconn()


@app.after_request
def log_response(response):
    duration = perf_counter() - g.start_time
    print(f"STOCK: Request took {duration:.7f} seconds")
    return response


# a cleanup function to return the connection to the pool
@app.teardown_request
def teardown_request(exception):
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


def get_item_from_db(item_id: str):
    # read-only item lookup operation
    cur = g.conn.cursor()
    cur.execute("SELECT stock, price FROM items WHERE id = %s", (item_id,))
    row = cur.fetchone()
    cur.close()
    if row is None:
        abort(400, f"Item: {item_id} not found!")
    return {"stock": row[0], "price": row[1]}


@app.post("/item/create/<price>")
def create_item(price: int):
    key = str(uuid.uuid4())
    cur = g.conn.cursor()
    cur.execute(
        "INSERT INTO items (id, stock, price) VALUES (%s, %s, %s)", (key, 0, int(price))
    )
    cur.close()
    return jsonify({"item_id": key})


@app.post("/batch_init/<n>/<starting_stock>/<item_price>")
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    cur = g.conn.cursor()
    for i in range(n):
        cur.execute(
            "INSERT INTO items (id, stock, price) VALUES (%s, %s, %s) "
            "ON CONFLICT (id) DO UPDATE SET stock = EXCLUDED.stock, price = EXCLUDED.price",
            (str(i), starting_stock, item_price),
        )
    cur.close()
    return jsonify({"msg": "Batch init for stock successful"})


@app.get("/find/<item_id>")
def find_item(item_id: str):
    item = get_item_from_db(item_id)
    return jsonify({"stock": item["stock"], "price": item["price"]})


@app.post("/add/<item_id>/<amount>")
def add_stock(item_id: str, amount: int):
    # check idempotency key
    cached = check_idempotency()
    if cached is not None:
        return cached

    # add stock
    cur = g.conn.cursor()
    cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"Item: {item_id} not found!")
    cur.execute(
        "UPDATE items SET stock = stock + %s WHERE id = %s RETURNING stock",
        (int(amount), item_id),
    )
    new_stock = cur.fetchone()[0]
    cur.close()

    # save idempotency key
    body = f"Item: {item_id} stock updated to: {new_stock}"
    save_idempotency(200, body)
    return Response(body, status=200)


@app.post("/subtract/<item_id>/<amount>")
def remove_stock(item_id: str, amount: int):
    # check idempotency key
    cached = check_idempotency()
    if cached is not None:
        return cached

    # subtract stock
    cur = g.conn.cursor()
    cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"Item: {item_id} not found!")
    current_stock = row[0]
    if current_stock - int(amount) < 0:
        cur.close()
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    cur.execute(
        "UPDATE items SET stock = stock - %s WHERE id = %s RETURNING stock",
        (int(amount), item_id),
    )
    new_stock = cur.fetchone()[0]
    cur.close()

    # save idempotency key
    body = f"Item: {item_id} stock updated to: {new_stock}"
    save_idempotency(200, body)
    return Response(body, status=200)


@app.post("/prepare/<txn_id>/<item_id>/<quantity>")
def prepare_transaction(txn_id: str, item_id: str, quantity: int):
    quantity = int(quantity)
    cur = g.conn.cursor()

    # check if the transaction is already prepared
    cur.execute(
        "SELECT 1 FROM prepared_transactions WHERE txn_id = %s AND item_id = %s",
        (txn_id, item_id),
    )
    if cur.fetchone() is not None:
        cur.close()
        return Response("Transaction already prepared", status=200)

    # lock item row for update
    cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
    row = cur.fetchone()

    # check if the item has enough stock
    if row is None:
        cur.close()
        abort(400, f"Item: {item_id} not found!")
    current_stock = row[0]
    if current_stock < quantity:
        cur.close()
        abort(400, f"Item: {item_id} has insufficient stock!")

    # deduct and record for possible rollback
    cur.execute(
        "UPDATE items SET stock = stock - %s WHERE id = %s", (quantity, item_id)
    )
    cur.execute(
        "INSERT INTO prepared_transactions (txn_id, item_id, quantity) VALUES (%s, %s, %s)",
        (txn_id, item_id, quantity),
    )
    cur.close()
    return Response("Transaction prepared", status=200)


@app.post("/commit/<txn_id>")
def commit_transaction(txn_id: str):
    cur = g.conn.cursor()

    # remove the rollback records
    cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
    cur.close()
    return Response("Transaction committed", status=200)


@app.post("/abort/<txn_id>")
def abort_transaction(txn_id: str):
    cur = g.conn.cursor()

    # fetch (all) rollback records for this transaction
    cur.execute(
        "SELECT item_id, quantity FROM prepared_transactions WHERE txn_id = %s",
        (txn_id,),
    )
    rows = cur.fetchall()

    # add back the stock for each item
    for item_id, quantity in rows:
        cur.execute(
            "UPDATE items SET stock = stock + %s WHERE id = %s", (quantity, item_id)
        )

    # remove the rollback records
    cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
    cur.close()

    # return success - even if no rows were found (duplicate abort or already committed) so is idempotent
    return Response("Transaction aborted", status=200)


# generate advisory lock id from idempotency key
def idempotency_token(key: str) -> int:
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2**31)


def check_idempotency():
    # check if this request was already processed. returns the cached response or None.
    idem_key = request.headers.get("Idempotency-Key")
    if not idem_key:
        return None
    cur = g.conn.cursor()
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (idempotency_token(idem_key),))
    cur.execute(
        "SELECT status_code, body FROM idempotency_keys WHERE key = %s", (idem_key,)
    )
    row = cur.fetchone()
    cur.close()
    if row is not None:
        return Response(row[1], status=row[0])
    return None


def save_idempotency(status_code, body):
    # save the result so future calls with the same key return this result.
    idem_key = request.headers.get("Idempotency-Key")
    if not idem_key:
        return
    cur = g.conn.cursor()
    cur.execute(
        "INSERT INTO idempotency_keys (key, status_code, body) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
        (idem_key, status_code, body),
    )
    cur.close()


def recovery_tpc():
    conn = conn_pool.getconn()

    # on startup, check for stale prepared transactions
    try:
        cur = conn.cursor()

        # get all stale prepared transactions
        cur.execute(
            "SELECT txn_id, item_id, quantity FROM prepared_transactions WHERE created_at < NOW() - INTERVAL '5 minutes'"
        )
        rows = cur.fetchall()

        # return if none found
        if not rows:
            cur.close()
            app.logger.info("RECOVERY: No stale prepared transactions found")
            return

        # process each - rollback stock and remove the rollback record
        for txn_id, item_id, quantity in rows:
            app.logger.warning(
                f"RECOVERY: Aborting stale prepared transaction txn={txn_id}, item={item_id}, quantity={quantity}"
            )
            cur.execute("SELECT stock FROM items WHERE id = %s FOR UPDATE", (item_id,))
            cur.execute(
                "UPDATE items SET stock = stock + %s WHERE id = %s", (quantity, item_id)
            )
            cur.execute(
                "DELETE FROM prepared_transactions WHERE txn_id = %s AND item_id = %s",
                (txn_id, item_id),
            )
            conn.commit()

        cur.close()
    # close connection and return
    finally:
        conn_pool.putconn(conn)


with app.app_context():
    try:
        recovery_tpc()
    except Exception as e:
        app.logger.warning(f"RECOVERY STOCK: Error during recovery: {e}")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
