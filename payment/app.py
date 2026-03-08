import gevent.monkey
gevent.monkey.patch_all()

import os
import uuid
import time
import atexit
import hashlib
import logging

import psycopg2
import psycopg2.pool

from time import perf_counter
from flask import Flask, jsonify, abort, request, Response, g


DB_ERROR_STR = "DB error"
app = Flask("payment-service")


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
                    f"PAYMENT: PostgreSQL not ready, retrying in {delay}s... (attempt {attempt+1}/{retries})"
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
    print(f"PAYMENT: Request took {duration:.7f} seconds")
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


def get_user_from_db(user_id: str):
    cur = g.conn.cursor()
    cur.execute("SELECT credit FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    if row is None:
        abort(400, f"User: {user_id} not found!")
    return {"credit": row[0]}


@app.post("/create_user")
def create_user():
    key = str(uuid.uuid4())
    cur = g.conn.cursor()
    cur.execute("INSERT INTO users (id, credit) VALUES (%s, %s)", (key, 0))
    cur.close()
    return jsonify({"user_id": key})


@app.post("/batch_init/<n>/<starting_money>")
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    cur = g.conn.cursor()
    for i in range(n):
        cur.execute(
            "INSERT INTO users (id, credit) VALUES (%s, %s) "
            "ON CONFLICT (id) DO UPDATE SET credit = EXCLUDED.credit",
            (str(i), starting_money),
        )
    cur.close()
    return jsonify({"msg": "Batch init for users successful"})


@app.get("/find_user/<user_id>")
def find_user(user_id: str):
    user = get_user_from_db(user_id)
    return jsonify({"user_id": user_id, "credit": user["credit"]})


@app.post("/add_funds/<user_id>/<amount>")
def add_credit(user_id: str, amount: int):
    # check idempotency key
    cached = check_idempotency()
    if cached is not None:
        return cached

    # add credit
    cur = g.conn.cursor()
    cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"User: {user_id} not found!")
    cur.execute(
        "UPDATE users SET credit = credit + %s WHERE id = %s RETURNING credit",
        (int(amount), user_id),
    )
    new_credit = cur.fetchone()[0]
    cur.close()

    # save idempotency key
    body = f"User: {user_id} credit updated to: {new_credit}"
    save_idempotency(200, body)
    return Response(body, status=200)


@app.post("/pay/<user_id>/<amount>")
def remove_credit(user_id: str, amount: int):
    # check idempotency key
    cached = check_idempotency()
    if cached is not None:
        return cached

    # remove credit
    cur = g.conn.cursor()
    cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"User: {user_id} not found!")
    current_credit = row[0]
    if current_credit - int(amount) < 0:
        cur.close()
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    cur.execute(
        "UPDATE users SET credit = credit - %s WHERE id = %s RETURNING credit",
        (int(amount), user_id),
    )
    new_credit = cur.fetchone()[0]
    cur.close()

    # save idempotency key
    body = f"User: {user_id} credit updated to: {new_credit}"
    save_idempotency(200, body)
    return Response(body, status=200)


@app.post("/prepare/<txn_id>/<user_id>/<amount>")
def prepare_transaction(txn_id: str, user_id: str, amount: int):
    amount = int(amount)
    cur = g.conn.cursor()

    # check if the transaction is already prepared
    cur.execute("SELECT 1 FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
    if cur.fetchone() is not None:
        cur.close()
        return Response("Transaction already prepared", status=200)

    # lock user row for update
    cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))

    # check if the user has enough credit
    row = cur.fetchone()
    if row is None:
        cur.close()
        abort(400, f"User: {user_id} not found!")

    current_credit = row[0]
    if current_credit < amount:
        cur.close()
        abort(400, f"User: {user_id} has insufficient credit!")

    # deduct and record for possible rollback
    cur.execute(
        "UPDATE users SET credit = credit - %s WHERE id = %s", (amount, user_id)
    )
    cur.execute(
        "INSERT INTO prepared_transactions (txn_id, user_id, amount) VALUES (%s, %s, %s)",
        (txn_id, user_id, amount),
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

    # fetch (only one) rollback record for this transaction
    cur.execute(
        "SELECT user_id, amount FROM prepared_transactions WHERE txn_id = %s", (txn_id,)
    )
    row = cur.fetchone()

    if row is not None:
        user_id, amount = row

        # lock the user row for update
        cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))

        # add back the credit
        if cur.fetchone() is not None:
            cur.execute(
                "UPDATE users SET credit = credit + %s WHERE id = %s", (amount, user_id)
            )

        # delete the rollback record
        cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
    cur.close()
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

        # get all stale prepared transactions (older than 5 minutes)
        cur.execute(
            "SELECT txn_id, user_id, amount FROM prepared_transactions WHERE created_at < NOW() - INTERVAL '5 minutes'"
        )
        rows = cur.fetchall()

        # return if none found
        if not rows:
            cur.close()
            app.logger.info("RECOVERY: No stale prepared transactions found")
            return

        # process each - restore credit and remove the rollback record
        for txn_id, user_id, amount in rows:
            app.logger.warning(
                f"RECOVERY: Aborting stale prepared transaction txn={txn_id}, user={user_id}, amount={amount}"
            )
            cur.execute("SELECT credit FROM users WHERE id = %s FOR UPDATE", (user_id,))
            if cur.fetchone() is not None:
                cur.execute(
                    "UPDATE users SET credit = credit + %s WHERE id = %s",
                    (amount, user_id),
                )
            cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
            conn.commit()

        cur.close()
    finally:
        conn_pool.putconn(conn)


with app.app_context():
    try:
        recovery_tpc()
    except Exception as e:
        app.logger.warning(f"RECOVERY PAYMENT: Error during recovery: {e}")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
