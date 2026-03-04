import logging
import os
import atexit
import uuid
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor, execute_values
from flask import Flask, jsonify, abort, Response, g
from time import perf_counter

DB_ERROR_STR = "DB error"

app = Flask("payment-service")

# --- DATABASE SETUP ---
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=20,
        host=os.environ['POSTGRES_HOST'],
        database=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        port=os.environ.get('POSTGRES_PORT', 5432)
    )
except Exception as e:
    app.logger.error(f"Failed to connect to Postgres: {e}")
    exit(1)

def get_db_conn():
    return db_pool.getconn()

@app.before_request
def start_timer():
    g.start_time = perf_counter()

@app.after_request
def log_response(response):
    duration = perf_counter() - g.start_time
    print(f"PAYMENT: Request took {duration:.7f} seconds")
    return response

def release_db_conn(conn):
    db_pool.putconn(conn)

@atexit.register
def close_db_pool():
    db_pool.closeall()

def init_db():
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id TEXT PRIMARY KEY,
                    credit BIGINT NOT NULL DEFAULT 0
                );
            """)
        conn.commit()
    except psycopg2.errors.UniqueViolation:
        # Table already exists (race condition with other workers), ignore
        conn.rollback()
    except Exception as e:
        conn.rollback()
        app.logger.warning(f"init_db error (may be harmless): {e}")
    finally:
        release_db_conn(conn)

with app.app_context():
    init_db()

# --- ROUTES ---

@app.post('/create_user')
def create_user():
    user_id = str(uuid.uuid4())
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (user_id, credit) VALUES (%s, 0)", (user_id,))
        conn.commit()
        return jsonify({'user_id': user_id})
    except Exception:
        conn.rollback()
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)

@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            data = [(str(i), int(starting_money)) for i in range(int(n))]
            # Use execute_values for much faster bulk insert
            execute_values(
                cur,
                "INSERT INTO users (user_id, credit) VALUES %s ON CONFLICT (user_id) DO UPDATE SET credit = EXCLUDED.credit",
                data,
                page_size=10000
            )
        conn.commit()
        return jsonify({"msg": "Batch init for users successful"})
    except Exception as e:
        conn.rollback()
        print(f"PAYMENT batch_init error: {str(e)}")
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)

@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT user_id, credit FROM users WHERE user_id = %s", (user_id,))
            user = cur.fetchone()
            if not user:
                abort(404, f"User: {user_id} not found!")
            return jsonify(user)
    finally:
        release_db_conn(conn)

@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET credit = credit + %s WHERE user_id = %s RETURNING credit",
                (int(amount), user_id)
            )
            result = cur.fetchone()
            if not result:
                abort(404, "User not found")
        conn.commit()
        return Response(f"User: {user_id} credit updated to: {result[0]}", status=200)
    except Exception:
        conn.rollback()
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)

@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    amount = int(amount)
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Atomic check-and-update
            cur.execute(
                "UPDATE users SET credit = credit - %s WHERE user_id = %s AND credit >= %s RETURNING credit",
                (amount, user_id, amount)
            )
            result = cur.fetchone()
            if not result:
                # Either user doesn't exist or credit is insufficient
                cur.execute("SELECT credit FROM users WHERE user_id = %s", (user_id,))
                user_exists = cur.fetchone()
                if not user_exists:
                    abort(404, "User not found")
                else:
                    abort(400, "Insufficient credit")
        conn.commit()
        return Response(f"User: {user_id} credit updated to: {result[0]}", status=200)
    except Exception as e:
        conn.rollback()
        if hasattr(e, 'code'): raise e # Re-raise Flask aborts
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)