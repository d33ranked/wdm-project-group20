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

app = Flask("stock-service")

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
    print(f"STOCK: Request took {duration:.7f} seconds")
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
                CREATE TABLE IF NOT EXISTS stock (
                    item_id TEXT PRIMARY KEY,
                    stock_count INTEGER NOT NULL DEFAULT 0,
                    price INTEGER NOT NULL DEFAULT 0
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

@app.post('/item/create/<price>')
def create_item(price: int):
    item_id = str(uuid.uuid4())
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO stock (item_id, stock_count, price) VALUES (%s, 0, %s)",
                (item_id, int(price))
            )
        conn.commit()
        return jsonify({'item_id': item_id})
    except Exception:
        conn.rollback()
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)

@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init(n: int, starting_stock: int, item_price: int):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            data = [(str(i), int(starting_stock), int(item_price)) for i in range(int(n))]
            # Use execute_values for much faster bulk insert
            execute_values(
                cur,
                "INSERT INTO stock (item_id, stock_count, price) VALUES %s "
                "ON CONFLICT (item_id) DO UPDATE SET stock_count = EXCLUDED.stock_count, price = EXCLUDED.price",
                data,
                page_size=10000
            )
        conn.commit()
        return jsonify({"msg": "Batch init for stock successful"})
    except Exception as e:
        conn.rollback()
        print(f"STOCK batch_init error: {str(e)}")
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)

@app.get('/find/<item_id>')
def find_item(item_id: str):
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT stock_count as stock, price FROM stock WHERE item_id = %s", (item_id,))
            item = cur.fetchone()
            if not item:
                abort(404, f"Item: {item_id} not found!")
            return jsonify(item)
    finally:
        release_db_conn(conn)

@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE stock SET stock_count = stock_count + %s WHERE item_id = %s RETURNING stock_count",
                (int(amount), item_id)
            )
            result = cur.fetchone()
            if not result:
                abort(404, "Item not found")
        conn.commit()
        return Response(f"Item: {item_id} stock updated to: {result[0]}", status=200)
    except Exception:
        conn.rollback()
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)

@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    amount = int(amount)
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Atomic check-and-update to prevent negative stock
            cur.execute(
                "UPDATE stock SET stock_count = stock_count - %s "
                "WHERE item_id = %s AND stock_count >= %s RETURNING stock_count",
                (amount, item_id, amount)
            )
            result = cur.fetchone()
            if not result:
                # Check if item exists to give better error msg
                cur.execute("SELECT stock_count FROM stock WHERE item_id = %s", (item_id,))
                exists = cur.fetchone()
                if not exists:
                    abort(404, "Item not found")
                else:
                    abort(400, "Insufficient stock")
        conn.commit()
        return Response(f"Item: {item_id} stock updated to: {result[0]}", status=200)
    except Exception as e:
        conn.rollback()
        if hasattr(e, 'code'): raise e
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)