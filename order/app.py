import logging
import os
import atexit
import uuid
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor, execute_values
from flask import Flask, jsonify, abort, Response, g
import requests
from time import perf_counter

FLAG_2PC = os.environ.get('ENABLE_2PC', "false") == "true"
FLAG_gRPC = os.environ.get('ENABLE_GRPC', "false") == "true"

if FLAG_2PC or FLAG_gRPC:
    import grpc
    import transaction_pb2
    import transaction_pb2_grpc


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ.get('GATEWAY_URL', 'http://gateway:80')

app = Flask("order-service")

# --- DATABASE SETUP ---
# We use a ThreadedConnectionPool for better performance with Gunicorn
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
                CREATE TABLE IF NOT EXISTS orders (
                    order_id UUID PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    paid BOOLEAN DEFAULT FALSE,
                    total_cost BIGINT DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS order_items (
                    order_id UUID REFERENCES orders(order_id) ON DELETE CASCADE,
                    item_id TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    PRIMARY KEY (order_id, item_id)
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

@app.before_request
def start_timer():
    g.start_time = perf_counter()

@app.after_request
def log_response(response):
    duration = perf_counter() - g.start_time
    print(f"ORDER: Request took {duration:.7f} seconds")
    return response




# --- HELPER FUNCTIONS ---

def send_request(method, url):
    try:
        res = requests.request(method, url)
        return res
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)

# --- ROUTES ---

@app.post('/create/<user_id>')
def create_order(user_id: str):
    order_id = str(uuid.uuid4())
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO orders (order_id, user_id) VALUES (%s, %s)",
                (order_id, user_id)
            )
        conn.commit()
    except Exception:
        conn.rollback()
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)
    return jsonify({'order_id': order_id})

@app.post('/batch_init/<num_orders>/<num_items>/<num_users>/<item_price>')
def batch_init_orders(num_orders: int, num_items: int, num_users: int, item_price: int):
    """Create orders in bulk. Creates random orders with random items."""
    import random
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Create orders with random user IDs and items
            orders_data = []
            order_costs = {}  # Track total cost per order
            for i in range(int(num_orders)):
                order_id = str(uuid.uuid4())
                user_id = str(random.randint(0, int(num_users) - 1))
                orders_data.append((order_id, user_id))
                order_costs[order_id] = 0
            
            # Use execute_values for much faster bulk insert
            execute_values(
                cur,
                "INSERT INTO orders (order_id, user_id) VALUES %s ON CONFLICT (order_id) DO NOTHING",
                orders_data,
                page_size=10000
            )
            
            # Add random items to orders - use dict to aggregate duplicates
            items_dict = {}  # (order_id, item_id) -> quantity
            for order_id, _ in orders_data:
                num_items_in_order = random.randint(1, 5)
                for _ in range(num_items_in_order):
                    item_id = str(random.randint(0, int(num_items) - 1))
                    quantity = random.randint(1, 10)
                    price = int(item_price)
                    total_cost = quantity * price
                    
                    # Aggregate duplicate (order_id, item_id) pairs
                    key = (order_id, item_id)
                    if key in items_dict:
                        items_dict[key] += quantity
                    else:
                        items_dict[key] = quantity
                    order_costs[order_id] += total_cost
            
            # Convert to list of tuples
            items_data = [(order_id, item_id, qty) for (order_id, item_id), qty in items_dict.items()]
            
            if items_data:
                execute_values(
                    cur,
                    "INSERT INTO order_items (order_id, item_id, quantity) VALUES %s ON CONFLICT (order_id, item_id) DO UPDATE SET quantity = order_items.quantity + EXCLUDED.quantity",
                    items_data,
                    page_size=10000
                )
            
            # Batch update total costs
            cost_updates = [(cost, order_id) for order_id, cost in order_costs.items()]
            cur.executemany(
                "UPDATE orders SET total_cost = total_cost + %s WHERE order_id = %s",
                cost_updates
            )
        
        conn.commit()
        return jsonify({"msg": "Batch init for orders successful"})
    except Exception as e:
        conn.rollback()
        print(f"ORDER batch_init error: {str(e)}")
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)

@app.get('/find/<order_id>')
def find_order(order_id: str):
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get Order details
            cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
            order = cur.fetchone()
            if not order:
                abort(404, "Order not found")
            
            # Get Items
            cur.execute("SELECT item_id, quantity FROM order_items WHERE order_id = %s", (order_id,))
            items = [(row['item_id'], row['quantity']) for row in cur.fetchall()]
            
            order['items'] = items
            return jsonify(order)
    finally:
        release_db_conn(conn)

@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    # 1. Verify item exists via Gateway
    item_reply = send_request("GET", f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        abort(400, "Item not found")
    
    price = item_reply.json()['price']
    added_cost = int(quantity) * price

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Update or Insert item (Upsert)
            cur.execute("""
                INSERT INTO order_items (order_id, item_id, quantity) 
                VALUES (%s, %s, %s)
                ON CONFLICT (order_id, item_id) 
                DO UPDATE SET quantity = order_items.quantity + EXCLUDED.quantity
            """, (order_id, item_id, int(quantity)))
            
            # Update total cost
            cur.execute(
                "UPDATE orders SET total_cost = total_cost + %s WHERE order_id = %s",
                (added_cost, order_id)
            )
        conn.commit()
    except Exception:
        conn.rollback()
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)
    
    return Response("Item added", status=200)

@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT user_id, total_cost, paid FROM orders WHERE order_id = %s", (order_id,))
            order = cur.fetchone()
            
            if not order or order['paid']:
                abort(400, "Order invalid or already paid")

            cur.execute("SELECT item_id, quantity FROM order_items WHERE order_id = %s", (order_id,))
            items = cur.fetchall()

        # Logic for stock subtraction
        removed_items = []
        for item in items:
            res = send_request("POST", f"{GATEWAY_URL}/stock/subtract/{item['item_id']}/{item['quantity']}")
            if res.status_code != 200:
                # Rollback stock
                for r_id, r_qty in removed_items:
                    send_request("POST", f"{GATEWAY_URL}/stock/add/{r_id}/{r_qty}")
                abort(400, f"Out of stock: {item['item_id']}")
            removed_items.append((item['item_id'], item['quantity']))

        # Payment
        pay_res = send_request("POST", f"{GATEWAY_URL}/payment/pay/{order['user_id']}/{order['total_cost']}")
        if pay_res.status_code != 200:
            for r_id, r_qty in removed_items:
                send_request("POST", f"{GATEWAY_URL}/stock/add/{r_id}/{r_qty}")
            abort(400, "Insufficient funds")

        # Mark as paid
        with conn.cursor() as cur:
            cur.execute("UPDATE orders SET paid = TRUE WHERE order_id = %s", (order_id,))
        conn.commit()
        
        return Response("Checkout successful", status=200)
    finally:
        release_db_conn(conn)

# Only define the safe_checkout function if 2PC is enabled
if FLAG_2PC:
    def get_stub(service_name):
        channel = grpc.insecure_channel(f"{service_name}:50051")
        return transaction_pb2_grpc.TransactionParticipantStub(channel)

    @app.post('/2pc/checkout/<order_id>')
    def safe_checkout(order_id: str):
        # 1. Fetch order details from local Postgres
        conn = get_db_conn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
                order = cur.fetchone()
                if not order or order['paid']:
                    return abort(400, "Invalid order or already paid")

                cur.execute("SELECT item_id, quantity FROM order_items WHERE order_id = %s", (order_id,))
                items = cur.fetchall()
                
            tx_id = f"tx_{order_id}"
            
            # 2. Prepare gRPC request
            grpc_items = [transaction_pb2.OrderItem(item_id=i['item_id'], quantity=i['quantity']) for i in items]
            prepare_req = transaction_pb2.PrepareRequest(
                transaction_id=tx_id,
                user_id=order['user_id'],
                order_id=order_id,
                total_cost=order['total_cost'],
                items=grpc_items
            )

            # 3. PHASE 1: PREPARE (External Participants + Local)
            stubs = {
                "stock": get_stub("stock-service"),
                "payment": get_stub("payment-service")
            }

            # Step A: Prepare Stock and Payment
            success_stock = stubs["stock"].Prepare(prepare_req).success
            success_payment = stubs["payment"].Prepare(prepare_req).success
            
            # Step B: Prepare Local (Order)
            success_order = False
            with conn.cursor() as cur:
                cur.execute("UPDATE orders SET paid = TRUE WHERE order_id = %s", (order_id,))
                cur.execute(f"PREPARE TRANSACTION '{tx_id}'")
                success_order = True
            conn.commit()

            # 4. PHASE 2: GLOBAL DECISION
            if success_stock and success_payment and success_order:
                # COMMIT ALL
                stubs["stock"].Commit(transaction_pb2.CommitRequest(transaction_id=tx_id))
                stubs["payment"].Commit(transaction_pb2.CommitRequest(transaction_id=tx_id))
                with conn.cursor() as cur:
                    cur.execute(f"COMMIT PREPARED '{tx_id}'")
                conn.commit()
                return Response("Checkout Successful", status=200)
            else:
                # ROLLBACK ALL
                raise Exception("Prepare phase failed")

        except Exception as e:
            # Global Rollback
            stubs["stock"].Rollback(transaction_pb2.RollbackRequest(transaction_id=tx_id))
            stubs["payment"].Rollback(transaction_pb2.RollbackRequest(transaction_id=tx_id))
            with conn.cursor() as cur:
                cur.execute(f"ROLLBACK PREPARED '{tx_id}'")
            conn.commit()
            return abort(400, f"Checkout failed: {str(e)}")
        finally:
            release_db_conn(conn)





if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)