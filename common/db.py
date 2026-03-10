import os
import time
import atexit
import logging

import psycopg2
import psycopg2.pool
from time import perf_counter
from flask import g


def create_conn_pool(service_name, retries=10, delay=2):
    for attempt in range(retries):
        try:
            pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=10, maxconn=100,
                host=os.environ["POSTGRES_HOST"],
                port=int(os.environ["POSTGRES_PORT"]),
                dbname=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
            )
            atexit.register(pool.closeall)
            return pool
        except psycopg2.OperationalError:
            if attempt < retries - 1:
                print(f"{service_name}: PostgreSQL not ready, retrying in {delay}s... "
                      f"(attempt {attempt + 1}/{retries})")
                time.sleep(delay)
            else:
                raise


def setup_flask_lifecycle(app, conn_pool, service_name):
    @app.before_request
    def _before():
        g.start_time = perf_counter()
        g.conn = conn_pool.getconn()

    @app.after_request
    def _after(response):
        duration = perf_counter() - g.start_time
        print(f"{service_name}: Request took {duration:.7f} seconds")
        return response

    @app.teardown_request
    def _teardown(exception):
        conn = g.pop("conn", None)
        if conn is not None:
            conn.rollback() if exception else conn.commit()
            conn_pool.putconn(conn)


def setup_gunicorn_logging(app):
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
