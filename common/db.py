"""
Shared database utilities for all microservices.

Provides connection pool creation with retry logic, Flask request lifecycle
hooks that manage per-request connections, and gunicorn logger wiring.
"""

import os
import time
import atexit
import logging

import psycopg2
import psycopg2.pool
from time import perf_counter
from flask import g


def create_conn_pool(service_name: str, retries: int = 10, delay: int = 2):
    """Create a threaded connection pool, retrying until PostgreSQL is ready.

    Called at module-load time in each service. The retry loop handles the
    Docker startup race where the application container starts before the
    database container has finished initialising.
    """
    for attempt in range(retries):
        try:
            pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=10,
                maxconn=100,
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
                print(
                    f"{service_name}: PostgreSQL not ready, retrying in {delay}s... "
                    f"(attempt {attempt + 1}/{retries})"
                )
                time.sleep(delay)
            else:
                raise


def setup_flask_lifecycle(app, conn_pool, service_name: str):
    """Register before/after/teardown hooks that manage per-request DB connections.

    Each HTTP request gets its own connection from the pool. On success the
    connection is committed; on exception it is rolled back. The connection is
    always returned to the pool in teardown, even if the request handler raised.
    """

    @app.before_request
    def _before_request():
        g.start_time = perf_counter()
        g.conn = conn_pool.getconn()

    @app.after_request
    def _after_request(response):
        duration = perf_counter() - g.start_time
        print(f"{service_name}: Request took {duration:.7f} seconds")
        return response

    @app.teardown_request
    def _teardown_request(exception):
        conn = g.pop("conn", None)
        if conn is not None:
            if exception:
                conn.rollback()
            else:
                conn.commit()
            conn_pool.putconn(conn)


def setup_gunicorn_logging(app):
    """Wire Flask's logger to gunicorn's logger so log output is unified.

    When running under gunicorn, Flask's default logger writes to its own
    handlers which may not appear in gunicorn's output. This makes Flask
    use gunicorn's handlers and log level instead.
    """
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
