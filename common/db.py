"""
Database helpers — connection pool creation.

Each Kafka worker thread borrows a connection from
the pool for the duration of one message, then returns it.
"""

import atexit
import logging
import os

import psycopg2
import psycopg2.pool

logger = logging.getLogger(__name__)


def create_conn_pool(service_name: str) -> psycopg2.pool.ThreadedConnectionPool:
    """
    Create a threaded connection pool for the given service.

    Reads connection parameters from environment variables:
      POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

    The pool is registered with atexit so all connections are cleanly closed
    when the process exits.
    """
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
    logger.info("%s: DB connection pool created", service_name)
    return pool