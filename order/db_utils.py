"""
Database utilities for high-availability PostgreSQL with Patroni.
Provides automatic primary discovery and connection pool failover.
"""
# TODO: Remove code duplication

import os
import time
import threading
import requests
import psycopg2
import psycopg2.pool


class HAConnectionPool:
    """
    High-availability connection pool that automatically discovers the primary
    PostgreSQL node via Patroni REST API and recreates the pool on failover.
    """

    def __init__(
        self,
        hosts: list[str],
        port: int,
        dbname: str,
        user: str,
        password: str,
        patroni_port: int = 8008,
        minconn: int = 10,
        maxconn: int = 100,
        service_name: str = "SERVICE",
    ):
        self.hosts = hosts
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.patroni_port = patroni_port
        self.minconn = minconn
        self.maxconn = maxconn
        self.service_name = service_name

        self._pool = None
        self._current_primary = None
        self._lock = threading.Lock()

        # Initial connection
        self._create_pool()

    def _get_primary_host(self) -> str:
        """
        Query Patroni REST API on each host to find the current primary.
        Returns the hostname of the primary node.
        """
        for host in self.hosts:
            try:
                # Patroni returns 200 on /primary only if this node is the primary
                resp = requests.get(
                    f"http://{host}:{self.patroni_port}/primary",
                    timeout=1
                )
                if resp.status_code == 200:
                    return host
            except requests.exceptions.RequestException:
                continue

        # Fallback: try /leader endpoint which returns leader info
        for host in self.hosts:
            try:
                resp = requests.get(
                    f"http://{host}:{self.patroni_port}/leader",
                    timeout=1
                )
                if resp.status_code == 200:
                    return host
            except requests.exceptions.RequestException:
                continue

        raise Exception(f"{self.service_name}: No primary found among hosts: {self.hosts}")

    def _create_pool(self, retries: int = 30, delay: float = 1.0):
        """
        Create a new connection pool connected to the current primary.
        Retries until successful or max retries exceeded.
        """
        for attempt in range(retries):
            try:
                primary_host = self._get_primary_host()
                new_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=self.minconn,
                    maxconn=self.maxconn,
                    host=primary_host,
                    port=self.port,
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password,
                )
                
                # Close old pool if exists
                if self._pool is not None:
                    try:
                        self._pool.closeall()
                    except Exception:
                        pass

                self._pool = new_pool
                self._current_primary = primary_host
                print(f"{self.service_name}: Connected to primary: {primary_host}")
                return

            except Exception as e:
                if attempt < retries - 1:
                    print(
                        f"{self.service_name}: Failed to connect (attempt {attempt + 1}/{retries}): {e}"
                    )
                    time.sleep(delay)
                else:
                    raise Exception(
                        f"{self.service_name}: Failed to create connection pool after {retries} attempts"
                    )

    def getconn(self):
        """
        Get a connection from the pool. If the connection fails,
        trigger a failover to find the new primary.
        """
        with self._lock:
            try:
                conn = self._pool.getconn()
                # Test the connection
                conn.cursor().execute("SELECT 1")
                return conn
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                print(f"{self.service_name}: Connection error, triggering failover: {e}")
                self._handle_failover()
                return self._pool.getconn()

    def putconn(self, conn, close: bool = False):
        """Return a connection to the pool."""
        try:
            self._pool.putconn(conn, close=close)
        except Exception:
            # Connection might be invalid, just close it
            try:
                conn.close()
            except Exception:
                pass

    def _handle_failover(self):
        """
        Handle failover by discovering the new primary and recreating the pool.
        """
        print(f"{self.service_name}: Initiating failover...")
        
        # Wait briefly for Patroni to complete failover
        time.sleep(0.5)
        
        # Recreate pool with new primary
        self._create_pool(retries=10, delay=0.5)
        
        print(f"{self.service_name}: Failover complete, new primary: {self._current_primary}")

    def closeall(self):
        """Close all connections in the pool."""
        if self._pool is not None:
            self._pool.closeall()

    @property
    def current_primary(self) -> str:
        """Return the current primary host."""
        return self._current_primary


def create_ha_pool(service_name: str = "SERVICE") -> HAConnectionPool:
    """
    Create an HA connection pool from environment variables.
    
    Expected environment variables:
    - POSTGRES_HOSTS: comma-separated list of host names (e.g., "order-db-1,order-db-2")
    - PATRONI_PORT: Patroni REST API port (default: 8008)
    - POSTGRES_PORT: PostgreSQL port (default: 5432)
    - POSTGRES_DB: Database name
    - POSTGRES_USER: Database user
    - POSTGRES_PASSWORD: Database password
    """
    hosts = os.environ["POSTGRES_HOSTS"].split(",")
    patroni_port = int(os.environ.get("PATRONI_PORT", 8008))
    postgres_port = int(os.environ.get("POSTGRES_PORT", 5432))
    dbname = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]

    return HAConnectionPool(
        hosts=hosts,
        port=postgres_port,
        dbname=dbname,
        user=user,
        password=password,
        patroni_port=patroni_port,
        service_name=service_name,
    )
