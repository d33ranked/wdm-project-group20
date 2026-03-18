"""
Database utilities for high-availability PostgreSQL with Patroni.

Key features:
- Automatic primary discovery via Patroni REST API (all hosts probed in parallel).
- Per-operation slow-query detection: if a DB call takes longer than
  `op_timeout` seconds, a background poller starts pinging /primary on all
  hosts once per second (rate-limited globally so many concurrent slow queries
  share one poller).  Whichever finishes first wins:
    * DB call finishes  → poller is cancelled, execution continues normally.
    * Poller finds a new primary → FailoverDetected is raised so the caller
      can swap the connection and retry the entire request from scratch.
- WrappedCursor: drop-in replacement for psycopg2 cursor that adds the above
  racing logic to execute(), fetchone(), fetchall(), and close().
- Retry decorator (retry_on_db_failure) for Flask route handlers.
"""

import os
import time
import functools
import threading
import requests
import psycopg2
import psycopg2.pool
import gevent
import gevent.event

# ---------------------------------------------------------------------------
# Sentinel exception – raised when the background poller wins the race.
# ---------------------------------------------------------------------------

class FailoverDetected(Exception):
    """Raised when a /primary poll wins the race against a slow DB operation."""


# ---------------------------------------------------------------------------
# WrappedCursor
# ---------------------------------------------------------------------------

class WrappedCursor:
    """
    Wraps a psycopg2 cursor and races every DB operation against a shared
    failover poller.  If the operation takes longer than `op_timeout` seconds
    the pool's background poller is activated.  If the poller finds a new
    primary before the operation finishes, FailoverDetected is raised.
    """

    def __init__(self, cursor, pool: "HAConnectionPool", op_timeout: float):
        self._cur = cursor
        self._pool = pool
        self._op_timeout = op_timeout

    # ------------------------------------------------------------------
    # Internal: run `fn` in a greenlet and race it against the poller.
    # ------------------------------------------------------------------
    def _race(self, fn, *args, **kwargs):
        """
        Run fn() in a greenlet.  If it doesn't finish within op_timeout,
        activate the shared background poller.  Then wait for whichever
        event fires first: fn finishing, or the pool's failover_event.
        Raises FailoverDetected if failover wins.
        """
        result_box = [None]
        exc_box = [None]
        done_event = gevent.event.Event()

        def run():
            try:
                result_box[0] = fn(*args, **kwargs)
            except Exception as e:
                exc_box[0] = e
            finally:
                done_event.set()

        worker = gevent.spawn(run)

        # Wait up to op_timeout for the operation to complete on its own.
        done_event.wait(timeout=self._op_timeout)

        if not done_event.is_set():
            # Operation is slow – activate the shared poller.
            print(f"{self._pool.service_name}: DB op slow (>{self._op_timeout}s), starting failover poller")
            self._pool._ensure_poller_running()

            # Now wait for either: operation done, or failover confirmed.
            while True:
                # Check both events with a short timeout so we can re-check.
                done_event.wait(timeout=0.1)
                if done_event.is_set():
                    break
                if self._pool._failover_event.is_set():
                    # Failover won – kill the worker greenlet and raise.
                    worker.kill(block=False)
                    raise FailoverDetected("Failover detected while waiting for DB operation")

        # Operation finished – no failover needed.
        if exc_box[0] is not None:
            raise exc_box[0]
        return result_box[0]

    # ------------------------------------------------------------------
    # Public cursor interface
    # ------------------------------------------------------------------

    def execute(self, sql, params=None):
        if params is not None:
            return self._race(self._cur.execute, sql, params)
        return self._race(self._cur.execute, sql)

    def fetchone(self):
        return self._race(self._cur.fetchone)

    def fetchall(self):
        return self._race(self._cur.fetchall)

    def close(self):
        try:
            self._cur.close()
        except Exception:
            pass

    # Delegate everything else (e.g. statusmessage, rowcount) directly.
    def __getattr__(self, name):
        return getattr(self._cur, name)


# ---------------------------------------------------------------------------
# HAConnectionPool
# ---------------------------------------------------------------------------

class HAConnectionPool:
    """
    High-availability connection pool that automatically discovers the primary
    PostgreSQL node via Patroni REST API and recreates the pool on failover.

    Also manages a shared background poller greenlet that pings /primary on
    all hosts in parallel at most once per second.  Many concurrent slow
    queries share this single poller – they all subscribe to _failover_event.
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
        op_timeout: float = 3.0,
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
        self.op_timeout = op_timeout

        self._pool = None
        self._current_primary = None
        self._lock = threading.Lock()

        # Shared poller state.
        # _failover_event: set when a new primary is confirmed by the poller.
        # _poller_greenlet: the currently running poller, or None.
        # _poller_lock: prevents multiple pollers from starting simultaneously.
        self._failover_event = gevent.event.Event()
        self._poller_greenlet = None
        self._poller_lock = threading.Lock()

        # Initial connection.
        self._create_pool()

    # ------------------------------------------------------------------
    # Primary discovery – all hosts probed in parallel.
    # ------------------------------------------------------------------

    def _get_primary_host(self) -> str:
        """
        Probe all Patroni hosts in parallel and return the first one that
        responds 200 to GET /primary.  Falls back to /leader if none do.
        """
        result_box = [None]
        found = threading.Event()

        def probe(host, endpoint):
            if found.is_set():
                return
            try:
                resp = requests.get(
                    f"http://{host}:{self.patroni_port}/{endpoint}",
                    timeout=1,
                )
                if resp.status_code == 200 and not found.is_set():
                    result_box[0] = host
                    found.set()
            except requests.exceptions.RequestException:
                pass

        # First round: /primary on all hosts in parallel.
        threads = [threading.Thread(target=probe, args=(h, "primary"), daemon=True)
                   for h in self.hosts]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=1.5)

        if result_box[0]:
            return result_box[0]

        # Second round: /leader on all hosts in parallel.
        found.clear()
        threads = [threading.Thread(target=probe, args=(h, "leader"), daemon=True)
                   for h in self.hosts]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=1.5)

        if result_box[0]:
            return result_box[0]

        raise Exception(f"{self.service_name}: No primary found among hosts: {self.hosts}")

    # ------------------------------------------------------------------
    # Pool creation / failover
    # ------------------------------------------------------------------

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

                # Close old pool if it exists.
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
                        f"{self.service_name}: Failed to connect "
                        f"(attempt {attempt + 1}/{retries}): {e}"
                    )
                    time.sleep(delay)
                else:
                    raise Exception(
                        f"{self.service_name}: Failed to create connection pool "
                        f"after {retries} attempts"
                    )

    def _handle_failover(self):
        """
        Blocking failover: rebuild the pool on the new primary.
        Called from getconn() when the connection probe fails at request start.
        Retry budget covers ttl (4s) + retry_timeout (3s) + buffer → 20×0.5s = 10s.
        """
        print(f"{self.service_name}: Initiating failover...")
        time.sleep(0.5)
        self._create_pool(retries=20, delay=0.5)
        print(f"{self.service_name}: Failover complete, new primary: {self._current_primary}")

    # ------------------------------------------------------------------
    # Background poller (shared, rate-limited to once per second)
    # ------------------------------------------------------------------

    def _ensure_poller_running(self):
        """
        Start the background poller greenlet if it isn't already running.
        Multiple callers all share the same greenlet – only one is ever active.
        """
        with self._poller_lock:
            if self._poller_greenlet is not None and not self._poller_greenlet.dead:
                return  # already running
            # Reset the failover event before starting a new polling round.
            self._failover_event.clear()
            self._poller_greenlet = gevent.spawn(self._poll_for_primary)

    def _poll_for_primary(self):
        """
        Background greenlet: ping /primary on all hosts in parallel once per
        second until a primary is found.  When found, rebuild the pool and set
        _failover_event so all waiting WrappedCursors are unblocked.
        """
        print(f"{self.service_name}: Background poller started")
        while True:
            try:
                new_primary = self._get_primary_host()
                # Found a primary – rebuild the pool if it changed.
                # Do NOT hold _lock while calling _create_pool: it sleeps
                # internally and would deadlock other greenlets waiting on _lock.
                if new_primary != self._current_primary:
                    print(f"{self.service_name}: Poller found new primary: {new_primary}")
                    self._create_pool(retries=1, delay=0)
                else:
                    print(f"{self.service_name}: Poller confirmed existing primary: {new_primary}")
                # Signal all waiting cursors regardless (primary may be same but
                # connection could have been re-established).
                self._failover_event.set()
                print(f"{self.service_name}: Background poller done, failover_event set")
                return
            except Exception:
                # No primary yet – wait 1 second and try again.
                gevent.sleep(1)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def getconn(self):
        """
        Get a connection from the pool, running a SELECT 1 probe.
        If the pool is exhausted, yields and retries every 0.1s for up to 30s
        rather than immediately failing.
        Triggers a blocking failover if the connection probe fails.

        IMPORTANT: _lock is never held across a gevent.sleep() call.  Holding
        a threading.Lock while yielding to the gevent hub blocks every other
        greenlet that needs the lock — causing a complete worker deadlock.
        """
        # Wait for a connection to become available if pool is exhausted.
        # Do NOT hold _lock here: we must be able to yield freely.
        for attempt in range(300):   # 300 x 0.1s = 30s max wait
            try:
                with self._lock:
                    conn = self._pool.getconn()
                break
            except psycopg2.pool.PoolError:
                if attempt == 299:
                    raise
                gevent.sleep(0.1)

        try:
            conn.cursor().execute("SELECT 1")
            return conn
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            print(f"{self.service_name}: Connection error, triggering failover: {e}")
            self._handle_failover()
            with self._lock:
                return self._pool.getconn()

    def cursor(self, conn) -> WrappedCursor:
        """Return a WrappedCursor for the given connection."""
        return WrappedCursor(conn.cursor(), self, self.op_timeout)

    def putconn(self, conn, close: bool = False):
        """Return a connection to the pool."""
        try:
            self._pool.putconn(conn, close=close)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    def closeall(self):
        """Close all connections in the pool."""
        if self._pool is not None:
            self._pool.closeall()

    @property
    def current_primary(self) -> str | None:
        return self._current_primary


# ---------------------------------------------------------------------------
# Retry decorator for Flask route handlers
# ---------------------------------------------------------------------------

def retry_on_db_failure(conn_pool: "HAConnectionPool", max_retries: int = 10, delay: float = 1.0):
    """
    Flask route decorator.  On FailoverDetected or psycopg2.OperationalError:
      1. Returns the dead connection to the pool (closing it).
      2. Waits for the pool's failover_event (set by the background poller)
         or falls back to a blocking _handle_failover() if the poller isn't
         running yet.
      3. Acquires a fresh connection and re-runs the entire handler.

    Import g from flask in the app file – the decorator reads/writes g.conn.
    """
    from flask import g

    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return f(*args, **kwargs)
                except FailoverDetected as e:
                    if attempt == max_retries:
                        raise
                    print(f"{conn_pool.service_name}: FailoverDetected in handler "
                          f"(attempt {attempt + 1}/{max_retries}): {e}")
                    _swap_connection(conn_pool, delay)
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    if attempt == max_retries:
                        raise
                    print(f"{conn_pool.service_name}: OperationalError in handler "
                          f"(attempt {attempt + 1}/{max_retries}): {e}")
                    _swap_connection(conn_pool, delay)
        return wrapper
    return decorator


def _swap_connection(conn_pool: "HAConnectionPool", delay: float):
    """
    Return the current g.conn to the pool (closing it) then wait for a healthy
    primary to become available before assigning a new g.conn.
    """
    from flask import g

    # Return the dead connection.
    old_conn = g.pop("conn", None)
    if old_conn is not None:
        conn_pool.putconn(old_conn, close=True)

    # Always clear the event first so we don't immediately pass through on a
    # stale set from a previous failover round.
    conn_pool._failover_event.clear()

    # Ensure the poller is running and wait for it to confirm a primary.
    conn_pool._ensure_poller_running()
    conn_pool._failover_event.wait(timeout=15)

    gevent.sleep(delay)
    g.conn = conn_pool.getconn()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_ha_pool(service_name: str = "SERVICE") -> HAConnectionPool:
    """
    Create an HA connection pool from environment variables.

    Expected environment variables:
    - POSTGRES_HOSTS: comma-separated list of host names (e.g. "payment-db-1,payment-db-2")
    - PATRONI_PORT:   Patroni REST API port (default: 8008)
    - POSTGRES_PORT:  PostgreSQL port (default: 5432)
    - POSTGRES_DB:    Database name
    - POSTGRES_USER:  Database user
    - POSTGRES_PASSWORD: Database password
    - DB_OP_TIMEOUT:  Seconds before a slow DB op triggers the poller (default: 3.0)
    """
    hosts = os.environ["POSTGRES_HOSTS"].split(",")
    patroni_port = int(os.environ.get("PATRONI_PORT", 8008))
    postgres_port = int(os.environ.get("POSTGRES_PORT", 5432))
    dbname = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    op_timeout = float(os.environ.get("DB_OP_TIMEOUT", 3.0))

    return HAConnectionPool(
        hosts=hosts,
        port=postgres_port,
        dbname=dbname,
        user=user,
        password=password,
        patroni_port=patroni_port,
        service_name=service_name,
        op_timeout=op_timeout,
    )
