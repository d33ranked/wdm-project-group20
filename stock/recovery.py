"""
Stock service — stale 2PC transaction recovery.

Periodically scans for prepared_transactions rows older than STALE_THRESHOLD_S
and rolls them back by restoring the reserved stock.  Only applies to 2PC —
SAGA transactions have no stale state (silence = success).

Item rows are locked in sorted order during recovery to stay consistent with
the deadlock-prevention strategy used in the normal transaction path.
"""

import logging
import os
import time
from collections import defaultdict
from psycopg2.extras import execute_batch

from common.kafka_helpers import publish_response

logger = logging.getLogger(__name__)

RECOVERY_INTERVAL_S  = int(os.environ.get("RECOVERY_INTERVAL_S", "60"))
STALE_THRESHOLD_S    = int(os.environ.get("STALE_THRESHOLD_MIN", "5")) * 60
RECOVERY_RESPONSE_TOPIC = "internal.responses"


def _rollback_stale_transactions(conn, producer):
    """Find and roll back all prepared transactions older than STALE_THRESHOLD_S."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT txn_id, item_id, quantity
            FROM prepared_transactions
            WHERE created_at < NOW() - INTERVAL '%s seconds'
            """,
            (STALE_THRESHOLD_S,),
        )
        rows = cur.fetchall()

    if not rows:
        logger.debug("Recovery: no stale transactions found")
        return

    # Group rows by txn_id — a transaction spans multiple items
    by_txn = defaultdict(list)
    for txn_id, item_id, quantity in rows:
        by_txn[txn_id].append((item_id, quantity))

    logger.warning("Recovery: found %d stale transaction(s) to roll back", len(by_txn))

    for txn_id, items in by_txn.items():
        try:
            with conn.cursor() as cur:
                # Re-check under lock — another thread may have already cleaned up
                cur.execute(
                    "SELECT item_id, quantity FROM prepared_transactions "
                    "WHERE txn_id = %s FOR UPDATE",
                    (txn_id,),
                )
                current = cur.fetchall()
                if not current:
                    logger.info("Recovery: txn=%s already resolved, skipping", txn_id)
                    conn.rollback()
                    continue

                # Lock item rows in sorted order before restoring stock
                item_ids = sorted(item_id for item_id, _ in current)
                cur.execute(
                    "SELECT id FROM items WHERE id = ANY(%s) ORDER BY id FOR UPDATE",
                    (item_ids,),
                )
                execute_batch(
                    cur,
                    "UPDATE items SET stock = stock + %s WHERE id = %s",
                    current
                    )
                cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))

            conn.commit()
            logger.warning("Recovery: rolled back stale txn=%s (%d items)", txn_id, len(current))

            # Notify the order service so it can compensate if still alive
            publish_response(
                producer,
                RECOVERY_RESPONSE_TOPIC,
                correlation_id=txn_id,
                status_code=200,
                body={"type": "stock.rolledback", "txn_id": txn_id, "reason": "stale recovery"},
            )

        except Exception as exc:
            logger.error("Recovery: failed to roll back txn=%s: %s", txn_id, exc, exc_info=True)
            try:
                conn.rollback()
            except Exception:
                pass


def run_recovery_loop(conn_pool, producer):
    """Periodically run stale-transaction recovery as a daemon thread."""
    logger.info(
        "Recovery loop started (interval=%ds, threshold=%ds)",
        RECOVERY_INTERVAL_S, STALE_THRESHOLD_S,
    )

    # Run once immediately at startup to clean up leftovers from previous instance
    conn = conn_pool.getconn()
    try:
        _rollback_stale_transactions(conn, producer)
    except Exception as exc:
        logger.error("Recovery startup scan failed: %s", exc, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn_pool.putconn(conn)

    while True:
        time.sleep(RECOVERY_INTERVAL_S)
        conn = conn_pool.getconn()
        try:
            _rollback_stale_transactions(conn, producer)
        except Exception as exc:
            logger.error("Recovery iteration failed: %s", exc, exc_info=True)
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            conn_pool.putconn(conn)