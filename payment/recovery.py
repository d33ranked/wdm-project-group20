"""
Payment service — stale transaction recovery.

Runs as a background daemon thread.  Every RECOVERY_INTERVAL_S seconds it
scans for prepared_transactions rows that are older than STALE_THRESHOLD_S
and rolls them back by refunding the reserved credit.

This covers the case where the order service (coordinator) crashed after
sending payment.reserve but before sending payment.commit or payment.rollback.
The credit would be stuck in a reserved state indefinitely without this.

Why auto-rollback is safe here:
  - The order service uses its own undo-log to decide whether to re-drive a
    transaction on restart.  If it recovers and re-sends payment.commit for a
    txn that we already rolled back, the rollback handler is idempotent and
    returns payment.rolledback — the order service will then detect the
    inconsistency and compensate.
  - The threshold (default 5 min) is intentionally generous to avoid racing
    with a slow but healthy coordinator.
"""

import logging
import time

from common.kafka_helpers import publish_response

logger = logging.getLogger(__name__)

RECOVERY_INTERVAL_S  = int(__import__("os").environ.get("RECOVERY_INTERVAL_S",  "60"))
STALE_THRESHOLD_S    = int(__import__("os").environ.get("STALE_THRESHOLD_MIN",   "5")) * 60
RECOVERY_RESPONSE_TOPIC = "internal.responses"


def _rollback_stale_transactions(conn, producer):
    """
    Find and roll back all prepared transactions older than STALE_THRESHOLD_S.

    Each rollback is committed individually so a crash mid-run doesn't block
    recovery of other transactions.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT txn_id, user_id, amount
            FROM prepared_transactions
            WHERE created_at < NOW() - INTERVAL '%s seconds'
            """,
            (STALE_THRESHOLD_S,),
        )
        stale = cur.fetchall()

    if not stale:
        logger.debug("Recovery: no stale transactions found")
        return

    logger.warning("Recovery: found %d stale transaction(s) to roll back", len(stale))

    for txn_id, user_id, amount in stale:
        try:
            with conn.cursor() as cur:
                # Re-fetch with lock in case another thread (e.g. a concurrent
                # rollback command) has already cleaned this up
                cur.execute(
                    "SELECT 1 FROM prepared_transactions WHERE txn_id = %s FOR UPDATE",
                    (txn_id,),
                )
                if cur.fetchone() is None:
                    logger.info("Recovery: txn=%s already resolved, skipping", txn_id)
                    conn.rollback()
                    continue

                # Restore credit and remove undo-log entry
                cur.execute(
                    "UPDATE users SET credit = credit + %s WHERE id = %s",
                    (amount, user_id),
                )
                cur.execute(
                    "DELETE FROM prepared_transactions WHERE txn_id = %s",
                    (txn_id,),
                )

            conn.commit()
            logger.warning(
                "Recovery: rolled back stale txn=%s user=%s refunded=%s",
                txn_id, user_id, amount,
            )

            # Publish a rollback event so the order service can react if it
            # is still alive and waiting for an outcome
            publish_response(
                producer,
                RECOVERY_RESPONSE_TOPIC,
                correlation_id=txn_id,
                status_code=200,
                body={"type": "payment.rolledback", "txn_id": txn_id, "reason": "stale recovery"},
            )

        except Exception as exc:
            logger.error("Recovery: failed to roll back txn=%s: %s", txn_id, exc, exc_info=True)
            try:
                conn.rollback()
            except Exception:
                pass


def run_recovery_loop(conn_pool, producer):
    """
    Periodically run stale-transaction recovery.

    Designed to be run as a daemon thread — exits when the main process exits.
    """
    logger.info(
        "Recovery loop started (interval=%ds, threshold=%ds)",
        RECOVERY_INTERVAL_S, STALE_THRESHOLD_S,
    )

    # Run once immediately at startup to clean up anything left from the
    # previous process instance
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
            logger.error("Recovery loop iteration failed: %s", exc, exc_info=True)
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            conn_pool.putconn(conn)