import asyncio
import logging
import os
from common.kafka_helpers import publish_response

logger = logging.getLogger("payment-recovery")

# Configuration
RECOVERY_INTERVAL_S = int(os.environ.get("RECOVERY_INTERVAL_S", "60"))
STALE_THRESHOLD_S = int(os.environ.get("STALE_THRESHOLD_MIN", "5")) * 60
RECOVERY_RESPONSE_TOPIC = "internal.responses"


async def _rollback_stale_transactions(pool, orch):
    """
    Find and roll back all prepared transactions older than STALE_THRESHOLD_S.
    Runs blocking DB logic in the executor to keep the event loop free.
    """
    loop = asyncio.get_event_loop()
    
    # 1. Fetch stale transactions
    def fetch_stale():
        conn = pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT txn_id, user_id, amount 
                    FROM prepared_transactions 
                    WHERE created_at < NOW() - INTERVAL '%s seconds'
                    """,
                    (STALE_THRESHOLD_S,),
                )
                return cur.fetchall()
        finally:
            pool.putconn(conn)

    stale_txns = await loop.run_in_executor(None, fetch_stale)

    if not stale_txns:
        logger.debug("No stale transactions found.")
        return

    logger.warning(f"Found {len(stale_txns)} stale transactions. Initiating recovery.")

    for txn_id, user_id, amount in stale_txns:
        # 2. Process each rollback in its own transaction
        def perform_rollback():
            conn = pool.getconn()
            try:
                with conn: # Atomic commit/rollback
                    with conn.cursor() as cur:
                        # Re-verify existence with lock
                        cur.execute("SELECT 1 FROM prepared_transactions WHERE txn_id = %s FOR UPDATE", (txn_id,))
                        if not cur.fetchone():
                            return False # Already handled
                        
                        # Refund and delete undo-log
                        cur.execute("UPDATE users SET credit = credit + %s WHERE id = %s", (amount, user_id))
                        cur.execute("DELETE FROM prepared_transactions WHERE txn_id = %s", (txn_id,))
                    
                    conn.commit()
                    return True
            finally:
                pool.putconn(conn)

        success = await loop.run_in_executor(None, perform_rollback)
        
        if success:
            logger.info(f"Successfully rolled back stale txn {txn_id}")
            #!TODO send msg via orchastrator that is not reacting to incomming msg?
            #orch....
            

async def run_stale_recovery_once(pool, orch):
    logger.debug("Running rollback stale transactions once")
    return _rollback_stale_transactions(pool, orch)

async def run_recovery_loop(pool, orch):
    """
    The main background loop.
    """
    logger.info(f"Recovery loop started (Interval: {RECOVERY_INTERVAL_S}s, Threshold: {STALE_THRESHOLD_S}s)")
    
    while True:
        await asyncio.sleep(RECOVERY_INTERVAL_S)

        try:
            logger.debug("Starting rollback stale transactions")
            await _rollback_stale_transactions(pool, orch)
        except Exception as e:
            logger.error(f"Error in recovery iteration: {e}", exc_info=True)
        