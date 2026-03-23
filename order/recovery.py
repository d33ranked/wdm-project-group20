"""
Order service — stale transaction recovery.

Runs as a background daemon thread.  Every RECOVERY_INTERVAL_S seconds it
scans for non-terminal sagas and TPC transactions and re-publishes their
pending Kafka messages.

The order service is the coordinator — it does not roll back stale records
the way payment/stock do.  Instead it re-drives forward: participants are
idempotent by txn_id so re-publishing is always safe.

SAGA recovery: each non-terminal state maps to exactly the message(s) that
are still outstanding.

TPC recovery: PREPARING → re-send both prepares, COMMITTING → re-send both
commits, ABORTING → re-send rollback to whoever voted YES.
"""

import logging
import os
import time

from common.idempotency import save_idempotency
from common.kafka_helpers import publish_response
from db import get_saga_for_update, advance_saga, mark_order_paid

logger = logging.getLogger(__name__)

RECOVERY_INTERVAL_S = int(os.environ.get("RECOVERY_INTERVAL_S", "30"))
RECOVERY_STARTUP_S = int(os.environ.get("RECOVERY_STARTUP_S", "0"))

_STOCK_SAGA_TOPIC   = "internal.stock.saga"
_PAYMENT_SAGA_TOPIC = "internal.payment.saga"
_STOCK_TPC_TOPIC    = "internal.stock.tpc"
_PAYMENT_TPC_TOPIC  = "internal.payment.tpc"
_GATEWAY_RESP_TOPIC = "gateway.responses"


def _send(producer, topic: str, corr_id: str, msg_type: str, body: dict) -> None:
    from kafka.errors import KafkaError
    message = {"type": msg_type, "correlation_id": corr_id, **body}
    try:
        producer.send(topic, key=corr_id, value=message)
        producer.flush(timeout=5)
        logger.info("Recovery: re-published %s corr=%s", msg_type, corr_id)
    except KafkaError as exc:
        logger.error("Recovery: failed to publish %s: %s", msg_type, exc)


# ---------------------------------------------------------------------------
# SAGA recovery
# ---------------------------------------------------------------------------

def _recover_sagas(conn, conn_pool, internal_producer, gateway_producer, age_seconds) -> None:
    from saga import SagaState, TERMINAL_STATES

    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, order_id, state, items_quantities, user_id, total_cost, "
            "       original_correlation_id, idempotency_key, failure_reason "
            "FROM sagas WHERE state NOT IN %s "
            "AND updated_at < NOW() - INTERVAL '%s seconds'",
            (TERMINAL_STATES, age_seconds),
        )
        rows = cur.fetchall()

    if not rows:
        logger.debug("Recovery: no stale sagas")
        return

    logger.warning("Recovery: %d stale saga(s) found", len(rows))

    for (saga_id, order_id, state, items_quantities, user_id, total_cost,
         orig_corr_id, idem_key, failure_reason) in rows:

        logger.info("Recovery: saga=%s state=%s", saga_id, state)
        items = [{"item_id": iid, "quantity": qty}
                 for iid, qty in items_quantities.items()]

        if state == SagaState.AWAITING_BOTH:
            _send(internal_producer, _STOCK_SAGA_TOPIC,
                  f"{saga_id}:saga:stock:execute", "stock.execute",
                  {"txn_id": saga_id, "items": items})
            _send(internal_producer, _PAYMENT_SAGA_TOPIC,
                  f"{saga_id}:saga:payment:execute", "payment.execute",
                  {"txn_id": saga_id, "user_id": user_id, "amount": total_cost})

        elif state == SagaState.AWAITING_PAYMENT:
            _send(internal_producer, _PAYMENT_SAGA_TOPIC,
                  f"{saga_id}:saga:payment:execute", "payment.execute",
                  {"txn_id": saga_id, "user_id": user_id, "amount": total_cost})

        elif state == SagaState.AWAITING_STOCK:
            _send(internal_producer, _STOCK_SAGA_TOPIC,
                  f"{saga_id}:saga:stock:execute", "stock.execute",
                  {"txn_id": saga_id, "items": items})

        elif state == SagaState.STOCK_FAILED_AWAITING_PAYMENT:
            _send(internal_producer, _PAYMENT_SAGA_TOPIC,
                  f"{saga_id}:saga:payment:execute", "payment.execute",
                  {"txn_id": saga_id, "user_id": user_id, "amount": total_cost})

        elif state == SagaState.PAYMENT_FAILED_AWAITING_STOCK:
            _send(internal_producer, _STOCK_SAGA_TOPIC,
                  f"{saga_id}:saga:stock:execute", "stock.execute",
                  {"txn_id": saga_id, "items": items})

        elif state == SagaState.COMPLETING:
            # Crashed between both-succeeded and mark-order-paid — redo directly
            conn2 = conn_pool.getconn()
            try:
                saga = get_saga_for_update(conn2, saga_id)
                if saga and saga["state"] == SagaState.COMPLETING:
                    with conn2.cursor() as cur:
                        mark_order_paid(conn2, order_id)
                        save_idempotency(cur, idem_key, 200, "Checkout successful")
                        advance_saga(conn2, saga_id, SagaState.COMPLETED)
                    conn2.commit()
                    publish_response(gateway_producer, _GATEWAY_RESP_TOPIC,
                                     orig_corr_id, 200, "Checkout successful")
                    logger.info("Recovery: completed saga=%s", saga_id)
            except Exception as exc:
                logger.error("Recovery: failed to complete saga=%s: %s",
                             saga_id, exc, exc_info=True)
                try:
                    conn2.rollback()
                except Exception:
                    pass
            finally:
                conn_pool.putconn(conn2)

        elif state == SagaState.ROLLING_BACK_STOCK:
            _send(internal_producer, _STOCK_SAGA_TOPIC,
                  f"{saga_id}:saga:stock:rollback", "stock.rollback",
                  {"txn_id": saga_id})

        elif state == SagaState.ROLLING_BACK_PAYMENT:
            _send(internal_producer, _PAYMENT_SAGA_TOPIC,
                  f"{saga_id}:saga:payment:rollback", "payment.rollback",
                  {"txn_id": saga_id})


# ---------------------------------------------------------------------------
# TPC recovery
# ---------------------------------------------------------------------------

def _recover_tpc(conn, internal_producer, age_seconds) -> None:
    from tpc import TpcStatus, TERMINAL_STATUSES

    with conn.cursor() as cur:
        cur.execute(
            "SELECT txn_id, status, items, user_id, total_cost, "
            "       stock_vote, payment_vote "
            "FROM transaction_log WHERE status NOT IN %s "
            "AND updated_at < NOW() - INTERVAL '%s seconds'",
            (TERMINAL_STATUSES, age_seconds),
        )
        rows = cur.fetchall()

    if not rows:
        logger.debug("Recovery: no stale TPC transactions")
        return

    logger.warning("Recovery: %d stale TPC transaction(s) found", len(rows))

    for txn_id, status, items, user_id, total_cost, stock_vote, payment_vote in rows:
        logger.info("Recovery: txn=%s status=%s", txn_id, status)

        if status == TpcStatus.PREPARING:
            # Re-send whichever prepares have not yet voted YES
            if stock_vote != "YES":
                _send(internal_producer, _STOCK_TPC_TOPIC,
                      f"{txn_id}:tpc:stock:prepare", "stock.prepare",
                      {"txn_id": txn_id, "items": items})
            if payment_vote != "YES":
                _send(internal_producer, _PAYMENT_TPC_TOPIC,
                      f"{txn_id}:tpc:payment:prepare", "payment.prepare",
                      {"txn_id": txn_id, "user_id": user_id, "amount": total_cost})

        elif status == TpcStatus.COMMITTING:
            # Re-send commit messages — idempotent, participants just delete undo-log
            _send(internal_producer, _STOCK_TPC_TOPIC,
                  f"{txn_id}:tpc:stock:commit", "stock.commit",
                  {"txn_id": txn_id})
            _send(internal_producer, _PAYMENT_TPC_TOPIC,
                  f"{txn_id}:tpc:payment:commit", "payment.commit",
                  {"txn_id": txn_id})

        elif status == TpcStatus.ABORTING:
            # Only roll back participants who voted YES (deducted something)
            if stock_vote == "YES":
                _send(internal_producer, _STOCK_TPC_TOPIC,
                      f"{txn_id}:tpc:stock:rollback", "stock.rollback",
                      {"txn_id": txn_id})
            if payment_vote == "YES":
                _send(internal_producer, _PAYMENT_TPC_TOPIC,
                      f"{txn_id}:tpc:payment:rollback", "payment.rollback",
                      {"txn_id": txn_id})


# ---------------------------------------------------------------------------
# Recovery loop
# ---------------------------------------------------------------------------
def run_once(conn_pool, internal_producer, gateway_producer) -> None:
    """Blocking startup pass — uses age=0 to catch everything pending."""
    _run_once(conn_pool, internal_producer, gateway_producer,age_seconds=RECOVERY_STARTUP_S)

def run_recovery_loop(conn_pool, internal_producer, gateway_producer) -> None:
    logger.info("Recovery loop started (interval=%ds)", RECOVERY_INTERVAL_S)
    while True:
        time.sleep(RECOVERY_INTERVAL_S)
        _run_once(conn_pool, internal_producer, gateway_producer,
                  age_seconds=RECOVERY_INTERVAL_S)

def _run_once(conn_pool, internal_producer, gateway_producer, age_seconds=RECOVERY_INTERVAL_S) -> None:
    conn = conn_pool.getconn()
    try:
        logger.info("Running recovery (age_threshold=%ds)", age_seconds)
        _recover_sagas(conn, conn_pool, internal_producer, gateway_producer,age_seconds=age_seconds)
        _recover_tpc(conn, internal_producer, age_seconds=age_seconds)
    except Exception as exc:
        logger.error("Recovery scan failed: %s", exc, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn_pool.putconn(conn)