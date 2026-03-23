"""
Order service — parallel SAGA coordinator over Kafka.

Stock and payment execute simultaneously — no waiting between them.
This halves happy-path latency compared to sequential execution.

Protocol
--------
  checkout_saga()
    → write sagas row (AWAITING_BOTH), commit
    → publish stock.execute AND payment.execute simultaneously

  Happy path (both succeed, any order):
    first response  → AWAITING_PAYMENT or AWAITING_STOCK
    second response → COMPLETING → mark order paid → COMPLETED

  One fails first (before other responds):
    stock.failed    in AWAITING_BOTH → STOCK_FAILED_AWAITING_PAYMENT
    payment.failed  in AWAITING_BOTH → PAYMENT_FAILED_AWAITING_STOCK

  Then the other responds:
    STOCK_FAILED_AWAITING_PAYMENT + payment.executed  → ROLLING_BACK_PAYMENT
    STOCK_FAILED_AWAITING_PAYMENT + payment.failed    → FAILED (nothing to undo)
    PAYMENT_FAILED_AWAITING_STOCK + stock.executed    → ROLLING_BACK_STOCK
    PAYMENT_FAILED_AWAITING_STOCK + stock.failed      → FAILED (nothing to undo)

  One succeeds then the other fails:
    AWAITING_PAYMENT + payment.failed → ROLLING_BACK_STOCK
    AWAITING_STOCK   + stock.failed   → ROLLING_BACK_PAYMENT

  Rollback responses:
    stock.rolledback  in ROLLING_BACK_STOCK   → ROLLED_BACK
    payment.rolledback in ROLLING_BACK_PAYMENT → ROLLED_BACK
    either rollback fails                      → ROLLBACK_FAILED (inconsistent)

Recovery
--------
  AWAITING_BOTH                  → re-send stock.execute + payment.execute
  AWAITING_PAYMENT               → re-send payment.execute
  AWAITING_STOCK                 → re-send stock.execute
  STOCK_FAILED_AWAITING_PAYMENT  → re-send payment.execute
  PAYMENT_FAILED_AWAITING_STOCK  → re-send stock.execute
  COMPLETING                     → redo mark-paid directly
  ROLLING_BACK_STOCK             → re-send stock.rollback
  ROLLING_BACK_PAYMENT           → re-send payment.rollback

Correlation ID convention
-------------------------
  {saga_id}:saga:stock:execute    → _on_stock_response
  {saga_id}:saga:payment:execute  → _on_payment_response
  {saga_id}:saga:stock:rollback   → _on_stock_rollback_response
  {saga_id}:saga:payment:rollback → _on_payment_rollback_response
"""

import logging
import uuid
from collections import defaultdict

from common.idempotency import get_advisory_lock, check_idempotency, save_idempotency
from common.kafka_helpers import publish_response
from db import (
    create_saga, get_saga_for_update, advance_saga,
    mark_order_paid, get_order_for_update,
)

logger = logging.getLogger(__name__)

_STOCK_SAGA_TOPIC   = "internal.stock.saga"
_PAYMENT_SAGA_TOPIC = "internal.payment.saga"
_GATEWAY_RESP_TOPIC = "gateway.responses"


class SagaState:
    AWAITING_BOTH                  = "AWAITING_BOTH"
    AWAITING_PAYMENT               = "AWAITING_PAYMENT"
    AWAITING_STOCK                 = "AWAITING_STOCK"
    STOCK_FAILED_AWAITING_PAYMENT  = "STOCK_FAILED_AWAITING_PAYMENT"
    PAYMENT_FAILED_AWAITING_STOCK  = "PAYMENT_FAILED_AWAITING_STOCK"
    COMPLETING                     = "COMPLETING"
    ROLLING_BACK_STOCK             = "ROLLING_BACK_STOCK"
    ROLLING_BACK_PAYMENT           = "ROLLING_BACK_PAYMENT"
    COMPLETED                      = "COMPLETED"
    FAILED                         = "FAILED"
    ROLLED_BACK                    = "ROLLED_BACK"
    ROLLBACK_FAILED                = "ROLLBACK_FAILED"


TERMINAL_STATES = (
    SagaState.COMPLETED,
    SagaState.FAILED,
    SagaState.ROLLED_BACK,
    SagaState.ROLLBACK_FAILED,
)

# Routing: suffix → handler — wired in init()
_HANDLERS: dict[str, callable] = {}

_conn_pool         = None
_internal_producer = None
_gateway_producer  = None


def init(conn_pool, internal_producer, gateway_producer) -> None:
    global _conn_pool, _internal_producer, _gateway_producer
    _conn_pool         = conn_pool
    _internal_producer = internal_producer
    _gateway_producer  = gateway_producer
    _HANDLERS[":saga:stock:execute"]    = _on_stock_response
    _HANDLERS[":saga:payment:execute"]  = _on_payment_response
    _HANDLERS[":saga:stock:rollback"]   = _on_stock_rollback_response
    _HANDLERS[":saga:payment:rollback"] = _on_payment_rollback_response


# ---------------------------------------------------------------------------
# Publishing helpers
# ---------------------------------------------------------------------------

def _send(topic: str, corr_id: str, msg_type: str, body: dict) -> None:
    from kafka.errors import KafkaError
    message = {"type": msg_type, "correlation_id": corr_id, **body}
    try:
        _internal_producer.send(topic, key=corr_id, value=message)
        _internal_producer.flush(timeout=5)
    except KafkaError as exc:
        logger.error("SAGA failed to publish %s: %s", msg_type, exc)
        raise


def _respond(corr_id: str, status_code: int, body) -> None:
    publish_response(_gateway_producer, _GATEWAY_RESP_TOPIC,
                     corr_id, status_code, body)


# ---------------------------------------------------------------------------
# Checkout entry point
# ---------------------------------------------------------------------------

def checkout_saga(conn, order_id: str, original_corr_id: str,
                  idem_key: str | None) -> tuple:
    """
    Initiate parallel SAGA — fire stock.execute and payment.execute together.
    Returns (None, None) — response sent asynchronously when saga completes.
    """
    with conn.cursor() as cur:
        # If client did not provide idempotency key, use corr id
        idem_key = original_corr_id if not idem_key else idem_key

        cached = check_idempotency(cur, idem_key)
        if cached:
            conn.commit()
            logger.warning(f"Idempotency hit on checkout of order: {order}, cached result is: {cached}")
            return cached

        try:
            order = get_order_for_update(conn, order_id)
        except ValueError as exc:
            conn.rollback()
            return 400, {"error": str(exc)}

        if order["paid"]:
            conn.rollback()
            return 200, f"Order {order_id} is already paid"

        items_quantities: dict[str, int] = defaultdict(int)
        for item_id, quantity in order["items"]:
            items_quantities[item_id] += quantity

        if not items_quantities:
            conn.rollback()
            return 200, "Order has no items"

        saga_id = str(uuid.uuid4())

        # Denormalize user_id + total_cost so recovery never needs orders table
        create_saga(
            conn, saga_id, order_id, SagaState.AWAITING_BOTH,
            dict(items_quantities),
            order["user_id"], order["total_cost"],
            original_corr_id, idem_key,
        )
    conn.commit()

    items = [{"item_id": iid, "quantity": qty}
             for iid, qty in items_quantities.items()]

    # Fire both simultaneously
    _send(_STOCK_SAGA_TOPIC,
          f"{saga_id}:saga:stock:execute", "stock.execute",
          {"txn_id": saga_id, "items": items})

    _send(_PAYMENT_SAGA_TOPIC,
          f"{saga_id}:saga:payment:execute", "payment.execute",
          {"txn_id": saga_id,
           "user_id": order["user_id"],
           "amount":  order["total_cost"]})

    logger.info("SAGA saga=%s started (parallel) for order=%s", saga_id, order_id)
    return None, None


# ---------------------------------------------------------------------------
# Response routing
# ---------------------------------------------------------------------------

def is_saga_message(corr_id: str) -> bool:
    return any(corr_id.endswith(s) for s in _HANDLERS)


def handle_response(corr_id: str, payload: dict, conn) -> None:
    for suffix, handler_fn in _HANDLERS.items():
        if not corr_id.endswith(suffix):
            continue

        saga_id = corr_id[: -len(suffix)]
        saga    = get_saga_for_update(conn, saga_id)

        if saga is None:
            logger.error("SAGA response for unknown saga=%s", saga_id)
            conn.rollback()
            return

        elif saga["state"] in TERMINAL_STATES:
            logger.info("SAGA saga=%s already terminal, ignoring duplicate", saga_id)
            conn.rollback()
            return

        handler_fn(conn, saga, payload)
        return

    logger.warning("No SAGA handler for corr_id=%s", corr_id)


# ---------------------------------------------------------------------------
# State machine handlers
# ---------------------------------------------------------------------------

def _on_stock_response(conn, saga: dict, payload: dict) -> None:
    """
    Handle stock.executed or stock.failed.
    Valid in: AWAITING_BOTH, AWAITING_STOCK, PAYMENT_FAILED_AWAITING_STOCK.
    """
    body    = payload.get("body") or {}
    state   = saga["state"]
    success = (
        payload.get("status_code") == 200
        and isinstance(body, dict)
        and body.get("type") == "stock.executed"
    )

    if success:
        if state == SagaState.AWAITING_BOTH:
            # Payment not yet known — record and wait
            advance_saga(conn, saga["id"], SagaState.AWAITING_PAYMENT,
                         stock_ok=True)
            conn.commit()
            logger.info("SAGA saga=%s stock ok, awaiting payment", saga["id"])

        elif state == SagaState.AWAITING_STOCK:
            # Payment already succeeded — both done, commit
            _complete(conn, saga)

        elif state == SagaState.PAYMENT_FAILED_AWAITING_STOCK:
            # Payment already failed — stock just succeeded, roll it back
            reason = saga["failure_reason"] or "Payment failed"
            _rollback_stock(conn, saga, reason)

        else:
            logger.warning("SAGA saga=%s unexpected stock success in state=%s",
                           saga["id"], state)
            conn.rollback()

    else:
        reason = _extract_reason(payload)

        if state == SagaState.AWAITING_BOTH:
            # Payment not yet known — record and wait
            advance_saga(conn, saga["id"],
                         SagaState.STOCK_FAILED_AWAITING_PAYMENT,
                         failure_reason=reason)
            conn.commit()
            logger.info("SAGA saga=%s stock failed, awaiting payment outcome",
                        saga["id"])

        elif state == SagaState.AWAITING_STOCK:
            # Payment already succeeded — roll it back
            _rollback_payment(conn, saga, reason)

        elif state == SagaState.PAYMENT_FAILED_AWAITING_STOCK:
            # Both failed — nothing to undo
            advance_saga(conn, saga["id"], SagaState.FAILED,
                         failure_reason=saga["failure_reason"] or reason)
            conn.commit()
            logger.info("SAGA saga=%s both failed", saga["id"])
            _respond(saga["original_correlation_id"], 400,
                     {"error": saga["failure_reason"] or reason})

        else:
            logger.warning("SAGA saga=%s unexpected stock failure in state=%s",
                           saga["id"], state)
            conn.rollback()


def _on_payment_response(conn, saga: dict, payload: dict) -> None:
    """
    Handle payment.executed or payment.failed.
    Valid in: AWAITING_BOTH, AWAITING_PAYMENT, STOCK_FAILED_AWAITING_PAYMENT.
    """
    body    = payload.get("body") or {}
    state   = saga["state"]
    success = (
        payload.get("status_code") == 200
        and isinstance(body, dict)
        and body.get("type") == "payment.executed"
    )

    if success:
        if state == SagaState.AWAITING_BOTH:
            # Stock not yet known — record and wait
            advance_saga(conn, saga["id"], SagaState.AWAITING_STOCK,
                         payment_ok=True)
            conn.commit()
            logger.info("SAGA saga=%s payment ok, awaiting stock", saga["id"])

        elif state == SagaState.AWAITING_PAYMENT:
            # Stock already succeeded — both done, commit
            _complete(conn, saga)

        elif state == SagaState.STOCK_FAILED_AWAITING_PAYMENT:
            # Stock already failed — payment just succeeded, roll it back
            reason = saga["failure_reason"] or "Stock failed"
            _rollback_payment(conn, saga, reason)

        else:
            logger.warning("SAGA saga=%s unexpected payment success in state=%s",
                           saga["id"], state)
            conn.rollback()

    else:
        reason = _extract_reason(payload)

        if state == SagaState.AWAITING_BOTH:
            # Stock not yet known — record and wait
            advance_saga(conn, saga["id"],
                         SagaState.PAYMENT_FAILED_AWAITING_STOCK,
                         failure_reason=reason)
            conn.commit()
            logger.info("SAGA saga=%s payment failed, awaiting stock outcome",
                        saga["id"])

        elif state == SagaState.AWAITING_PAYMENT:
            # Stock already succeeded — roll it back
            _rollback_stock(conn, saga, reason)

        elif state == SagaState.STOCK_FAILED_AWAITING_PAYMENT:
            # Both failed — nothing to undo
            advance_saga(conn, saga["id"], SagaState.FAILED,
                         failure_reason=saga["failure_reason"] or reason)
            conn.commit()
            logger.info("SAGA saga=%s both failed", saga["id"])
            _respond(saga["original_correlation_id"], 400,
                     {"error": saga["failure_reason"] or reason})

        else:
            logger.warning("SAGA saga=%s unexpected payment failure in state=%s",
                           saga["id"], state)
            conn.rollback()


def _on_stock_rollback_response(conn, saga: dict, payload: dict) -> None:
    """Handle stock.rolledback during ROLLING_BACK_STOCK."""
    body    = payload.get("body") or {}
    success = (
        payload.get("status_code") == 200
        and isinstance(body, dict)
        and body.get("type") == "stock.rolledback"
    )

    if success:
        advance_saga(conn, saga["id"], SagaState.ROLLED_BACK)
        conn.commit()
        logger.info("SAGA saga=%s stock rolled back", saga["id"])
        _respond(saga["original_correlation_id"], 400,
                 {"error": saga["failure_reason"] or "Checkout failed"})
    else:
        _rollback_failed(conn, saga, "stock")


def _on_payment_rollback_response(conn, saga: dict, payload: dict) -> None:
    """Handle payment.rolledback during ROLLING_BACK_PAYMENT."""
    body    = payload.get("body") or {}
    success = (
        payload.get("status_code") == 200
        and isinstance(body, dict)
        and body.get("type") == "payment.rolledback"
    )

    if success:
        advance_saga(conn, saga["id"], SagaState.ROLLED_BACK)
        conn.commit()
        logger.info("SAGA saga=%s payment rolled back", saga["id"])
        _respond(saga["original_correlation_id"], 400,
                 {"error": saga["failure_reason"] or "Checkout failed"})
    else:
        _rollback_failed(conn, saga, "payment")


# ---------------------------------------------------------------------------
# Action helpers
# ---------------------------------------------------------------------------

def _complete(conn, saga: dict) -> None:
    """
    Atomic success point — mark order paid and advance to COMPLETED.
    Once this commits the checkout is permanently done.
    """
    with conn.cursor() as cur:
        mark_order_paid(conn, saga["order_id"])
        save_idempotency(cur, saga["idempotency_key"], 200, "Checkout successful")
        advance_saga(conn, saga["id"], SagaState.COMPLETED,
                     stock_ok=True, payment_ok=True)
    conn.commit()
    logger.info("SAGA saga=%s completed, order=%s paid",
                saga["id"], saga["order_id"])
    _respond(saga["original_correlation_id"], 200, "Checkout successful")


def _rollback_stock(conn, saga: dict, reason: str) -> None:
    """Trigger stock compensation — payment failed after stock succeeded."""
    advance_saga(conn, saga["id"], SagaState.ROLLING_BACK_STOCK,
                 failure_reason=reason)
    conn.commit()
    _send(_STOCK_SAGA_TOPIC,
          f"{saga['id']}:saga:stock:rollback", "stock.rollback",
          {"txn_id": saga["id"]})
    logger.info("SAGA saga=%s rolling back stock: %s", saga["id"], reason)


def _rollback_payment(conn, saga: dict, reason: str) -> None:
    """Trigger payment compensation — stock failed after payment succeeded."""
    advance_saga(conn, saga["id"], SagaState.ROLLING_BACK_PAYMENT,
                 failure_reason=reason)
    conn.commit()
    _send(_PAYMENT_SAGA_TOPIC,
          f"{saga['id']}:saga:payment:rollback", "payment.rollback",
          {"txn_id": saga["id"]})
    logger.info("SAGA saga=%s rolling back payment: %s", saga["id"], reason)


def _rollback_failed(conn, saga: dict, which: str) -> None:
    """Compensation failed — system is inconsistent, needs manual fix."""
    logger.critical(
        "SAGA saga=%s %s ROLLBACK FAILED — inconsistency, manual fix required",
        saga["id"], which,
    )
    advance_saga(conn, saga["id"], SagaState.ROLLBACK_FAILED,
                 failure_reason=f"{which} rollback failed")
    conn.commit()
    _respond(saga["original_correlation_id"], 500,
             {"error": "Checkout failed and compensation failed — contact support"})


def _extract_reason(payload: dict) -> str:
    body = payload.get("body") or {}
    if isinstance(body, dict):
        return body.get("reason") or body.get("error") or "Unknown failure"
    return str(body) if body else "Unknown failure"