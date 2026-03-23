"""
Order service — parallel 2PC coordinator over Kafka.

Both stock.prepare and payment.prepare are sent simultaneously.
We wait for both votes before deciding commit or abort.

Protocol
--------
  checkout_tpc()
    → write transaction_log (PREPARING), commit
    → publish stock.prepare AND payment.prepare simultaneously

  _on_stock_vote() / _on_payment_vote()
    record vote, check if both votes are in:
      both YES  → advance to COMMITTING, mark order paid (atomic commit point)
                → publish stock.commit + payment.commit
                → respond success to gateway
      any NO    → advance to ABORTING, publish rollback to YES voters
                → if neither voted YES yet → go straight to ABORTED

  _on_stock_rolledback() / _on_payment_rolledback()
    → advance to ABORTED, respond failure to gateway

Recovery
--------
  PREPARING         → re-send stock.prepare + payment.prepare
  COMMITTING        → re-send stock.commit + payment.commit
  ABORTING          → re-send rollback to whoever voted YES
                      (tracked via stock_vote / payment_vote columns)
  COMMITTED/ABORTED → nothing

Correlation ID convention
-------------------------
  {txn_id}:tpc:stock:prepare    → _on_stock_vote
  {txn_id}:tpc:payment:prepare  → _on_payment_vote
  {txn_id}:tpc:stock:rollback   → _on_stock_rolledback
  {txn_id}:tpc:payment:rollback → _on_payment_rolledback
"""

import logging
import uuid
from collections import defaultdict

from common.idempotency import get_advisory_lock, check_idempotency, save_idempotency
from common.kafka_helpers import publish_response
from db import (
    create_transaction, get_txn_for_update, advance_txn,
    mark_order_paid, get_order_for_update,
)

logger = logging.getLogger(__name__)

_STOCK_TPC_TOPIC    = "internal.stock.tpc"
_PAYMENT_TPC_TOPIC  = "internal.payment.tpc"
_GATEWAY_RESP_TOPIC = "gateway.responses"


class TpcStatus:
    PREPARING  = "PREPARING"
    COMMITTING = "COMMITTING"
    ABORTING   = "ABORTING"
    COMMITTED  = "COMMITTED"
    ABORTED    = "ABORTED"


TERMINAL_STATUSES = (TpcStatus.COMMITTED, TpcStatus.ABORTED)

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
    _HANDLERS[":tpc:stock:prepare"]    = _on_stock_vote
    _HANDLERS[":tpc:payment:prepare"]  = _on_payment_vote
    _HANDLERS[":tpc:stock:rollback"]   = _on_stock_rolledback
    _HANDLERS[":tpc:payment:rollback"] = _on_payment_rolledback


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
        logger.error("TPC failed to publish %s: %s", msg_type, exc)
        raise


def _respond(corr_id: str, status_code: int, body) -> None:
    publish_response(_gateway_producer, _GATEWAY_RESP_TOPIC,
                     corr_id, status_code, body)


# ---------------------------------------------------------------------------
# Checkout entry point
# ---------------------------------------------------------------------------

def checkout_tpc(conn, order_id: str, original_corr_id: str,
                 idem_key: str | None) -> tuple:
    """
    Initiate parallel 2PC — send prepare to both participants simultaneously.
    Returns (None, None) — response sent asynchronously when protocol completes.
    """
    with conn.cursor() as cur:
        # If client did not provide idempotency key, use corr id
        idem_key = original_corr_id if not idem_key else idem_key

        cached = check_idempotency(cur, idem_key)
        if cached:
            conn.commit()
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

        txn_id = str(uuid.uuid4())
        items  = [{"item_id": iid, "quantity": qty}
                  for iid, qty in items_quantities.items()]

        # Write full record before any external call
        create_transaction(
            conn, txn_id, order_id, TpcStatus.PREPARING,
            items, order["user_id"], order["total_cost"],
            original_corr_id, idem_key,
        )
    conn.commit()

    # Send prepare to BOTH participants simultaneously
    _send(_STOCK_TPC_TOPIC,
          f"{txn_id}:tpc:stock:prepare", "stock.prepare",
          {"txn_id": txn_id, "items": items})

    _send(_PAYMENT_TPC_TOPIC,
          f"{txn_id}:tpc:payment:prepare", "payment.prepare",
          {"txn_id": txn_id,
           "user_id": order["user_id"],
           "amount":  order["total_cost"]})

    logger.info("TPC txn=%s started (parallel) for order=%s", txn_id, order_id)
    return None, None


# ---------------------------------------------------------------------------
# Response routing
# ---------------------------------------------------------------------------

def is_tpc_message(corr_id: str) -> bool:
    return any(corr_id.endswith(s) for s in _HANDLERS)


def handle_response(corr_id: str, payload: dict, conn) -> None:
    for suffix, handler_fn in _HANDLERS.items():
        if not corr_id.endswith(suffix):
            continue

        txn_id = corr_id[: -len(suffix)]
        txn    = get_txn_for_update(conn, txn_id)

        if txn is None:
            logger.error("TPC response for unknown txn=%s", txn_id)
            conn.rollback()
            return

        elif txn["status"] in TERMINAL_STATUSES:
            logger.info("TPC txn=%s already terminal, ignoring duplicate", txn_id)
            conn.rollback()
            return

        handler_fn(conn, txn, payload)
        return


# ---------------------------------------------------------------------------
# Vote handlers
# ---------------------------------------------------------------------------

def _on_stock_vote(conn, txn: dict, payload: dict) -> None:
    """
    Handle stock.prepared or stock.failed.
    Record the stock vote and check if both votes are now in.
    """
    body    = payload.get("body") or {}
    voted_yes = (
        payload.get("status_code") == 200
        and isinstance(body, dict)
        and body.get("type") == "stock.prepared"
    )

    vote = "YES" if voted_yes else "NO"
    logger.info("TPC txn=%s stock vote=%s", txn["txn_id"], vote)

    if not voted_yes:
        reason = _extract_reason(payload)
        _handle_no_vote(conn, txn, who_voted_no="stock", reason=reason)
        return

    # Stock voted YES — check if payment also voted
    if txn["payment_vote"] == "YES":
        # Both YES — commit
        advance_txn(conn, txn["txn_id"], TpcStatus.COMMITTING, stock_vote="YES")
        _commit(conn, txn)
    elif txn["payment_vote"] == "NO":
        # Payment already voted NO — roll back stock immediately
        advance_txn(conn, txn["txn_id"], TpcStatus.ABORTING, stock_vote="YES")
        conn.commit()
        _send(_STOCK_TPC_TOPIC,
              f"{txn['txn_id']}:tpc:stock:rollback", "stock.rollback",
              {"txn_id": txn["txn_id"]})
    else:
        # Payment not yet voted — record and wait
        advance_txn(conn, txn["txn_id"], TpcStatus.PREPARING, stock_vote="YES")
        conn.commit()
        logger.info("TPC txn=%s stock YES, awaiting payment vote", txn["txn_id"])


def _on_payment_vote(conn, txn: dict, payload: dict) -> None:
    """
    Handle payment.prepared or payment.failed.
    Record the payment vote and check if both votes are now in.
    """
    body    = payload.get("body") or {}
    voted_yes = (payload.get("status_code") == 200
                 and isinstance(body, dict)
                 and body.get("type") == "payment.prepared")

    vote = "YES" if voted_yes else "NO"
    logger.info("TPC txn=%s payment vote=%s", txn["txn_id"], vote)

    if not voted_yes:
        reason = _extract_reason(payload)
        _handle_no_vote(conn, txn, who_voted_no="payment", reason=reason)
        return

    # Payment voted YES — check if stock also voted
    if txn["stock_vote"] == "YES":
        # Both YES — commit
        advance_txn(conn, txn["txn_id"], TpcStatus.COMMITTING, payment_vote="YES")
        _commit(conn, txn)
    elif txn["stock_vote"] == "NO":
        # Stock already voted NO — roll back payment immediately
        advance_txn(conn, txn["txn_id"], TpcStatus.ABORTING, payment_vote="YES")
        conn.commit()
        _send(_PAYMENT_TPC_TOPIC,
              f"{txn['txn_id']}:tpc:payment:rollback", "payment.rollback",
              {"txn_id": txn["txn_id"]})
    else:
        # Stock not yet voted — record and wait
        advance_txn(conn, txn["txn_id"], TpcStatus.PREPARING, payment_vote="YES")
        conn.commit()
        logger.info("TPC txn=%s payment YES, awaiting stock vote", txn["txn_id"])


def _on_stock_rolledback(conn, txn: dict, payload: dict) -> None:
    """Stock undo-log cleared — transaction fully aborted."""
    advance_txn(conn, txn["txn_id"], TpcStatus.ABORTED)
    conn.commit()
    logger.info("TPC txn=%s aborted after stock rollback", txn["txn_id"])
    _respond(txn["original_correlation_id"], 400,
             {"error": txn.get("failure_reason") or "Checkout failed"})


def _on_payment_rolledback(conn, txn: dict, payload: dict) -> None:
    """Payment undo-log cleared — transaction fully aborted."""
    advance_txn(conn, txn["txn_id"], TpcStatus.ABORTED)
    conn.commit()
    logger.info("TPC txn=%s aborted after payment rollback", txn["txn_id"])
    _respond(txn["original_correlation_id"], 400,
             {"error": txn.get("failure_reason") or "Checkout failed"})


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _handle_no_vote(conn, txn: dict, who_voted_no: str, reason: str) -> None:
    """
    One participant voted NO.  If the other already voted YES, send it a
    rollback.  If the other also voted NO (or hasn't voted yet), go straight
    to ABORTED — there is nothing to undo.
    """
    other_vote = txn["payment_vote"] if who_voted_no == "stock" else txn["stock_vote"]

    if other_vote == "YES":
        # Other participant already committed — must roll it back
        advance_txn(conn, txn["txn_id"], TpcStatus.ABORTING,
                    **{f"{who_voted_no}_vote": "NO"})
        conn.commit()
        if who_voted_no == "stock":
            # Payment voted YES, stock voted NO — roll back payment
            _send(_PAYMENT_TPC_TOPIC,
                  f"{txn['txn_id']}:tpc:payment:rollback", "payment.rollback",
                  {"txn_id": txn["txn_id"]})
        else:
            # Stock voted YES, payment voted NO — roll back stock
            _send(_STOCK_TPC_TOPIC,
                  f"{txn['txn_id']}:tpc:stock:rollback", "stock.rollback",
                  {"txn_id": txn["txn_id"]})
        logger.info("TPC txn=%s %s NO, rolling back %s",
                    txn["txn_id"], who_voted_no,
                    "payment" if who_voted_no == "stock" else "stock")
    else:
        # Other hasn't voted YES yet — nothing to undo, go straight to ABORTED
        advance_txn(conn, txn["txn_id"], TpcStatus.ABORTED,
                    **{f"{who_voted_no}_vote": "NO"})
        conn.commit()
        logger.info("TPC txn=%s aborted (%s voted NO, other=%s)",
                    txn["txn_id"], who_voted_no, other_vote)
        _respond(txn["original_correlation_id"], 400, {"error": reason})


def _commit(conn, txn: dict) -> None:
    """
    Atomic commit point — mark order paid and advance to COMMITTED in one
    transaction.  Once this commits the checkout is permanently successful.
    Participant commit messages are fire-and-forget and retried by recovery.
    """
    with conn.cursor() as cur:
        mark_order_paid(conn, txn["order_id"])
        save_idempotency(cur, txn["idempotency_key"], 200, "Checkout successful")
        advance_txn(conn, txn["txn_id"], TpcStatus.COMMITTED)
    conn.commit()

    _send(_STOCK_TPC_TOPIC,
          f"{txn['txn_id']}:tpc:stock:commit", "stock.commit",
          {"txn_id": txn["txn_id"]})
    _send(_PAYMENT_TPC_TOPIC,
          f"{txn['txn_id']}:tpc:payment:commit", "payment.commit",
          {"txn_id": txn["txn_id"]})

    logger.info("TPC txn=%s committed, order=%s paid",
                txn["txn_id"], txn["order_id"])
    _respond(txn["original_correlation_id"], 200, "Checkout successful")


def _extract_reason(payload: dict) -> str:
    body = payload.get("body") or {}
    if isinstance(body, dict):
        return body.get("reason") or body.get("error") or "Unknown failure"
    return str(body) if body else "Unknown failure"