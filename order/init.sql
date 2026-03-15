-- Order service schema

CREATE TABLE IF NOT EXISTS orders (
    id         TEXT    PRIMARY KEY,
    paid       BOOLEAN NOT NULL DEFAULT FALSE,
    items      JSONB   NOT NULL DEFAULT '[]',
    user_id    TEXT    NOT NULL,
    total_cost INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key         TEXT      PRIMARY KEY,
    status_code INTEGER   NOT NULL,
    body        TEXT      NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 2PC coordinator log.
-- Both stock.prepare and payment.prepare are sent simultaneously.
-- stock_vote / payment_vote track individual participant responses.
--
-- Status flow:
--   PREPARING         both stock.prepare + payment.prepare sent, waiting for votes
--   COMMITTING        both voted YES, stock.commit + payment.commit sent
--   ABORTING_STOCK    payment voted NO (or stock voted NO after payment YES), stock.rollback sent
--   ABORTING_PAYMENT  stock voted NO after payment YES — payment.rollback sent
--   COMMITTED         terminal success
--   ABORTED           terminal failure
CREATE TABLE IF NOT EXISTS transaction_log (
    txn_id                  TEXT      PRIMARY KEY,
    order_id                TEXT      NOT NULL,
    status                  TEXT      NOT NULL,
    items                   JSONB     NOT NULL DEFAULT '[]',
    user_id                 TEXT      NOT NULL,
    total_cost              INTEGER   NOT NULL,
    original_correlation_id TEXT      NOT NULL,
    idempotency_key         TEXT,
    stock_vote              TEXT,     -- NULL | 'YES' | 'NO'
    payment_vote            TEXT,     -- NULL | 'YES' | 'NO'
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_txn_log_status
    ON transaction_log (status)
    WHERE status NOT IN ('COMMITTED', 'ABORTED');

-- SAGA orchestrator log — parallel stock + payment execution.
--
-- stock_ok / payment_ok track which participants have successfully executed.
-- failure_reason persists the error so recovery can return the right message.
--
-- Status flow:
--   AWAITING_BOTH                  both execute sent, waiting for both
--   AWAITING_PAYMENT               stock ok, waiting for payment
--   AWAITING_STOCK                 payment ok, waiting for stock
--   STOCK_FAILED_AWAITING_PAYMENT  stock failed, waiting for payment to know if rollback needed
--   PAYMENT_FAILED_AWAITING_STOCK  payment failed, waiting for stock to know if rollback needed
--   COMPLETING                     both ok, marking order paid
--   ROLLING_BACK_STOCK             payment failed after stock ok, stock.rollback sent
--   ROLLING_BACK_PAYMENT           stock failed after payment ok, payment.rollback sent
--   COMPLETED                      terminal success
--   FAILED                         terminal, both failed (nothing to compensate)
--   ROLLED_BACK                    terminal, compensation complete
--   ROLLBACK_FAILED                terminal, inconsistent — needs manual fix
CREATE TABLE IF NOT EXISTS sagas (
    id                      TEXT      PRIMARY KEY,
    order_id                TEXT      NOT NULL,
    state                   TEXT      NOT NULL,
    items_quantities        JSONB     NOT NULL,
    user_id                 TEXT      NOT NULL,
    total_cost              INTEGER   NOT NULL,
    original_correlation_id TEXT      NOT NULL,
    idempotency_key         TEXT,
    stock_ok                BOOLEAN   NOT NULL DEFAULT FALSE,
    payment_ok              BOOLEAN   NOT NULL DEFAULT FALSE,
    failure_reason          TEXT,
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sagas_state
    ON sagas (state)
    WHERE state NOT IN ('COMPLETED', 'FAILED', 'ROLLED_BACK', 'ROLLBACK_FAILED');