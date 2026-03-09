CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    paid BOOLEAN NOT NULL DEFAULT FALSE,
    items JSONB NOT NULL DEFAULT '[]',
    user_id TEXT NOT NULL,
    total_cost INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    status_code INTEGER NOT NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transaction_log (
    txn_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    status TEXT NOT NULL,
    prepared_stock JSONB NOT NULL DEFAULT '[]',
    prepared_payment BOOLEAN NOT NULL DEFAULT FALSE,
    user_id TEXT NOT NULL,
    total_cost INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sagas (
    id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    state TEXT NOT NULL,
    items_quantities JSONB NOT NULL,
    original_correlation_id TEXT NOT NULL,
    idempotency_key TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS saga_events (
    id SERIAL PRIMARY KEY,
    saga_id TEXT NOT NULL REFERENCES sagas(id),
    event TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
