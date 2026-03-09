CREATE TABLE IF NOT EXISTS items (
    id TEXT PRIMARY KEY,
    stock INTEGER NOT NULL DEFAULT 0,
    price INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    status_code INTEGER NOT NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS prepared_transactions (
    txn_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (txn_id, item_id) -- a transaction can have multiple items
);