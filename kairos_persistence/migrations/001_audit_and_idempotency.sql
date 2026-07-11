CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS event_audit (
    produced_at TIMESTAMPTZ NOT NULL,
    message_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    source TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    correlation_id TEXT,
    causation_id TEXT,
    payload JSONB NOT NULL,
    persisted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (produced_at, message_id)
);
SELECT create_hypertable('event_audit', 'produced_at', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS event_audit_message_id_idx ON event_audit(message_id);
CREATE INDEX IF NOT EXISTS event_audit_correlation_idx ON event_audit(correlation_id, produced_at DESC);
CREATE INDEX IF NOT EXISTS event_audit_topic_time_idx ON event_audit(topic, produced_at DESC);

CREATE TABLE IF NOT EXISTS message_inbox (
    consumer TEXT NOT NULL,
    message_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PROCESSING', 'COMPLETED', 'FAILED')),
    attempts INTEGER NOT NULL DEFAULT 1 CHECK (attempts > 0),
    lease_until TIMESTAMPTZ NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    result JSONB,
    error TEXT,
    PRIMARY KEY (consumer, message_id)
);
CREATE INDEX IF NOT EXISTS message_inbox_lease_idx ON message_inbox(status, lease_until);

CREATE TABLE IF NOT EXISTS message_outbox (
    id BIGSERIAL PRIMARY KEY,
    message_id TEXT NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE INDEX IF NOT EXISTS message_outbox_pending_idx
    ON message_outbox(created_at) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS execution_orders (
    client_order_id TEXT PRIMARY KEY,
    validated_order_id TEXT NOT NULL UNIQUE,
    correlation_id TEXT,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity NUMERIC NOT NULL CHECK (quantity > 0),
    price NUMERIC,
    reduce_only BOOLEAN NOT NULL DEFAULT FALSE,
    state TEXT NOT NULL,
    exchange_order_id TEXT,
    filled_quantity NUMERIC NOT NULL DEFAULT 0,
    average_fill_price NUMERIC,
    fees JSONB NOT NULL DEFAULT '{}'::jsonb,
    rejection_code TEXT,
    rejection_detail TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS execution_exchange_order_idx
    ON execution_orders(exchange, exchange_order_id) WHERE exchange_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS execution_state_idx ON execution_orders(state, updated_at);

CREATE TABLE IF NOT EXISTS account_snapshots (
    captured_at TIMESTAMPTZ NOT NULL,
    exchange TEXT NOT NULL,
    account_id TEXT NOT NULL,
    equity_usd NUMERIC NOT NULL,
    available_balance_usd NUMERIC NOT NULL,
    margin_used_usd NUMERIC NOT NULL,
    realized_pnl_usd NUMERIC NOT NULL DEFAULT 0,
    unrealized_pnl_usd NUMERIC NOT NULL DEFAULT 0,
    payload JSONB NOT NULL,
    PRIMARY KEY (captured_at, exchange, account_id)
);
SELECT create_hypertable('account_snapshots', 'captured_at', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS position_snapshots (
    captured_at TIMESTAMPTZ NOT NULL,
    exchange TEXT NOT NULL,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    signed_quantity NUMERIC NOT NULL,
    entry_price NUMERIC,
    mark_price NUMERIC,
    leverage NUMERIC,
    liquidation_price NUMERIC,
    protective_stop_order_id TEXT,
    payload JSONB NOT NULL,
    PRIMARY KEY (captured_at, exchange, account_id, symbol)
);
SELECT create_hypertable('position_snapshots', 'captured_at', if_not_exists => TRUE);

CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_event_counts
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', produced_at) AS bucket, topic, count(*) AS event_count
FROM event_audit
GROUP BY bucket, topic
WITH NO DATA;

SELECT add_retention_policy('event_audit', INTERVAL '365 days', if_not_exists => TRUE);
SELECT add_retention_policy('account_snapshots', INTERVAL '365 days', if_not_exists => TRUE);
SELECT add_retention_policy('position_snapshots', INTERVAL '365 days', if_not_exists => TRUE);
