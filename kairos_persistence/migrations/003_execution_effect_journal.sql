CREATE TABLE IF NOT EXISTS execution_effects (
    effect_key TEXT PRIMARY KEY,
    effect_type TEXT NOT NULL CHECK (
        effect_type IN ('PLACE_ORDER', 'CLOSE_POSITION', 'PROTECTIVE_STOP', 'CANCEL_ORDER', 'SET_LEVERAGE')
    ),
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    client_order_id TEXT,
    request_sha256 TEXT NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    request_payload JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PREPARED', 'CONFIRMED', 'RECONCILED', 'FAILED')),
    exchange_effect_id TEXT,
    response_payload JSONB,
    error TEXT,
    journal_head_sha256 TEXT NOT NULL CHECK (journal_head_sha256 ~ '^[0-9a-f]{64}$'),
    prepared_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_at TIMESTAMPTZ,
    reconciled_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS execution_effects_recovery_idx
    ON execution_effects(status, prepared_at)
    WHERE status IN ('PREPARED', 'FAILED');
CREATE INDEX IF NOT EXISTS execution_effects_client_order_idx
    ON execution_effects(exchange, client_order_id)
    WHERE client_order_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS execution_effect_events (
    sequence BIGSERIAL PRIMARY KEY,
    effect_key TEXT NOT NULL REFERENCES execution_effects(effect_key),
    phase TEXT NOT NULL CHECK (phase IN ('PREPARED', 'CONFIRMED', 'RECONCILED', 'FAILED')),
    event_payload JSONB NOT NULL,
    previous_event_sha256 TEXT,
    event_sha256 TEXT NOT NULL UNIQUE CHECK (event_sha256 ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (previous_event_sha256 IS NULL OR previous_event_sha256 ~ '^[0-9a-f]{64}$')
);
CREATE INDEX IF NOT EXISTS execution_effect_events_key_sequence_idx
    ON execution_effect_events(effect_key, sequence);

