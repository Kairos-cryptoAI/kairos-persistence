CREATE TABLE IF NOT EXISTS public_execution_events (
    trade_id TEXT NOT NULL REFERENCES execution_trades(trade_id),
    fact_key TEXT NOT NULL CHECK (length(fact_key) BETWEEN 1 AND 512),
    event_seq BIGINT NOT NULL CHECK (event_seq > 0),
    state_version BIGINT NOT NULL CHECK (state_version >= 0),
    event_id TEXT NOT NULL UNIQUE CHECK (event_id ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_id, fact_key),
    UNIQUE (trade_id, event_seq)
);

CREATE INDEX IF NOT EXISTS public_execution_events_scope_replay_idx
    ON public_execution_events(created_at, trade_id, event_seq);
