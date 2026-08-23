CREATE TABLE IF NOT EXISTS paper_canary_arms (
    arm_id TEXT PRIMARY KEY CHECK (arm_id ~ '^[0-9a-f]{64}$'),
    review_id TEXT NOT NULL UNIQUE CHECK (review_id ~ '^[0-9a-f]{64}$'),
    intent_id TEXT NOT NULL CHECK (intent_id ~ '^[0-9a-f]{64}$'),
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('LONG','SHORT')),
    review_payload JSONB NOT NULL,
    review_sha256 TEXT NOT NULL CHECK (review_sha256 ~ '^[0-9a-f]{64}$'),
    allocation_payload JSONB NOT NULL,
    allocation_sha256 TEXT NOT NULL CHECK (allocation_sha256 ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL CHECK (status IN ('ARMED','CONSUMED','EXPIRED')),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    consumed_at TIMESTAMPTZ,
    decided_at_ms BIGINT CHECK (decided_at_ms IS NULL OR decided_at_ms >= 0),
    CHECK ((status='CONSUMED') = (consumed_at IS NOT NULL)),
    CHECK ((status='CONSUMED') = (decided_at_ms IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS paper_canary_one_armed_session_idx
    ON paper_canary_arms(account_id) WHERE status='ARMED';

CREATE INDEX IF NOT EXISTS paper_canary_arms_status_expiry_idx
    ON paper_canary_arms(account_id, status, expires_at);
