-- Persist before a venue call, independently of the long-lived dispatch lock.
-- Unknown outcomes never permit another claim; reconciliation is read-only.
CREATE TABLE paper_canary_dispatch_claims (
    effect_id TEXT PRIMARY KEY CHECK (btrim(effect_id) <> ''),
    attempt_id TEXT NOT NULL UNIQUE REFERENCES paper_canary_attempts(attempt_id),
    session_id TEXT NOT NULL REFERENCES paper_canary_sessions(session_id),
    risk_decision_id TEXT NOT NULL CHECK (risk_decision_id ~ '^[0-9a-f]{64}$'),
    trade_id TEXT NOT NULL CHECK (trade_id ~ '^[0-9a-f]{64}$'),
    scope_sha256 TEXT NOT NULL CHECK (scope_sha256 ~ '^[0-9a-f]{64}$'),
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
