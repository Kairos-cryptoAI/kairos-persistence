-- Operational evidence only: no change to public/frozen trading contracts.
CREATE TABLE paper_canary_database_identity (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    instance_id UUID NOT NULL DEFAULT gen_random_uuid()
);
INSERT INTO paper_canary_database_identity(singleton) VALUES(TRUE);

CREATE TABLE paper_readonly_runs (
    run_id TEXT PRIMARY KEY CHECK (run_id ~ '^[0-9a-f]{64}$'),
    scope JSONB NOT NULL,
    scope_sha256 TEXT NOT NULL CHECK (scope_sha256 ~ '^[0-9a-f]{64}$'),
    database_instance_id UUID NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    sample_period_ms INTEGER NOT NULL CHECK (sample_period_ms BETWEEN 5000 AND 60000),
    sample_count INTEGER NOT NULL DEFAULT 0 CHECK (sample_count >= 0),
    head_sha256 TEXT NOT NULL CHECK (head_sha256 ~ '^[0-9a-f]{64}$'),
    state TEXT NOT NULL DEFAULT 'RECORDING' CHECK (state IN ('RECORDING','CERTIFIED'))
);
CREATE TABLE paper_readonly_samples (
    run_id TEXT NOT NULL REFERENCES paper_readonly_runs(run_id),
    seq INTEGER NOT NULL CHECK (seq > 0),
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    payload JSONB NOT NULL,
    previous_sha256 TEXT NOT NULL CHECK (previous_sha256 ~ '^[0-9a-f]{64}$'),
    sample_sha256 TEXT NOT NULL CHECK (sample_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY(run_id,seq)
);
CREATE TABLE paper_readonly_receipts (
    receipt_id TEXT PRIMARY KEY CHECK (receipt_id ~ '^[0-9a-f]{64}$'),
    run_id TEXT NOT NULL UNIQUE REFERENCES paper_readonly_runs(run_id),
    payload JSONB NOT NULL,
    certified_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE paper_canary_sessions (
    session_id TEXT PRIMARY KEY CHECK (session_id ~ '^[0-9a-f]{64}$'),
    receipt_id TEXT NOT NULL UNIQUE REFERENCES paper_readonly_receipts(receipt_id),
    scope JSONB NOT NULL,
    scope_sha256 TEXT NOT NULL CHECK (scope_sha256 ~ '^[0-9a-f]{64}$'),
    remote_account_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    plan JSONB NOT NULL,
    plan_sha256 TEXT NOT NULL CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
    operator_nonce TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ARMED','RUNNING','DRAINING','INCOMPLETE','ABORTED')),
    armed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    entry_deadline_at TIMESTAMPTZ NOT NULL,
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
    attempts_reserved INTEGER NOT NULL DEFAULT 0 CHECK (attempts_reserved BETWEEN 0 AND 10),
    last_progress_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    stop_reason TEXT,
    CHECK (attempts_reserved <= max_attempts),
    CHECK (entry_deadline_at > armed_at AND entry_deadline_at <= armed_at + interval '2 hours')
);
CREATE UNIQUE INDEX paper_canary_one_active_remote_account
    ON paper_canary_sessions(remote_account_id) WHERE state IN ('ARMED','RUNNING','DRAINING');
CREATE TABLE paper_canary_attempts (
    attempt_id TEXT PRIMARY KEY CHECK (attempt_id ~ '^[0-9a-f]{64}$'),
    session_id TEXT NOT NULL REFERENCES paper_canary_sessions(session_id),
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 1 AND 10),
    slot_id TEXT NOT NULL,
    arm_id TEXT NOT NULL UNIQUE REFERENCES paper_canary_arms(arm_id),
    review_id TEXT NOT NULL UNIQUE CHECK (review_id ~ '^[0-9a-f]{64}$'),
    intent_id TEXT NOT NULL CHECK (intent_id ~ '^[0-9a-f]{64}$'),
    symbol TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ARMED','CONSUMED','TERMINAL','BLOCKED')),
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    terminal_at TIMESTAMPTZ,
    terminal_reason TEXT,
    risk_decision_id TEXT,
    trade_id TEXT,
    entry_effect_id TEXT,
    UNIQUE(session_id,ordinal),
    UNIQUE(session_id,slot_id)
);
CREATE UNIQUE INDEX paper_canary_one_outstanding_attempt
    ON paper_canary_attempts(session_id) WHERE state IN ('ARMED','CONSUMED','BLOCKED');
