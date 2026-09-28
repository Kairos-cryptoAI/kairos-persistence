-- The proposal contract gained a directionless, research-only move alert.
-- Preserve the published 020 migration and widen only its SIM action check.
ALTER TABLE sim_llm_trade_proposals
    DROP CONSTRAINT sim_llm_trade_proposals_action_check;
ALTER TABLE sim_llm_trade_proposals
    ADD CONSTRAINT sim_llm_trade_proposals_action_check
    CHECK (action IN ('LONG_BIAS', 'SHORT_BIAS', 'VOLATILITY_ALERT', 'NO_PROPOSAL', 'DEFER'));

CREATE TABLE sim_research_decision_samples (
    sample_record_id TEXT PRIMARY KEY CHECK (sample_record_id ~ '^[0-9a-f]{64}$'),
    campaign_id TEXT NOT NULL,
    arm_id TEXT NOT NULL,
    sample_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    market_as_of_ts_ms BIGINT NOT NULL CHECK (market_as_of_ts_ms >= 0),
    paired_at_ts_ms BIGINT NOT NULL CHECK (paired_at_ts_ms >= market_as_of_ts_ms),
    sample_deadline_ts_ms BIGINT NOT NULL CHECK (sample_deadline_ts_ms > paired_at_ts_ms),
    market_snapshot_sha256 TEXT NOT NULL CHECK (market_snapshot_sha256 ~ '^[0-9a-f]{64}$'),
    strategy_id TEXT NOT NULL,
    strategy_revision TEXT NOT NULL,
    strategy_outcome TEXT NOT NULL CHECK (
        strategy_outcome IN ('LONG', 'SHORT', 'NO_INTENT', 'NOT_EVALUATED')
    ),
    strategy_evaluation_sha256 TEXT CHECK (
        strategy_evaluation_sha256 IS NULL OR strategy_evaluation_sha256 ~ '^[0-9a-f]{64}$'
    ),
    strategy_evidence_as_of_ts_ms BIGINT CHECK (
        strategy_evidence_as_of_ts_ms IS NULL OR
        strategy_evidence_as_of_ts_ms BETWEEN 0 AND market_as_of_ts_ms
    ),
    strategy_market_snapshot_sha256 TEXT CHECK (
        strategy_market_snapshot_sha256 IS NULL OR strategy_market_snapshot_sha256 ~ '^[0-9a-f]{64}$'
    ),
    strategy_intent_id TEXT CHECK (
        strategy_intent_id IS NULL OR strategy_intent_id ~ '^[0-9a-f]{64}$'
    ),
    strategy_intent_expires_at_ts_ms BIGINT CHECK (
        strategy_intent_expires_at_ts_ms IS NULL OR strategy_intent_expires_at_ts_ms > paired_at_ts_ms
    ),
    llm_outcome TEXT NOT NULL CHECK (
        llm_outcome IN (
            'LONG_BIAS', 'SHORT_BIAS', 'VOLATILITY_ALERT', 'NO_PROPOSAL',
            'DEFER', 'NOT_CALLED', 'CALL_FAILED'
        )
    ),
    llm_evidence_as_of_ts_ms BIGINT CHECK (
        llm_evidence_as_of_ts_ms IS NULL OR
        llm_evidence_as_of_ts_ms BETWEEN 0 AND market_as_of_ts_ms
    ),
    llm_market_snapshot_sha256 TEXT CHECK (
        llm_market_snapshot_sha256 IS NULL OR llm_market_snapshot_sha256 ~ '^[0-9a-f]{64}$'
    ),
    llm_proposal_id TEXT CHECK (llm_proposal_id IS NULL OR llm_proposal_id ~ '^[0-9a-f]{64}$'),
    llm_proposal_expires_at_ts_ms BIGINT CHECK (
        llm_proposal_expires_at_ts_ms IS NULL OR llm_proposal_expires_at_ts_ms > paired_at_ts_ms
    ),
    llm_completion_receipt_id TEXT CHECK (
        llm_completion_receipt_id IS NULL OR llm_completion_receipt_id ~ '^[0-9a-f]{64}$'
    ),
    llm_completion_started_at_ts_ms BIGINT CHECK (
        llm_completion_started_at_ts_ms IS NULL OR
        llm_completion_started_at_ts_ms BETWEEN market_as_of_ts_ms AND paired_at_ts_ms
    ),
    llm_completion_observed_at_ts_ms BIGINT CHECK (
        llm_completion_observed_at_ts_ms IS NULL OR
        llm_completion_observed_at_ts_ms BETWEEN market_as_of_ts_ms AND paired_at_ts_ms
    ),
    llm_failure_receipt_id TEXT CHECK (
        llm_failure_receipt_id IS NULL OR llm_failure_receipt_id ~ '^[0-9a-f]{64}$'
    ),
    llm_failure_class TEXT CHECK (
        llm_failure_class IS NULL OR llm_failure_class IN (
            'TIMEOUT', 'PROVIDER_ERROR', 'PROVIDER_RATE_LIMITED', 'PROVIDER_QUOTA_EXHAUSTED',
            'TRANSPORT_ERROR', 'INVALID_RESPONSE', 'CANCELLED'
        )
    ),
    llm_failure_started_at_ts_ms BIGINT CHECK (
        llm_failure_started_at_ts_ms IS NULL OR
        llm_failure_started_at_ts_ms BETWEEN market_as_of_ts_ms AND paired_at_ts_ms
    ),
    llm_failure_observed_at_ts_ms BIGINT CHECK (
        llm_failure_observed_at_ts_ms IS NULL OR
        llm_failure_observed_at_ts_ms BETWEEN market_as_of_ts_ms AND paired_at_ts_ms
    ),
    authority TEXT NOT NULL CHECK (authority = 'SIM_RESEARCH_ONLY'),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT sim_research_decision_samples_campaign_arm_sample_key
        UNIQUE (campaign_id, arm_id, sample_id),
    CONSTRAINT sim_research_decision_samples_strategy_evidence_check CHECK (
        (strategy_outcome = 'NOT_EVALUATED' AND strategy_evaluation_sha256 IS NULL
            AND strategy_evidence_as_of_ts_ms IS NULL
            AND strategy_market_snapshot_sha256 IS NULL AND strategy_intent_id IS NULL
            AND strategy_intent_expires_at_ts_ms IS NULL)
        OR
        (strategy_outcome <> 'NOT_EVALUATED' AND strategy_evaluation_sha256 IS NOT NULL
            AND strategy_evidence_as_of_ts_ms IS NOT NULL
            AND strategy_market_snapshot_sha256 IS NOT NULL
            AND strategy_market_snapshot_sha256 = market_snapshot_sha256
            AND (strategy_intent_id IS NOT NULL) = (strategy_outcome IN ('LONG', 'SHORT'))
            AND (strategy_intent_expires_at_ts_ms IS NOT NULL) = (strategy_outcome IN ('LONG', 'SHORT')))
    ),
    CONSTRAINT sim_research_decision_samples_llm_evidence_check CHECK (
        (llm_outcome = 'NOT_CALLED' AND llm_evidence_as_of_ts_ms IS NULL
            AND llm_market_snapshot_sha256 IS NULL AND llm_proposal_id IS NULL
            AND llm_proposal_expires_at_ts_ms IS NULL
            AND llm_completion_receipt_id IS NULL
            AND llm_completion_started_at_ts_ms IS NULL
            AND llm_completion_observed_at_ts_ms IS NULL
            AND llm_failure_receipt_id IS NULL AND llm_failure_class IS NULL
            AND llm_failure_started_at_ts_ms IS NULL AND llm_failure_observed_at_ts_ms IS NULL)
        OR
        (llm_outcome = 'CALL_FAILED' AND llm_evidence_as_of_ts_ms IS NOT NULL
            AND llm_evidence_as_of_ts_ms = market_as_of_ts_ms
            AND llm_market_snapshot_sha256 IS NOT NULL
            AND llm_market_snapshot_sha256 = market_snapshot_sha256
            AND llm_proposal_id IS NULL AND llm_proposal_expires_at_ts_ms IS NULL
            AND llm_completion_receipt_id IS NULL
            AND llm_completion_started_at_ts_ms IS NULL
            AND llm_completion_observed_at_ts_ms IS NULL
            AND llm_failure_receipt_id IS NOT NULL AND llm_failure_class IS NOT NULL
            AND llm_failure_started_at_ts_ms IS NOT NULL
            AND llm_failure_observed_at_ts_ms IS NOT NULL
            AND llm_failure_started_at_ts_ms <= llm_failure_observed_at_ts_ms)
        OR
        (llm_outcome NOT IN ('NOT_CALLED', 'CALL_FAILED') AND llm_evidence_as_of_ts_ms IS NOT NULL
            AND llm_market_snapshot_sha256 IS NOT NULL
            AND llm_market_snapshot_sha256 = market_snapshot_sha256
            AND llm_proposal_id IS NOT NULL
            AND llm_completion_receipt_id IS NOT NULL
            AND llm_completion_started_at_ts_ms IS NOT NULL
            AND llm_completion_observed_at_ts_ms IS NOT NULL
            AND llm_completion_started_at_ts_ms <= llm_completion_observed_at_ts_ms
            AND (llm_proposal_expires_at_ts_ms IS NOT NULL) =
                (llm_outcome IN ('LONG_BIAS', 'SHORT_BIAS', 'VOLATILITY_ALERT'))
            AND llm_failure_receipt_id IS NULL AND llm_failure_class IS NULL
            AND llm_failure_started_at_ts_ms IS NULL AND llm_failure_observed_at_ts_ms IS NULL)
    )
);

CREATE INDEX sim_research_decision_samples_campaign_arm_cursor_idx
    ON sim_research_decision_samples (campaign_id, arm_id, market_as_of_ts_ms, sample_id);

CREATE FUNCTION simulator_reject_research_decision_sample_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'sim_research_decision_samples is append-only';
END;
$$;

CREATE TRIGGER sim_research_decision_samples_no_update_delete
    BEFORE UPDATE OR DELETE ON sim_research_decision_samples
    FOR EACH ROW EXECUTE FUNCTION simulator_reject_research_decision_sample_mutation();

CREATE TRIGGER sim_research_decision_samples_no_truncate
    BEFORE TRUNCATE ON sim_research_decision_samples
    FOR EACH STATEMENT EXECUTE FUNCTION simulator_reject_research_decision_sample_mutation();

REVOKE UPDATE, DELETE, TRUNCATE ON TABLE sim_research_decision_samples FROM PUBLIC;
