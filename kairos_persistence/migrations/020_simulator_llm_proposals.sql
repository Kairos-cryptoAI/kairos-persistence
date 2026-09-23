CREATE TABLE sim_llm_trade_proposals (
    proposal_id TEXT PRIMARY KEY CHECK (proposal_id ~ '^[0-9a-f]{64}$'),
    campaign_id TEXT NOT NULL,
    arm_id TEXT NOT NULL,
    sample_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    market_as_of_ts_ms BIGINT NOT NULL CHECK (market_as_of_ts_ms >= 0),
    expires_at_ts_ms BIGINT NOT NULL CHECK (expires_at_ts_ms >= market_as_of_ts_ms),
    market_snapshot_sha256 TEXT NOT NULL CHECK (market_snapshot_sha256 ~ '^[0-9a-f]{64}$'),
    action TEXT NOT NULL CHECK (action IN ('LONG_BIAS', 'SHORT_BIAS', 'NO_PROPOSAL', 'DEFER')),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT sim_llm_trade_proposals_campaign_arm_sample_key
        UNIQUE (campaign_id, arm_id, sample_id)
);

CREATE INDEX sim_llm_trade_proposals_campaign_arm_cursor_idx
    ON sim_llm_trade_proposals (campaign_id, arm_id, market_as_of_ts_ms, sample_id);

CREATE FUNCTION simulator_reject_llm_proposal_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'sim_llm_trade_proposals is append-only';
END;
$$;

CREATE TRIGGER sim_llm_trade_proposals_no_update_delete
    BEFORE UPDATE OR DELETE ON sim_llm_trade_proposals
    FOR EACH ROW EXECUTE FUNCTION simulator_reject_llm_proposal_mutation();

CREATE TRIGGER sim_llm_trade_proposals_no_truncate
    BEFORE TRUNCATE ON sim_llm_trade_proposals
    FOR EACH STATEMENT EXECUTE FUNCTION simulator_reject_llm_proposal_mutation();

REVOKE UPDATE, DELETE, TRUNCATE ON TABLE sim_llm_trade_proposals FROM PUBLIC;
