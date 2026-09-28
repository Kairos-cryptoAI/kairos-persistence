-- SIM-only forward migration. Existing schedules/results/seals remain intact
-- and are grandfathered as protocol-less evidence. New scheduled campaigns
-- must commit one exact adaptive roster before results and coverage seals.
-- Acquire locks in the same stable order as the schedule -> window -> result
-- -> seal evidence graph while replacing the guards transactionally.
LOCK TABLE sim_research_observation_schedules,
           sim_research_observation_windows,
           sim_research_decision_samples,
           sim_research_coverage_seals
    IN SHARE ROW EXCLUSIVE MODE;

-- Existing rows receive FALSE. New schedules inherit TRUE so a protocol can
-- be preregistered only for campaigns created under this migration.
ALTER TABLE sim_research_observation_schedules
    ADD COLUMN adaptive_protocol_registration_allowed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE sim_research_observation_schedules
    ALTER COLUMN adaptive_protocol_registration_allowed SET DEFAULT TRUE;

ALTER TABLE sim_research_decision_samples
    ADD COLUMN arm_protocol_digest TEXT CHECK (
        arm_protocol_digest IS NULL OR arm_protocol_digest ~ '^[0-9a-f]{64}$'
    );

ALTER TABLE sim_research_coverage_seals
    ADD COLUMN candidate_protocol_digest TEXT CHECK (
        candidate_protocol_digest IS NULL OR candidate_protocol_digest ~ '^[0-9a-f]{64}$'
    );

CREATE TABLE sim_research_adaptive_candidate_protocols (
    campaign_id TEXT PRIMARY KEY REFERENCES sim_research_observation_schedules(campaign_id),
    schedule_digest TEXT NOT NULL CHECK (schedule_digest ~ '^[0-9a-f]{64}$'),
    protocol_digest TEXT NOT NULL UNIQUE CHECK (protocol_digest ~ '^[0-9a-f]{64}$'),
    arm_digests JSONB NOT NULL CHECK (
        jsonb_typeof(arm_digests) = 'object'
        AND arm_digests ?& ARRAY['strategy-only', 'strategy-review', 'llm-proposal-research']
        AND (arm_digests - ARRAY['strategy-only', 'strategy-review', 'llm-proposal-research']) = '{}'::jsonb
        AND arm_digests->>'strategy-only' IS NOT NULL
        AND arm_digests->>'strategy-review' IS NOT NULL
        AND arm_digests->>'llm-proposal-research' IS NOT NULL
        AND arm_digests->>'strategy-only' ~ '^[0-9a-f]{64}$'
        AND arm_digests->>'strategy-review' ~ '^[0-9a-f]{64}$'
        AND arm_digests->>'llm-proposal-research' ~ '^[0-9a-f]{64}$'
    ),
    authority TEXT NOT NULL CHECK (authority = 'SIM_RESEARCH_ONLY'),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    freeze_txid BIGINT NOT NULL DEFAULT txid_current(),
    frozen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT sim_research_adaptive_protocol_schedule_campaign_key
        UNIQUE (campaign_id, schedule_digest)
);

CREATE FUNCTION simulator_guard_adaptive_candidate_protocol_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    roster sim_research_observation_schedules%ROWTYPE;
BEGIN
    PERFORM pg_advisory_xact_lock(849621, hashtext(NEW.campaign_id));
    SELECT * INTO roster FROM sim_research_observation_schedules
        WHERE campaign_id = NEW.campaign_id FOR SHARE;
    IF NOT FOUND OR roster.schedule_digest <> NEW.schedule_digest
       OR NOT roster.adaptive_protocol_registration_allowed
       OR roster.freeze_txid = txid_current() THEN
        RAISE EXCEPTION 'adaptive candidate protocol requires an existing eligible committed SIM schedule';
    END IF;
    IF EXISTS (SELECT 1 FROM sim_research_decision_samples WHERE campaign_id = NEW.campaign_id)
       OR EXISTS (SELECT 1 FROM sim_research_coverage_seals WHERE campaign_id = NEW.campaign_id) THEN
        RAISE EXCEPTION 'adaptive candidate protocol cannot be registered after results or a coverage seal';
    END IF;
    IF NEW.payload->>'campaign_id' IS DISTINCT FROM NEW.campaign_id
       OR NEW.payload->>'schedule_digest' IS DISTINCT FROM NEW.schedule_digest
       OR NEW.payload->>'protocol_digest' IS DISTINCT FROM NEW.protocol_digest
       OR NEW.payload->>'authority' IS DISTINCT FROM 'SIM_RESEARCH_ONLY'
       OR NEW.payload->>'contract_version' IS DISTINCT FROM 'adaptive-candidate-protocol.v1'
       OR NEW.payload->>'source' IS DISTINCT FROM 'kairos-adaptive-candidate-protocol'
       OR jsonb_typeof(NEW.payload->'arms') IS DISTINCT FROM 'array'
       OR (CASE WHEN jsonb_typeof(NEW.payload->'arms') = 'array'
                THEN jsonb_array_length(NEW.payload->'arms') ELSE -1 END) <> 3
       OR jsonb_typeof(NEW.payload->'arms'->0) IS DISTINCT FROM 'object'
       OR jsonb_typeof(NEW.payload->'arms'->1) IS DISTINCT FROM 'object'
       OR jsonb_typeof(NEW.payload->'arms'->2) IS DISTINCT FROM 'object'
       OR NEW.payload->'arms'->0->>'arm_id' IS DISTINCT FROM 'strategy-only'
       OR NEW.payload->'arms'->1->>'arm_id' IS DISTINCT FROM 'strategy-review'
       OR NEW.payload->'arms'->2->>'arm_id' IS DISTINCT FROM 'llm-proposal-research'
       OR (CASE WHEN jsonb_typeof(NEW.payload->'arms'->0) = 'object'
                THEN (NEW.payload->'arms'->0) - ARRAY[
                    'arm_id', 'candidate_id', 'candidate_revision', 'artifact_sha256',
                    'input_feature_sha256', 'decision_mapping_sha256',
                    'hypothetical_exit_sha256', 'cost_model_sha256'
                ]::TEXT[]
                ELSE '{"invalid_arm": true}'::jsonb END) <> '{}'::jsonb
       OR NOT (NEW.payload->'arms'->0 ?& ARRAY[
           'arm_id', 'candidate_id', 'candidate_revision', 'artifact_sha256', 'input_feature_sha256',
           'decision_mapping_sha256', 'hypothetical_exit_sha256', 'cost_model_sha256'
       ])
       OR (CASE WHEN jsonb_typeof(NEW.payload->'arms'->1) = 'object'
                THEN (NEW.payload->'arms'->1) - ARRAY[
                    'arm_id', 'candidate_id', 'candidate_revision', 'artifact_sha256',
                    'input_feature_sha256', 'decision_mapping_sha256',
                    'hypothetical_exit_sha256', 'cost_model_sha256', 'provider', 'model',
                    'prompt_sha256', 'schema_sha256'
                ]::TEXT[]
                ELSE '{"invalid_arm": true}'::jsonb END) <> '{}'::jsonb
       OR NOT (NEW.payload->'arms'->1 ?& ARRAY[
           'arm_id', 'candidate_id', 'candidate_revision', 'artifact_sha256', 'input_feature_sha256',
           'decision_mapping_sha256', 'hypothetical_exit_sha256', 'cost_model_sha256',
           'provider', 'model', 'prompt_sha256', 'schema_sha256'
       ])
       OR (CASE WHEN jsonb_typeof(NEW.payload->'arms'->2) = 'object'
                THEN (NEW.payload->'arms'->2) - ARRAY[
                    'arm_id', 'candidate_id', 'candidate_revision', 'artifact_sha256',
                    'input_feature_sha256', 'decision_mapping_sha256',
                    'hypothetical_exit_sha256', 'cost_model_sha256', 'provider', 'model',
                    'prompt_sha256', 'schema_sha256'
                ]::TEXT[]
                ELSE '{"invalid_arm": true}'::jsonb END) <> '{}'::jsonb
       OR NOT (NEW.payload->'arms'->2 ?& ARRAY[
           'arm_id', 'candidate_id', 'candidate_revision', 'artifact_sha256', 'input_feature_sha256',
           'decision_mapping_sha256', 'hypothetical_exit_sha256', 'cost_model_sha256',
           'provider', 'model', 'prompt_sha256', 'schema_sha256'
       ])
       OR NEW.payload->'arms'->1->>'provider' IS NULL
       OR NEW.payload->'arms'->1->>'provider' NOT IN ('openai', 'deepseek')
       OR NEW.payload->'arms'->2->>'provider' IS NULL
       OR NEW.payload->'arms'->2->>'provider' NOT IN ('openai', 'deepseek')
       OR NEW.payload->'arms'->1->>'model' IS NULL
       OR NEW.payload->'arms'->2->>'model' IS NULL
       OR NEW.payload->'arms'->1->>'prompt_sha256' IS NULL
       OR NEW.payload->'arms'->2->>'prompt_sha256' IS NULL
       OR NEW.payload->'arms'->1->>'schema_sha256' IS NULL
       OR NEW.payload->'arms'->2->>'schema_sha256' IS NULL THEN
        RAISE EXCEPTION 'adaptive candidate protocol payload does not match the fixed three-arm contract';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER sim_research_adaptive_protocol_insert_guard
    BEFORE INSERT ON sim_research_adaptive_candidate_protocols
    FOR EACH ROW EXECUTE FUNCTION simulator_guard_adaptive_candidate_protocol_insert();

CREATE FUNCTION simulator_reject_adaptive_candidate_protocol_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'SIM adaptive candidate protocols are append-only';
END;
$$;

CREATE TRIGGER sim_research_adaptive_protocol_no_update_delete
    BEFORE UPDATE OR DELETE ON sim_research_adaptive_candidate_protocols
    FOR EACH ROW EXECUTE FUNCTION simulator_reject_adaptive_candidate_protocol_mutation();
CREATE TRIGGER sim_research_adaptive_protocol_no_truncate
    BEFORE TRUNCATE ON sim_research_adaptive_candidate_protocols
    FOR EACH STATEMENT EXECUTE FUNCTION simulator_reject_adaptive_candidate_protocol_mutation();
REVOKE UPDATE, DELETE, TRUNCATE ON TABLE sim_research_adaptive_candidate_protocols FROM PUBLIC;

CREATE OR REPLACE FUNCTION simulator_guard_scheduled_research_sample()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    roster sim_research_observation_schedules%ROWTYPE;
    window_row sim_research_observation_windows%ROWTYPE;
    protocol_row sim_research_adaptive_candidate_protocols%ROWTYPE;
BEGIN
    PERFORM pg_advisory_xact_lock(849621, hashtext(NEW.campaign_id));
    SELECT * INTO roster FROM sim_research_observation_schedules
        WHERE campaign_id = NEW.campaign_id;
    IF NOT FOUND THEN
        -- Preserve legacy unscheduled SIM fixtures only.
        RETURN NEW;
    END IF;
    IF EXISTS (SELECT 1 FROM sim_research_coverage_seals WHERE campaign_id = NEW.campaign_id) THEN
        RAISE EXCEPTION 'sealed SIM research campaign rejects further results';
    END IF;
    IF NEW.arm_id NOT IN ('strategy-only', 'strategy-review', 'llm-proposal-research') THEN
        RAISE EXCEPTION 'SIM research result is outside the frozen arm set';
    END IF;
    SELECT * INTO protocol_row FROM sim_research_adaptive_candidate_protocols
        WHERE campaign_id = NEW.campaign_id;
    IF NOT FOUND OR protocol_row.freeze_txid = txid_current()
       OR protocol_row.schedule_digest <> roster.schedule_digest THEN
        RAISE EXCEPTION 'scheduled SIM research result requires a previously committed exact adaptive protocol';
    END IF;
    IF NEW.arm_protocol_digest IS NULL
       OR NEW.arm_protocol_digest IS DISTINCT FROM protocol_row.arm_digests->>NEW.arm_id THEN
        RAISE EXCEPTION 'SIM research result arm digest differs from the frozen adaptive protocol';
    END IF;
    SELECT * INTO window_row FROM sim_research_observation_windows
        WHERE campaign_id = NEW.campaign_id AND sample_id = NEW.sample_id;
    IF NOT FOUND OR NEW.symbol <> window_row.symbol OR NEW.timeframe <> window_row.timeframe
        OR NEW.market_as_of_ts_ms <> window_row.market_as_of_ts_ms
        OR (window_row.market_snapshot_sha256 IS NOT NULL AND
            NEW.market_snapshot_sha256 <> window_row.market_snapshot_sha256)
        OR NEW.paired_at_ts_ms <> window_row.paired_at_ts_ms
        OR NEW.sample_deadline_ts_ms <> window_row.sample_deadline_ts_ms THEN
        RAISE EXCEPTION 'SIM research result differs from the frozen window';
    END IF;
    IF NEW.strategy_id <> roster.strategy_id OR NEW.strategy_revision <> roster.strategy_revision THEN
        RAISE EXCEPTION 'SIM research result differs from the frozen strategy';
    END IF;
    IF NEW.arm_id = 'strategy-only' AND NEW.llm_outcome <> 'NOT_CALLED' THEN
        RAISE EXCEPTION 'strategy-only SIM research arm cannot contain an LLM result';
    END IF;
    IF EXISTS (
        SELECT 1 FROM sim_research_decision_samples prior
        WHERE prior.campaign_id = NEW.campaign_id AND prior.sample_id = NEW.sample_id
          AND (prior.market_snapshot_sha256 IS DISTINCT FROM NEW.market_snapshot_sha256
               OR prior.strategy_outcome IS DISTINCT FROM NEW.strategy_outcome
               OR prior.strategy_evaluation_sha256 IS DISTINCT FROM NEW.strategy_evaluation_sha256
               OR prior.strategy_evidence_as_of_ts_ms IS DISTINCT FROM NEW.strategy_evidence_as_of_ts_ms
               OR prior.strategy_market_snapshot_sha256 IS DISTINCT FROM NEW.strategy_market_snapshot_sha256
               OR prior.strategy_intent_id IS DISTINCT FROM NEW.strategy_intent_id
               OR prior.strategy_intent_expires_at_ts_ms IS DISTINCT FROM NEW.strategy_intent_expires_at_ts_ms)
    ) THEN
        RAISE EXCEPTION 'matched SIM research arms disagree on snapshot or baseline strategy lineage';
    END IF;
    NEW.recorded_at := clock_timestamp();
    IF NEW.recorded_at <= roster.frozen_at THEN
        RAISE EXCEPTION 'SIM research result predates its frozen schedule';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION simulator_guard_adaptive_coverage_seal_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    roster sim_research_observation_schedules%ROWTYPE;
    protocol_row sim_research_adaptive_candidate_protocols%ROWTYPE;
    observed_count BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(849621, hashtext(NEW.campaign_id));
    SELECT * INTO roster FROM sim_research_observation_schedules
        WHERE campaign_id = NEW.campaign_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'SIM coverage seal requires a preregistered schedule';
    END IF;
    SELECT * INTO protocol_row FROM sim_research_adaptive_candidate_protocols
        WHERE campaign_id = NEW.campaign_id;
    IF NOT FOUND OR protocol_row.freeze_txid = txid_current()
       OR protocol_row.schedule_digest <> roster.schedule_digest
       OR NEW.candidate_protocol_digest IS NULL
       OR NEW.candidate_protocol_digest IS DISTINCT FROM protocol_row.protocol_digest
       OR NEW.schedule_digest IS DISTINCT FROM roster.schedule_digest THEN
        RAISE EXCEPTION 'new scheduled SIM coverage seal requires a previously committed exact adaptive protocol';
    END IF;
    SELECT count(*) INTO observed_count FROM sim_research_decision_samples
        WHERE campaign_id = NEW.campaign_id;
    IF NEW.expected_result_count <> roster.window_count * 3
       OR observed_count <> NEW.expected_result_count THEN
        RAISE EXCEPTION 'SIM coverage seal does not represent the exact scheduled result count';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER sim_research_adaptive_coverage_seal_guard
    BEFORE INSERT ON sim_research_coverage_seals
    FOR EACH ROW EXECUTE FUNCTION simulator_guard_adaptive_coverage_seal_insert();
