-- SIM-only forward migration: preserve the published 022 source and upgrade
-- its insert guard for exact matched baseline lineage. No existing result is
-- changed; a 022-only isolated SIM database advances through this migration.
-- Hold inserts until commit so no old-guard row can slip between validation
-- and the atomic replacement. Scan every scheduled campaign, sealed or not.
LOCK TABLE sim_research_decision_samples IN SHARE ROW EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM sim_research_decision_samples prior
        JOIN sim_research_decision_samples other
          ON other.campaign_id = prior.campaign_id
         AND other.sample_id = prior.sample_id
         AND other.arm_id > prior.arm_id
        JOIN sim_research_observation_schedules roster
          ON roster.campaign_id = prior.campaign_id
        WHERE prior.market_snapshot_sha256 IS DISTINCT FROM other.market_snapshot_sha256
           OR prior.strategy_outcome IS DISTINCT FROM other.strategy_outcome
           OR prior.strategy_evaluation_sha256 IS DISTINCT FROM other.strategy_evaluation_sha256
           OR prior.strategy_evidence_as_of_ts_ms IS DISTINCT FROM other.strategy_evidence_as_of_ts_ms
           OR prior.strategy_market_snapshot_sha256 IS DISTINCT FROM other.strategy_market_snapshot_sha256
           OR prior.strategy_intent_id IS DISTINCT FROM other.strategy_intent_id
           OR prior.strategy_intent_expires_at_ts_ms IS DISTINCT FROM other.strategy_intent_expires_at_ts_ms
    ) THEN
        RAISE EXCEPTION 'preexisting scheduled SIM research arms disagree on baseline strategy lineage';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION simulator_guard_scheduled_research_sample()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    roster sim_research_observation_schedules%ROWTYPE;
    window_row sim_research_observation_windows%ROWTYPE;
BEGIN
    PERFORM pg_advisory_xact_lock(849621, hashtext(NEW.campaign_id));
    SELECT * INTO roster FROM sim_research_observation_schedules
        WHERE campaign_id = NEW.campaign_id;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;
    IF EXISTS (SELECT 1 FROM sim_research_coverage_seals WHERE campaign_id = NEW.campaign_id) THEN
        RAISE EXCEPTION 'sealed SIM research campaign rejects further results';
    END IF;
    IF NEW.arm_id NOT IN ('strategy-only', 'strategy-review', 'llm-proposal-research') THEN
        RAISE EXCEPTION 'SIM research result is outside the frozen arm set';
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
