-- SIM-only preregistered matched research. No PAPER/runtime migration owns this.
CREATE TABLE sim_research_observation_schedules (
    campaign_id TEXT PRIMARY KEY,
    schedule_digest TEXT NOT NULL UNIQUE CHECK (schedule_digest ~ '^[0-9a-f]{64}$'),
    strategy_id TEXT NOT NULL,
    strategy_revision TEXT NOT NULL,
    window_count INTEGER NOT NULL CHECK (window_count BETWEEN 1 AND 50000),
    authority TEXT NOT NULL CHECK (authority = 'SIM_RESEARCH_ONLY'),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    freeze_txid BIGINT NOT NULL DEFAULT txid_current(),
    frozen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE sim_research_observation_windows (
    campaign_id TEXT NOT NULL REFERENCES sim_research_observation_schedules(campaign_id),
    sample_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    market_as_of_ts_ms BIGINT NOT NULL CHECK (market_as_of_ts_ms >= 0),
    market_snapshot_sha256 TEXT CHECK (
        market_snapshot_sha256 IS NULL OR market_snapshot_sha256 ~ '^[0-9a-f]{64}$'
    ),
    paired_at_ts_ms BIGINT NOT NULL CHECK (paired_at_ts_ms >= market_as_of_ts_ms),
    sample_deadline_ts_ms BIGINT NOT NULL CHECK (sample_deadline_ts_ms > paired_at_ts_ms),
    PRIMARY KEY (campaign_id, sample_id),
    UNIQUE (campaign_id, symbol, timeframe, market_as_of_ts_ms)
);

CREATE TABLE sim_research_coverage_seals (
    campaign_id TEXT PRIMARY KEY REFERENCES sim_research_observation_schedules(campaign_id),
    coverage_digest TEXT NOT NULL UNIQUE CHECK (coverage_digest ~ '^[0-9a-f]{64}$'),
    schedule_digest TEXT NOT NULL REFERENCES sim_research_observation_schedules(schedule_digest),
    expected_result_count INTEGER NOT NULL CHECK (expected_result_count >= 3),
    result_ids_sha256 TEXT NOT NULL CHECK (result_ids_sha256 ~ '^[0-9a-f]{64}$'),
    authority TEXT NOT NULL CHECK (authority = 'SIM_RESEARCH_ONLY'),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    sealed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION simulator_reject_research_roster_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'SIM research schedule and coverage seals are append-only';
END;
$$;

CREATE TRIGGER sim_research_schedules_no_update_delete
    BEFORE UPDATE OR DELETE ON sim_research_observation_schedules
    FOR EACH ROW EXECUTE FUNCTION simulator_reject_research_roster_mutation();
CREATE TRIGGER sim_research_schedules_no_truncate
    BEFORE TRUNCATE ON sim_research_observation_schedules
    FOR EACH STATEMENT EXECUTE FUNCTION simulator_reject_research_roster_mutation();
CREATE TRIGGER sim_research_windows_no_update_delete
    BEFORE UPDATE OR DELETE ON sim_research_observation_windows
    FOR EACH ROW EXECUTE FUNCTION simulator_reject_research_roster_mutation();
CREATE TRIGGER sim_research_windows_no_truncate
    BEFORE TRUNCATE ON sim_research_observation_windows
    FOR EACH STATEMENT EXECUTE FUNCTION simulator_reject_research_roster_mutation();
CREATE TRIGGER sim_research_coverage_seals_no_update_delete
    BEFORE UPDATE OR DELETE ON sim_research_coverage_seals
    FOR EACH ROW EXECUTE FUNCTION simulator_reject_research_roster_mutation();
CREATE TRIGGER sim_research_coverage_seals_no_truncate
    BEFORE TRUNCATE ON sim_research_coverage_seals
    FOR EACH STATEMENT EXECUTE FUNCTION simulator_reject_research_roster_mutation();

REVOKE UPDATE, DELETE, TRUNCATE ON TABLE sim_research_observation_schedules FROM PUBLIC;
REVOKE UPDATE, DELETE, TRUNCATE ON TABLE sim_research_observation_windows FROM PUBLIC;
REVOKE UPDATE, DELETE, TRUNCATE ON TABLE sim_research_coverage_seals FROM PUBLIC;

-- Window rows can only be inserted by the same atomic transaction that first
-- registers the schedule. No later transaction can extend its sample set.
CREATE FUNCTION simulator_guard_research_window_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    parent_txid BIGINT;
BEGIN
    SELECT freeze_txid INTO parent_txid
        FROM sim_research_observation_schedules WHERE campaign_id = NEW.campaign_id;
    IF parent_txid IS NULL OR parent_txid <> txid_current() THEN
        RAISE EXCEPTION 'SIM research window cannot be added after schedule freeze';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER sim_research_windows_freeze_guard
    BEFORE INSERT ON sim_research_observation_windows
    FOR EACH ROW EXECUTE FUNCTION simulator_guard_research_window_insert();

-- Legacy unscheduled SIM fixtures remain readable. A new scheduled campaign
-- must be frozen while it has zero results; thereafter every insert is checked
-- against that immutable roster, including inserts outside the Python API.
CREATE FUNCTION simulator_guard_scheduled_research_sample()
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
          AND (prior.market_snapshot_sha256 <> NEW.market_snapshot_sha256
               OR prior.strategy_outcome <> NEW.strategy_outcome)
    ) THEN
        RAISE EXCEPTION 'matched SIM research arms disagree on snapshot or baseline strategy';
    END IF;
    NEW.recorded_at := clock_timestamp();
    IF NEW.recorded_at <= roster.frozen_at THEN
        RAISE EXCEPTION 'SIM research result predates its frozen schedule';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER sim_research_decision_samples_schedule_guard
    BEFORE INSERT ON sim_research_decision_samples
    FOR EACH ROW EXECUTE FUNCTION simulator_guard_scheduled_research_sample();
