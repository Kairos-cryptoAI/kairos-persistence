-- Additive SIM-only evidence. Old schedules and geometry-only coverage seals
-- remain immutable historical records and cannot be adopted retrospectively.
ALTER TABLE sim_research_observation_schedules
    ADD COLUMN independent_evidence_registration_allowed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE sim_research_observation_schedules
    ALTER COLUMN independent_evidence_registration_allowed SET DEFAULT TRUE;

CREATE TABLE sim_research_evidence_campaigns (
    campaign_id TEXT PRIMARY KEY REFERENCES sim_research_observation_schedules(campaign_id),
    schedule_digest TEXT NOT NULL CHECK (schedule_digest ~ '^[0-9a-f]{64}$'),
    candidate_protocol_digest TEXT NOT NULL CHECK (candidate_protocol_digest ~ '^[0-9a-f]{64}$'),
    authority TEXT NOT NULL CHECK (authority = 'SIM_RESEARCH_ONLY'),
    payload_json TEXT NOT NULL CHECK (octet_length(payload_json) <= 262144 AND jsonb_typeof(payload_json::jsonb)='object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    enrollment_txid BIGINT NOT NULL DEFAULT txid_current(),
    enrolled_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION simulator_guard_evidence_enrollment() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    roster sim_research_observation_schedules%ROWTYPE;
    protocol sim_research_adaptive_candidate_protocols%ROWTYPE;
BEGIN
    PERFORM pg_advisory_xact_lock(849621, hashtext(NEW.campaign_id));
    SELECT * INTO roster FROM sim_research_observation_schedules WHERE campaign_id=NEW.campaign_id;
    SELECT * INTO protocol FROM sim_research_adaptive_candidate_protocols WHERE campaign_id=NEW.campaign_id;
    IF roster.campaign_id IS NULL OR protocol.campaign_id IS NULL
       OR NOT roster.independent_evidence_registration_allowed
       OR roster.freeze_txid=txid_current() OR protocol.freeze_txid=txid_current()
       OR NEW.schedule_digest IS DISTINCT FROM roster.schedule_digest
       OR NEW.candidate_protocol_digest IS DISTINCT FROM protocol.protocol_digest
       OR NEW.payload_json::jsonb IS DISTINCT FROM jsonb_build_object(
          'campaign_id',NEW.campaign_id,'schedule_digest',NEW.schedule_digest,
          'candidate_protocol_digest',NEW.candidate_protocol_digest,'authority','SIM_RESEARCH_ONLY') THEN
        RAISE EXCEPTION 'independent evidence requires an exact new previously committed SIM roster';
    END IF;
    IF NOT EXISTS(SELECT 1 FROM sim_research_evidence_campaigns WHERE campaign_id=NEW.campaign_id)
       AND (EXISTS(SELECT 1 FROM sim_research_decision_samples WHERE campaign_id=NEW.campaign_id)
            OR EXISTS(SELECT 1 FROM sim_research_coverage_seals WHERE campaign_id=NEW.campaign_id)) THEN
        RAISE EXCEPTION 'independent evidence enrollment cannot adopt results or seals retrospectively';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER sim_research_evidence_enrollment_guard BEFORE INSERT ON sim_research_evidence_campaigns
    FOR EACH ROW EXECUTE FUNCTION simulator_guard_evidence_enrollment();

CREATE TABLE sim_research_source_receipts (
    receipt_sha256 TEXT PRIMARY KEY CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$'),
    campaign_id TEXT NOT NULL REFERENCES sim_research_evidence_campaigns(campaign_id),
    sample_id TEXT NOT NULL,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('MARKET_SNAPSHOT','NEWS','MACRO')),
    source_name TEXT NOT NULL,
    reference TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    payload_json TEXT NOT NULL CHECK (octet_length(payload_json) <= 262144 AND jsonb_typeof(payload_json::jsonb)='object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY(campaign_id,sample_id) REFERENCES sim_research_observation_windows(campaign_id,sample_id),
    UNIQUE(campaign_id,sample_id,source_kind,source_name,reference)
);
CREATE UNIQUE INDEX sim_research_one_market_source ON sim_research_source_receipts(campaign_id,sample_id)
    WHERE source_kind='MARKET_SNAPSHOT';

CREATE TABLE sim_research_evaluation_receipts (
    receipt_sha256 TEXT PRIMARY KEY CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$'),
    campaign_id TEXT NOT NULL REFERENCES sim_research_evidence_campaigns(campaign_id),
    sample_id TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK (octet_length(payload_json) <= 262144 AND jsonb_typeof(payload_json::jsonb)='object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY(campaign_id,sample_id) REFERENCES sim_research_observation_windows(campaign_id,sample_id),
    UNIQUE(campaign_id,sample_id)
);

CREATE TABLE sim_research_llm_attempt_starts (
    attempt_id TEXT PRIMARY KEY CHECK (attempt_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    campaign_id TEXT NOT NULL REFERENCES sim_research_evidence_campaigns(campaign_id),
    arm_id TEXT NOT NULL CHECK (arm_id IN ('strategy-review','llm-proposal-research')),
    sample_id TEXT NOT NULL,
    receipt_sha256 TEXT NOT NULL UNIQUE CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$'),
    payload_json TEXT NOT NULL CHECK (octet_length(payload_json) <= 262144 AND jsonb_typeof(payload_json::jsonb)='object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY(campaign_id,sample_id) REFERENCES sim_research_observation_windows(campaign_id,sample_id),
    UNIQUE(campaign_id,arm_id,sample_id),
    UNIQUE(attempt_id,receipt_sha256)
);

CREATE TABLE sim_research_llm_attempt_terminals (
    attempt_id TEXT PRIMARY KEY REFERENCES sim_research_llm_attempt_starts(attempt_id),
    receipt_sha256 TEXT NOT NULL UNIQUE CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$'),
    start_receipt_sha256 TEXT NOT NULL CHECK (start_receipt_sha256 ~ '^[0-9a-f]{64}$'),
    terminal_status TEXT NOT NULL CHECK (terminal_status IN ('COMPLETED','FAILED','UNRESOLVED')),
    observed_at_ts_ms BIGINT NOT NULL CHECK (observed_at_ts_ms>=0),
    payload_json TEXT NOT NULL CHECK (octet_length(payload_json) <= 262144 AND jsonb_typeof(payload_json::jsonb)='object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY(attempt_id,start_receipt_sha256) REFERENCES sim_research_llm_attempt_starts(attempt_id,receipt_sha256)
);

CREATE FUNCTION simulator_guard_independent_evidence_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    body JSONB := NEW.payload_json::jsonb;
    campaign TEXT;
    enrollment sim_research_evidence_campaigns%ROWTYPE;
    window_row sim_research_observation_windows%ROWTYPE;
    start_row sim_research_llm_attempt_starts%ROWTYPE;
    protocol_body JSONB;
    arm_body JSONB;
    observed_body JSONB;
    provenance_body JSONB;
    field_name TEXT;
    source_id TEXT;
    source_body JSONB;
    roster sim_research_observation_schedules%ROWTYPE;
BEGIN
    IF TG_TABLE_NAME='sim_research_llm_attempt_terminals' THEN
        SELECT * INTO start_row FROM sim_research_llm_attempt_starts WHERE attempt_id=NEW.attempt_id;
        campaign := start_row.campaign_id;
    ELSE campaign := NEW.campaign_id;
    END IF;
    PERFORM pg_advisory_xact_lock(849621,hashtext(campaign));
    SELECT * INTO enrollment FROM sim_research_evidence_campaigns WHERE campaign_id=campaign;
    IF enrollment.campaign_id IS NULL OR enrollment.enrollment_txid=txid_current()
       OR body->>'authority' IS DISTINCT FROM 'SIM_RESEARCH_ONLY'
       OR body->>'receipt_sha256' IS DISTINCT FROM NEW.receipt_sha256 THEN
        RAISE EXCEPTION 'independent evidence requires a previously committed enrolled SIM campaign';
    END IF;
    IF TG_TABLE_NAME='sim_research_llm_attempt_terminals' THEN
        IF body->>'attempt_id' IS DISTINCT FROM NEW.attempt_id
           OR body->>'start_receipt_sha256' IS DISTINCT FROM NEW.start_receipt_sha256
           OR body->>'terminal_status' IS DISTINCT FROM NEW.terminal_status
           OR (body->>'observed_at_ts_ms')::BIGINT IS DISTINCT FROM NEW.observed_at_ts_ms
           OR NEW.observed_at_ts_ms < (start_row.payload_json::jsonb->>'attempt_started_at_ts_ms')::BIGINT THEN
            RAISE EXCEPTION 'terminal evidence differs from admitted attempt identity';
        END IF;
        IF NEW.terminal_status='UNRESOLVED' THEN
            IF COALESCE(body->'proposal','null'::jsonb)<>'null'::jsonb
               OR COALESCE(body->'completion','null'::jsonb)<>'null'::jsonb
               OR COALESCE(body->'failure','null'::jsonb)<>'null'::jsonb THEN
                RAISE EXCEPTION 'unresolved attempt cannot fabricate a model decision or failure';
            END IF;
            RETURN NEW;
        ELSIF NEW.terminal_status='FAILED' THEN
            observed_body := body->'failure';
            provenance_body := observed_body;
            IF jsonb_typeof(observed_body) IS DISTINCT FROM 'object'
               OR COALESCE(body->'proposal','null'::jsonb)<>'null'::jsonb
               OR COALESCE(body->'completion','null'::jsonb)<>'null'::jsonb
               OR (observed_body->>'failure_observed_at_ts_ms')::BIGINT IS DISTINCT FROM NEW.observed_at_ts_ms THEN
                RAISE EXCEPTION 'failed attempt requires only its actual observed failure';
            END IF;
        ELSE
            observed_body := body->'completion';
            provenance_body := body->'proposal'->'model_provenance';
            IF jsonb_typeof(observed_body) IS DISTINCT FROM 'object'
               OR jsonb_typeof(body->'proposal') IS DISTINCT FROM 'object'
               OR COALESCE(body->'failure','null'::jsonb)<>'null'::jsonb
               OR body->'proposal'->>'proposal_id' IS DISTINCT FROM observed_body->>'proposal_id'
               OR provenance_body IS DISTINCT FROM observed_body->'model_provenance'
               OR (observed_body->>'response_observed_at_ts_ms')::BIGINT IS DISTINCT FROM NEW.observed_at_ts_ms THEN
                RAISE EXCEPTION 'completed attempt requires its exact proposal and gateway completion';
            END IF;
            FOREACH field_name IN ARRAY ARRAY['campaign_id','arm_id','sample_id','symbol','timeframe',
                'market_as_of_ts_ms','market_snapshot_sha256'] LOOP
                IF body->'proposal'->field_name IS DISTINCT FROM observed_body->field_name THEN
                    RAISE EXCEPTION 'proposal scope differs from its completed attempt';
                END IF;
            END LOOP;
        END IF;
        FOREACH field_name IN ARRAY ARRAY['attempt_id','campaign_id','arm_id','sample_id','symbol','timeframe',
            'market_as_of_ts_ms','market_snapshot_sha256','sample_deadline_ts_ms','attempt_started_at_ts_ms'] LOOP
            IF observed_body->field_name IS DISTINCT FROM start_row.payload_json::jsonb->field_name THEN
                RAISE EXCEPTION 'terminal scope differs from durable admitted attempt';
            END IF;
        END LOOP;
        FOREACH field_name IN ARRAY ARRAY['provider','requested_model','prompt_sha256','budget_reservation_id'] LOOP
            IF provenance_body->field_name IS DISTINCT FROM start_row.payload_json::jsonb->field_name THEN
                RAISE EXCEPTION 'terminal route differs from durable admitted attempt';
            END IF;
        END LOOP;
        RETURN NEW;
    END IF;
    SELECT * INTO window_row FROM sim_research_observation_windows WHERE campaign_id=campaign AND sample_id=NEW.sample_id;
    IF window_row.sample_id IS NULL OR body->>'campaign_id' IS DISTINCT FROM campaign
       OR body->>'sample_id' IS DISTINCT FROM NEW.sample_id
       OR body->>'schedule_digest' IS DISTINCT FROM enrollment.schedule_digest
       OR body->>'candidate_protocol_digest' IS DISTINCT FROM enrollment.candidate_protocol_digest THEN
        RAISE EXCEPTION 'independent evidence differs from frozen enrolled sample identity';
    END IF;
    IF NOT EXISTS(SELECT 1 FROM sim_research_source_receipts WHERE receipt_sha256=NEW.receipt_sha256)
       AND TG_TABLE_NAME='sim_research_source_receipts' THEN
        IF EXISTS(SELECT 1 FROM sim_research_decision_samples WHERE campaign_id=campaign AND sample_id=NEW.sample_id) THEN
            RAISE EXCEPTION 'source evidence cannot be added after sample results';
        END IF;
    END IF;
    IF TG_TABLE_NAME='sim_research_source_receipts' THEN
        IF body->>'source_kind' IS DISTINCT FROM NEW.source_kind
           OR body->>'source_name' IS DISTINCT FROM NEW.source_name
           OR body->>'reference' IS DISTINCT FROM NEW.reference
           OR body->>'content_sha256' IS DISTINCT FROM NEW.content_sha256 THEN
            RAISE EXCEPTION 'source evidence indexed identity differs from its payload';
        END IF;
        IF jsonb_typeof(body->'content') IS DISTINCT FROM 'object'
           OR (body->>'observed_at_ts_ms')::BIGINT IS NULL
           OR (body->>'source_as_of_ts_ms')::BIGINT IS NULL
           OR (body->>'observed_at_ts_ms')::BIGINT < (body->>'source_as_of_ts_ms')::BIGINT THEN
            RAISE EXCEPTION 'source evidence lacks bounded content or honest observation clocks';
        END IF;
        IF NEW.source_kind='MARKET_SNAPSHOT' AND (
            (body->>'source_as_of_ts_ms')::BIGINT IS DISTINCT FROM window_row.market_as_of_ts_ms
            OR (window_row.market_snapshot_sha256 IS NOT NULL AND NEW.content_sha256<>window_row.market_snapshot_sha256)) THEN
            RAISE EXCEPTION 'market source differs from frozen snapshot';
        END IF;
    ELSIF TG_TABLE_NAME='sim_research_evaluation_receipts' THEN
        IF EXISTS(SELECT 1 FROM sim_research_decision_samples WHERE campaign_id=campaign AND sample_id=NEW.sample_id)
           AND NOT EXISTS(SELECT 1 FROM sim_research_evaluation_receipts WHERE receipt_sha256=NEW.receipt_sha256) THEN
            RAISE EXCEPTION 'evaluation evidence cannot be fabricated after sample results';
        END IF;
        SELECT * INTO roster FROM sim_research_observation_schedules WHERE campaign_id=campaign;
        IF body->>'strategy_id' IS DISTINCT FROM roster.strategy_id
           OR body->>'strategy_revision' IS DISTINCT FROM roster.strategy_revision
           OR body->>'evaluator_sha256' IS DISTINCT FROM roster.payload->>'evaluator_sha256'
           OR body->>'symbol' IS DISTINCT FROM window_row.symbol
           OR body->>'timeframe' IS DISTINCT FROM window_row.timeframe
           OR (body->>'evidence_as_of_ts_ms')::BIGINT IS DISTINCT FROM window_row.market_as_of_ts_ms
           OR (body->>'evaluated_at_ts_ms')::BIGINT IS NULL
           OR (body->>'evaluated_at_ts_ms')::BIGINT < window_row.market_as_of_ts_ms
           OR (body->>'evaluated_at_ts_ms')::BIGINT > window_row.paired_at_ts_ms
           OR jsonb_typeof(body->'source_receipt_sha256s') IS DISTINCT FROM 'array'
           OR jsonb_array_length(body->'source_receipt_sha256s') NOT BETWEEN 1 AND 128 THEN
            RAISE EXCEPTION 'evaluation scope differs from frozen evaluator or causal window';
        END IF;
        FOR source_id IN SELECT jsonb_array_elements_text(body->'source_receipt_sha256s') LOOP
            SELECT payload_json::jsonb INTO source_body FROM sim_research_source_receipts
                WHERE receipt_sha256=source_id AND campaign_id=campaign AND sample_id=NEW.sample_id;
            IF source_body IS NULL OR (source_body->>'observed_at_ts_ms')::BIGINT > window_row.market_as_of_ts_ms THEN
                RAISE EXCEPTION 'evaluation source is absent or unavailable at frozen clock';
            END IF;
        END LOOP;
        IF NOT EXISTS(SELECT 1 FROM sim_research_source_receipts
            WHERE campaign_id=campaign AND sample_id=NEW.sample_id AND source_kind='MARKET_SNAPSHOT'
              AND content_sha256=body->>'market_snapshot_sha256'
              AND receipt_sha256 IN (SELECT jsonb_array_elements_text(body->'source_receipt_sha256s'))) THEN
            RAISE EXCEPTION 'evaluation lacks independently saved exact market snapshot';
        END IF;
    ELSIF TG_TABLE_NAME='sim_research_llm_attempt_starts' THEN
        IF body->>'attempt_id' IS DISTINCT FROM NEW.attempt_id OR body->>'arm_id' IS DISTINCT FROM NEW.arm_id
           OR body->>'budget_reservation_id' IS DISTINCT FROM NEW.attempt_id
           OR body->>'symbol' IS DISTINCT FROM window_row.symbol
           OR body->>'timeframe' IS DISTINCT FROM window_row.timeframe
           OR (body->>'market_as_of_ts_ms')::BIGINT IS DISTINCT FROM window_row.market_as_of_ts_ms
           OR (body->>'sample_deadline_ts_ms')::BIGINT IS DISTINCT FROM window_row.sample_deadline_ts_ms
           OR (body->>'attempt_started_at_ts_ms')::BIGINT IS NULL
           OR (body->>'attempt_started_at_ts_ms')::BIGINT < window_row.market_as_of_ts_ms
           OR (body->>'attempt_started_at_ts_ms')::BIGINT >= window_row.sample_deadline_ts_ms THEN
            RAISE EXCEPTION 'attempt admission identity or frozen clock differs';
        END IF;
        IF NOT EXISTS(SELECT 1 FROM sim_research_source_receipts
            WHERE campaign_id=campaign AND sample_id=NEW.sample_id AND source_kind='MARKET_SNAPSHOT'
              AND content_sha256=body->>'market_snapshot_sha256'
              AND (payload_json::jsonb->>'observed_at_ts_ms')::BIGINT<=window_row.market_as_of_ts_ms) THEN
            RAISE EXCEPTION 'attempt requires an independently saved causal market snapshot';
        END IF;
        SELECT payload INTO protocol_body FROM sim_research_adaptive_candidate_protocols WHERE campaign_id=campaign;
        SELECT value INTO arm_body FROM jsonb_array_elements(protocol_body->'arms') WHERE value->>'arm_id'=NEW.arm_id;
        IF body->>'provider' IS DISTINCT FROM arm_body->>'provider'
           OR body->>'requested_model' IS DISTINCT FROM arm_body->>'model'
           OR body->>'prompt_sha256' IS DISTINCT FROM arm_body->>'prompt_sha256'
           OR body->>'arm_protocol_digest' IS DISTINCT FROM (
                SELECT arm_digests->>NEW.arm_id FROM sim_research_adaptive_candidate_protocols WHERE campaign_id=campaign) THEN
            RAISE EXCEPTION 'attempt route differs from the preregistered model arm';
        END IF;
        IF EXISTS(SELECT 1 FROM sim_research_decision_samples WHERE campaign_id=campaign AND sample_id=NEW.sample_id AND arm_id=NEW.arm_id)
           AND NOT EXISTS(SELECT 1 FROM sim_research_llm_attempt_starts WHERE attempt_id=NEW.attempt_id) THEN
            RAISE EXCEPTION 'attempt cannot be fabricated after model arm results';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION simulator_reject_independent_evidence_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'SIM independent evidence and attempt history are append-only'; END;
$$;

DO $$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['sim_research_evidence_campaigns','sim_research_source_receipts',
        'sim_research_evaluation_receipts','sim_research_llm_attempt_starts','sim_research_llm_attempt_terminals'] LOOP
        IF table_name<>'sim_research_evidence_campaigns' THEN
            EXECUTE format('CREATE TRIGGER %I BEFORE INSERT ON %I FOR EACH ROW EXECUTE FUNCTION simulator_guard_independent_evidence_insert()',table_name||'_insert_guard',table_name);
        END IF;
        EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION simulator_reject_independent_evidence_mutation()',table_name||'_no_update_delete',table_name);
        EXECUTE format('CREATE TRIGGER %I BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION simulator_reject_independent_evidence_mutation()',table_name||'_no_truncate',table_name);
        EXECUTE format('REVOKE UPDATE,DELETE,TRUNCATE ON TABLE %I FROM PUBLIC',table_name);
    END LOOP;
END;
$$;

CREATE FUNCTION simulator_guard_source_qualified_sample() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    evaluation JSONB;
    source_body JSONB;
    attempt_id_value TEXT;
    terminal_body JSONB;
BEGIN
    PERFORM pg_advisory_xact_lock(849621,hashtext(NEW.campaign_id));
    IF NOT EXISTS(SELECT 1 FROM sim_research_evidence_campaigns WHERE campaign_id=NEW.campaign_id) THEN
        RETURN NEW; -- unchanged legacy geometry-only evidence, never source-qualified
    END IF;
    SELECT payload_json::jsonb INTO source_body FROM sim_research_source_receipts
        WHERE campaign_id=NEW.campaign_id AND sample_id=NEW.sample_id AND source_kind='MARKET_SNAPSHOT';
    IF source_body IS NULL OR source_body->>'content_sha256' IS DISTINCT FROM NEW.market_snapshot_sha256
       OR (source_body->>'observed_at_ts_ms')::BIGINT > NEW.market_as_of_ts_ms THEN
        RAISE EXCEPTION 'source-qualified observation requires independent causal market evidence';
    END IF;
    IF NEW.strategy_outcome<>'NOT_EVALUATED' THEN
        SELECT payload_json::jsonb INTO evaluation FROM sim_research_evaluation_receipts
            WHERE receipt_sha256=NEW.strategy_evaluation_sha256 AND campaign_id=NEW.campaign_id AND sample_id=NEW.sample_id;
        IF evaluation IS NULL OR evaluation->>'market_snapshot_sha256' IS DISTINCT FROM NEW.market_snapshot_sha256
           OR (evaluation->>'evaluated_at_ts_ms')::BIGINT > NEW.paired_at_ts_ms
           OR COALESCE(evaluation->'intent'->>'side','NO_INTENT') IS DISTINCT FROM NEW.strategy_outcome
           OR evaluation->'intent'->>'intent_id' IS DISTINCT FROM NEW.strategy_intent_id THEN
            RAISE EXCEPTION 'source-qualified sample differs from independent strategy evaluation';
        END IF;
    ELSIF EXISTS(SELECT 1 FROM sim_research_evaluation_receipts WHERE campaign_id=NEW.campaign_id AND sample_id=NEW.sample_id) THEN
        RAISE EXCEPTION 'source-qualified sample suppresses independent strategy evaluation';
    END IF;
    SELECT attempt_id INTO attempt_id_value FROM sim_research_llm_attempt_starts
        WHERE campaign_id=NEW.campaign_id AND arm_id=NEW.arm_id AND sample_id=NEW.sample_id;
    IF attempt_id_value IS NULL THEN
        IF NEW.llm_outcome<>'NOT_CALLED' THEN
            RAISE EXCEPTION 'source-qualified model observation lacks a durable admitted attempt';
        END IF;
        RETURN NEW;
    END IF;
    SELECT payload_json::jsonb INTO terminal_body FROM sim_research_llm_attempt_terminals WHERE attempt_id=attempt_id_value;
    IF terminal_body IS NULL OR terminal_body->>'terminal_status'='UNRESOLVED'
       OR (terminal_body->>'observed_at_ts_ms')::BIGINT > NEW.paired_at_ts_ms THEN
        RAISE EXCEPTION 'unfinished ambiguous or late attempt cannot become a causal model result';
    END IF;
    IF terminal_body->>'terminal_status'='FAILED' THEN
        IF NEW.llm_outcome<>'CALL_FAILED'
           OR terminal_body->'failure'->>'failure_receipt_id' IS DISTINCT FROM NEW.llm_failure_receipt_id THEN
            RAISE EXCEPTION 'source-qualified sample suppresses durable model failure';
        END IF;
    ELSIF terminal_body->'proposal'->>'proposal_id' IS DISTINCT FROM NEW.llm_proposal_id
       OR terminal_body->'proposal'->>'action' IS DISTINCT FROM NEW.llm_outcome
       OR terminal_body->'completion'->>'completion_receipt_id' IS DISTINCT FROM NEW.llm_completion_receipt_id THEN
        RAISE EXCEPTION 'source-qualified sample changes durable model completion';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER sim_research_source_qualified_result_guard BEFORE INSERT ON sim_research_decision_samples
    FOR EACH ROW EXECUTE FUNCTION simulator_guard_source_qualified_sample();

CREATE TABLE sim_research_source_qualified_seals (
    campaign_id TEXT PRIMARY KEY REFERENCES sim_research_evidence_campaigns(campaign_id),
    receipt_sha256 TEXT NOT NULL UNIQUE CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$'),
    coverage_digest TEXT NOT NULL UNIQUE REFERENCES sim_research_coverage_seals(coverage_digest),
    payload_json TEXT NOT NULL CHECK (octet_length(payload_json)<=262144 AND jsonb_typeof(payload_json::jsonb)='object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE FUNCTION simulator_guard_source_qualified_seal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    body JSONB := NEW.payload_json::jsonb;
    coverage_body JSONB;
BEGIN
    PERFORM pg_advisory_xact_lock(849621,hashtext(NEW.campaign_id));
    SELECT payload INTO coverage_body FROM sim_research_coverage_seals
        WHERE campaign_id=NEW.campaign_id AND coverage_digest=NEW.coverage_digest;
    IF coverage_body IS NULL OR body->>'campaign_id' IS DISTINCT FROM NEW.campaign_id
       OR body->>'receipt_sha256' IS DISTINCT FROM NEW.receipt_sha256
       OR body->>'authority' IS DISTINCT FROM 'SIM_RESEARCH_ONLY'
       OR body->>'contract_version' IS DISTINCT FROM 'research-source-qualified-coverage.v1'
       OR body->>'qualification' IS DISTINCT FROM 'INDEPENDENT_SOURCE_REPLAY_ONLY'
       OR body->'economic_qualification' IS DISTINCT FROM 'false'::jsonb
       OR body->'paper_qualification' IS DISTINCT FROM 'false'::jsonb
       OR body->'live_orders_allowed' IS DISTINCT FROM 'false'::jsonb
       OR body->'coverage' IS DISTINCT FROM coverage_body THEN
        RAISE EXCEPTION 'independent source seal is engineering-only and must match its immutable coverage';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER sim_research_source_qualified_seals_insert_guard
    BEFORE INSERT ON sim_research_source_qualified_seals
    FOR EACH ROW EXECUTE FUNCTION simulator_guard_source_qualified_seal();
CREATE TRIGGER sim_research_source_qualified_seals_no_update_delete
    BEFORE UPDATE OR DELETE ON sim_research_source_qualified_seals
    FOR EACH ROW EXECUTE FUNCTION simulator_reject_independent_evidence_mutation();
CREATE TRIGGER sim_research_source_qualified_seals_no_truncate BEFORE TRUNCATE ON sim_research_source_qualified_seals
    FOR EACH STATEMENT EXECUTE FUNCTION simulator_reject_independent_evidence_mutation();
REVOKE UPDATE,DELETE,TRUNCATE ON TABLE sim_research_source_qualified_seals FROM PUBLIC;
