-- Explicit RESEARCH_CAMPAIGN only. SIM25/RUNTIME17/CONTROLLED_RUNTIME are unchanged.
-- Separate campaign journal: legacy source equality and sample triggers stay intact.
CREATE TABLE sim_adaptive_campaign_plans (
    campaign_id TEXT PRIMARY KEY REFERENCES sim_research_observation_schedules(campaign_id),
    receipt_sha256 TEXT UNIQUE NOT NULL CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$'),
    payload_json TEXT NOT NULL CHECK (octet_length(payload_json)<=262144 AND jsonb_typeof(payload_json::jsonb)='object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    registration_txid BIGINT NOT NULL DEFAULT txid_current()
);
CREATE TABLE sim_adaptive_window_claims (
    campaign_id TEXT NOT NULL REFERENCES sim_adaptive_campaign_plans(campaign_id),
    sample_id TEXT NOT NULL,
    claim_id TEXT UNIQUE NOT NULL CHECK (claim_id ~ '^[0-9a-f]{64}$'),
    receipt_sha256 TEXT UNIQUE NOT NULL CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$'),
    payload_json TEXT NOT NULL CHECK (octet_length(payload_json)<=262144 AND jsonb_typeof(payload_json::jsonb)='object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY(campaign_id,sample_id),
    FOREIGN KEY(campaign_id,sample_id) REFERENCES sim_research_observation_windows(campaign_id,sample_id)
);
CREATE TABLE sim_adaptive_campaign_receipts (
    receipt_sha256 TEXT PRIMARY KEY CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$'),
    campaign_id TEXT NOT NULL REFERENCES sim_adaptive_campaign_plans(campaign_id),
    sample_id TEXT NOT NULL,
    arm_id TEXT NOT NULL CHECK (arm_id IN ('all','strategy-only','strategy-review','llm-proposal-research')),
    kind TEXT NOT NULL CHECK (kind IN ('source','bundle','evaluation','start','terminal','review','cost','sample','outcome')),
    slot_key TEXT NOT NULL CHECK (length(slot_key) BETWEEN 1 AND 128),
    payload_json TEXT NOT NULL CHECK (octet_length(payload_json)<=262144 AND jsonb_typeof(payload_json::jsonb)='object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(campaign_id,sample_id,arm_id,kind,slot_key),
    FOREIGN KEY(campaign_id,sample_id) REFERENCES sim_research_observation_windows(campaign_id,sample_id)
);
CREATE INDEX sim_adaptive_campaign_receipt_roster ON sim_adaptive_campaign_receipts(campaign_id,kind,sample_id,arm_id);
CREATE UNIQUE INDEX sim_adaptive_one_start_per_arm ON sim_adaptive_campaign_receipts(campaign_id,sample_id,arm_id) WHERE kind='start';
CREATE TABLE sim_adaptive_campaign_denominators (
    campaign_id TEXT PRIMARY KEY REFERENCES sim_adaptive_campaign_plans(campaign_id),
    receipt_sha256 TEXT UNIQUE NOT NULL CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$'),
    payload_json TEXT NOT NULL CHECK (octet_length(payload_json)<=262144 AND jsonb_typeof(payload_json::jsonb)='object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE FUNCTION simulator_guard_adaptive_campaign_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    body JSONB := NEW.payload_json::jsonb;
    roster sim_research_observation_schedules%ROWTYPE;
    protocol sim_research_adaptive_candidate_protocols%ROWTYPE;
    plan sim_adaptive_campaign_plans%ROWTYPE;
    admitted JSONB;
    allow_late BOOLEAN := false;
BEGIN
    PERFORM pg_advisory_xact_lock(849621,hashtext(NEW.campaign_id));
    IF TG_TABLE_NAME='sim_adaptive_campaign_receipts' THEN
      allow_late := NEW.kind IN ('terminal','review')
        OR (NEW.kind='cost' AND body->>'stage'<>'RESERVATION_REQUESTED');
      IF NEW.kind='terminal' THEN
        SELECT payload_json::jsonb INTO admitted FROM sim_adaptive_campaign_receipts
          WHERE campaign_id=NEW.campaign_id AND sample_id=NEW.sample_id
            AND arm_id=NEW.arm_id AND kind='start' AND slot_key=NEW.slot_key;
        IF admitted IS NULL OR body->>'attempt_id' IS DISTINCT FROM admitted->>'attempt_id'
           OR body->>'start_receipt_sha256' IS DISTINCT FROM admitted->>'receipt_sha256' THEN
            RAISE EXCEPTION 'campaign terminal requires its exact prior admitted START';
        END IF;
      END IF;
    END IF;
    IF body->>'receipt_sha256' IS DISTINCT FROM NEW.receipt_sha256
       OR body->>'authority' IS DISTINCT FROM 'SIM_RESEARCH_ONLY'
       OR COALESCE(body->>'campaign_id',admitted->>'campaign_id') IS DISTINCT FROM NEW.campaign_id THEN
        RAISE EXCEPTION 'adaptive campaign receipt identity mismatch';
    END IF;
    SELECT * INTO roster FROM sim_research_observation_schedules WHERE campaign_id=NEW.campaign_id;
    SELECT * INTO protocol FROM sim_research_adaptive_candidate_protocols WHERE campaign_id=NEW.campaign_id;
    IF TG_TABLE_NAME='sim_adaptive_campaign_plans' THEN
        IF roster.campaign_id IS NULL OR protocol.campaign_id IS NULL
           OR roster.freeze_txid=txid_current() OR protocol.freeze_txid=txid_current()
           OR NOT roster.independent_evidence_registration_allowed
           OR body->>'schedule_digest' IS DISTINCT FROM roster.schedule_digest
           OR body->>'candidate_protocol_digest' IS DISTINCT FROM protocol.protocol_digest THEN
            RAISE EXCEPTION 'adaptive campaign requires previously committed exact frozen identity';
        END IF;
        IF NOT EXISTS(SELECT 1 FROM sim_adaptive_campaign_plans WHERE campaign_id=NEW.campaign_id)
           AND (EXISTS(SELECT 1 FROM sim_research_observation_windows
                       WHERE campaign_id=NEW.campaign_id
                         AND market_as_of_ts_ms <= floor(extract(epoch FROM clock_timestamp())*1000)::BIGINT)
                OR EXISTS(SELECT 1 FROM sim_research_decision_samples WHERE campaign_id=NEW.campaign_id)
                OR EXISTS(SELECT 1 FROM sim_research_evidence_campaigns WHERE campaign_id=NEW.campaign_id)) THEN
            RAISE EXCEPTION 'adaptive campaign cannot adopt past windows or existing evidence';
        END IF;
        RETURN NEW;
    END IF;
    SELECT * INTO plan FROM sim_adaptive_campaign_plans WHERE campaign_id=NEW.campaign_id;
    IF plan.campaign_id IS NULL OR plan.registration_txid=txid_current() THEN
        RAISE EXCEPTION 'adaptive campaign requires committed open preregistration';
    END IF;
    IF EXISTS(SELECT 1 FROM sim_adaptive_campaign_denominators WHERE campaign_id=NEW.campaign_id)
       AND NOT allow_late THEN
        RAISE EXCEPTION 'denominator snapshot rejects new input/outcomes but retains late actual cost/completion';
    END IF;
    IF TG_TABLE_NAME<>'sim_adaptive_campaign_denominators' THEN
        IF COALESCE(body->>'sample_id',admitted->>'sample_id') IS DISTINCT FROM NEW.sample_id THEN
            RAISE EXCEPTION 'adaptive campaign sample identity mismatch';
        END IF;
    END IF;
    IF TG_TABLE_NAME='sim_adaptive_campaign_denominators'
       AND (body->>'qualification' IS DISTINCT FROM 'SCHEDULED_DENOMINATOR_ONLY'
            OR body->'economic_qualification' IS DISTINCT FROM 'false'::jsonb
            OR body->'live_orders_allowed' IS DISTINCT FROM 'false'::jsonb) THEN
        RAISE EXCEPTION 'adaptive denominator is never economic or trading approval';
    END IF;
    RETURN NEW;
END;
$$;
DO $$ DECLARE table_name TEXT; BEGIN
    FOREACH table_name IN ARRAY ARRAY['sim_adaptive_campaign_plans','sim_adaptive_window_claims',
            'sim_adaptive_campaign_receipts','sim_adaptive_campaign_denominators'] LOOP
        EXECUTE format('CREATE TRIGGER %I BEFORE INSERT ON %I FOR EACH ROW EXECUTE FUNCTION simulator_guard_adaptive_campaign_insert()',table_name||'_guard',table_name);
        EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION simulator_reject_independent_evidence_mutation()',table_name||'_immutable',table_name);
        EXECUTE format('CREATE TRIGGER %I BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION simulator_reject_independent_evidence_mutation()',table_name||'_no_truncate',table_name);
        EXECUTE format('REVOKE UPDATE,DELETE,TRUNCATE ON %I FROM PUBLIC',table_name);
    END LOOP;
END $$;
