-- Opt-in controlled-runtime ONLY. No automatic adoption, role creation or grants.
-- The separately reviewed non-superuser kairos_operator login is the only latch writer.
-- Runtime receives SELECT on latch/commands and SELECT,INSERT on admissions/claims.
-- Never grant the operator role to runtime; runtime must not own these tables.
CREATE TABLE operator_controls (
    scope_key TEXT PRIMARY KEY,
    scope_payload JSONB NOT NULL,
    scope_sha256 TEXT NOT NULL CHECK (scope_sha256 ~ '^[0-9a-f]{64}$'),
    version BIGINT NOT NULL CHECK (version > 0),
    state TEXT NOT NULL CHECK (state IN ('ARMED','DISARMED','KILLED')),
    session_id TEXT REFERENCES paper_canary_sessions(session_id),
    expires_at_ms BIGINT,
    audit_head_sha256 TEXT NOT NULL CHECK (audit_head_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK ((state='ARMED' AND session_id IS NOT NULL AND expires_at_ms > 0)
        OR (state<>'ARMED' AND session_id IS NULL AND expires_at_ms IS NULL))
);
CREATE TABLE operator_control_commands (
    command_id TEXT PRIMARY KEY CHECK (command_id ~ '^[0-9a-f]{64}$'),
    scope_key TEXT NOT NULL,
    version BIGINT NOT NULL,
    command_payload JSONB NOT NULL,
    command_sha256 TEXT NOT NULL CHECK (command_sha256 ~ '^[0-9a-f]{64}$'),
    event_payload JSONB NOT NULL,
    event_sha256 TEXT NOT NULL CHECK (event_sha256 ~ '^[0-9a-f]{64}$'),
    actor TEXT NOT NULL CHECK (actor='kairos_operator'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (scope_key,version)
);
CREATE TABLE operator_control_admissions (
    decision_id TEXT PRIMARY KEY CHECK (decision_id ~ '^[0-9a-f]{64}$'),
    scope_key TEXT NOT NULL REFERENCES operator_controls(scope_key),
    version BIGINT NOT NULL,
    decision_sha256 TEXT NOT NULL CHECK (decision_sha256 ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE operator_control_dispatch_claims (
    effect_id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL UNIQUE REFERENCES operator_control_admissions(decision_id),
    scope_key TEXT NOT NULL,
    version BIGINT NOT NULL,
    payload JSONB NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE FUNCTION operator_control_latch_writer() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF current_user <> 'kairos_operator' OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles
        WHERE rolname=current_user AND (rolsuper OR rolbypassrls OR NOT rolcanlogin)
    ) THEN
        RAISE EXCEPTION 'operator latch requires the separately reviewed non-superuser operator role';
    END IF;
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'operator latch cannot be deleted'; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER operator_control_latch_writer BEFORE INSERT OR UPDATE OR DELETE
    ON operator_controls FOR EACH ROW EXECUTE FUNCTION operator_control_latch_writer();
CREATE TRIGGER operator_command_writer BEFORE INSERT ON operator_control_commands
    FOR EACH ROW EXECUTE FUNCTION operator_control_latch_writer();
CREATE FUNCTION operator_control_append_only() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'operator evidence is append-only'; END $$;
CREATE TRIGGER operator_commands_immutable BEFORE UPDATE OR DELETE ON operator_control_commands
    FOR EACH ROW EXECUTE FUNCTION operator_control_append_only();
CREATE TRIGGER operator_admissions_immutable BEFORE UPDATE OR DELETE ON operator_control_admissions
    FOR EACH ROW EXECUTE FUNCTION operator_control_append_only();
CREATE TRIGGER operator_claims_immutable BEFORE UPDATE OR DELETE ON operator_control_dispatch_claims
    FOR EACH ROW EXECUTE FUNCTION operator_control_append_only();
REVOKE ALL ON operator_controls,operator_control_commands,operator_control_admissions,
    operator_control_dispatch_claims FROM PUBLIC;
