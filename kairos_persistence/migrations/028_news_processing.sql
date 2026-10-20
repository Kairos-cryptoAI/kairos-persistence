-- Explicit controlled-runtime profile only. No legacy/runtime/SIM adoption.
-- Reserve before any model call. An interrupted claim is never automatically
-- reclaimed: a lost response is not evidence that the provider was not called.
CREATE TABLE news_processing (
    scope_key TEXT NOT NULL CHECK (scope_key ~ '^[0-9a-f]{64}$'),
    event_key TEXT NOT NULL CHECK (event_key ~ '^[0-9a-f]{64}$'),
    attempt_id TEXT NOT NULL CHECK (attempt_id ~ '^[0-9a-f]{64}$'),
    state TEXT NOT NULL CHECK (state IN ('CLAIMED', 'COMPLETED')),
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (scope_key, event_key),
    CHECK ((state = 'CLAIMED' AND completed_at IS NULL)
        OR (state = 'COMPLETED' AND completed_at IS NOT NULL))
);
CREATE FUNCTION news_processing_preserve_claim() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'news processing claims cannot be deleted';
    END IF;
    IF (NEW.scope_key, NEW.event_key, NEW.attempt_id, NEW.claimed_at)
        IS DISTINCT FROM (OLD.scope_key, OLD.event_key, OLD.attempt_id, OLD.claimed_at)
        OR OLD.state <> 'CLAIMED' OR NEW.state <> 'COMPLETED' THEN
        RAISE EXCEPTION 'news processing identity is immutable; only completion is allowed';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER news_processing_preserve_claim BEFORE UPDATE OR DELETE ON news_processing
    FOR EACH ROW EXECUTE FUNCTION news_processing_preserve_claim();
REVOKE ALL ON news_processing FROM PUBLIC;
