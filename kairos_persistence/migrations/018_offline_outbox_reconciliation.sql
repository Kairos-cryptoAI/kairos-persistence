-- A deliberately narrow offline reconciliation can leave a publish outcome
-- ambiguous at the transport/DB ACK boundary.  Preserve that fact durably and
-- keep the normal retry dispatcher away from the row until an authoritative
-- operator reconciliation is performed.

ALTER TABLE message_outbox
    ADD COLUMN IF NOT EXISTS reconciliation_state TEXT NOT NULL DEFAULT 'NONE',
    ADD COLUMN IF NOT EXISTS reconciliation_id TEXT,
    ADD COLUMN IF NOT EXISTS reconciliation_started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS reconciliation_outcome_at TIMESTAMPTZ;

ALTER TABLE message_outbox
    ADD CONSTRAINT message_outbox_reconciliation_state
    CHECK (reconciliation_state IN ('NONE', 'PUBLISHING', 'PUBLISH_OUTCOME_UNKNOWN', 'ACKNOWLEDGED')),
    ADD CONSTRAINT message_outbox_reconciliation_identity
    CHECK (
        reconciliation_state = 'NONE'
        OR (reconciliation_id IS NOT NULL AND btrim(reconciliation_id) <> '')
    );

CREATE INDEX message_outbox_reconciliation_pending_idx
    ON message_outbox(reconciliation_state, id)
    WHERE published_at IS NULL AND dead_lettered_at IS NULL;
