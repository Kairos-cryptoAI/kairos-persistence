ALTER TABLE message_inbox
    ADD COLUMN IF NOT EXISTS payload_sha256 TEXT;

ALTER TABLE message_inbox
    ADD CONSTRAINT message_inbox_payload_sha256_format
    CHECK (payload_sha256 IS NULL OR payload_sha256 ~ '^[0-9a-f]{64}$');

ALTER TABLE message_outbox
    ADD COLUMN IF NOT EXISTS payload_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS lease_owner TEXT,
    ADD COLUMN IF NOT EXISTS dead_lettered_at TIMESTAMPTZ;

ALTER TABLE message_outbox
    ADD CONSTRAINT message_outbox_payload_sha256_format
    CHECK (payload_sha256 IS NULL OR payload_sha256 ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT message_outbox_lease_pair
    CHECK ((lease_until IS NULL) = (lease_owner IS NULL));

DROP INDEX IF EXISTS message_outbox_pending_idx;
CREATE INDEX message_outbox_dispatch_idx
    ON message_outbox(available_at, id)
    WHERE published_at IS NULL AND dead_lettered_at IS NULL;
