-- Scope durable dispatch to one logical producer and prevent causal overtaking.

ALTER TABLE message_outbox
    ADD COLUMN producer text;

UPDATE message_outbox
   SET producer = CASE
       WHEN payload->>'source' = 'kairos-paper-canary' THEN 'kairos-risk-manager'
       ELSE COALESCE(NULLIF(payload->>'source', ''), 'legacy')
   END;

ALTER TABLE message_outbox
    ALTER COLUMN producer SET NOT NULL,
    ADD CONSTRAINT message_outbox_producer_not_empty CHECK (btrim(producer) <> '');

DROP INDEX IF EXISTS message_outbox_dispatch_idx;
CREATE INDEX message_outbox_dispatch_idx
    ON message_outbox(producer, available_at, id)
    WHERE published_at IS NULL AND dead_lettered_at IS NULL;
