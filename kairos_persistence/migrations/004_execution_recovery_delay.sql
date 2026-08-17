ALTER TABLE execution_effects
    ADD COLUMN IF NOT EXISTS recovery_after TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE execution_effects
    ALTER COLUMN recovery_after SET DEFAULT (now() + INTERVAL '2 minutes');

DROP INDEX IF EXISTS execution_effects_recovery_idx;
CREATE INDEX execution_effects_recovery_idx
    ON execution_effects(status, recovery_after, prepared_at)
    WHERE status IN ('PREPARED', 'FAILED');
