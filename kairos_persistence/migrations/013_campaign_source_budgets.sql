-- One immutable campaign binding per provider prevents changing campaign IDs
-- or billing months from minting a fresh qualification allowance. Historical
-- reservations stay in their original ledger and are counted, not copied.
CREATE TABLE IF NOT EXISTS campaign_source_budgets (
    source TEXT PRIMARY KEY CHECK (source IN ('openai', 'deepseek', 'x')),
    campaign_id TEXT NOT NULL CHECK (btrim(campaign_id) <> ''),
    budget_microusd BIGINT NOT NULL CHECK (budget_microusd > 0),
    historical_cost_microusd BIGINT NOT NULL CHECK (historical_cost_microusd >= 0),
    historical_evidence_sha256 TEXT NOT NULL
        CHECK (historical_evidence_sha256 ~ '^[0-9a-f]{64}$'),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((source = 'openai' AND budget_microusd <= 12000000)
        OR (source = 'deepseek' AND budget_microusd <= 1000000)
        OR (source = 'x' AND budget_microusd <= 2000000))
);

CREATE INDEX IF NOT EXISTS source_usage_campaign_idx
    ON source_usage_reservations(source, status);
