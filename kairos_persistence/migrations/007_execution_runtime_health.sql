CREATE TABLE IF NOT EXISTS execution_runtime_health (
    environment TEXT NOT NULL,
    account_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    auth_age_ms BIGINT NOT NULL CHECK (auth_age_ms >= 0),
    auth_expires_in_ms BIGINT CHECK (auth_expires_in_ms IS NULL OR auth_expires_in_ms >= 0),
    local_mutation_reserve INTEGER NOT NULL CHECK (local_mutation_reserve >= 0),
    local_mutation_capacity INTEGER NOT NULL CHECK (local_mutation_capacity > 0),
    local_mutation_window_ms BIGINT NOT NULL CHECK (local_mutation_window_ms > 0),
    venue_rate_limit_observable BOOLEAN NOT NULL,
    venue_rate_limit_reserve INTEGER CHECK (
        venue_rate_limit_reserve IS NULL OR venue_rate_limit_reserve >= 0
    ),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (environment, account_id, exchange),
    CHECK (local_mutation_reserve <= local_mutation_capacity)
);

CREATE INDEX IF NOT EXISTS execution_runtime_health_observed_idx
    ON execution_runtime_health(observed_at DESC);
