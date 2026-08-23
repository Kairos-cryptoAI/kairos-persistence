CREATE TABLE IF NOT EXISTS execution_mutation_budget_scopes (
    environment TEXT NOT NULL,
    account_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    capacity INTEGER NOT NULL CHECK (capacity > 0 AND capacity <= 30),
    window_ms INTEGER NOT NULL CHECK (window_ms >= 60000),
    compensation_reserve INTEGER NOT NULL CHECK (
        compensation_reserve >= 4 AND compensation_reserve < capacity
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (environment, account_id, exchange)
);

CREATE TABLE IF NOT EXISTS execution_mutation_reservations (
    environment TEXT NOT NULL,
    account_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    effect_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    compensation BOOLEAN NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (environment, account_id, exchange, effect_id),
    FOREIGN KEY (environment, account_id, exchange)
        REFERENCES execution_mutation_budget_scopes(environment, account_id, exchange)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS execution_mutation_reservations_window_idx
    ON execution_mutation_reservations(environment, account_id, exchange, occurred_at DESC);
