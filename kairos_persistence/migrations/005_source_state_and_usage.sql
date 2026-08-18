CREATE TABLE IF NOT EXISTS source_cursors (
    service TEXT NOT NULL CHECK (btrim(service) <> ''),
    source TEXT NOT NULL CHECK (btrim(source) <> ''),
    cursor_key TEXT NOT NULL CHECK (btrim(cursor_key) <> ''),
    cursor_value TEXT NOT NULL CHECK (cursor_value ~ '^(0|[1-9][0-9]{0,19})$'),
    cursor_numeric NUMERIC(20, 0) NOT NULL CHECK (cursor_numeric >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (service, source, cursor_key),
    CHECK (cursor_value::numeric = cursor_numeric)
);

CREATE TABLE IF NOT EXISTS source_usage_reservations (
    service TEXT NOT NULL CHECK (btrim(service) <> ''),
    source TEXT NOT NULL CHECK (btrim(source) <> ''),
    reservation_id TEXT NOT NULL CHECK (btrim(reservation_id) <> ''),
    billing_month DATE NOT NULL,
    reserved_units INTEGER NOT NULL CHECK (reserved_units > 0),
    unit_cost_microusd BIGINT NOT NULL CHECK (unit_cost_microusd >= 0),
    reserved_cost_microusd BIGINT NOT NULL CHECK (reserved_cost_microusd >= 0),
    actual_units INTEGER CHECK (actual_units >= 0),
    actual_cost_microusd BIGINT CHECK (actual_cost_microusd >= 0),
    status TEXT NOT NULL CHECK (status IN ('RESERVED', 'COMMITTED', 'RELEASED')),
    reserved_at TIMESTAMPTZ NOT NULL,
    finalized_at TIMESTAMPTZ,
    PRIMARY KEY (service, source, reservation_id),
    CHECK (reserved_cost_microusd = reserved_units::bigint * unit_cost_microusd),
    CHECK (
        (status = 'RESERVED' AND actual_units IS NULL AND actual_cost_microusd IS NULL
         AND finalized_at IS NULL)
        OR
        (status = 'COMMITTED' AND actual_units IS NOT NULL AND actual_cost_microusd IS NOT NULL
         AND finalized_at IS NOT NULL
         AND actual_units <= reserved_units
         AND actual_cost_microusd = actual_units::bigint * unit_cost_microusd)
        OR
        (status = 'RELEASED' AND actual_units IS NULL AND actual_cost_microusd IS NULL
         AND finalized_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS source_usage_month_idx
    ON source_usage_reservations(service, source, billing_month, status);
