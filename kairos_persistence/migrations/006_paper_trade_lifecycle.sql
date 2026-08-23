-- PAPER trade lifecycle, effect lineage, and restart recovery barrier.
-- This migration is additive: the legacy DRY_RUN journal remains readable.

ALTER TABLE execution_effects
    ADD COLUMN IF NOT EXISTS environment TEXT,
    ADD COLUMN IF NOT EXISTS account_id TEXT,
    ADD COLUMN IF NOT EXISTS trade_id TEXT,
    ADD COLUMN IF NOT EXISTS order_role TEXT;

ALTER TABLE execution_effects
    DROP CONSTRAINT IF EXISTS execution_effects_effect_type_check;
ALTER TABLE execution_effects
    ADD CONSTRAINT execution_effects_effect_type_check CHECK (
        effect_type IN (
            'PLACE_ORDER', 'CLOSE_POSITION', 'PROTECTIVE_STOP', 'TAKE_PROFIT',
            'CANCEL_ORDER', 'CANCEL_TPSL', 'TIMEOUT_CLOSE', 'EMERGENCY_CLOSE',
            'SET_LEVERAGE'
        )
    );
ALTER TABLE execution_effects
    DROP CONSTRAINT IF EXISTS execution_effects_lineage_check;
ALTER TABLE execution_effects
    DROP CONSTRAINT IF EXISTS execution_effects_order_role_check;
ALTER TABLE execution_effects
    ADD CONSTRAINT execution_effects_order_role_check CHECK (
        order_role IS NULL OR order_role IN (
            'ENTRY', 'STOP_LOSS', 'TAKE_PROFIT', 'TIMEOUT_EXIT', 'EMERGENCY_EXIT'
        )
    );
ALTER TABLE execution_effects
    ADD CONSTRAINT execution_effects_lineage_check CHECK (
        (environment IS NULL AND account_id IS NULL AND trade_id IS NULL AND order_role IS NULL)
        OR (
            environment IS NOT NULL AND environment <> ''
            AND account_id IS NOT NULL AND account_id <> ''
            AND trade_id IS NOT NULL AND trade_id ~ '^[0-9a-f]{64}$'
            AND order_role IS NOT NULL
        )
    );
CREATE INDEX IF NOT EXISTS execution_effects_trade_idx
    ON execution_effects(environment, account_id, trade_id, prepared_at)
    WHERE trade_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS execution_trades (
    trade_id TEXT PRIMARY KEY CHECK (trade_id ~ '^[0-9a-f]{64}$'),
    strategy_intent_id TEXT NOT NULL CHECK (strategy_intent_id ~ '^[0-9a-f]{64}$'),
    risk_decision_id TEXT NOT NULL UNIQUE CHECK (risk_decision_id ~ '^[0-9a-f]{64}$'),
    risk_decision_sha256 TEXT NOT NULL CHECK (risk_decision_sha256 ~ '^[0-9a-f]{64}$'),
    risk_decision_payload JSONB NOT NULL,
    strategy_id TEXT NOT NULL,
    strategy_revision TEXT NOT NULL,
    trading_mode TEXT NOT NULL CHECK (trading_mode IN ('PAPER', 'LIVE')),
    environment TEXT NOT NULL,
    profile TEXT NOT NULL CHECK (profile IN ('DEV', 'DEMO', 'PROD')),
    exchange TEXT NOT NULL,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    venue_symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    quantity DOUBLE PRECISION NOT NULL CHECK (quantity > 0 AND quantity < 'Infinity'::float8),
    leverage DOUBLE PRECISION NOT NULL CHECK (leverage >= 1 AND leverage <= 125),
    stop_price DOUBLE PRECISION NOT NULL CHECK (stop_price > 0 AND stop_price < 'Infinity'::float8),
    target_price DOUBLE PRECISION NOT NULL CHECK (target_price > 0 AND target_price < 'Infinity'::float8),
    entry_eligible_at TIMESTAMPTZ NOT NULL,
    entry_expires_at TIMESTAMPTZ NOT NULL,
    max_holding_ms BIGINT NOT NULL CHECK (max_holding_ms > 0),
    state TEXT NOT NULL CHECK (state IN (
        'RECEIVED', 'ENTRY_PENDING', 'PROTECTING', 'ACTIVE',
        'EXITING_STOP', 'EXITING_TARGET', 'EXITING_TIMEOUT', 'EXITING_EMERGENCY',
        'FLAT', 'CANCELLED', 'FAILED_BLOCKED'
    )),
    entry_client_order_id TEXT NOT NULL UNIQUE,
    entry_exchange_order_id TEXT,
    stop_client_order_id TEXT,
    stop_exchange_order_id TEXT,
    target_client_order_id TEXT,
    target_exchange_order_id TEXT,
    close_client_order_id TEXT,
    close_exchange_order_id TEXT,
    filled_quantity DOUBLE PRECISION NOT NULL DEFAULT 0
        CHECK (filled_quantity >= 0 AND filled_quantity < 'Infinity'::float8),
    first_fill_at TIMESTAMPTZ,
    timeout_at TIMESTAMPTZ,
    last_reconciled_at TIMESTAMPTZ,
    reconciliation_detail TEXT,
    state_version BIGINT NOT NULL DEFAULT 0,
    next_execution_event_seq BIGINT NOT NULL DEFAULT 1 CHECK (next_execution_event_seq > 0),
    journal_head_sha256 TEXT NOT NULL CHECK (journal_head_sha256 ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (entry_expires_at > entry_eligible_at),
    CHECK (trading_mode <> 'PAPER' OR (profile = 'DEV' AND exchange = 'evedex')),
    CHECK (trading_mode <> 'PAPER' OR venue_symbol ~ '^[A-Z0-9]+:DEV$'),
    CHECK (trading_mode <> 'LIVE' OR profile = 'PROD'),
    CHECK (filled_quantity <= quantity),
    CHECK (
        (side='BUY' AND stop_price < target_price)
        OR (side='SELL' AND stop_price > target_price)
    ),
    CHECK ((first_fill_at IS NULL) = (timeout_at IS NULL))
);

-- There can be at most one non-terminal idea per symbol and account.
CREATE UNIQUE INDEX IF NOT EXISTS execution_trades_one_active_symbol_idx
    ON execution_trades(environment, account_id, symbol)
    WHERE state NOT IN ('FLAT', 'CANCELLED');
CREATE UNIQUE INDEX IF NOT EXISTS execution_trades_stop_client_order_idx
    ON execution_trades(environment, account_id, exchange, stop_client_order_id)
    WHERE stop_client_order_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS execution_trades_target_client_order_idx
    ON execution_trades(environment, account_id, exchange, target_client_order_id)
    WHERE target_client_order_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS execution_trades_close_client_order_idx
    ON execution_trades(environment, account_id, exchange, close_client_order_id)
    WHERE close_client_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS execution_trades_recovery_idx
    ON execution_trades(environment, account_id, state, updated_at)
    WHERE state NOT IN ('FLAT', 'CANCELLED');

CREATE TABLE IF NOT EXISTS execution_trade_events (
    sequence BIGSERIAL PRIMARY KEY,
    trade_id TEXT NOT NULL REFERENCES execution_trades(trade_id),
    from_state TEXT,
    to_state TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_payload JSONB NOT NULL,
    previous_event_sha256 TEXT,
    event_sha256 TEXT NOT NULL UNIQUE CHECK (event_sha256 ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (previous_event_sha256 IS NULL OR previous_event_sha256 ~ '^[0-9a-f]{64}$')
);
CREATE INDEX IF NOT EXISTS execution_trade_events_trade_sequence_idx
    ON execution_trade_events(trade_id, sequence);

CREATE TABLE IF NOT EXISTS execution_recovery_state (
    environment TEXT NOT NULL,
    account_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    entries_blocked BOOLEAN NOT NULL DEFAULT TRUE,
    recovery_epoch BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    detail TEXT NOT NULL DEFAULT 'startup recovery has not completed',
    PRIMARY KEY (environment, account_id, exchange)
);

CREATE TABLE IF NOT EXISTS account_equity_state (
    environment TEXT NOT NULL,
    account_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    trading_day DATE NOT NULL,
    day_start_equity_usd DOUBLE PRECISION NOT NULL
        CHECK (day_start_equity_usd > 0 AND day_start_equity_usd < 'Infinity'::float8),
    peak_equity_usd DOUBLE PRECISION NOT NULL
        CHECK (peak_equity_usd > 0 AND peak_equity_usd < 'Infinity'::float8),
    last_equity_usd DOUBLE PRECISION NOT NULL
        CHECK (last_equity_usd > 0 AND last_equity_usd < 'Infinity'::float8),
    first_captured_at TIMESTAMPTZ NOT NULL,
    last_captured_at TIMESTAMPTZ NOT NULL,
    reconciliation_seq BIGINT NOT NULL DEFAULT 1 CHECK (reconciliation_seq > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (environment, account_id, exchange, trading_day),
    CHECK (first_captured_at <= last_captured_at),
    CHECK (peak_equity_usd >= day_start_equity_usd AND peak_equity_usd >= last_equity_usd)
);
