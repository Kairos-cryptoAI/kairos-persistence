-- Isolated durable journal for kairos-sim.  These tables represent recorded
-- Binance inputs and model facts only; no table has a venue/account/profile
-- column or can grant PAPER, LIVE, Trial 15, or alpha readiness.

CREATE TABLE sim_tapes (
    tape_id TEXT PRIMARY KEY CHECK (btrim(tape_id) <> ''),
    market_data_venue TEXT NOT NULL CHECK (market_data_venue = 'BINANCE_UM'),
    state TEXT NOT NULL DEFAULT 'OPEN' CHECK (state IN ('OPEN', 'SEALED', 'BLOCKED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sealed_at TIMESTAMPTZ,
    blocked_at TIMESTAMPTZ,
    blocked_reason TEXT,
    tape_sha256 TEXT CHECK (tape_sha256 IS NULL OR tape_sha256 ~ '^[0-9a-f]{64}$'),
    seal_payload_sha256 TEXT CHECK (seal_payload_sha256 IS NULL OR seal_payload_sha256 ~ '^[0-9a-f]{64}$'),
    seal_payload JSONB,
    book_frame_count BIGINT NOT NULL DEFAULT 0 CHECK (book_frame_count >= 0),
    book_chain_head_sha256 TEXT CHECK (book_chain_head_sha256 IS NULL OR book_chain_head_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (
        (state = 'SEALED') = (
            sealed_at IS NOT NULL
            AND tape_sha256 IS NOT NULL
            AND seal_payload_sha256 IS NOT NULL
            AND seal_payload IS NOT NULL
        )
    ),
    CHECK (state <> 'BLOCKED' OR blocked_at IS NOT NULL)
);

CREATE TABLE sim_closed_bars (
    tape_id TEXT NOT NULL REFERENCES sim_tapes(tape_id),
    symbol TEXT NOT NULL CHECK (symbol IN ('BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT')),
    open_time_ms BIGINT NOT NULL CHECK (open_time_ms >= 0 AND open_time_ms % 60000 = 0),
    close_time_ms BIGINT NOT NULL CHECK (close_time_ms = open_time_ms + 59999),
    bar_sha256 TEXT NOT NULL CHECK (bar_sha256 ~ '^[0-9a-f]{64}$'),
    previous_bar_sha256 TEXT CHECK (previous_bar_sha256 IS NULL OR previous_bar_sha256 ~ '^[0-9a-f]{64}$'),
    chain_sha256 TEXT NOT NULL CHECK (chain_sha256 ~ '^[0-9a-f]{64}$'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tape_id, symbol, open_time_ms),
    UNIQUE (tape_id, symbol, bar_sha256),
    UNIQUE (tape_id, symbol, chain_sha256)
);
CREATE INDEX sim_closed_bars_tail_idx ON sim_closed_bars(tape_id, symbol, open_time_ms DESC);

-- ``tape_sequence`` is global across every symbol in one tape.  The source
-- contract links each frame to the immediately preceding global frame.
CREATE TABLE sim_book_frames (
    tape_id TEXT NOT NULL REFERENCES sim_tapes(tape_id),
    tape_sequence BIGINT NOT NULL CHECK (tape_sequence > 0),
    symbol TEXT NOT NULL CHECK (symbol IN ('BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT')),
    stream_epoch TEXT NOT NULL CHECK (btrim(stream_epoch) <> ''),
    exchange_update_id BIGINT NOT NULL CHECK (exchange_update_id > 0),
    exchange_at_ms BIGINT NOT NULL CHECK (exchange_at_ms >= 0),
    received_at_ms BIGINT NOT NULL CHECK (received_at_ms >= exchange_at_ms),
    persisted_at_ms BIGINT NOT NULL CHECK (persisted_at_ms >= received_at_ms),
    continuity TEXT NOT NULL CHECK (continuity IN ('ADMITTED', 'GAP', 'RECONNECT', 'UNKNOWN', 'UNAVAILABLE')),
    frame_sha256 TEXT NOT NULL CHECK (frame_sha256 ~ '^[0-9a-f]{64}$'),
    previous_frame_sha256 TEXT CHECK (previous_frame_sha256 IS NULL OR previous_frame_sha256 ~ '^[0-9a-f]{64}$'),
    chain_sha256 TEXT NOT NULL CHECK (chain_sha256 ~ '^[0-9a-f]{64}$'),
    raw_payload_sha256 TEXT NOT NULL CHECK (raw_payload_sha256 ~ '^[0-9a-f]{64}$'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tape_id, tape_sequence),
    UNIQUE (tape_id, frame_sha256),
    UNIQUE (tape_id, chain_sha256)
);
CREATE INDEX sim_book_frames_symbol_tail_idx
    ON sim_book_frames(tape_id, symbol, tape_sequence DESC);

CREATE TABLE sim_sessions (
    session_id TEXT PRIMARY KEY CHECK (session_id ~ '^[0-9a-f]{64}$'),
    tape_id TEXT NOT NULL REFERENCES sim_tapes(tape_id),
    tape_sha256 TEXT NOT NULL CHECK (tape_sha256 ~ '^[0-9a-f]{64}$'),
    assumptions_sha256 TEXT NOT NULL CHECK (assumptions_sha256 ~ '^[0-9a-f]{64}$'),
    state TEXT NOT NULL DEFAULT 'OPEN' CHECK (state IN ('OPEN', 'COMPLETED', 'BLOCKED')),
    started_at_ms BIGINT NOT NULL CHECK (started_at_ms >= 0),
    ends_at_ms BIGINT NOT NULL CHECK (ends_at_ms > started_at_ms),
    execution_environment TEXT NOT NULL CHECK (execution_environment = 'SIMULATED'),
    paper_qualification_eligible BOOLEAN NOT NULL DEFAULT FALSE CHECK (paper_qualification_eligible = FALSE),
    trial15_eligible BOOLEAN NOT NULL DEFAULT FALSE CHECK (trial15_eligible = FALSE),
    alpha_claim BOOLEAN NOT NULL DEFAULT FALSE CHECK (alpha_claim = FALSE),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX sim_sessions_tape_idx ON sim_sessions(tape_id, created_at);

CREATE TABLE sim_risk_decisions (
    decision_id TEXT PRIMARY KEY CHECK (decision_id ~ '^[0-9a-f]{64}$'),
    session_id TEXT NOT NULL REFERENCES sim_sessions(session_id),
    intent_id TEXT NOT NULL CHECK (intent_id ~ '^[0-9a-f]{64}$'),
    review_id TEXT NOT NULL CHECK (review_id ~ '^[0-9a-f]{64}$'),
    selected_book_frame_sha256 TEXT CHECK (selected_book_frame_sha256 IS NULL OR selected_book_frame_sha256 ~ '^[0-9a-f]{64}$'),
    symbol TEXT NOT NULL CHECK (symbol IN ('BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT')),
    side TEXT NOT NULL CHECK (side IN ('LONG', 'SHORT')),
    approved BOOLEAN NOT NULL,
    rejection_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    quantity DOUBLE PRECISION NOT NULL CHECK (quantity >= 0),
    price_cap DOUBLE PRECISION,
    decided_at_ms BIGINT NOT NULL CHECK (decided_at_ms >= 0),
    execution_environment TEXT NOT NULL CHECK (execution_environment = 'SIMULATED'),
    paper_qualification_eligible BOOLEAN NOT NULL DEFAULT FALSE CHECK (paper_qualification_eligible = FALSE),
    trial15_eligible BOOLEAN NOT NULL DEFAULT FALSE CHECK (trial15_eligible = FALSE),
    alpha_claim BOOLEAN NOT NULL DEFAULT FALSE CHECK (alpha_claim = FALSE),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (decision_id, session_id),
    CHECK (
        (approved AND quantity > 0 AND price_cap IS NOT NULL AND cardinality(rejection_reasons) = 0)
        OR (NOT approved AND quantity = 0 AND price_cap IS NULL AND cardinality(rejection_reasons) > 0)
    )
);
CREATE INDEX sim_risk_decisions_session_symbol_idx
    ON sim_risk_decisions(session_id, symbol, decided_at_ms);

CREATE TABLE sim_admissions (
    admission_id TEXT PRIMARY KEY CHECK (admission_id ~ '^[0-9a-f]{64}$'),
    decision_id TEXT NOT NULL,
    session_id TEXT NOT NULL REFERENCES sim_sessions(session_id),
    intent_id TEXT NOT NULL CHECK (intent_id ~ '^[0-9a-f]{64}$'),
    review_id TEXT NOT NULL CHECK (review_id ~ '^[0-9a-f]{64}$'),
    selected_book_frame_sha256 TEXT NOT NULL CHECK (selected_book_frame_sha256 ~ '^[0-9a-f]{64}$'),
    symbol TEXT NOT NULL CHECK (symbol IN ('BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT')),
    side TEXT NOT NULL CHECK (side IN ('LONG', 'SHORT')),
    quantity DOUBLE PRECISION NOT NULL CHECK (quantity > 0),
    price_cap DOUBLE PRECISION NOT NULL CHECK (price_cap > 0),
    admitted_at_ms BIGINT NOT NULL CHECK (admitted_at_ms >= 0),
    execution_environment TEXT NOT NULL CHECK (execution_environment = 'SIMULATED'),
    paper_qualification_eligible BOOLEAN NOT NULL DEFAULT FALSE CHECK (paper_qualification_eligible = FALSE),
    trial15_eligible BOOLEAN NOT NULL DEFAULT FALSE CHECK (trial15_eligible = FALSE),
    alpha_claim BOOLEAN NOT NULL DEFAULT FALSE CHECK (alpha_claim = FALSE),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (session_id, admission_id),
    UNIQUE (session_id, intent_id),
    UNIQUE (decision_id, admission_id),
    FOREIGN KEY (decision_id, session_id)
        REFERENCES sim_risk_decisions(decision_id, session_id)
);
CREATE INDEX sim_admissions_session_symbol_idx ON sim_admissions(session_id, symbol, admitted_at_ms);

CREATE TABLE sim_trades (
    trade_id TEXT PRIMARY KEY CHECK (trade_id ~ '^[0-9a-f]{64}$'),
    session_id TEXT NOT NULL REFERENCES sim_sessions(session_id),
    admission_id TEXT NOT NULL,
    intent_id TEXT NOT NULL CHECK (intent_id ~ '^[0-9a-f]{64}$'),
    symbol TEXT NOT NULL CHECK (symbol IN ('BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT')),
    side TEXT NOT NULL CHECK (side IN ('LONG', 'SHORT')),
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'ACTIVE', 'FLAT', 'UNRESOLVED', 'NO_FILL', 'BLOCKED')),
    state_version BIGINT NOT NULL DEFAULT 0 CHECK (state_version >= 0),
    next_event_seq BIGINT NOT NULL DEFAULT 1 CHECK (next_event_seq > 0),
    journal_head_sha256 TEXT CHECK (journal_head_sha256 IS NULL OR journal_head_sha256 ~ '^[0-9a-f]{64}$'),
    event_count BIGINT NOT NULL DEFAULT 0 CHECK (event_count >= 0),
    execution_environment TEXT NOT NULL CHECK (execution_environment = 'SIMULATED'),
    paper_qualification_eligible BOOLEAN NOT NULL DEFAULT FALSE CHECK (paper_qualification_eligible = FALSE),
    trial15_eligible BOOLEAN NOT NULL DEFAULT FALSE CHECK (trial15_eligible = FALSE),
    alpha_claim BOOLEAN NOT NULL DEFAULT FALSE CHECK (alpha_claim = FALSE),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL,
    created_at_ms BIGINT NOT NULL CHECK (created_at_ms >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (session_id, admission_id),
    UNIQUE (session_id, admission_id, trade_id),
    FOREIGN KEY (session_id, admission_id)
        REFERENCES sim_admissions(session_id, admission_id)
);
CREATE INDEX sim_trades_session_symbol_idx ON sim_trades(session_id, symbol, updated_at);

CREATE TABLE sim_commands (
    command_id TEXT PRIMARY KEY CHECK (command_id ~ '^[0-9a-f]{64}$'),
    session_id TEXT NOT NULL,
    admission_id TEXT NOT NULL,
    intent_id TEXT NOT NULL CHECK (intent_id ~ '^[0-9a-f]{64}$'),
    trade_id TEXT NOT NULL,
    symbol TEXT NOT NULL CHECK (symbol IN ('BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT')),
    side TEXT NOT NULL CHECK (side IN ('LONG', 'SHORT')),
    order_side TEXT NOT NULL CHECK (order_side IN ('BUY', 'SELL')),
    command_kind TEXT NOT NULL CHECK (command_kind IN ('ENTRY_IOC', 'STOP_EXIT_IOC', 'TARGET_EXIT_IOC', 'TIMEOUT_EXIT_IOC')),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL CHECK (status IN ('PREPARED', 'COMPLETED', 'FAILED')),
    payload JSONB NOT NULL,
    prepared_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    error TEXT,
    CHECK ((status = 'COMPLETED') = (completed_at IS NOT NULL)),
    UNIQUE (command_id, session_id, admission_id, intent_id, trade_id),
    FOREIGN KEY (session_id, admission_id, trade_id)
        REFERENCES sim_trades(session_id, admission_id, trade_id)
);
CREATE INDEX sim_commands_session_symbol_idx ON sim_commands(session_id, symbol, prepared_at, command_id);

CREATE TABLE sim_command_receipts (
    receipt_id TEXT PRIMARY KEY CHECK (receipt_id ~ '^[0-9a-f]{64}$'),
    command_id TEXT NOT NULL UNIQUE,
    session_id TEXT NOT NULL,
    admission_id TEXT NOT NULL,
    intent_id TEXT NOT NULL CHECK (intent_id ~ '^[0-9a-f]{64}$'),
    trade_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('FILLED', 'PARTIAL', 'NO_FILL', 'BLOCKED')),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (command_id, session_id, admission_id, intent_id, trade_id)
        REFERENCES sim_commands(command_id, session_id, admission_id, intent_id, trade_id),
    FOREIGN KEY (session_id, admission_id, trade_id)
        REFERENCES sim_trades(session_id, admission_id, trade_id)
);

-- This is a private, canonicalized snapshot of the pure IOC kernel state.
-- It is intentionally not a public trading contract and has no venue linkage.
CREATE TABLE sim_liquidity_states (
    session_id TEXT NOT NULL REFERENCES sim_sessions(session_id),
    symbol TEXT NOT NULL CHECK (symbol IN ('BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT')),
    state_schema_version TEXT NOT NULL CHECK (btrim(state_schema_version) <> ''),
    state_sha256 TEXT NOT NULL CHECK (state_sha256 ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, symbol)
);

CREATE TABLE sim_trade_events (
    event_id TEXT PRIMARY KEY CHECK (event_id ~ '^[0-9a-f]{64}$'),
    trade_id TEXT NOT NULL REFERENCES sim_trades(trade_id),
    event_seq BIGINT NOT NULL CHECK (event_seq > 0),
    previous_event_sha256 TEXT CHECK (previous_event_sha256 IS NULL OR previous_event_sha256 ~ '^[0-9a-f]{64}$'),
    event_sha256 TEXT NOT NULL CHECK (event_sha256 ~ '^[0-9a-f]{64}$'),
    from_state TEXT CHECK (from_state IS NULL OR from_state IN ('PENDING', 'ACTIVE', 'FLAT', 'UNRESOLVED', 'NO_FILL', 'BLOCKED')),
    to_state TEXT NOT NULL CHECK (to_state IN ('PENDING', 'ACTIVE', 'FLAT', 'UNRESOLVED', 'NO_FILL', 'BLOCKED')),
    event_type TEXT NOT NULL,
    occurred_at_ms BIGINT NOT NULL CHECK (occurred_at_ms >= 0),
    command_id TEXT CHECK (command_id IS NULL OR command_id ~ '^[0-9a-f]{64}$'),
    receipt_id TEXT CHECK (receipt_id IS NULL OR receipt_id ~ '^[0-9a-f]{64}$'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (trade_id, event_seq),
    UNIQUE (trade_id, event_id),
    UNIQUE (trade_id, event_sha256)
);

CREATE TABLE sim_results (
    result_id TEXT PRIMARY KEY CHECK (result_id ~ '^[0-9a-f]{64}$'),
    session_id TEXT NOT NULL,
    admission_id TEXT NOT NULL,
    intent_id TEXT NOT NULL CHECK (intent_id ~ '^[0-9a-f]{64}$'),
    trade_id TEXT NOT NULL UNIQUE,
    terminal_event_id TEXT NOT NULL,
    final_state TEXT NOT NULL CHECK (final_state IN ('FLAT', 'UNRESOLVED', 'NO_FILL', 'BLOCKED')),
    execution_environment TEXT NOT NULL CHECK (execution_environment = 'SIMULATED'),
    venue_execution_observed BOOLEAN NOT NULL DEFAULT FALSE CHECK (venue_execution_observed = FALSE),
    paper_qualification_eligible BOOLEAN NOT NULL DEFAULT FALSE CHECK (paper_qualification_eligible = FALSE),
    trial15_eligible BOOLEAN NOT NULL DEFAULT FALSE CHECK (trial15_eligible = FALSE),
    alpha_claim BOOLEAN NOT NULL DEFAULT FALSE CHECK (alpha_claim = FALSE),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL,
    completed_at_ms BIGINT NOT NULL CHECK (completed_at_ms >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (session_id, admission_id, trade_id)
        REFERENCES sim_trades(session_id, admission_id, trade_id),
    FOREIGN KEY (trade_id, terminal_event_id)
        REFERENCES sim_trade_events(trade_id, event_id)
);

CREATE TABLE sim_session_receipts (
    receipt_id TEXT PRIMARY KEY CHECK (receipt_id ~ '^[0-9a-f]{64}$'),
    session_id TEXT NOT NULL UNIQUE REFERENCES sim_sessions(session_id),
    tape_id TEXT NOT NULL REFERENCES sim_tapes(tape_id),
    tape_sha256 TEXT NOT NULL CHECK (tape_sha256 ~ '^[0-9a-f]{64}$'),
    receipt_state TEXT NOT NULL CHECK (receipt_state IN ('COMPLETED', 'BLOCKED')),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL,
    completed_at_ms BIGINT NOT NULL CHECK (completed_at_ms >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
