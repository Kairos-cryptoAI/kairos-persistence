"""Integration boundaries for the isolated ``kairos-sim`` durable journal.

The optional integration case accepts only an explicitly provisioned disposable
database.  It never falls back to the runtime/PAPER DSN, so running the normal
unit suite cannot migrate or write the PAPER contour by accident.
"""

from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit
from uuid import uuid4

import asyncpg
import pytest
from kairos_core import (
    CandidateReviewTier,
    CandidateReviewV1,
    CandidateRouteV1,
    ClosedBarEventV1,
    ExitPlanV1,
    ReasoningEffort,
    RecordedBookLevelV1,
    RecordedTopNBookFrameV1,
    RecordedTopNBookFrameV2,
    ReviewDecision,
    Side,
    SimulationAdmissionV2,
    SimulationAssumptionsV1,
    SimulationCommandReceiptV1,
    SimulationCommandV1,
    SimulationFillLevelV1,
    SimulationResultV1,
    SimulationRiskDecisionV1,
    SimulationSessionReceiptV1,
    SimulationSessionV1,
    SimulationStrategyRefV1,
    SimulationTradeEventV2,
    SimulationTradeJournalHeadV1,
    SimulationTradeV1,
    StrategyIntentV1,
    StrategyProvenanceV1,
)

from kairos_persistence import Database, MigrationProfile, PersistenceSettings, SimulationRepository
from kairos_persistence.database_target import connect_verified_database, require_database_target_url

_T0 = 1_800_000_000_000
_SimulationSymbol = Literal["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
_SYMBOLS: tuple[_SimulationSymbol, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT")
_SIM_DB_PREFIX = "kairos_sim_test_"
_SIMPLE_DATABASE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,62}\Z")


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _identity(value: str | None) -> str:
    if value is None:
        raise AssertionError("test fixture requires a canonical contract identity")
    return value


def _settings() -> tuple[PersistenceSettings, str]:
    """Read only the explicit disposable target used by an integration job."""

    database_url = os.getenv("KAIROS_SIM_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("KAIROS_SIM_TEST_DATABASE_URL is required for simulator integration tests")
    database_name = urlsplit(database_url).path.removeprefix("/")
    if not (database_name.startswith(_SIM_DB_PREFIX) and _SIMPLE_DATABASE_NAME.fullmatch(database_name)):
        raise RuntimeError("simulator integration target must be a uniquely named kairos_sim_test database")
    require_database_target_url(database_url, database_name, local_only=True)
    return PersistenceSettings(database_url=database_url), database_name


def _bar(symbol: str, *, open_time_ms: int = _T0) -> ClosedBarEventV1:
    return ClosedBarEventV1(
        source="sim-recorder-test",
        symbol=symbol,
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + 59_999,
        open=100.0,
        high=102.0,
        low=99.0,
        close=101.0,
        base_volume=20.0,
        quote_volume=2_010.0,
        taker_buy_base_volume=11.0,
        taker_buy_quote_volume=1_110.0,
    )


def _intent(bar: ClosedBarEventV1) -> StrategyIntentV1:
    return StrategyIntentV1(
        source="sim-strategy-test",
        strategy_id="regime-aligned-right-tail",
        strategy_revision="v1",
        symbol="BTCUSDT",
        side=Side.LONG,
        decision_ts_ms=_T0 + 59_999,
        entry_eligible_ts_ms=_T0 + 60_000,
        entry_expires_ts_ms=_T0 + 120_000,
        reference_price=100.0,
        signal_strength=0.5,
        gross_reward_bps=500.0,
        exit_plan=ExitPlanV1(stop_price=95.0, target_price=105.0, max_holding_ms=180_000),
        provenance=StrategyProvenanceV1(
            strategy_code_sha256=_hash("strategy"),
            config_sha256=_hash("config"),
            input_window_sha256=_hash("window"),
            features_sha256=_hash("features"),
            input_bar_sha256s=(_identity(bar.bar_sha256),),
        ),
    )


def _review(intent: StrategyIntentV1) -> CandidateReviewV1:
    route = CandidateRouteV1(
        source="sim-router-test",
        intent=intent,
        review_tier=CandidateReviewTier.NORMAL,
        requested_reasoning_effort=ReasoningEffort.MEDIUM,
        routed_at_ms=_T0 + 60_000,
        review_deadline_ms=_T0 + 80_000,
    )
    return CandidateReviewV1(
        source="sim-review-test",
        route=route,
        intent=intent,
        decision=ReviewDecision.ALLOW,
        priority=0,
        reviewed_at_ms=_T0 + 60_050,
        reviewer="DETERMINISTIC",
        reason_codes=("SIM_INTEGRATION_ALLOW",),
    )


def _assumptions() -> SimulationAssumptionsV1:
    return SimulationAssumptionsV1(
        latency_ms=25,
        maximum_book_age_ms=5_000,
        maximum_frame_latency_ms=1_000,
        depth_participation_fraction=0.1,
        adverse_slippage_bps=2.0,
        taker_fee_bps=5.0,
        price_tick=0.1,
        quantity_step=0.001,
    )


def test_simulator_schema_has_no_paper_execution_tables_or_authority() -> None:
    sql = (
        Path(__file__).parents[1] / "kairos_persistence" / "migrations" / "017_simulator_journal.sql"
    ).read_text(encoding="utf-8")

    for table in (
        "sim_tapes",
        "sim_closed_bars",
        "sim_book_frames",
        "sim_risk_decisions",
        "sim_admissions",
        "sim_trades",
        "sim_commands",
        "sim_command_receipts",
        "sim_trade_events",
        "sim_results",
    ):
        assert f"CREATE TABLE {table}" in sql
    assert "PRIMARY KEY (tape_id, tape_sequence)" in sql
    assert "FOREIGN KEY (decision_id, session_id)" in sql
    assert "FOREIGN KEY (command_id, session_id, admission_id, intent_id, trade_id)" in sql
    assert "order_side TEXT NOT NULL CHECK (order_side IN ('BUY', 'SELL'))" in sql
    assert "execution_effects" not in sql
    assert "paper_trade" not in sql
    assert "execution_environment = 'SIMULATED'" in sql
    assert "paper_qualification_eligible BOOLEAN NOT NULL DEFAULT FALSE" in sql


def test_simulator_target_rejects_runtime_or_unqualified_database_names() -> None:
    with pytest.raises(RuntimeError, match="kairos_sim_test"):
        original = os.environ.pop("KAIROS_SIM_TEST_DATABASE_URL", None)
        try:
            os.environ["KAIROS_SIM_TEST_DATABASE_URL"] = "postgresql://kairos:kairos@localhost:5432/kairos"
            _settings()
        finally:
            if original is None:
                os.environ.pop("KAIROS_SIM_TEST_DATABASE_URL", None)
            else:
                os.environ["KAIROS_SIM_TEST_DATABASE_URL"] = original


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simulator_journal_is_idempotent_and_replays_only_sealed_recorded_inputs() -> None:
    settings, database_name = _settings()
    database = Database(settings, migration_profile=MigrationProfile.SIMULATOR)
    await connect_verified_database(database, database_name, local_only=True)
    try:
        await database.migrate()
        repository = SimulationRepository(database.pool)
        tape_id = f"tape-{uuid4().hex}"
        bars = {symbol: _bar(symbol) for symbol in _SYMBOLS}
        for bar in bars.values():
            assert await repository.record_closed_bar(tape_id, bar)
        assert not await repository.record_closed_bar(tape_id, bars["BTCUSDT"])
        following_bars = {symbol: _bar(symbol, open_time_ms=_T0 + 60_000) for symbol in _SYMBOLS}
        for bar in following_bars.values():
            assert await repository.record_closed_bar(tape_id, bar)

        frames: dict[str, RecordedTopNBookFrameV1] = {}
        previous_frame_sha256: str | None = None
        for sequence, symbol in enumerate(_SYMBOLS, start=1):
            frame = RecordedTopNBookFrameV1(
                source="sim-recorder-test",
                tape_id=tape_id,
                stream_epoch="test-epoch-1",
                symbol=symbol,
                tape_sequence=sequence,
                exchange_update_id=100 + sequence,
                exchange_at_ms=_T0 + 60_000 + sequence,
                received_at_ms=_T0 + 60_010 + sequence,
                persisted_at_ms=_T0 + 60_020 + sequence,
                raw_payload_sha256=_hash(f"raw-book-{symbol}"),
                previous_frame_sha256=previous_frame_sha256,
                continuity="ADMITTED",
                bids=(RecordedBookLevelV1(price=99.9, quantity=2.0),),
                asks=(RecordedBookLevelV1(price=100.1, quantity=2.0),),
            )
            assert await repository.record_book_frame(frame)
            frames[symbol] = frame
            previous_frame_sha256 = frame.frame_sha256

        seal = await repository.seal_tape(tape_id, sealed_at_ms=_T0 + 130_000)
        assert seal.execution_environment == "SIMULATED"
        assert await repository.verify_tape(tape_id)
        first_page = await repository.load_closed_bar_page(tape_id, "BTCUSDT", limit=1)
        assert first_page == (bars["BTCUSDT"],)
        next_page = await repository.load_closed_bar_page(
            tape_id,
            "BTCUSDT",
            after_open_time_ms=first_page[-1].open_time_ms,
            limit=10,
        )
        assert next_page == (following_bars["BTCUSDT"],)
        assert (
            await repository.load_closed_bar_page(
                tape_id,
                "BTCUSDT",
                after_open_time_ms=next_page[-1].open_time_ms,
                limit=10,
            )
            == ()
        )
        with pytest.raises(ValueError, match="stored bar boundary"):
            await repository.load_closed_bar_page(
                tape_id,
                "BTCUSDT",
                after_open_time_ms=_T0 + 30_000,
            )

        session = SimulationSessionV1(
            source="market-simulator-test",
            tape_id=tape_id,
            tape_sha256=_identity(seal.tape_sha256),
            assumptions=_assumptions(),
            strategy_allowlist=(
                SimulationStrategyRefV1(strategy_id="regime-aligned-right-tail", strategy_revision="v1"),
            ),
            started_at_ms=_T0 + 60_000,
            ends_at_ms=_T0 + 300_000,
        )
        assert await repository.create_session(session)
        assert not await repository.create_session(session)

        intent = _intent(bars["BTCUSDT"])
        review = _review(intent)
        unrecorded_intent = _intent(_bar("BTCUSDT", open_time_ms=_T0 + 60_000))
        unrecorded_review = _review(unrecorded_intent)
        unrecorded_decision = SimulationRiskDecisionV1(
            source="simulation-risk-test",
            session=session,
            intent=unrecorded_intent,
            review=unrecorded_review,
            selected_book_frame=frames["BTCUSDT"],
            approved=True,
            quantity=0.01,
            price_cap=100.2,
            decided_at_ms=_T0 + 60_100,
        )
        with pytest.raises(ValueError, match="provenance"):
            await repository.record_risk_decision(unrecorded_decision)
        decision = SimulationRiskDecisionV1(
            source="simulation-risk-test",
            session=session,
            intent=intent,
            review=review,
            selected_book_frame=frames["BTCUSDT"],
            approved=True,
            quantity=0.01,
            price_cap=100.2,
            decided_at_ms=_T0 + 60_100,
        )
        assert await repository.record_risk_decision(decision)
        admission = SimulationAdmissionV2(
            source="market-simulator-test",
            decision=decision,
            admitted_at_ms=_T0 + 60_110,
        )
        assert await repository.create_admission(admission)
        trade = SimulationTradeV1(
            source="market-simulator-test", admission=admission, created_at_ms=_T0 + 60_111
        )
        assert await repository.create_trade(trade)
        session_id = _identity(trade.session_id)
        admission_id = _identity(trade.admission_id)
        intent_id = _identity(trade.intent_id)
        trade_id = _identity(trade.trade_id)

        admitted = SimulationTradeEventV2(
            source="market-simulator-test",
            session_id=session_id,
            admission_id=admission_id,
            intent_id=intent_id,
            trade_id=trade_id,
            event_seq=1,
            event_type="ADMITTED",
            to_state="PENDING",
            occurred_at_ms=_T0 + 60_112,
            symbol="BTCUSDT",
            side="LONG",
            reason_codes=("SIM_ADMISSION",),
        )
        assert await repository.append_trade_event(admitted)

        entry_command = SimulationCommandV1(
            source="market-simulator-test",
            trade=trade,
            command_kind="ENTRY_IOC",
            quantity=0.01,
            price_cap=100.2,
            submitted_at_ms=_T0 + 60_120,
            persisted_at_ms=_T0 + 60_121,
            eligible_at_ms=_T0 + 60_120,
            expires_at_ms=_T0 + 120_000,
        )
        prepared = await repository.prepare_command(entry_command)
        assert prepared.created and prepared.status == "PREPARED"
        assert await repository.list_prepared_commands(session_id) == (entry_command,)
        assert (
            await repository.load_latest_book_frame(
                tape_id,
                "BTCUSDT",
                as_of_ms=_T0 + 60_021,
            )
        ) == frames["BTCUSDT"]
        assert (
            await repository.load_latest_book_frame(
                tape_id,
                "BTCUSDT",
                as_of_ms=_T0 + 60_019,
            )
        ) is None
        entry_receipt = SimulationCommandReceiptV1(
            source="market-simulator-test",
            command=entry_command,
            model_frame_sha256=frames["BTCUSDT"].frame_sha256,
            status="FILLED",
            reason_codes=("MODEL_IOC",),
            arrival_at_ms=_T0 + 60_130,
            filled_quantity=0.01,
            cancelled_quantity=0.0,
            average_price=100.2,
            notional_quote=1.002,
            fee_quote=0.00501,
            arrival_mid_price=100.0,
            implementation_shortfall_quote=0.002,
            level_fills=(
                SimulationFillLevelV1(
                    book_price=100.1, execution_price=100.2, quantity=0.01, fee_quote=0.00501
                ),
            ),
        )
        entered = SimulationTradeEventV2(
            source="market-simulator-test",
            session_id=session_id,
            admission_id=admission_id,
            intent_id=intent_id,
            trade_id=trade_id,
            event_seq=2,
            previous_event_sha256=admitted.event_id,
            event_type="ENTRY_FILLED",
            from_state="PENDING",
            to_state="ACTIVE",
            occurred_at_ms=_T0 + 60_130,
            symbol="BTCUSDT",
            side="LONG",
            command_id=entry_command.command_id,
            receipt_id=entry_receipt.receipt_id,
            filled_quantity=entry_receipt.filled_quantity,
            average_price=entry_receipt.average_price,
            model_frame_sha256=entry_receipt.model_frame_sha256,
            reason_codes=("MODEL_IOC",),
        )
        first_completion = await repository.complete_command(
            entry_command,
            entry_receipt,
            model_state_schema_version="sim-liquidity-state.v1",
            model_state_payload={"session_id": session_id, "symbol": "BTCUSDT", "remaining": []},
            events=(entered,),
        )
        assert first_completion.created
        assert await repository.list_prepared_commands(session_id) == ()
        assert await repository.load_command_receipt(_identity(entry_command.command_id)) == entry_receipt
        assert not (
            await repository.complete_command(
                entry_command,
                entry_receipt,
                model_state_schema_version="sim-liquidity-state.v1",
                model_state_payload={"session_id": session_id, "symbol": "BTCUSDT", "remaining": []},
                events=(entered,),
            )
        ).created

        exit_command = SimulationCommandV1(
            source="market-simulator-test",
            trade=trade,
            command_kind="TIMEOUT_EXIT_IOC",
            quantity=0.01,
            price_cap=99.9,
            submitted_at_ms=_T0 + 180_000,
            persisted_at_ms=_T0 + 180_001,
            eligible_at_ms=_T0 + 180_000,
            expires_at_ms=_T0 + 240_000,
        )
        assert (await repository.prepare_command(exit_command)).created
        exit_receipt = SimulationCommandReceiptV1(
            source="market-simulator-test",
            command=exit_command,
            model_frame_sha256=frames["BTCUSDT"].frame_sha256,
            status="FILLED",
            reason_codes=("MODEL_TIMEOUT_IOC",),
            arrival_at_ms=_T0 + 180_010,
            filled_quantity=0.01,
            cancelled_quantity=0.0,
            average_price=99.9,
            notional_quote=0.999,
            fee_quote=0.004995,
            arrival_mid_price=100.0,
            implementation_shortfall_quote=0.001,
            level_fills=(
                SimulationFillLevelV1(
                    book_price=99.9, execution_price=99.9, quantity=0.01, fee_quote=0.004995
                ),
            ),
        )
        flat = SimulationTradeEventV2(
            source="market-simulator-test",
            session_id=session_id,
            admission_id=admission_id,
            intent_id=intent_id,
            trade_id=trade_id,
            event_seq=3,
            previous_event_sha256=entered.event_id,
            event_type="TIMEOUT_TRIGGERED",
            from_state="ACTIVE",
            to_state="FLAT",
            occurred_at_ms=_T0 + 180_010,
            symbol="BTCUSDT",
            side="LONG",
            command_id=exit_command.command_id,
            receipt_id=exit_receipt.receipt_id,
            filled_quantity=exit_receipt.filled_quantity,
            average_price=exit_receipt.average_price,
            model_frame_sha256=exit_receipt.model_frame_sha256,
            reason_codes=("SIM_TIMEOUT",),
        )
        assert (
            await repository.complete_command(
                exit_command,
                exit_receipt,
                model_state_schema_version="sim-liquidity-state.v1",
                model_state_payload={"session_id": session_id, "symbol": "BTCUSDT", "remaining": []},
                events=(flat,),
            )
        ).created
        journal = await repository.load_trade_journal(trade_id)
        assert journal is not None
        assert journal.trade == trade
        assert journal.state == "FLAT"
        assert journal.next_event_seq == 4
        assert journal.journal_head_sha256 == flat.event_id
        assert journal.events == (admitted, entered, flat)
        assert await repository.list_terminal_trades_without_result(session_id) == (trade,)
        result = SimulationResultV1(
            source="market-simulator-test",
            session_id=session_id,
            admission_id=admission_id,
            intent_id=intent_id,
            trade_id=trade_id,
            terminal_event_id=_identity(flat.event_id),
            completed_at_ms=_T0 + 180_011,
            final_state="FLAT",
            entry_filled_quantity=0.01,
            exit_filled_quantity=0.01,
            entry_average_price=100.2,
            exit_average_price=99.9,
            model_realized_pnl_quote=-0.003,
            model_fee_quote=0.010005,
            reason_codes=("SIM_TIMEOUT",),
        )
        assert await repository.record_result(result)
        assert await repository.list_terminal_trades_without_result(session_id) == ()
        receipt = SimulationSessionReceiptV1(
            source="market-simulator-test",
            session=session,
            receipt_state="COMPLETED",
            completed_at_ms=_T0 + 180_012,
            command_count=2,
            trade_journals=(
                SimulationTradeJournalHeadV1(
                    trade_id=trade_id,
                    state="FLAT",
                    event_count=3,
                    journal_head_sha256=_identity(flat.event_id),
                ),
            ),
        )
        assert await repository.record_session_receipt(receipt)
        assert await repository.verify_trade_chain(trade_id)
        state = await repository.load_liquidity_state(session_id, "BTCUSDT")
        assert state is not None and state[0] == "sim-liquidity-state.v1"

        blocked_tape_id = f"tape-barrier-{uuid4().hex}"
        barrier = RecordedTopNBookFrameV1(
            source="sim-recorder-test",
            tape_id=blocked_tape_id,
            stream_epoch="test-epoch-1",
            symbol="BTCUSDT",
            tape_sequence=1,
            exchange_update_id=1,
            exchange_at_ms=_T0,
            received_at_ms=_T0 + 1,
            persisted_at_ms=_T0 + 2,
            raw_payload_sha256=_hash("blocked-book"),
            continuity="GAP",
        )
        assert await repository.record_book_frame(barrier)
        with pytest.raises(ValueError, match="gap or reconnect"):
            await repository.seal_tape(blocked_tape_id, sealed_at_ms=_T0 + 1_000)
    finally:
        await database.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simulator_v2_book_frames_preserve_raw_evidence_and_resume_cursor() -> None:
    settings, database_name = _settings()
    database = Database(settings, migration_profile=MigrationProfile.SIMULATOR)
    await connect_verified_database(database, database_name, local_only=True)
    try:
        await database.migrate()
        repository = SimulationRepository(database.pool)
        tape_id = f"tape-v2-{uuid4().hex}"
        raw_payload = '{"lastUpdateId":101,"bids":[["99.9","2"]],"asks":[["100.1","2"]]}'
        frame = RecordedTopNBookFrameV2(
            source="sim-recorder-test",
            tape_id=tape_id,
            stream_epoch="test-epoch-1",
            symbol="BTCUSDT",
            tape_sequence=1,
            exchange_update_id=101,
            exchange_at_ms=_T0 + 60_000,
            received_at_ms=_T0 + 60_010,
            persisted_at_ms=_T0 + 60_020,
            raw_payload=raw_payload,
            raw_payload_sha256=_hash(raw_payload),
            continuity="ADMITTED",
            source_reason="SNAPSHOT_RECEIVED",
            bids=(RecordedBookLevelV1(price=99.9, quantity=2.0),),
            asks=(RecordedBookLevelV1(price=100.1, quantity=2.0),),
        )

        assert await repository.load_open_book_recording_cursor(tape_id) is None
        assert await repository.record_book_frame(frame)
        assert not await repository.record_book_frame(frame)
        cursor = await repository.load_open_book_recording_cursor(tape_id)
        assert cursor is not None
        assert cursor.tape_id == tape_id
        assert cursor.next_tape_sequence == 2
        assert cursor.previous_frame_sha256 == frame.frame_sha256
        assert cursor.symbol_cursors[0].symbol == "BTCUSDT"
        durable = await database.pool.fetchrow(
            """SELECT frame_contract_version, source_reason, raw_payload_text
               FROM sim_book_frames WHERE tape_id=$1 AND tape_sequence=1""",
            tape_id,
        )
        assert durable is not None
        assert dict(durable) == {
            "frame_contract_version": "sim-book-frame.v2",
            "source_reason": "SNAPSHOT_RECEIVED",
            "raw_payload_text": raw_payload,
        }
        with pytest.raises(asyncpg.CheckViolationError):
            await database.pool.execute(
                "UPDATE sim_book_frames SET source_reason=NULL WHERE tape_id=$1 AND tape_sequence=1",
                tape_id,
            )
        assert await repository.verify_tape(tape_id)
    finally:
        await database.close()
