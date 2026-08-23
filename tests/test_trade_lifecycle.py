from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from kairos_core import (
    CandidateReviewTier,
    CandidateReviewV1,
    CandidateRouteV1,
    EntryPolicy,
    EvedexProfile,
    ExitPlanV1,
    OrderRole,
    ReasoningEffort,
    ReviewDecision,
    RiskTradeDecisionV1,
    Side,
    StrategyIntentV1,
    StrategyProvenanceV1,
    TradeExecutionEventType,
    TradeExecutionEventV1,
    TradeLifecycleState,
    TradingMode,
    VenueQualityV1,
)

from kairos_persistence import (
    AuditRepository,
    Database,
    MessageIdentityConflict,
    NewTrade,
    PersistenceSettings,
    TradeLifecycleRepository,
    TradeMutationResult,
    TradeState,
    validate_trade_transition,
)

T0 = 1_800_000_000_000


def _decision() -> RiskTradeDecisionV1:
    intent = StrategyIntentV1(
        source="strategy-engine",
        strategy_id="canary",
        strategy_revision="v1",
        symbol="BTCUSDT",
        side=Side.LONG,
        decision_ts_ms=T0 + 59_999,
        entry_eligible_ts_ms=T0 + 60_000,
        entry_expires_ts_ms=T0 + 120_000,
        reference_price=100.0,
        signal_strength=0.7,
        gross_reward_bps=500.0,
        exit_plan=ExitPlanV1(stop_price=95.0, target_price=105.0, max_holding_ms=180_000),
        provenance=StrategyProvenanceV1(
            strategy_code_sha256="a" * 64,
            config_sha256="b" * 64,
            input_window_sha256="c" * 64,
            features_sha256="d" * 64,
            input_bar_sha256s=("a" * 64, "b" * 64),
        ),
    )
    route = CandidateRouteV1(
        source="router",
        intent=intent,
        review_tier=CandidateReviewTier.NORMAL,
        requested_reasoning_effort=ReasoningEffort.MEDIUM,
        routed_at_ms=T0 + 60_100,
        review_deadline_ms=T0 + 119_000,
        evidence_ids=("quant:fixture",),
    )
    review = CandidateReviewV1(
        source="aggregator",
        route=route,
        intent=intent,
        decision=ReviewDecision.ALLOW,
        priority=60,
        reviewed_at_ms=T0 + 60_200,
        reviewer="DETERMINISTIC",
        reason_codes=("NO_CONFLICT",),
    )
    venue = VenueQualityV1(
        source="venue-gate",
        profile=EvedexProfile.DEV,
        symbol="BTCUSD:DEV",
        observed_at_ms=T0 + 60_300,
        expires_at_ms=T0 + 65_000,
        reference_timestamp_ms=T0 + 60_000,
        book_timestamp_ms=T0 + 60_100,
        reference_mid_price=100.0,
        best_bid=100.49,
        best_ask=100.51,
        venue_mid_price=100.5,
        basis_bps=50.0,
        spread_bps=(100.51 - 100.49) / 100.5 * 10_000,
        assessed_notional_usd=10.051,
        depth_usd=5_000.0,
        buy_slippage_bps=1.0,
        sell_slippage_bps=1.2,
        taker_fee_bps=5.0,
        reference_age_ms=300,
        book_age_ms=200,
        latency_ms=80,
        timestamp_skew_ms=100,
        entry_allowed=True,
    )
    quantity = 0.1
    fees = quantity * (venue.best_ask + intent.exit_plan.stop_price) * venue.taker_fee_bps / 10_000
    slippage = quantity * venue.venue_mid_price * venue.buy_slippage_bps / 10_000
    worst_loss = quantity * abs(venue.best_ask - intent.exit_plan.stop_price) + fees + slippage
    return RiskTradeDecisionV1(
        source="risk-manager",
        intent=intent,
        review=review,
        venue_quality=venue,
        approved=True,
        decided_at_ms=T0 + 60_400,
        entry_policy=EntryPolicy.NEXT_BAR_MARKET,
        trading_mode=TradingMode.PAPER,
        evedex_profile=EvedexProfile.DEV,
        account_id="paper-dev-1",
        venue_symbol="BTCUSD:DEV",
        quantity=quantity,
        leverage=1.0,
        notional_usd=quantity * venue.best_ask,
        loss_budget_usd=1.0,
        worst_case_loss_usd=worst_loss,
        worst_entry_price=venue.best_ask,
        estimated_fees_usd=fees,
        estimated_slippage_usd=slippage,
        exit_plan=intent.exit_plan,
    )


def _trade(**updates: object) -> NewTrade:
    decision = _decision()
    assert decision.trade_id is not None
    assert decision.intent.intent_id is not None
    assert decision.decision_id is not None
    values: dict[str, object] = {
        "trade_id": decision.trade_id,
        "strategy_intent_id": decision.intent.intent_id,
        "risk_decision_id": decision.decision_id,
        "risk_decision_payload": decision.to_payload(),
        "strategy_id": "canary",
        "strategy_revision": "v1",
        "trading_mode": "PAPER",
        "environment": "evedex-dev",
        "profile": "DEV",
        "exchange": "evedex",
        "account_id": "paper-dev-1",
        "symbol": "BTCUSDT",
        "venue_symbol": "BTCUSD:DEV",
        "side": "BUY",
        "quantity": 0.1,
        "leverage": 1.0,
        "stop_price": 95.0,
        "target_price": 105.0,
        "entry_eligible_at": datetime.fromtimestamp((T0 + 60_000) / 1_000, tz=UTC),
        "entry_expires_at": datetime.fromtimestamp((T0 + 120_000) / 1_000, tz=UTC),
        "max_holding_ms": 180_000,
        "entry_client_order_id": "00395:0123456789ABCDEF0123456789",
    }
    values.update(updates)
    return NewTrade(**values)  # type: ignore[arg-type]


def test_trade_geometry_and_lineage_are_fail_closed() -> None:
    assert TradeState is TradeLifecycleState
    _trade().validate()

    with pytest.raises(ValueError, match="BUY stop"):
        _trade(stop_price=106.0).validate()
    with pytest.raises(ValueError, match="PAPER or LIVE"):
        _trade(trading_mode="DRY_RUN").validate()
    with pytest.raises(ValueError, match="timezone-aware"):
        _trade(entry_expires_at=datetime(2026, 8, 23, 12, 1)).validate()
    with pytest.raises(ValueError, match="finite"):
        _trade(quantity=float("nan")).validate()
    with pytest.raises(ValueError, match="venue_symbol"):
        _trade(venue_symbol="ETHUSD:DEV").validate()
    payload_with_unknown = _decision().to_payload()
    payload_with_unknown["unknown"] = True
    with pytest.raises(ValueError, match="Extra inputs"):
        _trade(risk_decision_payload=payload_with_unknown).validate()


def test_lifecycle_requires_protection_before_active_and_serializes_exit_choice() -> None:
    validate_trade_transition(TradeState.RECEIVED, TradeState.ENTRY_PENDING)
    validate_trade_transition(TradeState.ENTRY_PENDING, TradeState.PROTECTING)
    validate_trade_transition(TradeState.PROTECTING, TradeState.ACTIVE)
    validate_trade_transition(TradeState.PROTECTING, TradeState.EXITING_STOP)
    validate_trade_transition(TradeState.PROTECTING, TradeState.EXITING_TARGET)
    validate_trade_transition(TradeState.ACTIVE, TradeState.ACTIVE)
    validate_trade_transition(TradeState.ACTIVE, TradeState.EXITING_TIMEOUT)
    validate_trade_transition(TradeState.EXITING_TIMEOUT, TradeState.FLAT)

    with pytest.raises(ValueError, match="invalid trade transition"):
        validate_trade_transition(TradeState.ENTRY_PENDING, TradeState.ACTIVE)
    with pytest.raises(ValueError, match="invalid trade transition"):
        validate_trade_transition(TradeState.EXITING_STOP, TradeState.EXITING_TARGET)
    with pytest.raises(ValueError, match="invalid trade transition"):
        validate_trade_transition(TradeState.FLAT, TradeState.ACTIVE)


def test_migration_enforces_one_active_trade_and_restart_barrier() -> None:
    migration = (
        Path(__file__).parents[1] / "kairos_persistence" / "migrations" / "006_paper_trade_lifecycle.sql"
    ).read_text(encoding="utf-8")

    assert "execution_trades_one_active_symbol_idx" in migration
    assert "execution_recovery_state" in migration
    assert "execution_trade_events" in migration
    assert "TAKE_PROFIT" in migration
    assert "TIMEOUT_CLOSE" in migration
    assert "venue_symbol ~ '^[A-Z0-9]+:DEV$'" in migration
    assert "WHERE state NOT IN ('FLAT', 'CANCELLED');" in migration

    public_migration = (
        Path(__file__).parents[1] / "kairos_persistence" / "migrations" / "008_public_execution_events.sql"
    ).read_text(encoding="utf-8")
    assert "public_execution_events" in public_migration
    assert "UNIQUE (trade_id, event_seq)" in public_migration
    assert "payload_sha256" in public_migration


@pytest.mark.asyncio
async def test_atomic_mutation_arguments_fail_before_database_access() -> None:
    repository = TradeLifecycleRepository(None)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="fact_key"):
        await repository.create_with_execution_event(
            _trade(),
            fact_key=" not-normalized ",
            build=lambda _sequence: None,  # type: ignore[arg-type,return-value]
        )
    with pytest.raises(TypeError, match="callable"):
        await repository.transition_with_execution_event(
            _trade().trade_id,
            TradeState.ENTRY_PENDING,
            event_type="ENTRY_PREPARED",
            fact_key="entry-prepared",
            build=None,  # type: ignore[arg-type]
        )
    with pytest.raises(TypeError, match="include_terminal"):
        await repository.list_trades_for_scope(
            environment="evedex-dev",
            account_id="paper-dev-1",
            exchange="evedex",
            include_terminal=1,  # type: ignore[arg-type]
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_atomic_trade_mutations_roll_back_replay_and_audit_terminal_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url = os.getenv("KAIROS_PERSISTENCE_DATABASE_URL")
    if not database_url:
        pytest.skip("KAIROS_PERSISTENCE_DATABASE_URL is required for integration tests")
    database = Database(PersistenceSettings(database_url=database_url))
    await database.connect()
    await database.migrate()
    repository = TradeLifecycleRepository(database.pool)
    trade_input = _trade()
    trade_id = trade_input.trade_id
    event_ids: list[str] = []

    def execution_event(
        sequence: int,
        *,
        event_type: TradeExecutionEventType,
        state: TradeLifecycleState,
        occurred_at_ms: int,
    ) -> TradeExecutionEventV1:
        detail: dict[str, Any] = {}
        if event_type is TradeExecutionEventType.EFFECT_PREPARED:
            detail = {
                "effect_id": f"entry-place:{trade_id}",
                "order_role": OrderRole.ENTRY,
                "client_order_id": trade_input.entry_client_order_id,
                "requested_quantity": trade_input.quantity,
            }
        return TradeExecutionEventV1(
            source="execution-engine",
            event_seq=sequence,
            occurred_at_ms=occurred_at_ms,
            event_type=event_type,
            lifecycle_state=state,
            trading_mode=TradingMode.PAPER,
            evedex_profile=EvedexProfile.DEV,
            account_id=trade_input.account_id,
            venue_symbol=trade_input.venue_symbol,
            strategy_id=trade_input.strategy_id,
            strategy_revision=trade_input.strategy_revision,
            intent_id=trade_input.strategy_intent_id,
            risk_decision_id=trade_input.risk_decision_id,
            trade_id=trade_id,
            **detail,
        )

    received = lambda sequence: execution_event(  # noqa: E731
        sequence,
        event_type=TradeExecutionEventType.DECISION_RECEIVED,
        state=TradeLifecycleState.RECEIVED,
        occurred_at_ms=T0 + 60_400,
    )
    prepared = lambda sequence: execution_event(  # noqa: E731
        sequence,
        event_type=TradeExecutionEventType.EFFECT_PREPARED,
        state=TradeLifecycleState.ENTRY_PENDING,
        occurred_at_ms=T0 + 60_500,
    )
    cancelled = lambda sequence: execution_event(  # noqa: E731
        sequence,
        event_type=TradeExecutionEventType.ENTRY_CANCELLED,
        state=TradeLifecycleState.CANCELLED,
        occurred_at_ms=T0 + 60_600,
    )
    original_enqueue_outbox = AuditRepository.enqueue_outbox

    async def fail_after_outbox_insert(
        audit: AuditRepository,
        connection: Any,
        message_id: str,
        topic: str,
        payload: str,
        payload_sha256: str,
    ) -> bool:
        await original_enqueue_outbox(
            audit,
            connection,
            message_id,
            topic,
            payload,
            payload_sha256,
        )
        raise RuntimeError("injected failure after outbox insert")

    def fail_event_builder(_sequence: int) -> TradeExecutionEventV1:
        raise RuntimeError("injected event builder failure")

    try:
        await database.pool.execute("DELETE FROM public_execution_events WHERE trade_id=$1", trade_id)
        await database.pool.execute("DELETE FROM execution_trade_events WHERE trade_id=$1", trade_id)
        await database.pool.execute("DELETE FROM execution_trades WHERE trade_id=$1", trade_id)

        with pytest.raises(RuntimeError, match="event builder failure"):
            await repository.create_with_execution_event(
                trade_input,
                fact_key="decision-builder-failure",
                build=fail_event_builder,
            )
        assert await repository.get(trade_id) is None

        monkeypatch.setattr(AuditRepository, "enqueue_outbox", fail_after_outbox_insert)
        with pytest.raises(RuntimeError, match="after outbox insert"):
            await repository.create_with_execution_event(
                trade_input,
                fact_key="decision-received",
                build=received,
            )
        assert await repository.get(trade_id) is None
        assert (
            await database.pool.fetchval(
                "SELECT count(*) FROM execution_trade_events WHERE trade_id=$1", trade_id
            )
            == 0
        )
        assert (
            await database.pool.fetchval(
                "SELECT count(*) FROM public_execution_events WHERE trade_id=$1", trade_id
            )
            == 0
        )
        failed_create_event_id = received(1).event_id
        assert failed_create_event_id is not None
        assert (
            await database.pool.fetchval(
                "SELECT count(*) FROM event_audit WHERE message_id=$1", failed_create_event_id
            )
            == 0
        )
        assert (
            await database.pool.fetchval(
                "SELECT count(*) FROM message_outbox WHERE message_id=$1", failed_create_event_id
            )
            == 0
        )

        monkeypatch.setattr(AuditRepository, "enqueue_outbox", original_enqueue_outbox)
        created = await repository.create_with_execution_event(
            trade_input,
            fact_key="decision-received",
            build=received,
        )
        assert isinstance(created, TradeMutationResult)
        assert created.trade.state is TradeState.RECEIVED
        assert created.trade.state_version == 0
        assert created.event.event_seq == 1
        assert created.event.event_id is not None
        event_ids.append(created.event.event_id)

        with pytest.raises(RuntimeError, match="event builder failure"):
            await repository.transition_with_execution_event(
                trade_id,
                TradeState.ENTRY_PENDING,
                event_type="ENTRY_PREPARED",
                event_payload={"reason": "builder-failure"},
                fact_key="entry-builder-failure",
                build=fail_event_builder,
            )
        after_builder_failure = await repository.get(trade_id)
        assert after_builder_failure is not None
        assert after_builder_failure.state is TradeState.RECEIVED
        assert after_builder_failure.state_version == 0

        monkeypatch.setattr(AuditRepository, "enqueue_outbox", fail_after_outbox_insert)
        with pytest.raises(RuntimeError, match="after outbox insert"):
            await repository.transition_with_execution_event(
                trade_id,
                TradeState.ENTRY_PENDING,
                event_type="ENTRY_PREPARED",
                event_payload={"reason": "atomic-test"},
                fact_key="entry-prepared",
                build=prepared,
            )
        rolled_back = await repository.get(trade_id)
        assert rolled_back is not None
        assert rolled_back.state is TradeState.RECEIVED
        assert rolled_back.state_version == 0
        assert (
            await database.pool.fetchval(
                "SELECT count(*) FROM execution_trade_events WHERE trade_id=$1", trade_id
            )
            == 1
        )
        assert (
            await database.pool.fetchval(
                "SELECT count(*) FROM public_execution_events WHERE trade_id=$1", trade_id
            )
            == 1
        )
        failed_transition_event_id = prepared(2).event_id
        assert failed_transition_event_id is not None
        assert (
            await database.pool.fetchval(
                "SELECT count(*) FROM event_audit WHERE message_id=$1", failed_transition_event_id
            )
            == 0
        )
        assert (
            await database.pool.fetchval(
                "SELECT count(*) FROM message_outbox WHERE message_id=$1", failed_transition_event_id
            )
            == 0
        )

        monkeypatch.setattr(AuditRepository, "enqueue_outbox", original_enqueue_outbox)
        pending_results = await asyncio.gather(
            *(
                repository.transition_with_execution_event(
                    trade_id,
                    TradeState.ENTRY_PENDING,
                    event_type="ENTRY_PREPARED",
                    event_payload={"reason": "atomic-test"},
                    fact_key="entry-prepared",
                    build=prepared,
                )
                for _ in range(2)
            )
        )
        pending = pending_results[0]
        assert pending_results[1].event == pending.event
        assert pending_results[1].trade.state_version == 1
        assert pending.trade.state_version == 1
        assert pending.event.event_seq == 2
        assert pending.event.event_id is not None
        event_ids.append(pending.event.event_id)

        terminal = await repository.transition_with_execution_event(
            trade_id,
            TradeState.CANCELLED,
            event_type="ENTRY_EXPIRED",
            event_payload={"reason": "fixture-expiry"},
            fact_key="entry-cancelled",
            build=cancelled,
        )
        assert terminal.trade.state is TradeState.CANCELLED
        assert terminal.event.event_id is not None
        event_ids.append(terminal.event.event_id)

        replay = await repository.transition_with_execution_event(
            trade_id,
            TradeState.ENTRY_PENDING,
            event_type="ENTRY_PREPARED",
            event_payload={"reason": "atomic-test"},
            fact_key="entry-prepared",
            build=prepared,
        )
        assert replay.trade.state is TradeState.CANCELLED
        assert replay.trade.state_version == 2
        assert replay.event == pending.event
        assert (
            await database.pool.fetchval(
                "SELECT count(*) FROM execution_trade_events WHERE trade_id=$1", trade_id
            )
            == 3
        )
        with pytest.raises(MessageIdentityConflict, match="different transition request"):
            await repository.transition_with_execution_event(
                trade_id,
                TradeState.ENTRY_PENDING,
                event_type="ENTRY_PREPARED",
                event_payload={"reason": "changed"},
                fact_key="entry-prepared",
                build=prepared,
            )

        creation_replay = await repository.create_with_execution_event(
            trade_input,
            fact_key="decision-received",
            build=received,
        )
        assert creation_replay.trade.state is TradeState.CANCELLED
        assert creation_replay.event == created.event
        assert [
            item.trade_id
            for item in await repository.list_trades_for_scope(
                environment=trade_input.environment,
                account_id=trade_input.account_id,
                exchange=trade_input.exchange,
            )
        ] == [trade_id]
        assert (
            await repository.list_trades_for_scope(
                environment=trade_input.environment,
                account_id=trade_input.account_id,
                exchange=trade_input.exchange,
                include_terminal=False,
            )
            == []
        )
        assert await repository.verify_chain(trade_id)
        assert (
            await database.pool.fetchval(
                "SELECT count(*) FROM message_outbox WHERE message_id=ANY($1::text[])", event_ids
            )
            == 3
        )
    finally:
        monkeypatch.setattr(AuditRepository, "enqueue_outbox", original_enqueue_outbox)
        if event_ids:
            await database.pool.execute(
                "DELETE FROM message_outbox WHERE message_id=ANY($1::text[])", event_ids
            )
            await database.pool.execute("DELETE FROM event_audit WHERE message_id=ANY($1::text[])", event_ids)
        await database.pool.execute("DELETE FROM public_execution_events WHERE trade_id=$1", trade_id)
        await database.pool.execute("DELETE FROM execution_trade_events WHERE trade_id=$1", trade_id)
        await database.pool.execute("DELETE FROM execution_trades WHERE trade_id=$1", trade_id)
        await database.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_public_execution_fact_and_outbox_are_atomic_replay_safe_and_scoped() -> None:
    database_url = os.getenv("KAIROS_PERSISTENCE_DATABASE_URL")
    if not database_url:
        pytest.skip("KAIROS_PERSISTENCE_DATABASE_URL is required for integration tests")
    database = Database(PersistenceSettings(database_url=database_url))
    await database.connect()
    await database.migrate()
    repository = TradeLifecycleRepository(database.pool)
    trade_input = _trade()
    decision = RiskTradeDecisionV1.model_validate(trade_input.risk_decision_payload)
    trade_id = trade_input.trade_id
    event_ids: list[str] = []

    def received(sequence: int, *, occurred_at_ms: int = T0 + 60_400) -> TradeExecutionEventV1:
        return TradeExecutionEventV1(
            source="execution-engine",
            event_seq=sequence,
            occurred_at_ms=occurred_at_ms,
            event_type=TradeExecutionEventType.DECISION_RECEIVED,
            lifecycle_state=TradeLifecycleState.RECEIVED,
            trading_mode=TradingMode.PAPER,
            evedex_profile=EvedexProfile.DEV,
            account_id=trade_input.account_id,
            venue_symbol=trade_input.venue_symbol,
            strategy_id=trade_input.strategy_id,
            strategy_revision=trade_input.strategy_revision,
            intent_id=trade_input.strategy_intent_id,
            risk_decision_id=trade_input.risk_decision_id,
            trade_id=trade_id,
        )

    try:
        await database.pool.execute("DELETE FROM public_execution_events WHERE trade_id=$1", trade_id)
        await database.pool.execute("DELETE FROM execution_trade_events WHERE trade_id=$1", trade_id)
        await database.pool.execute("DELETE FROM execution_trades WHERE trade_id=$1", trade_id)
        trade = await repository.create(trade_input)
        first = await repository.append_execution_event(
            trade_id,
            fact_key="decision-received",
            expected_state_version=trade.state_version,
            build=received,
        )
        assert first.event_seq == 1
        assert first.event_id is not None
        event_ids.append(first.event_id)
        assert (
            await database.pool.fetchval(
                "SELECT count(*) FROM message_outbox WHERE message_id=$1", first.event_id
            )
            == 1
        )

        pending = await repository.transition(
            trade_id,
            TradeState.ENTRY_PENDING,
            event_type="ENTRY_EFFECT_READY",
        )
        assert (
            await repository.append_execution_event(
                trade_id,
                fact_key="decision-received",
                expected_state_version=0,
                build=received,
            )
            == first
        )
        with pytest.raises(MessageIdentityConflict, match="different content"):
            await repository.append_execution_event(
                trade_id,
                fact_key="decision-received",
                expected_state_version=0,
                build=lambda sequence: received(sequence, occurred_at_ms=T0 + 60_401),
            )

        def prepared(sequence: int) -> TradeExecutionEventV1:
            return TradeExecutionEventV1(
                source="execution-engine",
                event_seq=sequence,
                occurred_at_ms=T0 + 60_500,
                event_type=TradeExecutionEventType.EFFECT_PREPARED,
                lifecycle_state=TradeLifecycleState.ENTRY_PENDING,
                trading_mode=TradingMode.PAPER,
                evedex_profile=EvedexProfile.DEV,
                account_id=trade_input.account_id,
                venue_symbol=trade_input.venue_symbol,
                strategy_id=trade_input.strategy_id,
                strategy_revision=trade_input.strategy_revision,
                intent_id=trade_input.strategy_intent_id,
                risk_decision_id=trade_input.risk_decision_id,
                trade_id=trade_id,
                effect_id=f"entry-place:{trade_id}",
                order_role=OrderRole.ENTRY,
                client_order_id=trade_input.entry_client_order_id,
                requested_quantity=decision.quantity,
            )

        second = await repository.append_execution_event(
            trade_id,
            fact_key="entry-effect-prepared",
            expected_state_version=pending.state_version,
            build=prepared,
        )
        assert second.event_seq == 2
        assert second.event_id is not None
        event_ids.append(second.event_id)
        events = await repository.list_execution_events(trade_id)
        assert [event.event_seq for event in events] == [1, 2]
        scoped = await repository.list_execution_events_for_scope(
            environment=trade_input.environment,
            account_id=trade_input.account_id,
            exchange=trade_input.exchange,
        )
        assert [event.event_id for event in scoped] == [first.event_id, second.event_id]

        first_entered = asyncio.Event()
        release_first = asyncio.Event()
        second_entered = asyncio.Event()

        async def first_holder() -> None:
            async with repository.account_lock(
                environment="evedex-dev",
                account_id="remote-dev-account",
                exchange="evedex",
            ):
                first_entered.set()
                await release_first.wait()

        async def second_holder() -> None:
            await first_entered.wait()
            async with repository.account_lock(
                environment="evedex-dev",
                account_id="remote-dev-account",
                exchange="evedex",
            ):
                second_entered.set()

        first_task = asyncio.create_task(first_holder())
        second_task = asyncio.create_task(second_holder())
        await first_entered.wait()
        await asyncio.sleep(0.05)
        assert not second_entered.is_set()
        release_first.set()
        await asyncio.gather(first_task, second_task)
        assert second_entered.is_set()
    finally:
        if event_ids:
            await database.pool.execute(
                "DELETE FROM message_outbox WHERE message_id=ANY($1::text[])", event_ids
            )
            await database.pool.execute("DELETE FROM event_audit WHERE message_id=ANY($1::text[])", event_ids)
        await database.pool.execute("DELETE FROM public_execution_events WHERE trade_id=$1", trade_id)
        await database.pool.execute("DELETE FROM execution_trade_events WHERE trade_id=$1", trade_id)
        await database.pool.execute("DELETE FROM execution_trades WHERE trade_id=$1", trade_id)
        await database.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_trade_lifecycle_recovery_race_chain_and_equity_are_durable() -> None:
    database_url = os.getenv("KAIROS_PERSISTENCE_DATABASE_URL")
    if not database_url:
        pytest.skip("KAIROS_PERSISTENCE_DATABASE_URL is required for integration tests")
    database = Database(PersistenceSettings(database_url=database_url))
    await database.connect()
    await database.migrate()
    repository = TradeLifecycleRepository(database.pool)
    trade = _trade()
    trade_id = trade.trade_id
    environment = trade.environment
    account_id = trade.account_id
    exchange = trade.exchange

    await database.pool.execute("DELETE FROM execution_trade_events WHERE trade_id=$1", trade_id)
    await database.pool.execute("DELETE FROM execution_trades WHERE trade_id=$1", trade_id)
    await database.pool.execute(
        "DELETE FROM execution_recovery_state WHERE environment=$1 AND account_id=$2 AND exchange=$3",
        environment,
        account_id,
        exchange,
    )
    await database.pool.execute(
        "DELETE FROM account_equity_state WHERE environment=$1 AND account_id=$2 AND exchange=$3",
        environment,
        account_id,
        exchange,
    )
    try:
        created = await repository.create(trade)
        assert created == await repository.create(trade)
        with pytest.raises(MessageIdentityConflict):
            await repository.create(replace(trade, entry_client_order_id="different-entry-id"))

        first_recovery = await repository.begin_recovery(
            environment=environment,
            account_id=account_id,
            exchange=exchange,
        )
        second_recovery = await repository.begin_recovery(
            environment=environment,
            account_id=account_id,
            exchange=exchange,
        )
        assert second_recovery.recovery_epoch == first_recovery.recovery_epoch + 1
        with pytest.raises(RuntimeError, match="stale"):
            await repository.complete_recovery(
                environment=environment,
                account_id=account_id,
                exchange=exchange,
                expected_epoch=first_recovery.recovery_epoch,
                detail="old process finished late",
            )
        assert not await repository.entries_allowed(
            environment=environment,
            account_id=account_id,
            exchange=exchange,
        )
        completed = await repository.complete_recovery(
            environment=environment,
            account_id=account_id,
            exchange=exchange,
            expected_epoch=second_recovery.recovery_epoch,
            detail="authoritative venue state reconciled",
        )
        assert not completed.entries_blocked

        pending = await repository.transition(
            trade_id,
            TradeState.ENTRY_PENDING,
            event_type="ENTRY_PREPARED",
            entry_exchange_order_id="venue-entry-1",
        )
        first_fill_at = datetime(2027, 1, 15, 8, tzinfo=UTC)
        protecting = await repository.transition(
            trade_id,
            TradeState.PROTECTING,
            event_type="ENTRY_PARTIAL_FILL",
            filled_quantity=0.04,
            first_fill_at=first_fill_at,
            stop_client_order_id="stop-client-1",
            stop_exchange_order_id="venue-stop-1",
        )
        assert protecting.timeout_at == first_fill_at + timedelta(milliseconds=trade.max_holding_ms)
        with pytest.raises(ValueError, match="monotonic"):
            await repository.transition(
                trade_id,
                TradeState.PROTECTING,
                event_type="BAD_FILL",
                filled_quantity=0.03,
            )
        with pytest.raises(MessageIdentityConflict, match="first_fill_at"):
            await repository.transition(
                trade_id,
                TradeState.PROTECTING,
                event_type="BAD_CLOCK",
                filled_quantity=0.05,
                first_fill_at=first_fill_at + timedelta(seconds=1),
            )
        with pytest.raises(MessageIdentityConflict, match="stop_client_order_id"):
            await repository.transition(
                trade_id,
                TradeState.PROTECTING,
                event_type="BAD_STOP_ID",
                stop_client_order_id="different-stop-id",
            )

        active = await repository.transition(
            trade_id,
            TradeState.ACTIVE,
            event_type="TARGET_RECONCILED",
            target_client_order_id="target-client-1",
            target_exchange_order_id="venue-target-1",
        )
        active = await repository.transition(
            trade_id,
            TradeState.ACTIVE,
            event_type="LATE_ENTRY_FILL_RECONCILED",
            filled_quantity=trade.quantity,
            first_fill_at=first_fill_at,
        )
        assert active.timeout_at == protecting.timeout_at
        assert pending.entry_exchange_order_id == "venue-entry-1"

        exit_results = await asyncio.gather(
            repository.transition(trade_id, TradeState.EXITING_STOP, event_type="STOP_TRIGGERED"),
            repository.transition(trade_id, TradeState.EXITING_TARGET, event_type="TARGET_TRIGGERED"),
            return_exceptions=True,
        )
        winners = [result for result in exit_results if not isinstance(result, BaseException)]
        losers = [result for result in exit_results if isinstance(result, BaseException)]
        assert len(winners) == 1
        assert len(losers) == 1
        assert isinstance(losers[0], ValueError)
        winner = winners[0]
        assert winner.state in {TradeState.EXITING_STOP, TradeState.EXITING_TARGET}
        await repository.transition(trade_id, TradeState.FLAT, event_type="EXIT_RECONCILED_FLAT")
        assert await repository.verify_chain(trade_id)
        assert await repository.next_event_sequence(trade_id) == 1
        assert await repository.next_event_sequence(trade_id) == 2

        first_equity = await repository.record_equity(
            environment=environment,
            account_id=account_id,
            exchange=exchange,
            captured_at=first_fill_at,
            equity_usd=100.0,
        )
        drawdown = await repository.record_equity(
            environment=environment,
            account_id=account_id,
            exchange=exchange,
            captured_at=first_fill_at + timedelta(minutes=1),
            equity_usd=90.0,
        )
        new_peak = await repository.record_equity(
            environment=environment,
            account_id=account_id,
            exchange=exchange,
            captured_at=first_fill_at + timedelta(minutes=2),
            equity_usd=120.0,
        )
        earlier_snapshot = await repository.record_equity(
            environment=environment,
            account_id=account_id,
            exchange=exchange,
            captured_at=first_fill_at - timedelta(minutes=1),
            equity_usd=80.0,
        )
        assert first_equity.reconciliation_seq == 1
        assert drawdown.day_start_equity_usd == 100.0
        assert drawdown.peak_equity_usd == 100.0
        assert new_peak.peak_equity_usd == 120.0
        assert new_peak.reconciliation_seq == 3
        assert earlier_snapshot.day_start_equity_usd == 80.0
        assert earlier_snapshot.last_equity_usd == 120.0
        assert earlier_snapshot.first_captured_at == first_fill_at - timedelta(minutes=1)
        assert earlier_snapshot.last_captured_at == first_fill_at + timedelta(minutes=2)
        assert earlier_snapshot.reconciliation_seq == 4

        await database.pool.execute(
            """UPDATE execution_trades
               SET risk_decision_payload=jsonb_set(
                 risk_decision_payload,'{account_id}','\"tampered-account\"'::jsonb)
               WHERE trade_id=$1""",
            trade_id,
        )
        with pytest.raises(MessageIdentityConflict, match="fingerprint"):
            await repository.get(trade_id)
    finally:
        await database.pool.execute("DELETE FROM execution_trade_events WHERE trade_id=$1", trade_id)
        await database.pool.execute("DELETE FROM execution_trades WHERE trade_id=$1", trade_id)
        await database.pool.execute(
            "DELETE FROM execution_recovery_state WHERE environment=$1 AND account_id=$2 AND exchange=$3",
            environment,
            account_id,
            exchange,
        )
        await database.pool.execute(
            "DELETE FROM account_equity_state WHERE environment=$1 AND account_id=$2 AND exchange=$3",
            environment,
            account_id,
            exchange,
        )
        await database.close()
