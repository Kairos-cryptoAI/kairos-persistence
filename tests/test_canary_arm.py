from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest
from kairos_core import (
    CandidateReviewTier,
    CandidateReviewV1,
    CandidateRouteV1,
    EvidenceReferenceV1,
    ExitPlanV1,
    MarketRegime,
    ReasoningEffort,
    ReviewDecision,
    Side,
    StrategicAllocation,
    StrategicTrigger,
    StrategyIntentV1,
    StrategyProvenanceV1,
    canonical_sha256,
)

from kairos_persistence import (
    Database,
    PaperCanaryArmRepository,
    PersistenceSettings,
)


def _canary(
    now_ms: int,
    *,
    symbol: str = "BTCUSDT",
    metadata_updates: dict[str, str] | None = None,
    include_instrument_evidence: bool = True,
    instrument_content_sha256: str | None = None,
    instrument_reference: str | None = None,
    route_evidence_ids: tuple[str, ...] | None = None,
) -> tuple[CandidateReviewV1, StrategicAllocation]:
    eligible_ms = (now_ms // 60_000 + 1) * 60_000
    bar_sha256 = "e" * 64
    venue_updated_at_ms = eligible_ms - 1
    venue_symbols = {
        "BTCUSDT": "BTCUSD:DEV",
        "ETHUSDT": "ETHUSD:DEV",
        "SOLUSDT": "SOLUSD:DEV",
        "BNBUSDT": "BNBUSD:DEV",
        "XRPUSDT": "XRPUSD:DEV",
    }
    venue_symbol = venue_symbols[symbol]
    instrument_rule = {
        "domain": "evedex-dev-instrument-rule.v1",
        "venue_symbol": venue_symbol,
        "venue_trading": "all",
        "venue_market_state": "OPEN",
        "venue_updated_at_ms": venue_updated_at_ms,
        "venue_lot_size": "1",
        "venue_price_increment": "0.1",
        "venue_quantity_increment": "0.001",
        "venue_multiplier": "1",
        "venue_min_volume_usd": "5",
        "venue_min_price": "0.1",
        "venue_max_price": "1000000",
        "venue_min_quantity": "0.001",
        "venue_max_quantity": "1000",
    }
    instrument_sha256 = canonical_sha256(instrument_rule)
    evidence_items = [
        EvidenceReferenceV1(
            kind="closed_bar",
            reference=f"BINANCE_UM:{symbol}:{eligible_ms - 60_000}",
            content_sha256=bar_sha256,
            observed_at_ms=eligible_ms - 1,
        )
    ]
    if include_instrument_evidence:
        evidence_items.append(
            EvidenceReferenceV1(
                kind="venue_instrument",
                reference=instrument_reference or f"EVEDEX_DEV:{venue_symbol}:{venue_updated_at_ms}",
                content_sha256=instrument_content_sha256 or instrument_sha256,
                observed_at_ms=venue_updated_at_ms,
            )
        )
    evidence = tuple(evidence_items)
    metadata = {
        "account_id": "paper-canary-test",
        "alpha_claim": "false",
        "canary_entry_order": "MARKETABLE_IOC_LIMIT",
        "canary_quantity": "0.05",
        "entry_policy": "NEXT_BAR_MARKET",
        "instrument_rules_sha256": instrument_sha256,
        "purpose": "technical_execution_canary",
        "venue_lot_size": "1",
        "venue_market_state": "OPEN",
        "venue_max_price": "1000000",
        "venue_max_quantity": "1000",
        "venue_min_price": "0.1",
        "venue_min_quantity": "0.001",
        "venue_min_volume_usd": "5",
        "venue_multiplier": "1",
        "venue_price_increment": "0.1",
        "venue_quantity_increment": "0.001",
        "venue_symbol": venue_symbol,
        "venue_trading": "all",
        "venue_updated_at_ms": str(venue_updated_at_ms),
    }
    metadata.update(metadata_updates or {})
    intent = StrategyIntentV1(
        source="kairos-paper-canary",
        strategy_id="technical-canary",
        strategy_revision="1",
        symbol=symbol,
        side=Side.LONG,
        decision_ts_ms=eligible_ms - 1,
        entry_eligible_ts_ms=eligible_ms,
        entry_expires_ts_ms=eligible_ms + 60_000,
        reference_price=100.0,
        signal_strength=0.0,
        gross_reward_bps=200.0,
        exit_plan=ExitPlanV1(
            stop_price=99.0,
            target_price=102.0,
            max_holding_ms=120_000,
        ),
        provenance=StrategyProvenanceV1(
            strategy_code_sha256="a" * 64,
            config_sha256="b" * 64,
            input_window_sha256="c" * 64,
            features_sha256="d" * 64,
            input_bar_sha256s=(bar_sha256,),
        ),
        evidence=evidence,
        metadata=tuple(metadata.items()),
    )
    route = CandidateRouteV1(
        source="kairos-paper-canary",
        intent=intent,
        review_tier=CandidateReviewTier.NORMAL,
        requested_reasoning_effort=ReasoningEffort.MEDIUM,
        correlation_id=intent.intent_id,
        causation_id=intent.message_id,
        routed_at_ms=eligible_ms - 1,
        review_deadline_ms=eligible_ms + 60_000,
        evidence_ids=route_evidence_ids or (bar_sha256, instrument_sha256),
    )
    review = CandidateReviewV1(
        source="kairos-paper-canary",
        correlation_id=intent.intent_id,
        causation_id=route.message_id,
        route=route,
        intent=intent,
        decision=ReviewDecision.ALLOW,
        priority=0,
        reviewed_at_ms=eligible_ms,
        reviewer="DETERMINISTIC",
        reason_codes=("TECHNICAL_CANARY_MANUAL_POLICY",),
        evidence=evidence,
    )
    rationale = "Manually armed EVEDEX DEV technical canary; no alpha or LLM claim."
    allocation_identity = {
        "causation_id": intent.message_id,
        "contract_version": "technical-canary-allocation.v1",
        "correlation_id": intent.intent_id,
        "max_gross_leverage": 1.0,
        "produced_at_ms": eligible_ms,
        "regime": MarketRegime.BULL.value,
        "rationale": rationale,
        "schema_version": "1.0",
        "source": "kairos-paper-canary",
        "stable_reserve_pct": 0.9975,
        "strategy_weights": {"technical-canary": 0.0025},
        "triggered_by": StrategicTrigger.SCHEDULE.value,
    }
    allocation = StrategicAllocation(
        source="kairos-paper-canary",
        message_id=canonical_sha256(allocation_identity),
        correlation_id=intent.intent_id,
        causation_id=intent.message_id,
        produced_at=datetime.fromtimestamp(eligible_ms / 1_000, tz=UTC),
        regime=MarketRegime.BULL,
        stable_reserve_pct=0.9975,
        strategy_weights={"technical-canary": 0.0025},
        max_gross_leverage=1.0,
        triggered_by=StrategicTrigger.SCHEDULE,
        rationale=rationale,
    )
    return review, allocation


def test_canary_binding_is_exact_and_does_not_accept_macro_substitution() -> None:
    review, allocation = _canary(1_800_000_000_000)
    PaperCanaryArmRepository._validate_binding(review, allocation)
    with pytest.raises(ValueError, match="1x"):
        PaperCanaryArmRepository._validate_binding(
            review,
            allocation.model_copy(update={"max_gross_leverage": 2.0}),
        )
    with pytest.raises(ValueError, match="0.25%"):
        PaperCanaryArmRepository._validate_binding(
            review,
            allocation.model_copy(update={"strategy_weights": {"technical-canary": 0.003}}),
        )
    with pytest.raises(ValueError, match="deterministic ALLOW"):
        PaperCanaryArmRepository._validate_binding(
            review.model_copy(update={"priority": 1}),
            allocation,
        )
    with pytest.raises(ValueError, match="account does not match"):
        PaperCanaryArmRepository._validate_account_binding("another-paper-account", review)


def test_canary_binding_rejects_instrument_rule_tampering_and_noncanonical_decimals() -> None:
    tampered, tampered_allocation = _canary(
        1_800_000_000_000,
        metadata_updates={"venue_trading": "partial"},
    )
    with pytest.raises(ValueError, match="fixed DEV policy"):
        PaperCanaryArmRepository._validate_binding(tampered, tampered_allocation)

    noncanonical, noncanonical_allocation = _canary(
        1_800_000_000_000,
        metadata_updates={"canary_quantity": "0.050"},
    )
    with pytest.raises(ValueError, match="canonical positive decimal"):
        PaperCanaryArmRepository._validate_binding(noncanonical, noncanonical_allocation)

    misaligned, misaligned_allocation = _canary(
        1_800_000_000_000,
        metadata_updates={"canary_quantity": "0.0505"},
    )
    with pytest.raises(ValueError, match="exact venue quantity increment"):
        PaperCanaryArmRepository._validate_binding(misaligned, misaligned_allocation)


def test_canary_binding_rejects_missing_or_tampered_instrument_evidence_and_hash() -> None:
    missing, missing_allocation = _canary(
        1_800_000_000_000,
        include_instrument_evidence=False,
    )
    with pytest.raises(ValueError, match="closed-bar and venue-instrument evidence"):
        PaperCanaryArmRepository._validate_binding(missing, missing_allocation)

    wrong_reference, wrong_reference_allocation = _canary(
        1_800_000_000_000,
        instrument_reference="EVEDEX_DEV:BTCUSD:DEV:1",
    )
    with pytest.raises(ValueError, match="venue-instrument evidence"):
        PaperCanaryArmRepository._validate_binding(wrong_reference, wrong_reference_allocation)

    forged_sha = "f" * 64
    forged, forged_allocation = _canary(
        1_800_000_000_000,
        metadata_updates={"instrument_rules_sha256": forged_sha},
        instrument_content_sha256=forged_sha,
        route_evidence_ids=("e" * 64, forged_sha),
    )
    with pytest.raises(ValueError, match="canonical payload"):
        PaperCanaryArmRepository._validate_binding(forged, forged_allocation)

    wrong_lineage, wrong_lineage_allocation = _canary(
        1_800_000_000_000,
        route_evidence_ids=("e" * 64, "f" * 64),
    )
    with pytest.raises(ValueError, match="bar and instrument-rule lineage"):
        PaperCanaryArmRepository._validate_binding(wrong_lineage, wrong_lineage_allocation)


def test_canary_binding_rejects_tampered_bar_provenance_and_misaligned_rules() -> None:
    review, allocation = _canary(1_800_000_000_000)
    evidence = list(review.intent.evidence)
    bar_index = next(index for index, item in enumerate(evidence) if item.kind == "closed_bar")
    evidence[bar_index] = evidence[bar_index].model_copy(update={"reference": "BINANCE_UM:BTCUSDT:1"})
    intent = review.intent.model_copy(update={"evidence": tuple(evidence)})
    route = review.route.model_copy(update={"intent": intent})
    tampered_review = review.model_copy(
        update={"intent": intent, "route": route, "evidence": intent.evidence}
    )
    with pytest.raises(ValueError, match="closed-bar evidence"):
        PaperCanaryArmRepository._validate_binding(tampered_review, allocation)

    bad_price, bad_price_allocation = _canary(
        1_800_000_000_000,
        metadata_updates={"venue_min_price": "0.15"},
    )
    with pytest.raises(ValueError, match="price bounds are not exact"):
        PaperCanaryArmRepository._validate_binding(bad_price, bad_price_allocation)

    bad_min_quantity, bad_min_quantity_allocation = _canary(
        1_800_000_000_000,
        metadata_updates={"venue_min_quantity": "0.0015"},
    )
    with pytest.raises(ValueError, match="min quantity is not an exact"):
        PaperCanaryArmRepository._validate_binding(
            bad_min_quantity,
            bad_min_quantity_allocation,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_canary_arm_is_single_use_durable_and_exact_review_bound() -> None:
    database_url = os.getenv("KAIROS_PERSISTENCE_DATABASE_URL")
    if not database_url:
        pytest.skip("KAIROS_PERSISTENCE_DATABASE_URL is required for integration tests")
    database = Database(PersistenceSettings(database_url=database_url))
    await database.connect()
    await database.migrate()
    repository = PaperCanaryArmRepository(database.pool)
    now_ms = int(datetime.now(UTC).timestamp() * 1_000)
    first_review, first_allocation = _canary(now_ms, symbol="BTCUSDT")
    second_review, second_allocation = _canary(now_ms + 1, symbol="ETHUSDT")
    unarmed_review, _ = _canary(now_ms + 2, symbol="SOLUSDT")
    account_id = "paper-canary-test"
    review_ids = [
        first_review.message_id,
        second_review.message_id,
        unarmed_review.message_id,
    ]
    try:
        await database.pool.execute("DELETE FROM paper_canary_arms WHERE account_id=$1", account_id)
        await database.pool.execute("DELETE FROM message_outbox WHERE message_id=ANY($1::text[])", review_ids)
        await database.pool.execute("DELETE FROM event_audit WHERE message_id=ANY($1::text[])", review_ids)

        armed = await repository.arm(
            account_id=account_id,
            review=first_review,
            allocation=first_allocation,
        )
        assert armed.status == "ARMED"
        assert (
            await database.pool.fetchval(
                "SELECT count(*) FROM message_outbox WHERE message_id=$1",
                first_review.message_id,
            )
            == 1
        )
        with pytest.raises(ValueError, match="already armed"):
            await repository.arm(
                account_id=account_id,
                review=second_review,
                allocation=second_allocation,
            )

        consumed = await repository.consume(
            account_id=account_id,
            review=first_review,
        )
        assert consumed is not None
        assert consumed.status == "CONSUMED"
        assert consumed.decided_at_ms is not None
        replay = await repository.consume(
            account_id=account_id,
            review=first_review,
        )
        assert replay == consumed
        assert await repository.consume(account_id=account_id, review=unarmed_review) is None

        second = await repository.arm(
            account_id=account_id,
            review=second_review,
            allocation=second_allocation,
        )
        assert second.status == "ARMED"
    finally:
        await database.pool.execute("DELETE FROM paper_canary_arms WHERE account_id=$1", account_id)
        await database.pool.execute("DELETE FROM message_outbox WHERE message_id=ANY($1::text[])", review_ids)
        await database.pool.execute("DELETE FROM event_audit WHERE message_id=ANY($1::text[])", review_ids)
        await database.close()
