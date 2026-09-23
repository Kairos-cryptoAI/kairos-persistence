from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from kairos_core.contracts import ClosedBarEventV1, VenueQualityV1
from kairos_core.enums import EvedexProfile, ReviewDecision, Side

from kairos_persistence.cockpit_snapshot import (
    _COCKPIT_VENUE_SYMBOLS,
    SYMBOLS,
    CockpitSnapshotError,
    CockpitSnapshotRepository,
    _bars,
    _decisions,
    _indicators,
    _venue_quality,
)
from kairos_persistence.config import PersistenceSettings
from kairos_persistence.database import Database


def closed_bar(symbol: str = "BTCUSDT", *, open_time_ms: int = 1_700_000_040_000) -> dict[str, object]:
    event = ClosedBarEventV1(
        source="quant-scouts",
        symbol=symbol,
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + 59_999,
        open=100.0,
        high=102.0,
        low=99.0,
        close=101.0,
        base_volume=2.0,
        quote_volume=201.0,
        taker_buy_base_volume=1.0,
        taker_buy_quote_volume=100.5,
    )
    return event.to_payload()


def test_cockpit_bars_are_closed_canonical_and_chronological() -> None:
    rows = [
        {"symbol": "BTCUSDT", "payload": closed_bar(open_time_ms=1_700_000_100_000)},
        {"symbol": "BTCUSDT", "payload": closed_bar()},
    ]

    projected = _bars(rows)

    assert len(projected) == len(SYMBOLS)
    assert [bar["close"] for bar in projected["BTCUSDT"]] == [101.0, 101.0]
    assert projected["ETHUSDT"] == []
    assert projected["BTCUSDT"][0]["closed_at"] < projected["BTCUSDT"][1]["closed_at"]


def test_cockpit_rejects_duplicate_or_invalid_closed_bars() -> None:
    duplicate = {"symbol": "BTCUSDT", "payload": closed_bar()}
    with pytest.raises(CockpitSnapshotError, match="not unique"):
        _bars([duplicate, duplicate])

    malformed = {"symbol": "BTCUSDT", "payload": {"contract_version": "closed-bar.v1"}}
    with pytest.raises(ValueError):
        _bars([malformed])


def test_cockpit_indicators_are_empty_when_no_durable_snapshots_exist() -> None:
    projected = _indicators([])

    assert set(projected) == set(SYMBOLS)
    assert all(values == [] for values in projected.values())


def test_cockpit_never_fabricates_unavailable_venue_measurements() -> None:
    now = datetime(2026, 9, 23, 8, 0, tzinfo=UTC)

    projected = _venue_quality([], now=now)

    assert set(projected) == set(SYMBOLS)
    assert all(value["state"] == "UNAVAILABLE" for value in projected.values())
    assert all(value["spread_bps"] is None for value in projected.values())


def test_cockpit_venue_quality_reads_dev_symbols_and_maps_them_to_market_symbols() -> None:
    now = datetime(2026, 9, 23, 8, 0, tzinfo=UTC)
    observed_at_ms = int(now.timestamp() * 1_000)
    event = VenueQualityV1(
        source="venue-gate",
        profile=EvedexProfile.DEV,
        symbol="BTCUSD:DEV",
        observed_at_ms=observed_at_ms,
        expires_at_ms=observed_at_ms + 5_000,
        reference_timestamp_ms=observed_at_ms - 1_000,
        book_timestamp_ms=observed_at_ms - 500,
        reference_mid_price=99.0,
        best_bid=99.9,
        best_ask=100.1,
        venue_mid_price=100.0,
        basis_bps=(100.0 - 99.0) / 99.0 * 10_000,
        spread_bps=(100.1 - 99.9) / 100.0 * 10_000,
        assessed_notional_usd=1_000.0,
        depth_usd=10_000.0,
        buy_slippage_bps=0.5,
        sell_slippage_bps=0.6,
        taker_fee_bps=5.0,
        reference_age_ms=1_000,
        book_age_ms=500,
        latency_ms=20,
        timestamp_skew_ms=500,
        entry_allowed=True,
    )

    projected = _venue_quality([event.to_payload()], now=now)

    assert _COCKPIT_VENUE_SYMBOLS == (
        "BTCUSD:DEV",
        "ETHUSD:DEV",
        "SOLUSD:DEV",
        "BNBUSD:DEV",
        "XRPUSD:DEV",
    )
    assert projected["BTCUSDT"]["state"] == "HEALTHY"
    assert projected["BTCUSDT"]["spread_bps"] == pytest.approx(20.0)
    assert projected["ETHUSDT"]["state"] == "UNAVAILABLE"


def test_cockpit_preserves_llm_defer_and_does_not_infer_execution() -> None:
    now = datetime(2026, 9, 23, 8, 0, tzinfo=UTC)
    decision = SimpleNamespace(
        decision_id=uuid4(),
        trade_id=uuid4(),
        produced_at=now,
        intent=SimpleNamespace(
            symbol="BTCUSDT",
            strategy_id="adaptive-v1",
            side=Side.LONG,
            entry_expires_ts_ms=int(now.timestamp() * 1_000) + 60_000,
        ),
        review=SimpleNamespace(reviewer="LLM", decision=ReviewDecision.DEFER),
        approved=False,
        rejection_reasons=("LLM_DEFER",),
    )

    projected, execution = _decisions([decision], [], now=now)  # type: ignore[list-item]

    assert projected[0]["llm_review"] == "DEFER"
    assert projected[0]["risk_decision"] == "DEFER"
    assert projected[0]["candidate_state"] == "REJECTED"
    assert projected[0]["execution_state"] == "NOT_REQUESTED"
    assert execution == {}


def test_cockpit_repository_requires_an_explicit_read_only_runtime_database() -> None:
    settings = PersistenceSettings(database_url="postgresql://user:pass@localhost:5432/kairos")
    writable_database = Database(settings, read_only=False)
    with pytest.raises(ValueError, match="read-only"):
        CockpitSnapshotRepository(writable_database)

    simulator_database = Database(
        PersistenceSettings(database_url="postgresql://user:pass@localhost:5432/kairos_sim_cockpit_test"),
        migration_profile="simulator",
        read_only=True,
    )
    with pytest.raises(ValueError, match="simulator"):
        CockpitSnapshotRepository(simulator_database)
