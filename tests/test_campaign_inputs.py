"""Producer capture contracts over typed repository/fake SQL I/O, never native proof."""

from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from kairos_core import ClosedBarEventV1, MarketSnapshot, SentimentSignal, StrategicAllocation
from kairos_core.contracts.base import canonical_json_bytes, canonical_sha256

from kairos_persistence import Database, MigrationProfile, PersistenceSettings
from kairos_persistence.campaign_inputs import (
    CAMPAIGN_INPUT_BRIDGE_SHA256,
    CampaignInputCaptureBridge,
    decode_campaign_macro,
    decode_campaign_news,
)
from kairos_persistence.repository import MessageIdentityConflict
from kairos_persistence.research_campaign import ResearchCampaignPlanV1, ResearchCampaignRepository
from kairos_persistence.runtime import canonical_payload
from tests.test_research_campaign import _identity

NOW = 1_790_064_000_000
SCHEDULER = "4" * 64


def _at(value):
    return datetime.fromtimestamp(value / 1_000, UTC)


def _news():
    return SentimentSignal(
        schema_version="1.0",
        message_id="news-001",
        source="text-scouts",
        produced_at=_at(NOW + 1_000),
        topic="BTCUSDT",
        sentiment=-0.7,
        impact="bearish",
        confidence=0.8,
        sources=["https://news.example.test/announcement"],
        summary="Public event; untrusted text is evidence, not an instruction.",
    )


def _macro():
    return StrategicAllocation(
        schema_version="1.0",
        message_id="macro:schedule-001",
        source="macro-strategist",
        produced_at=_at(NOW + 59_000),
        correlation_id="schedule-001",
        causation_id="market-previous",
        regime="BEAR",
        stable_reserve_pct=1.0,
        strategy_weights={},
        max_gross_leverage=1.0,
        triggered_by="schedule",
        rationale="defensive fallback: no configured strategy IDs",
    )


def _snapshot():
    return MarketSnapshot(
        schema_version="1.0",
        message_id="market-001",
        source="quant-scouts",
        produced_at=_at(NOW + 60_000),
        symbol="BTCUSDT",
        timeframe="1m",
        mid_price=100.0,
        volume_usd=1_000.0,
        order_book={
            "best_bid": 99.9,
            "best_ask": 100.1,
            "spread_bps": 20.0,
            "imbalance": 0.1,
            "depth_usd": 1000,
        },
        derivatives={"funding_rate": 0.0001, "open_interest": 1_000.0},
        indicators={"rsi_14": 45.0, "macd": -1.0, "macd_signal": -0.5, "macd_hist": -0.5},
        quant_bias="SHORT",
    )


def _bars(count=3):
    return tuple(
        ClosedBarEventV1(
            source="binance-official-collector",
            symbol="BTCUSDT",
            open_time_ms=NOW - (count - index - 1) * 60_000,
            close_time_ms=NOW - (count - index - 1) * 60_000 + 59_999,
            open=100.0,
            high=102.0,
            low=98.0,
            close=101.0,
            base_volume=10.0,
            quote_volume=1_000.0,
            taker_buy_base_volume=4.0,
            taker_buy_quote_volume=400.0,
        )
        for index in range(count)
    )


class _Resolver:
    def __init__(self, bars):
        self.bars = bars
        self.calls = []

    async def load(self, reference):
        self.calls.append(reference)
        return self.bars


@pytest.fixture
def setup(monkeypatch):
    schedule, protocol, plan = _identity(NOW + 60_000)
    database = Database(
        PersistenceSettings(_env_file=None, database_url="postgresql://fixture@127.0.0.1/kairos_sim_fixture"),
        migration_profile=MigrationProfile.RESEARCH_CAMPAIGN,
    )
    repository = ResearchCampaignRepository(database, causal_scheduler_sha256=SCHEDULER)
    state = SimpleNamespace(
        repository=repository,
        schedule=schedule,
        protocol=protocol,
        plan=plan,
        now=NOW + 61_000,
        stores=[],
        rows={},
        loads=0,
        transactions=0,
        mutation=None,
        frozen=False,
    )

    async def fetchval(query, *_args):
        assert "clock_timestamp()" in query
        return state.now

    connection = SimpleNamespace(fetchval=fetchval)

    @asynccontextmanager
    async def transaction():
        state.transactions += 1
        yield connection

    async def lock(*_args):
        return None

    async def context(*_args):
        state.loads += 1
        if state.mutation is not None:
            callback, state.mutation = state.mutation, None
            callback()
        return state.plan, state.schedule, state.protocol

    async def row(_connection, campaign, sample, arm, kind, slot):
        if kind == "bundle" and state.frozen:
            return {"unneeded_for_refusal": True}
        return state.rows.get((campaign, sample, arm, kind, slot))

    async def store(_connection, receipt, kind, arm, slot):
        encoded, digest = canonical_payload(receipt.model_dump(mode="json"))
        state.stores.append(receipt.receipt_sha256)
        state.rows[(receipt.campaign_id, receipt.sample_id, arm, kind, slot)] = {
            "receipt_sha256": receipt.receipt_sha256,
            "payload_json": encoded,
            "payload_sha256": digest,
        }

    monkeypatch.setattr(database, "transaction", transaction)
    monkeypatch.setattr(repository, "_lock", lock)
    monkeypatch.setattr(repository, "_context", context)
    monkeypatch.setattr(repository, "_row", row)
    monkeypatch.setattr(repository, "_store", store)
    resolver = _Resolver(_bars())
    state.resolver = resolver
    state.bridge = CampaignInputCaptureBridge(repository, bar_window_resolver=resolver)
    return state


async def _capture(setup, kind, payload=None, **extra):
    method, source_name, default = {
        "news": (setup.bridge.capture_news, "news", _news),
        "macro": (setup.bridge.capture_macro, "macro", _macro),
        "market": (setup.bridge.capture_market, "bars", _snapshot),
    }[kind]
    values = dict(
        campaign_id=setup.schedule.campaign_id,
        sample_id="one",
        source_name=source_name,
        payload=default() if payload is None else payload,
    )
    if kind == "market":
        values.update(bars=setup.resolver.bars, bar_window_reference="public-bars-001")
    values.update(extra)
    return await method(**values)


@pytest.mark.parametrize("wire", [False, True])
async def test_news_preserves_full_message_and_independent_db_observation(setup, wire):
    message = _news()
    payload = message.model_dump(mode="json") if wire else message
    receipt = await _capture(setup, "news", payload)
    assert receipt.source_kind == "NEWS" and receipt.reference == message.message_id
    assert receipt.source_as_of_ts_ms == NOW + 1_000
    assert receipt.observed_at_ts_ms == setup.now != receipt.source_as_of_ts_ms
    assert receipt.content["message"] == message.model_dump(mode="json")
    assert receipt.content["provenance"] == {
        "raw_article_bytes": "UNAVAILABLE",
        "model_completion": "UNAVAILABLE",
    }
    assert receipt.content_sha256 == canonical_sha256(receipt.content)
    assert len(setup.stores) == 1


async def test_macro_preserves_allocation_identity_and_honest_missing_provenance(setup):
    message = _macro()
    receipt = await _capture(setup, "macro", message)
    assert receipt.content["message"] == message.model_dump(mode="json")
    assert receipt.content["provenance"] == {
        "upstream_context": "UNAVAILABLE",
        "model_completion": "UNAVAILABLE",
    }
    assert receipt.reference == message.message_id and receipt.source_as_of_ts_ms == NOW + 59_000
    assert receipt.observed_at_ts_ms == setup.now


async def test_market_stores_only_verified_compact_context(setup):
    receipt = await _capture(setup, "market")
    content = receipt.content
    assert content["contract_version"] == "campaign-market-context.v1"
    assert content["bar_count"] == 3 and content["first_open_time_ms"] == NOW - 120_000
    assert content["bar_window_reference"] == "public-bars-001"
    assert content["bar_window_sha256"] == canonical_sha256(
        {"bars": [bar.identity_payload() for bar in setup.resolver.bars]}
    )
    assert content["anchor_bar"] == setup.resolver.bars[-1].model_dump(mode="json")
    assert "bars" not in content and len(canonical_json_bytes(content)) < 8_192
    assert setup.resolver.calls == ["public-bars-001"]
    assert receipt.source_as_of_ts_ms == NOW + 60_000 and receipt.observed_at_ts_ms == setup.now


async def test_large_history_is_a_descriptor_not_inline_receipt(setup):
    setup.resolver.bars = _bars(500)
    receipt = await _capture(setup, "market")
    assert receipt.content["bar_count"] == 500
    assert len(canonical_json_bytes(receipt.content)) < 8_192
    assert len(setup.stores) == 1


@pytest.mark.parametrize("kind", ["news", "macro", "market"])
@pytest.mark.parametrize("field", ["message_id", "source", "schema_version", "produced_at"])
async def test_wire_cannot_manufacture_missing_envelope_defaults(setup, kind, field):
    payload = {"news": _news, "macro": _macro, "market": _snapshot}[kind]().model_dump(mode="json")
    del payload[field]
    with pytest.raises(ValueError):
        await _capture(setup, kind, payload)
    assert setup.transactions == 0 and setup.stores == []


@pytest.mark.parametrize("kind", ["news", "macro", "market"])
@pytest.mark.parametrize("field", ["message_id", "produced_at"])
async def test_typed_identity_and_clock_defaults_are_not_original_producer_facts(setup, kind, field):
    original = {"news": _news, "macro": _macro, "market": _snapshot}[kind]()
    body = original.model_dump(mode="json")
    del body[field]
    payload = type(original).model_validate(body)
    assert field not in payload.model_fields_set
    with pytest.raises(ValueError, match="legacy defaults"):
        await _capture(setup, kind, payload)
    assert setup.transactions == 0 and setup.stores == [] and setup.resolver.calls == []


@pytest.mark.parametrize("kind", ["news", "macro", "market"])
@pytest.mark.parametrize("wire", [False, True])
async def test_submillisecond_producer_clock_is_never_floored_before_capture(setup, kind, wire):
    original = {"news": _news, "macro": _macro, "market": _snapshot}[kind]()
    produced_at = original.produced_at + timedelta(microseconds=500)
    payload = (
        original.model_dump(mode="json") | {"produced_at": produced_at.isoformat()}
        if wire
        else original.model_copy(update={"produced_at": produced_at})
    )
    with pytest.raises(ValueError, match="exact millisecond"):
        await _capture(setup, kind, payload)
    assert setup.transactions == 0 and setup.stores == [] and setup.resolver.calls == []


@pytest.mark.parametrize("kind", ["news", "macro", "market"])
@pytest.mark.parametrize(
    "field,value",
    [
        ("source", "other-producer"),
        ("schema_version", "1.1"),
        ("message_id", "https://unexpected.test/id"),
        ("produced_at", "2026-10-03T00:00:00"),
        ("caller_observed_at", NOW),
    ],
)
async def test_wire_identity_schema_unknown_and_naive_time_fail_before_repository(setup, kind, field, value):
    payload = {"news": _news, "macro": _macro, "market": _snapshot}[kind]().model_dump(mode="json")
    payload[field] = value
    with pytest.raises(ValueError):
        await _capture(setup, kind, payload)
    assert setup.transactions == 0 and setup.stores == []


@pytest.mark.parametrize("field", ["order_book", "derivatives", "indicators"])
async def test_unknown_nested_market_fields_are_not_silently_ignored(setup, field):
    payload = _snapshot().model_dump(mode="json")
    payload[field]["invented_future_data"] = True
    with pytest.raises(ValueError):
        await _capture(setup, "market", payload)
    assert setup.transactions == 0 and setup.resolver.calls == []


@pytest.mark.parametrize("value", [True, "-0.7", float("nan"), float("inf")])
async def test_untyped_or_nonfinite_news_scores_fail_before_write(setup, value):
    payload = _news().model_dump(mode="json")
    payload["sentiment"] = value
    with pytest.raises(ValueError):
        await _capture(setup, "news", payload)
    assert setup.transactions == 0 and setup.stores == []


async def test_recursive_huge_or_non_json_payload_fails_before_repository(setup):
    for field, value in (("summary", "x" * 262_144), ("sources", [object()])):
        payload = _news().model_dump(mode="json")
        payload[field] = value
        with pytest.raises(ValueError):
            await _capture(setup, "news", payload)
    payload = _news().model_dump(mode="json")
    payload[True] = "non-string JSON key"
    with pytest.raises(ValueError):
        await _capture(setup, "news", payload)
    assert setup.transactions == 0 and setup.stores == []


async def test_same_source_replay_reuses_first_db_time_but_changed_bytes_refuse(setup):
    payload = _news().model_dump(mode="json")
    first = await _capture(setup, "news", payload)
    setup.now += 300
    replay = await _capture(setup, "news", dict(reversed(tuple(payload.items()))))
    assert replay == first and replay.observed_at_ts_ms != setup.now
    payload["summary"] = "Changed output under the same producer identity."
    with pytest.raises(MessageIdentityConflict):
        await _capture(setup, "news", payload)
    assert len(setup.stores) == 1


async def test_input_nested_json_detaches_before_first_repository_await(setup):
    payload = _news().model_dump(mode="json")
    original = deepcopy(payload)
    setup.mutation = lambda: payload["sources"].append("https://late.example.test/not-observed")
    receipt = await _capture(setup, "news", payload)
    assert receipt.content["message"] == original != payload


@pytest.mark.parametrize("bad_reference", ["https://example.test/window", "D:/window", "../window", ""])
async def test_resolver_cannot_be_given_implicit_url_or_filesystem_instructions(setup, bad_reference):
    with pytest.raises(ValueError):
        await _capture(setup, "market", bar_window_reference=bad_reference)
    assert setup.resolver.calls == [] and setup.transactions == 0


async def test_market_never_creates_default_resolver(setup):
    setup.bridge = CampaignInputCaptureBridge(setup.repository)
    with pytest.raises(ValueError, match="explicitly injected"):
        await _capture(setup, "market")
    assert setup.transactions == 0 and setup.stores == []


@pytest.mark.parametrize("mode", ["gap", "reverse", "duplicate", "canonical_mutation", "wrong_type"])
async def test_passed_window_must_be_full_canonical_and_gap_free(setup, mode):
    bars = setup.resolver.bars
    bad = {
        "gap": (bars[0], bars[-1]),
        "reverse": tuple(reversed(bars)),
        "duplicate": (bars[0], bars[0], bars[1]),
        "canonical_mutation": (bars[0].model_copy(update={"close": 100.5}), *bars[1:]),
        "wrong_type": (bars[0].model_dump(mode="json"), *bars[1:]),
    }[mode]
    with pytest.raises((ValueError, TypeError)):
        await _capture(setup, "market", bars=bad)
    assert setup.resolver.calls == [] and setup.transactions == 0


@pytest.mark.parametrize("mode", ["count", "body", "gap"])
async def test_readonly_resolved_window_must_independently_match_passed_window(setup, mode):
    supplied = setup.resolver.bars
    if mode == "count":
        setup.resolver.bars = supplied[1:]
    elif mode == "body":
        body = supplied[0].model_dump(mode="json") | {"close": 100.5, "bar_sha256": None}
        setup.resolver.bars = (ClosedBarEventV1.model_validate(body), *supplied[1:])
    else:
        setup.resolver.bars = (supplied[0], supplied[-1])
    with pytest.raises(ValueError):
        await _capture(setup, "market", bars=supplied)
    assert setup.transactions == 0 and setup.stores == []


@pytest.mark.parametrize("field,value", [("symbol", "ETHUSDT"), ("timeframe", "1h")])
async def test_snapshot_and_anchor_scope_cannot_be_mixed(setup, field, value):
    payload = _snapshot().model_dump(mode="json") | {field: value}
    with pytest.raises(ValueError):
        await _capture(setup, "market", payload)
    assert setup.stores == []


@pytest.mark.parametrize("kind", ["news", "macro", "market"])
async def test_unregistered_sample_or_source_cannot_write(setup, kind):
    for values in ({"sample_id": "other"}, {"source_name": "unregistered"}, {"campaign_id": "other"}):
        with pytest.raises(ValueError):
            await _capture(setup, kind, **values)
    assert setup.stores == []


async def test_other_scheduler_or_mutated_plan_is_not_adopted(setup):
    body = setup.plan.model_dump(mode="json") | {"scheduler_sha256": "9" * 64, "receipt_sha256": None}
    setup.plan = ResearchCampaignPlanV1.model_validate(body)
    with pytest.raises(ValueError, match="preregistered"):
        await _capture(setup, "news")
    setup.plan = setup.plan.model_copy(update={"scheduler_sha256": SCHEDULER})
    with pytest.raises(MessageIdentityConflict):
        await _capture(setup, "news")
    assert setup.stores == []


async def test_frozen_bundle_and_future_event_are_never_repaired(setup):
    setup.frozen = True
    with pytest.raises(MessageIdentityConflict):
        await _capture(setup, "news")
    setup.frozen = False
    payload = _news().model_dump(mode="json") | {"produced_at": _at(setup.now + 1).isoformat()}
    with pytest.raises(ValueError):
        await _capture(setup, "news", payload)
    assert setup.stores == []


def test_bridge_requires_real_optin_recorder_and_has_no_start_or_consumer(setup):
    with pytest.raises(TypeError):
        CampaignInputCaptureBridge(SimpleNamespace())
    ordinary = ResearchCampaignRepository(setup.repository._database)
    with pytest.raises(ValueError, match="opt-in"):
        CampaignInputCaptureBridge(ordinary)
    for name in ("run", "start", "subscribe", "publish", "dispatch", "migrate", "create_campaign"):
        assert not hasattr(setup.bridge, name)


def test_bridge_artifact_is_hash_bound_to_policy_and_actual_canonical_module():
    import hashlib

    from kairos_persistence import campaign_inputs

    implementation = hashlib.sha256(Path(campaign_inputs.__file__).read_bytes().replace(b"\r\n", b"\n"))
    expected = canonical_sha256(
        {
            "contract_version": "campaign-input-bridge.v1",
            "implementation_sha256": implementation.hexdigest(),
            "envelope": "explicit-known-json-fields-schema-1.0-aware-time",
            "sources": ["macro-strategist", "quant-scouts", "text-scouts"],
            "clock": "repository-postgresql-observation-only",
            "window": "injected-readonly-resolver-full-gapfree-identity-hash",
            "authority": "SIM_RESEARCH_ONLY-no-subscription-or-dispatch",
            "maximum_content_bytes": 240_000,
            "maximum_bar_count": 50_000,
        }
    )
    assert CAMPAIGN_INPUT_BRIDGE_SHA256 == expected


@pytest.mark.parametrize("kind,decoder", [("news", decode_campaign_news), ("macro", decode_campaign_macro)])
async def test_saved_wrapper_decoder_is_pure_strict_and_preserves_message(setup, kind, decoder):
    receipt = await _capture(setup, kind)
    transactions, writes = setup.transactions, len(setup.stores)
    parsed = decoder(receipt.content)
    assert parsed.model_dump(mode="json") == receipt.content["message"]
    assert setup.transactions == transactions and len(setup.stores) == writes


@pytest.mark.parametrize("kind,decoder", [("news", decode_campaign_news), ("macro", decode_campaign_macro)])
@pytest.mark.parametrize("mutation", ["extra", "version", "provenance", "message", "default"])
async def test_decoder_rejects_changed_wrapper_and_never_queries_repository(setup, kind, decoder, mutation):
    receipt = await _capture(setup, kind)
    value = deepcopy(receipt.content)
    if mutation == "extra":
        value["caller_acceptance"] = True
    elif mutation == "version":
        value["contract_version"] = "campaign-input.v2"
    elif mutation == "provenance":
        value["provenance"]["model_completion"] = "VERIFIED"
    elif mutation == "message":
        value["message"]["unknown_model_field"] = True
    else:
        del value["message"]["correlation_id"]
    transactions, writes = setup.transactions, len(setup.stores)
    with pytest.raises(ValueError):
        decoder(value)
    assert setup.transactions == transactions and len(setup.stores) == writes


@pytest.mark.parametrize("kind,decoder", [("news", decode_campaign_news), ("macro", decode_campaign_macro)])
async def test_decoder_cannot_adopt_object_pairs_as_stored_json_content(setup, kind, decoder):
    receipt = await _capture(setup, kind)
    transactions, writes = setup.transactions, len(setup.stores)
    with pytest.raises(TypeError, match="JSON object"):
        decoder(list(receipt.content.items()))
    assert setup.transactions == transactions and len(setup.stores) == writes


@pytest.mark.parametrize("kind,decoder", [("news", decode_campaign_news), ("macro", decode_campaign_macro)])
async def test_saved_wrapper_decoder_refuses_submillisecond_clock_without_rounding(setup, kind, decoder):
    receipt = await _capture(setup, kind)
    content = deepcopy(receipt.content)
    produced_at = datetime.fromisoformat(content["message"]["produced_at"]) + timedelta(microseconds=500)
    content["message"]["produced_at"] = produced_at.isoformat()
    transactions, writes = setup.transactions, len(setup.stores)
    with pytest.raises(ValueError, match="exact millisecond"):
        decoder(content)
    assert setup.transactions == transactions and len(setup.stores) == writes


@pytest.mark.parametrize("kind,field", [("news", "confidence"), ("macro", "strategy_weights")])
async def test_optional_producer_fields_are_not_fabricated_from_defaults(setup, kind, field):
    payload = {"news": _news, "macro": _macro}[kind]().model_dump(mode="json")
    del payload[field]
    with pytest.raises(ValueError):
        await _capture(setup, kind, payload)
    assert setup.transactions == 0 and setup.stores == []


@pytest.mark.parametrize("weight", [-0.1, 1.1])
async def test_macro_bridge_retains_strict_producer_weight_bounds(setup, weight):
    payload = _macro().model_dump(mode="json") | {
        "stable_reserve_pct": 0.0,
        "strategy_weights": {"baseline": weight},
    }
    with pytest.raises(ValueError):
        await _capture(setup, "macro", payload)
    assert setup.transactions == 0 and setup.stores == []
