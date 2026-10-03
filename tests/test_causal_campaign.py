"""Offline contract/repository composition; these SQL-shaped tests are not PG acceptance."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from math import ceil, floor
from types import SimpleNamespace

import pytest
from kairos_core import (
    ClosedBarEventV1,
    EvidenceReferenceV1,
    LLMProposalCompletionReceiptV1,
    LLMProposalModelProvenanceV1,
    LLMTradeProposalV1,
    SentimentSignal,
    StrategicAllocation,
    StrategyIntentV1,
    canonical_sha256,
)

from kairos_persistence import Database, MigrationProfile, PersistenceSettings
from kairos_persistence.causal_campaign import (
    CampaignMarketContextV1,
    CausalBaselineResultV1,
    ResearchCausalStrategyEvaluationReceiptV1,
    campaign_bar_window_sha256,
    decode_campaign_evaluation_row,
)
from kairos_persistence.repository import MessageIdentityConflict
from kairos_persistence.research_campaign import (
    ResearchArmOutcomeV1,
    ResearchCampaignRepository,
    ResearchCausalBundleV1,
    ResearchWindowClaimV1,
    decode_campaign_sample_row,
)
from kairos_persistence.research_evidence import (
    ResearchLLMAttemptStartV1,
    ResearchLLMAttemptTerminalV1,
    ResearchSourceReceiptV1,
    ResearchStrategyEvaluationReceiptV1,
)
from kairos_persistence.runtime import canonical_payload

_SCHEDULER = "8" * 64
_CUTOFF = 121_000


def _bars():
    return tuple(
        ClosedBarEventV1(
            source="binance-capture",
            symbol="BTCUSDT",
            open_time_ms=open_time,
            close_time_ms=open_time + 59_999,
            open=100,
            close=100,
            high=101,
            low=99,
            base_volume=10,
            quote_volume=1_000,
            taker_buy_base_volume=5,
            taker_buy_quote_volume=500,
        )
        for open_time in (0, 60_000)
    )


def _context():
    bars = _bars()
    return CampaignMarketContextV1(
        anchor_bar=bars[-1],
        bar_window_reference="window:btc-001",
        bar_window_sha256=campaign_bar_window_sha256(bars),
        bar_count=len(bars),
        first_open_time_ms=0,
        market_snapshot=dict(
            source="kairos-quant-scouts",
            message_id="original-market-001",
            produced_at=datetime.fromtimestamp(120.1, UTC),
            symbol="BTCUSDT",
            timeframe="1m",
            mid_price=100,
            volume_usd=1_000,
            order_book=dict(best_bid=99, best_ask=101, spread_bps=200, imbalance=0, depth_usd=1_000),
            derivatives=dict(funding_rate=0, open_interest=1_000),
            indicators=dict(rsi_14=50, macd=0, macd_signal=0, macd_hist=0),
        ),
    )


def _intent(context):
    return StrategyIntentV1(
        source="kairos-strategy-engine",
        strategy_id="offline-fixture",
        strategy_revision="v1",
        symbol="BTCUSDT",
        side="LONG",
        decision_ts_ms=context.anchor_bar.close_time_ms,
        entry_eligible_ts_ms=120_000,
        entry_expires_ts_ms=180_000,
        reference_price=100,
        signal_strength=0.5,
        gross_reward_bps=100,
        exit_plan=dict(stop_price=99, target_price=101, max_holding_ms=60_000),
        provenance=dict(
            strategy_code_sha256="a" * 64,
            config_sha256="b" * 64,
            input_window_sha256=context.bar_window_sha256,
            features_sha256="c" * 64,
            input_bar_sha256s=tuple(str(x.bar_sha256) for x in _bars()),
        ),
    )


def _producer_content(kind, name):
    common = dict(message_id="source:" + name, produced_at=datetime.fromtimestamp(120.2, UTC))
    if kind == "NEWS":
        message = SentimentSignal(
            source="text-scouts",
            topic="BTCUSDT",
            sentiment=-0.5,
            impact="bearish",
            summary="Synthetic public news input",
            **common,
        )
        provenance = {"raw_article_bytes": "UNAVAILABLE", "model_completion": "UNAVAILABLE"}
        contract = "campaign-news-input.v1"
    else:
        message = StrategicAllocation(
            source="macro-strategist",
            regime="BEAR",
            stable_reserve_pct=1.0,
            strategy_weights={},
            max_gross_leverage=1.0,
            **common,
        )
        provenance = {"upstream_context": "UNAVAILABLE", "model_completion": "UNAVAILABLE"}
        contract = "campaign-macro-input.v1"
    return dict(contract_version=contract, message=message.model_dump(mode="json"), provenance=provenance)


def _row(receipt, *, kind, arm="all", slot="one", recorded_at=121_100):
    encoded, digest = canonical_payload(receipt.model_dump(mode="json"))
    return dict(
        payload_json=encoded,
        payload_sha256=digest,
        receipt_sha256=receipt.receipt_sha256,
        campaign_id=getattr(receipt, "campaign_id", ""),
        sample_id=getattr(receipt, "sample_id", ""),
        kind=kind,
        arm_id=arm,
        slot_key=slot,
        recorded_at_ts_ms=recorded_at,
        causal_recorded_at_ts_ms=recorded_at,
    )


class _Connection:
    def __init__(self):
        self.rows = []
        self.claim = None
        self.now = 121_100
        self.writes = 0
        self.insert_clock = None

    @asynccontextmanager
    async def transaction(self):
        previous, writes = list(self.rows), self.writes
        try:
            yield
        except Exception:
            self.rows, self.writes = previous, writes
            raise

    async def fetchval(self, sql, *_args):
        assert "clock_timestamp()" in sql
        return self.now

    async def execute(self, sql, *args):
        if sql.startswith("SELECT pg_advisory_xact_lock"):
            return
        assert sql.startswith("INSERT INTO sim_adaptive_campaign_receipts")
        receipt_sha, campaign, sample, arm, kind, slot, payload, digest = args
        self.rows.append(
            dict(
                receipt_sha256=receipt_sha,
                campaign_id=campaign,
                sample_id=sample,
                arm_id=arm,
                kind=kind,
                slot_key=slot,
                payload_json=payload,
                payload_sha256=digest,
                recorded_at_ts_ms=self.insert_clock or self.now,
                causal_recorded_at_ts_ms=self.insert_clock or self.now,
            )
        )
        self.writes += 1

    async def fetchrow(self, sql, *args):
        if "sim_adaptive_window_claims" in sql:
            return self.claim
        if "WHERE receipt_sha256=$1" in sql:
            return next((x for x in self.rows if x["receipt_sha256"] == args[0]), None)
        if "kind='start' AND slot_key=$1" in sql:
            return next((x for x in self.rows if x["kind"] == "start" and x["slot_key"] == args[0]), None)
        if "kind='start'" in sql:
            return next(
                (
                    x
                    for x in self.rows
                    if x["kind"] == "start" and (x["campaign_id"], x["sample_id"], x["arm_id"]) == args
                ),
                None,
            )
        assert "arm_id=$3 AND kind=$4 AND slot_key=$5" in sql
        return next(
            (
                x
                for x in self.rows
                if (x["campaign_id"], x["sample_id"], x["arm_id"], x["kind"], x["slot_key"]) == args
            ),
            None,
        )

    async def fetch(self, sql, *args):
        if "kind='start'" in sql:
            return [
                x
                for x in self.rows
                if x["kind"] == "start" and (x["campaign_id"], x["sample_id"], x["arm_id"]) == args
            ]
        assert "LIMIT 65" in sql
        return [x for x in self.rows if (x["campaign_id"], x["sample_id"]) == args]


class _Pool:
    def __init__(self, connection):
        self.connection = connection

    @asynccontextmanager
    async def acquire(self):
        yield self.connection


def _fixture(monkeypatch, *, enabled=True, intent=True):
    # Local unit-only fixture import; no native target or production dependency injection.
    from tests.test_research_campaign import _identity

    schedule, protocol, plan = _identity(_CUTOFF - 2_000)
    plan = type(plan).model_validate(
        {**plan.model_dump(mode="json"), "receipt_sha256": None, "scheduler_sha256": _SCHEDULER}
    )
    context = _context()
    connection = _Connection()
    database = Database(
        PersistenceSettings(_env_file=None, database_url="postgresql://localhost/kairos_sim_unit_causal"),
        migration_profile=MigrationProfile.RESEARCH_CAMPAIGN,
    )
    database._pool = _Pool(connection)
    repository = ResearchCampaignRepository(database, causal_scheduler_sha256=_SCHEDULER if enabled else None)

    async def stored_context(_connection, campaign):
        assert campaign == plan.campaign_id
        return plan, schedule, protocol

    monkeypatch.setattr(repository, "_context", stored_context)
    claim = ResearchWindowClaimV1(
        campaign_id=plan.campaign_id,
        sample_id="one",
        plan_receipt_sha256=plan.receipt_sha256,
        claim_id=canonical_sha256({"plan_receipt_sha256": plan.receipt_sha256, "sample_id": "one"}),
        claimed_at_ts_ms=_CUTOFF,
    )
    connection.claim = _row(claim, kind="claim")
    sources = tuple(
        ResearchSourceReceiptV1(
            campaign_id=plan.campaign_id,
            sample_id="one",
            schedule_digest=schedule.schedule_digest,
            candidate_protocol_digest=protocol.protocol_digest,
            source_kind=kind,
            source_name=name,
            reference=context.market_snapshot.message_id if kind == "MARKET_SNAPSHOT" else "source:" + name,
            source_as_of_ts_ms=120_100 if kind == "MARKET_SNAPSHOT" else 120_200,
            observed_at_ts_ms=120_500,
            content=context.model_dump(mode="json")
            if kind == "MARKET_SNAPSHOT"
            else _producer_content(kind, name),
        )
        for kind, name in (("MACRO", "macro"), ("MARKET_SNAPSHOT", "bars"), ("NEWS", "news"))
    )
    connection.rows.extend(
        _row(x, kind="source", slot=f"{x.source_kind}:{x.source_name}", recorded_at=120_500) for x in sources
    )
    market = next(x for x in sources if x.source_kind == "MARKET_SNAPSHOT")
    bundle = ResearchCausalBundleV1(
        campaign_id=plan.campaign_id,
        sample_id="one",
        claim_id=claim.claim_id,
        market_snapshot_sha256=market.content_sha256,
        source_receipt_sha256s=tuple(sorted(str(x.receipt_sha256) for x in sources)),
        frozen_at_ts_ms=_CUTOFF,
    )
    connection.rows.append(_row(bundle, kind="bundle", recorded_at=_CUTOFF))
    result = CausalBaselineResultV1(
        context_source_receipt_sha256=market.receipt_sha256,
        bar_window_sha256=context.bar_window_sha256,
        bar_count=context.bar_count,
        intent=_intent(context) if intent else None,
    )
    return SimpleNamespace(
        repository=repository,
        connection=connection,
        context=context,
        result=result,
        claim=claim,
        bundle=bundle,
        plan=plan,
        schedule=schedule,
        protocol=protocol,
        sources=sources,
    )


async def _completed(fixture, *, action="SHORT_BIAS"):
    repo, connection = fixture.repository, fixture.connection
    evaluation = await repo.record_causal_evaluation(
        claim=fixture.claim, bundle=fixture.bundle, result=fixture.result
    )
    connection.now = 121_200
    start = ResearchLLMAttemptStartV1(
        attempt_id="fixture-proposal-once",
        campaign_id=fixture.plan.campaign_id,
        sample_id="one",
        arm_id="llm-proposal-research",
        schedule_digest=fixture.schedule.schedule_digest,
        candidate_protocol_digest=fixture.protocol.protocol_digest,
        arm_protocol_digest=fixture.protocol.arm_digest("llm-proposal-research"),
        symbol="BTCUSDT",
        timeframe="1m",
        market_as_of_ts_ms=_CUTOFF,
        market_snapshot_sha256=fixture.bundle.market_snapshot_sha256,
        sample_deadline_ts_ms=_CUTOFF + 8_000,
        provider="openai",
        requested_model="test-not-a-provider-model",
        prompt_sha256="2" * 64,
        budget_reservation_id="fixture-proposal-once",
        attempt_started_at_ts_ms=connection.now,
    )
    assert await repo.start_attempt(start)
    connection.now = 121_300
    source = next(x for x in fixture.sources if x.source_kind == "NEWS")
    proposal = LLMTradeProposalV1(
        campaign_id=start.campaign_id,
        sample_id=start.sample_id,
        arm_id=start.arm_id,
        symbol=start.symbol,
        timeframe=start.timeframe,
        market_as_of_ts_ms=start.market_as_of_ts_ms,
        market_snapshot_sha256=start.market_snapshot_sha256,
        expires_at_ts_ms=_CUTOFF + 8_000,
        action=action,
        rationale="Synthetic offline advisory result, not an order.",
        evidence=(
            EvidenceReferenceV1(
                kind=source.source_kind,
                reference=source.reference,
                content_sha256=source.content_sha256,
                observed_at_ms=source.observed_at_ts_ms,
            ),
        ),
        model_provenance=LLMProposalModelProvenanceV1(
            provider=start.provider,
            requested_model=start.requested_model,
            resolved_model=start.requested_model,
            request_id="fixture-request",
            prompt_sha256=start.prompt_sha256,
            response_sha256="9" * 64,
            budget_reservation_id=start.budget_reservation_id,
            latency_ms=100,
            cost_usd=0,
        ),
    )
    completion = LLMProposalCompletionReceiptV1(
        campaign_id=start.campaign_id,
        sample_id=start.sample_id,
        arm_id=start.arm_id,
        symbol=start.symbol,
        timeframe=start.timeframe,
        market_as_of_ts_ms=start.market_as_of_ts_ms,
        market_snapshot_sha256=start.market_snapshot_sha256,
        sample_deadline_ts_ms=start.sample_deadline_ts_ms,
        attempt_id=start.attempt_id,
        proposal_id=proposal.proposal_id,
        model_provenance=proposal.model_provenance,
        attempt_started_at_ts_ms=start.attempt_started_at_ts_ms,
        response_observed_at_ts_ms=connection.now,
    )
    terminal = ResearchLLMAttemptTerminalV1(
        attempt_id=start.attempt_id,
        start_receipt_sha256=start.receipt_sha256,
        terminal_status="COMPLETED",
        observed_at_ts_ms=connection.now,
        proposal=proposal,
        completion=completion,
    )
    assert await repo.finish_attempt(terminal)
    connection.now = 121_400
    return evaluation, start, terminal


def test_context_window_hash_is_real_bar_payload_not_snapshot_or_envelope():
    context = _context()
    assert context.bar_window_sha256 == canonical_sha256({"bars": [x.identity_payload() for x in _bars()]})
    assert context.bar_window_sha256 != canonical_sha256(context.model_dump(mode="json"))
    assert context.anchor_bar.close_time_ms < int(context.market_snapshot.produced_at.timestamp() * 1000)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing-envelope",
        "naive-clock",
        "subms-clock",
        "unknown-field",
        "nested-unknown",
        "wrong-symbol",
        "wrong-timeframe",
        "before-anchor",
        "wrong-count",
        "url-reference",
    ],
)
def test_context_rejects_fabricated_envelope_geometry_and_unbounded_reference(mutation):
    value = _context().model_dump(mode="json")
    if mutation == "missing-envelope":
        value["market_snapshot"].pop("message_id")
    elif mutation == "naive-clock":
        value["market_snapshot"]["produced_at"] = "1970-01-01T00:02:00.100"
    elif mutation == "subms-clock":
        value["market_snapshot"]["produced_at"] = "1970-01-01T00:02:00.100500Z"
    elif mutation == "unknown-field":
        value["market_snapshot"]["ignored"] = True
    elif mutation == "nested-unknown":
        value["market_snapshot"]["order_book"]["ignored"] = True
    elif mutation == "wrong-symbol":
        value["market_snapshot"]["symbol"] = "ETHUSDT"
    elif mutation == "wrong-timeframe":
        value["market_snapshot"]["timeframe"] = "5m"
    elif mutation == "before-anchor":
        value["market_snapshot"]["produced_at"] = "1970-01-01T00:01:59.998Z"
    elif mutation == "wrong-count":
        value["bar_count"] = 3
    else:
        value["bar_window_reference"] = "https://example.com/window"
    with pytest.raises(ValueError):
        CampaignMarketContextV1.model_validate(value)


@pytest.mark.parametrize("bars", [[], (), list(_bars()), (_bars()[1], _bars()[0])])
def test_window_digest_rejects_mutable_empty_or_reordered_history(bars):
    with pytest.raises(ValueError):
        campaign_bar_window_sha256(bars)


@pytest.mark.parametrize("enabled", [False, True])
async def test_causal_writes_require_explicit_exact_plan_optin(monkeypatch, enabled):
    fixture = _fixture(monkeypatch, enabled=enabled)
    if enabled:
        fixture.repository._causal_scheduler_sha256 = "7" * 64
    with pytest.raises(MessageIdentityConflict, match="scheduler identity"):
        await fixture.repository.record_causal_evaluation(
            claim=fixture.claim, bundle=fixture.bundle, result=fixture.result
        )
    assert fixture.connection.writes == 0


@pytest.mark.parametrize("has_intent", [True, False])
async def test_saved_causal_evaluation_preserves_earlier_real_anchor_and_exact_replay(
    monkeypatch, has_intent
):
    f = _fixture(monkeypatch, intent=has_intent)
    evaluation = await f.repository.record_causal_evaluation(claim=f.claim, bundle=f.bundle, result=f.result)
    assert evaluation.evidence_as_of_ts_ms == _CUTOFF
    assert evaluation.anchor_bar_close_ts_ms == 119_999
    assert evaluation.market_snapshot_sha256 != evaluation.anchor_bar_sha256
    assert evaluation.intent == f.result.intent
    assert await f.repository.load_evaluation(evaluation.receipt_sha256) == evaluation
    f.connection.now += 90_000
    assert (
        await f.repository.record_causal_evaluation(claim=f.claim, bundle=f.bundle, result=f.result)
        == evaluation
    )
    assert f.connection.writes == 1
    assert not hasattr(evaluation, "as_evidence")  # Cannot impersonate a Core V1 attestation.
    if has_intent:
        with pytest.raises(ValueError):
            ResearchStrategyEvaluationReceiptV1.model_validate(
                {
                    k: v
                    for k, v in evaluation.model_dump(mode="json").items()
                    if k
                    not in (
                        "context_source_receipt_sha256",
                        "anchor_bar_sha256",
                        "anchor_bar_close_ts_ms",
                        "bar_window_sha256",
                        "bar_count",
                        "receipt_sha256",
                        "contract_version",
                    )
                }
            )


@pytest.mark.parametrize(
    "field,value",
    [("bar_window_sha256", "0" * 64), ("bar_count", 1), ("context_source_receipt_sha256", "0" * 64)],
)
async def test_evaluator_cannot_substitute_window_or_context(monkeypatch, field, value):
    f = _fixture(monkeypatch)
    result = CausalBaselineResultV1.model_validate({**f.result.model_dump(mode="json"), field: value})
    with pytest.raises(MessageIdentityConflict, match="captured market context"):
        await f.repository.record_causal_evaluation(claim=f.claim, bundle=f.bundle, result=result)
    assert f.connection.writes == 0


async def test_new_pair_replays_saved_opposite_actions_without_core_sample_or_new_call(monkeypatch):
    f = _fixture(monkeypatch)
    evaluation, start, terminal = await _completed(f)
    pair = await f.repository.record_causal_pair(
        campaign_id=f.plan.campaign_id,
        sample_id="one",
        evaluation_receipt_sha256=evaluation.receipt_sha256,
        attempt_id=start.attempt_id,
    )
    assert (pair.strategy_outcome, pair.llm_outcome) == ("LONG", "SHORT_BIAS")
    assert not hasattr(pair, "sample_record_id") and not hasattr(pair, "coverage_digest")
    assert pair.terminal_receipt_sha256 == terminal.receipt_sha256
    assert decode_campaign_sample_row(_row(pair, kind="sample")) == pair
    assert await f.repository.load_causal_pair(pair.receipt_sha256) == pair
    state = await f.repository.window_state(f.plan.campaign_id, "one")
    assert state[("sample", start.arm_id, "one")] == pair
    before = f.connection.writes
    f.connection.now += 90_000
    assert (
        await f.repository.record_causal_pair(
            campaign_id=f.plan.campaign_id,
            sample_id="one",
            evaluation_receipt_sha256=evaluation.receipt_sha256,
            attempt_id=start.attempt_id,
        )
        == pair
    )
    assert f.connection.writes == before


@pytest.mark.parametrize("intent,action", [(False, "SHORT_BIAS"), (True, "NO_PROPOSAL"), (True, "DEFER")])
async def test_pair_records_no_intent_or_explicit_abstention_not_a_missing_call(monkeypatch, intent, action):
    f = _fixture(monkeypatch, intent=intent)
    evaluation, start, _ = await _completed(f, action=action)
    pair = await f.repository.record_causal_pair(
        campaign_id=f.plan.campaign_id,
        sample_id="one",
        evaluation_receipt_sha256=evaluation.receipt_sha256,
        attempt_id=start.attempt_id,
    )
    assert pair.strategy_outcome == ("LONG" if intent else "NO_INTENT")
    assert pair.llm_outcome == action


@pytest.mark.parametrize(
    "failure", ["late-pair", "late-recording", "ambiguous-start", "wrong-arm", "foreign-attempt"]
)
async def test_pair_rejects_late_unresolved_or_foreign_history(monkeypatch, failure):
    f = _fixture(monkeypatch)
    evaluation, start, _ = await _completed(f)
    arm, attempt = "llm-proposal-research", start.attempt_id
    if failure == "late-pair":
        f.connection.now = _CUTOFF + 3_001
    elif failure == "late-recording":
        f.connection.insert_clock = _CUTOFF + 3_001
    elif failure == "ambiguous-start":
        f.connection.rows = [x for x in f.connection.rows if x["kind"] != "terminal"]
    elif failure == "wrong-arm":
        arm = "strategy-review"
    else:
        attempt = "foreign-attempt"
    before = f.connection.writes
    with pytest.raises(MessageIdentityConflict):
        await f.repository.record_causal_pair(
            campaign_id=f.plan.campaign_id,
            sample_id="one",
            arm_id=arm,
            evaluation_receipt_sha256=evaluation.receipt_sha256,
            attempt_id=attempt,
        )
    assert f.connection.writes == before
    assert not any(x["kind"] == "sample" for x in f.connection.rows)


@pytest.mark.parametrize("corruption", ["digest", "contract", "clock", "anchor", "scope"])
async def test_closed_decoder_and_independent_pair_reject_tampered_evaluation(monkeypatch, corruption):
    f = _fixture(monkeypatch)
    evaluation, start, _ = await _completed(f)
    row = next(x for x in f.connection.rows if x["kind"] == "evaluation")
    if corruption == "digest":
        row["payload_sha256"] = "0" * 64
    else:
        value = evaluation.model_dump(mode="json")
        value["receipt_sha256"] = None
        if corruption == "contract":
            value["contract_version"] = "unknown-evaluation.v99"
            encoded, digest = canonical_payload(value)
            row.update(payload_json=encoded, payload_sha256=digest)
        else:
            value[
                {"clock": "evidence_as_of_ts_ms", "anchor": "anchor_bar_sha256", "scope": "sample_id"}[
                    corruption
                ]
            ] = _CUTOFF + 1 if corruption == "clock" else "0" * 64 if corruption == "anchor" else "foreign"
            if corruption == "anchor":
                value["intent"] = None  # Typed but does not match the independently saved context.
            mutated = ResearchCausalStrategyEvaluationReceiptV1.model_validate(value)
            row.update(_row(mutated, kind="evaluation"))
            evaluation = mutated
    with pytest.raises((MessageIdentityConflict, ValueError)):
        if corruption in ("digest", "contract"):
            decode_campaign_evaluation_row(row)
        else:
            await f.repository.record_causal_pair(
                campaign_id=f.plan.campaign_id,
                sample_id="one",
                evaluation_receipt_sha256=evaluation.receipt_sha256,
                attempt_id=start.attempt_id,
            )


async def test_causal_outcome_requires_the_same_independent_pair_and_model_history(monkeypatch):
    f = _fixture(monkeypatch)
    evaluation, start, terminal = await _completed(f)
    pair = await f.repository.record_causal_pair(
        campaign_id=f.plan.campaign_id,
        sample_id="one",
        evaluation_receipt_sha256=evaluation.receipt_sha256,
        attempt_id=start.attempt_id,
    )
    outcome = ResearchArmOutcomeV1(
        campaign_id=f.plan.campaign_id,
        sample_id="one",
        arm_id=start.arm_id,
        claim_id=f.claim.claim_id,
        status="PROPOSAL",
        observed_at_ts_ms=f.connection.now,
        bundle_receipt_sha256=f.bundle.receipt_sha256,
        evaluation_receipt_sha256=evaluation.receipt_sha256,
        attempt_id=start.attempt_id,
        decision_receipt_sha256=terminal.receipt_sha256,
        causal_sample_receipt_sha256=pair.receipt_sha256,
    )
    assert await f.repository.record_outcome(outcome)
    assert not await f.repository.record_outcome(outcome)
    # This exact typed family cannot be promoted to the legacy Core V1 sample path.
    from kairos_core import ResearchDecisionSampleV1

    assert type(pair) is not ResearchDecisionSampleV1


async def test_legacy_evaluator_cannot_adopt_or_overwrite_new_causal_context(monkeypatch):
    f = _fixture(monkeypatch)
    with pytest.raises(MessageIdentityConflict, match="legacy evaluator"):
        await f.repository.record_evaluation(claim=f.claim, bundle=f.bundle, intent=None)
    assert f.connection.writes == 0
    evaluation = await f.repository.record_causal_evaluation(claim=f.claim, bundle=f.bundle, result=f.result)
    with pytest.raises(MessageIdentityConflict, match="legacy evaluator"):
        await f.repository.record_evaluation(claim=f.claim, bundle=f.bundle, intent=None)
    assert await f.repository.load_evaluation(evaluation.receipt_sha256) == evaluation
    assert f.connection.writes == 1


@pytest.mark.parametrize("kind", ["evaluation", "sample"])
async def test_closed_family_decoders_reject_wrong_receipt_kind(monkeypatch, kind):
    f = _fixture(monkeypatch)
    evaluation, start, _ = await _completed(f)
    if kind == "evaluation":
        receipt, decoder = evaluation, decode_campaign_evaluation_row
    else:
        receipt = await f.repository.record_causal_pair(
            campaign_id=f.plan.campaign_id,
            sample_id="one",
            evaluation_receipt_sha256=evaluation.receipt_sha256,
            attempt_id=start.attempt_id,
        )
        decoder = decode_campaign_sample_row
    with pytest.raises(MessageIdentityConflict, match="different receipt kind"):
        decoder(_row(receipt, kind="source"))


async def test_default_repository_denies_new_family_attempt_completion_and_outcome(monkeypatch):
    f = _fixture(monkeypatch)
    evaluation, start, terminal = await _completed(f)
    f.repository._causal_scheduler_sha256 = None
    before = f.connection.writes
    with pytest.raises(MessageIdentityConflict, match="scheduler identity"):
        await f.repository.start_attempt(start)
    with pytest.raises(MessageIdentityConflict, match="scheduler identity"):
        await f.repository.finish_attempt(terminal)
    outcome = ResearchArmOutcomeV1(
        campaign_id=f.plan.campaign_id,
        sample_id="one",
        arm_id=start.arm_id,
        claim_id=f.claim.claim_id,
        status="PROPOSAL",
        observed_at_ts_ms=f.connection.now,
        bundle_receipt_sha256=f.bundle.receipt_sha256,
        evaluation_receipt_sha256=evaluation.receipt_sha256,
        attempt_id=start.attempt_id,
        decision_receipt_sha256=terminal.receipt_sha256,
    )
    with pytest.raises(MessageIdentityConflict, match="scheduler identity"):
        await f.repository.record_outcome(outcome)
    assert f.connection.writes == before


async def test_default_repository_cannot_capture_new_context_even_through_direct_api(monkeypatch):
    f = _fixture(monkeypatch, enabled=False)
    f.connection.rows = []
    with pytest.raises(MessageIdentityConflict, match="scheduler identity"):
        await f.repository.capture_source(
            campaign_id=f.plan.campaign_id,
            sample_id="one",
            source_kind="MARKET_SNAPSHOT",
            source_name="bars",
            reference="original:market",
            source_as_of_ts_ms=120_200,
            content=f.context.model_dump(mode="json"),
        )
    assert f.connection.writes == 0


async def test_saved_pair_replay_refuses_late_actual_db_recording(monkeypatch):
    f = _fixture(monkeypatch)
    evaluation, start, _ = await _completed(f)
    await f.repository.record_causal_pair(
        campaign_id=f.plan.campaign_id,
        sample_id="one",
        evaluation_receipt_sha256=evaluation.receipt_sha256,
        attempt_id=start.attempt_id,
    )
    next(x for x in f.connection.rows if x["kind"] == "sample")["causal_recorded_at_ts_ms"] = _CUTOFF + 3_001
    before = f.connection.writes
    with pytest.raises(MessageIdentityConflict, match="actual database recording"):
        await f.repository.record_causal_pair(
            campaign_id=f.plan.campaign_id,
            sample_id="one",
            evaluation_receipt_sha256=evaluation.receipt_sha256,
            attempt_id=start.attempt_id,
        )
    assert f.connection.writes == before


@pytest.mark.parametrize("corruption", ["wrong-backend", "stale-proposal", "stale-intent"])
async def test_pair_independently_revalidates_backend_and_candidate_expiry(monkeypatch, corruption):
    f = _fixture(monkeypatch)
    evaluation, start, terminal = await _completed(f)
    if corruption == "stale-intent":
        payload = evaluation.model_dump(mode="json")
        payload["receipt_sha256"] = None
        payload["intent"]["entry_expires_ts_ms"] = f.connection.now
        payload["intent"]["intent_id"] = None
        payload["intent"] = StrategyIntentV1.model_validate(payload["intent"]).model_dump(mode="json")
        evaluation = ResearchCausalStrategyEvaluationReceiptV1.model_validate(payload)
        next(x for x in f.connection.rows if x["kind"] == "evaluation").update(
            _row(evaluation, kind="evaluation")
        )
    else:
        payload = terminal.model_dump(mode="json")
        payload["receipt_sha256"] = None
        payload["proposal"]["proposal_id"] = None
        if corruption == "wrong-backend":
            for name in ("proposal", "completion"):
                payload[name]["model_provenance"]["resolved_model"] = "unregistered-backend"
        else:
            payload["proposal"]["expires_at_ts_ms"] = f.connection.now
        proposal = LLMTradeProposalV1.model_validate(payload["proposal"])
        payload["proposal"] = proposal.model_dump(mode="json")
        payload["completion"]["proposal_id"] = proposal.proposal_id
        payload["completion"]["completion_receipt_id"] = None
        terminal = ResearchLLMAttemptTerminalV1.model_validate(payload)
        next(x for x in f.connection.rows if x["kind"] == "terminal").update(
            _row(terminal, kind="terminal", arm=start.arm_id, slot=start.attempt_id, recorded_at=121_300)
        )
    before = f.connection.writes
    with pytest.raises(MessageIdentityConflict):
        await f.repository.record_causal_pair(
            campaign_id=f.plan.campaign_id,
            sample_id="one",
            evaluation_receipt_sha256=evaluation.receipt_sha256,
            attempt_id=start.attempt_id,
        )
    assert f.connection.writes == before


@pytest.mark.parametrize(
    "corruption",
    [
        "late-source",
        "foreign-source",
        "future-snapshot",
        "wrong-reference",
        "wrong-event-clock",
        "raw-upstream-claim",
    ],
)
async def test_evaluation_reloads_all_original_causal_sources_not_caller_attestation(monkeypatch, corruption):
    f = _fixture(monkeypatch)
    source = (
        f.sources[0]
        if corruption != "future-snapshot"
        else next(x for x in f.sources if x.source_kind == "MARKET_SNAPSHOT")
    )
    payload = source.model_dump(mode="json")
    payload.update(receipt_sha256=None, content_sha256=None)
    if corruption == "late-source":
        payload["observed_at_ts_ms"] = _CUTOFF + 1
    elif corruption == "foreign-source":
        payload["sample_id"] = "foreign"
    elif corruption == "future-snapshot":
        payload["content"]["market_snapshot"]["produced_at"] = "1970-01-01T00:02:00.600Z"
    elif corruption == "wrong-reference":
        payload["reference"] = "forged:producer-message"
    elif corruption == "wrong-event-clock":
        payload["source_as_of_ts_ms"] -= 1
    else:
        payload["content"]["provenance"]["upstream_context"] = "SELF_ATTESTED_AVAILABLE"
    changed = ResearchSourceReceiptV1.model_validate(payload)
    next(x for x in f.connection.rows if x["receipt_sha256"] == source.receipt_sha256).update(
        _row(changed, kind="source", slot=f"{changed.source_kind}:{changed.source_name}")
    )
    payload = f.bundle.model_dump(mode="json")
    payload.update(
        receipt_sha256=None,
        source_receipt_sha256s=tuple(
            sorted(
                changed.receipt_sha256 if x == source.receipt_sha256 else x
                for x in f.bundle.source_receipt_sha256s
            )
        ),
    )
    if corruption == "future-snapshot":
        payload["market_snapshot_sha256"] = changed.content_sha256
    bundle = ResearchCausalBundleV1.model_validate(payload)
    next(x for x in f.connection.rows if x["kind"] == "bundle").update(_row(bundle, kind="bundle"))
    with pytest.raises(MessageIdentityConflict):
        await f.repository.record_causal_evaluation(claim=f.claim, bundle=bundle, result=f.result)
    assert f.connection.writes == 0


@pytest.mark.parametrize("enabled,expected", [(False, _CUTOFF), (True, _CUTOFF + 1)])
async def test_new_public_clock_is_conservative_but_legacy_floor_is_unchanged(monkeypatch, enabled, expected):
    f = _fixture(monkeypatch, enabled=enabled)

    async def fractional_clock(sql):
        assert "clock_timestamp()" in sql
        return ceil(_CUTOFF + 0.5) if "ceil(" in sql else floor(_CUTOFF + 0.5)

    monkeypatch.setattr(f.connection, "fetchval", fractional_clock)
    assert await f.repository.clock() == expected
    f.connection.rows = []
    captured = await f.repository.capture_source(
        campaign_id=f.plan.campaign_id,
        sample_id="one",
        source_kind="NEWS",
        source_name="news",
        reference="news:fractional",
        source_as_of_ts_ms=120_200,
        content={"actual": "news"},
    )
    assert captured.observed_at_ts_ms == expected


async def test_new_family_fractional_post_cutoff_source_cannot_be_frozen(monkeypatch):
    f = _fixture(monkeypatch)
    f.connection.rows = [
        x for x in f.connection.rows if x["kind"] == "source" and x["slot_key"] != "MARKET_SNAPSHOT:bars"
    ]

    async def fractional_clock(sql):
        return ceil(_CUTOFF + 0.5) if "ceil(" in sql else floor(_CUTOFF + 0.5)

    monkeypatch.setattr(f.connection, "fetchval", fractional_clock)
    captured = await f.repository.capture_source(
        campaign_id=f.plan.campaign_id,
        sample_id="one",
        source_kind="MARKET_SNAPSHOT",
        source_name="bars",
        reference="original:market",
        source_as_of_ts_ms=120_200,
        content=f.context.model_dump(mode="json"),
    )
    assert captured.observed_at_ts_ms == _CUTOFF + 1
    with pytest.raises(MessageIdentityConflict):
        await f.repository.freeze_bundle(f.claim)
    assert not any(x["kind"] == "bundle" for x in f.connection.rows)


async def test_new_family_fractional_actual_completion_is_late_not_truncated_timely(monkeypatch):
    f = _fixture(monkeypatch)
    evaluation, start, terminal = await _completed(f)
    terminal_row = next(x for x in f.connection.rows if x["kind"] == "terminal")
    terminal_row.update(recorded_at_ts_ms=_CUTOFF + 3_000, causal_recorded_at_ts_ms=_CUTOFF + 3_001)
    f.connection.now = _CUTOFF + 3_100
    payload = dict(
        campaign_id=f.plan.campaign_id,
        sample_id="one",
        arm_id=start.arm_id,
        claim_id=f.claim.claim_id,
        observed_at_ts_ms=f.connection.now,
        bundle_receipt_sha256=f.bundle.receipt_sha256,
        evaluation_receipt_sha256=evaluation.receipt_sha256,
        attempt_id=start.attempt_id,
        decision_receipt_sha256=terminal.receipt_sha256,
    )
    with pytest.raises(MessageIdentityConflict, match="independent attempt history"):
        await f.repository.record_outcome(ResearchArmOutcomeV1(**payload, status="PROPOSAL"))
    assert await f.repository.record_outcome(ResearchArmOutcomeV1(**payload, status="LATE"))


async def test_new_baseline_actual_insertion_crossing_cutoff_is_late_for_all_arms(monkeypatch):
    f = _fixture(monkeypatch)
    evaluation = await f.repository.record_causal_evaluation(claim=f.claim, bundle=f.bundle, result=f.result)
    row = next(x for x in f.connection.rows if x["kind"] == "evaluation")
    row.update(recorded_at_ts_ms=_CUTOFF + 3_000, causal_recorded_at_ts_ms=_CUTOFF + 3_001)
    f.connection.now = _CUTOFF + 3_100
    assert (
        await f.repository.evaluation_recorded_at(
            campaign_id=f.plan.campaign_id,
            sample_id="one",
            evaluation_receipt_sha256=evaluation.receipt_sha256,
        )
        == _CUTOFF + 3_001
    )
    with pytest.raises(MessageIdentityConflict, match="different receipt"):
        await f.repository.evaluation_recorded_at(
            campaign_id=f.plan.campaign_id, sample_id="one", evaluation_receipt_sha256="0" * 64
        )
    for arm, forbidden in (
        ("strategy-only", "BASELINE"),
        ("strategy-review", "NO_INTENT"),
        ("llm-proposal-research", "BUDGET_BLOCKED"),
    ):
        payload = dict(
            campaign_id=f.plan.campaign_id,
            sample_id="one",
            arm_id=arm,
            claim_id=f.claim.claim_id,
            observed_at_ts_ms=f.connection.now,
            bundle_receipt_sha256=f.bundle.receipt_sha256,
            evaluation_receipt_sha256=evaluation.receipt_sha256,
        )
        with pytest.raises(MessageIdentityConflict, match="late evaluator"):
            await f.repository.record_outcome(ResearchArmOutcomeV1(**payload, status=forbidden))
        assert await f.repository.record_outcome(ResearchArmOutcomeV1(**payload, status="LATE"))
    assert not any(x["kind"] == "start" for x in f.connection.rows)


async def test_legacy_evaluation_recording_accessor_keeps_floor_clock(monkeypatch):
    f = _fixture(monkeypatch, enabled=False, intent=False)
    old = ResearchStrategyEvaluationReceiptV1(
        campaign_id=f.plan.campaign_id,
        sample_id="one",
        schedule_digest=f.schedule.schedule_digest,
        candidate_protocol_digest=f.protocol.protocol_digest,
        strategy_id=f.schedule.strategy_id,
        strategy_revision=f.schedule.strategy_revision,
        symbol="BTCUSDT",
        timeframe="1m",
        evidence_as_of_ts_ms=_CUTOFF,
        evaluated_at_ts_ms=_CUTOFF + 100,
        market_snapshot_sha256=f.bundle.market_snapshot_sha256,
        evaluator_sha256=f.schedule.evaluator_sha256,
        source_receipt_sha256s=f.bundle.source_receipt_sha256s,
        intent=None,
    )
    row = _row(old, kind="evaluation", recorded_at=_CUTOFF + 100)
    row["causal_recorded_at_ts_ms"] = _CUTOFF + 101
    f.connection.rows.append(row)
    assert (
        await f.repository.evaluation_recorded_at(
            campaign_id=f.plan.campaign_id, sample_id="one", evaluation_receipt_sha256=old.receipt_sha256
        )
        == _CUTOFF + 100
    )
