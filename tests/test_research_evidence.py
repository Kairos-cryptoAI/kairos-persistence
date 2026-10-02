"""Independent source evidence and durable, honest SIM attempt histories."""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import asyncpg
import pytest
from kairos_core import (
    AdaptiveCandidateProtocolV1,
    EvidenceReferenceV1,
    LLMCallFailureV1,
    LLMProposalAdaptiveCandidateArmV1,
    LLMProposalCompletionReceiptV1,
    LLMProposalModelProvenanceV1,
    LLMTradeProposalV1,
    ResearchObservationScheduleV1,
    ResearchObservationWindowV1,
    StrategyOnlyAdaptiveCandidateArmV1,
    StrategyReviewAdaptiveCandidateArmV1,
    canonical_sha256,
)
from kairos_core.research_pairing import ScheduledResearchSampleV1, build_research_decision_sample

from kairos_persistence import (
    Database,
    MessageIdentityConflict,
    MigrationProfile,
    PersistenceSettings,
    ResearchAdaptiveCandidateProtocolRepository,
    ResearchEvidenceRepository,
    ResearchLLMAttemptStartV1,
    ResearchLLMAttemptTerminalV1,
    ResearchObservationScheduleRepository,
    ResearchSourceReceiptV1,
    ResearchStrategyEvaluationReceiptV1,
)
from kairos_persistence.database_target import connect_verified_database, require_database_target_url
from kairos_persistence.research_evidence import _decode_row, _verify_model
from kairos_persistence.runtime import canonical_payload

_T0 = 1_760_000_000_000
_CONTENT = {"symbol": "BTCUSDT", "close": 100, "closed": True}
_SNAPSHOT = canonical_sha256(_CONTENT)


def _schedule(campaign_id: str = "engineering-evidence-v1"):
    return ResearchObservationScheduleV1(
        campaign_id=campaign_id,
        strategy_id="test-baseline",
        strategy_revision="frozen-v1",
        source_set_sha256="a" * 64,
        evaluator_sha256="b" * 64,
        windows=(
            ResearchObservationWindowV1(
                sample_id="sample-001",
                symbol="BTCUSDT",
                timeframe="1m",
                market_as_of_ts_ms=_T0,
                market_snapshot_sha256=_SNAPSHOT,
                paired_at_ts_ms=_T0 + 5_000,
                sample_deadline_ts_ms=_T0 + 10_000,
            ),
        ),
    )


def _protocol(schedule):
    common = dict(
        candidate_id="test",
        candidate_revision="v1",
        artifact_sha256="1" * 64,
        input_feature_sha256="2" * 64,
        decision_mapping_sha256="3" * 64,
        hypothetical_exit_sha256="4" * 64,
        cost_model_sha256="5" * 64,
    )
    return AdaptiveCandidateProtocolV1(
        campaign_id=schedule.campaign_id,
        schedule_digest=schedule.schedule_digest,
        arms=(
            StrategyOnlyAdaptiveCandidateArmV1(**common),
            StrategyReviewAdaptiveCandidateArmV1(
                **common,
                provider="openai",
                model="test-model",
                prompt_sha256="6" * 64,
                schema_sha256="7" * 64,
            ),
            LLMProposalAdaptiveCandidateArmV1(
                **common,
                provider="openai",
                model="test-model",
                prompt_sha256="6" * 64,
                schema_sha256="7" * 64,
            ),
        ),
    )


def _source(schedule=None, protocol=None, **changes):
    schedule = schedule or _schedule()
    protocol = protocol or _protocol(schedule)
    values = dict(
        campaign_id=schedule.campaign_id,
        sample_id="sample-001",
        schedule_digest=schedule.schedule_digest,
        candidate_protocol_digest=protocol.protocol_digest,
        source_kind="MARKET_SNAPSHOT",
        source_name="saved-bars",
        reference="bar-001",
        source_as_of_ts_ms=_T0,
        observed_at_ts_ms=_T0,
        content=_CONTENT,
    )
    values.update(changes)
    return ResearchSourceReceiptV1(**values)


def _evaluation(source, schedule=None, protocol=None, **changes):
    schedule = schedule or _schedule()
    protocol = protocol or _protocol(schedule)
    values = dict(
        campaign_id=schedule.campaign_id,
        sample_id="sample-001",
        schedule_digest=schedule.schedule_digest,
        candidate_protocol_digest=protocol.protocol_digest,
        strategy_id=schedule.strategy_id,
        strategy_revision=schedule.strategy_revision,
        symbol="BTCUSDT",
        timeframe="1m",
        evidence_as_of_ts_ms=_T0,
        evaluated_at_ts_ms=_T0 + 1,
        market_snapshot_sha256=_SNAPSHOT,
        evaluator_sha256=schedule.evaluator_sha256,
        source_receipt_sha256s=(source.receipt_sha256,),
    )
    values.update(changes)
    return ResearchStrategyEvaluationReceiptV1(**values)


def _start(schedule=None, protocol=None, **changes):
    schedule = schedule or _schedule()
    protocol = protocol or _protocol(schedule)
    values = dict(
        attempt_id="research-attempt-001",
        campaign_id=schedule.campaign_id,
        arm_id="strategy-review",
        sample_id="sample-001",
        schedule_digest=schedule.schedule_digest,
        candidate_protocol_digest=protocol.protocol_digest,
        arm_protocol_digest=protocol.arm_digest("strategy-review"),
        symbol="BTCUSDT",
        timeframe="1m",
        market_as_of_ts_ms=_T0,
        market_snapshot_sha256=_SNAPSHOT,
        sample_deadline_ts_ms=_T0 + 10_000,
        provider="openai",
        requested_model="test-model",
        prompt_sha256="6" * 64,
        budget_reservation_id="research-attempt-001",
        attempt_started_at_ts_ms=_T0 + 10,
    )
    values.update(changes)
    return ResearchLLMAttemptStartV1(**values)


def _failure(start, *, observed_at=None):
    return LLMCallFailureV1(
        campaign_id=start.campaign_id,
        arm_id=start.arm_id,
        sample_id=start.sample_id,
        symbol=start.symbol,
        timeframe=start.timeframe,
        market_as_of_ts_ms=start.market_as_of_ts_ms,
        market_snapshot_sha256=start.market_snapshot_sha256,
        sample_deadline_ts_ms=start.sample_deadline_ts_ms,
        attempt_id=start.attempt_id,
        provider=start.provider,
        requested_model=start.requested_model,
        prompt_sha256=start.prompt_sha256,
        budget_reservation_id=start.budget_reservation_id,
        attempt_started_at_ts_ms=start.attempt_started_at_ts_ms,
        failure_observed_at_ts_ms=observed_at or _T0 + 200,
        failure_class="TIMEOUT",
    )


def _terminal(start, failure=None, **changes):
    failure = failure or _failure(start)
    values = dict(
        attempt_id=start.attempt_id,
        start_receipt_sha256=start.receipt_sha256,
        terminal_status="FAILED",
        observed_at_ts_ms=failure.failure_observed_at_ts_ms,
        failure=failure,
    )
    values.update(changes)
    return ResearchLLMAttemptTerminalV1(**values)


def _sample(schedule, protocol, evaluation, arm_id, failure=None):
    window = schedule.windows[0]
    planned = ScheduledResearchSampleV1(
        campaign_id=schedule.campaign_id,
        arm_id=arm_id,
        sample_id=window.sample_id,
        symbol=window.symbol,
        timeframe=window.timeframe,
        market_as_of_ts_ms=window.market_as_of_ts_ms,
        market_snapshot_sha256=_SNAPSHOT,
        strategy_id=schedule.strategy_id,
        strategy_revision=schedule.strategy_revision,
        paired_at_ts_ms=window.paired_at_ts_ms,
        sample_deadline_ts_ms=window.sample_deadline_ts_ms,
    )
    sample = build_research_decision_sample(
        planned,
        strategy_evaluation=evaluation.as_evidence(arm_id),
        llm_failure=failure,
        llm_was_called=failure is not None,
    )
    return type(sample).model_validate(
        {**sample.to_payload(), "sample_record_id": None, "arm_protocol_digest": protocol.arm_digest(arm_id)}
    )


def _completed(start, source, **changes):
    proposal = LLMTradeProposalV1(
        campaign_id=start.campaign_id,
        arm_id=start.arm_id,
        sample_id=start.sample_id,
        symbol=start.symbol,
        timeframe=start.timeframe,
        market_as_of_ts_ms=start.market_as_of_ts_ms,
        market_snapshot_sha256=start.market_snapshot_sha256,
        expires_at_ts_ms=start.sample_deadline_ts_ms,
        action="SHORT_BIAS",
        rationale="Synthetic no-intent disagreement for deterministic replay only.",
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
            resolved_model="fixture-v1",
            request_id="test-request",
            prompt_sha256=start.prompt_sha256,
            response_sha256="8" * 64,
            budget_reservation_id=start.budget_reservation_id,
            latency_ms=190,
            cost_usd=0,
        ),
    )
    completion = LLMProposalCompletionReceiptV1(
        campaign_id=start.campaign_id,
        arm_id=start.arm_id,
        sample_id=start.sample_id,
        symbol=start.symbol,
        timeframe=start.timeframe,
        market_as_of_ts_ms=start.market_as_of_ts_ms,
        market_snapshot_sha256=start.market_snapshot_sha256,
        sample_deadline_ts_ms=start.sample_deadline_ts_ms,
        attempt_id=start.attempt_id,
        proposal_id=proposal.proposal_id,
        model_provenance=proposal.model_provenance,
        attempt_started_at_ts_ms=start.attempt_started_at_ts_ms,
        response_observed_at_ts_ms=_T0 + 200,
    )
    values = dict(
        attempt_id=start.attempt_id,
        start_receipt_sha256=start.receipt_sha256,
        terminal_status="COMPLETED",
        observed_at_ts_ms=_T0 + 200,
        proposal=proposal,
        completion=completion,
    )
    values.update(changes)
    return ResearchLLMAttemptTerminalV1(**values)


def test_source_is_independent_content_addressed_and_round_trips():
    source = _source()
    assert source.content_sha256 == _SNAPSHOT
    assert source.receipt_sha256 == canonical_sha256(source.identity_payload())
    assert ResearchSourceReceiptV1.model_validate_json(source.model_dump_json()) == source
    assert _source(receipt_sha256=None) == source


def test_source_json_resource_and_exact_type_bounds_precede_serialization():
    nested = {}
    for _ in range(34):
        nested = {"nested": nested}
    cyclic = {}
    cyclic["self"] = cyclic
    for content in (nested, cyclic, {"many": list(range(10001))}, {"tuple": (1, 2)}, {1: "bad"}):
        with pytest.raises(ValueError):
            _source(content=content)


def test_completed_terminal_matches_full_proposal_scope():
    terminal = _completed(_start(), _source())
    assert terminal.proposal.action.value == "SHORT_BIAS"
    changed = LLMProposalCompletionReceiptV1.model_validate(
        {
            **terminal.completion.model_dump(mode="json"),
            "completion_receipt_id": None,
            "symbol": "ETHUSDT",
        }
    )
    with pytest.raises(ValueError, match="scope"):
        _completed(_start(), _source(), completion=changed)


@pytest.mark.parametrize(
    "changes",
    [
        {"content_sha256": "0" * 64},
        {"receipt_sha256": "0" * 64},
        {"source_kind": "ORDERS"},
        {"observed_at_ts_ms": _T0 - 1},
        {"content": {"x": float("nan")}},
        {"unknown": True},
        {"content": {"text": "x" * 262144}},
        {"campaign_id": " campaign"},
    ],
)
def test_source_rejects_changed_unbounded_or_non_json_evidence(changes):
    with pytest.raises((ValueError, TypeError)):
        _source(**changes)


def test_no_intent_is_saved_not_inferred_from_absence():
    receipt = _evaluation(_source())
    evidence = receipt.as_evidence("llm-proposal-research")
    assert receipt.intent is None
    assert evidence.evaluation_sha256 == receipt.receipt_sha256
    assert evidence.intent_id is None


@pytest.mark.parametrize(
    "changes",
    [
        {"source_receipt_sha256s": ()},
        {"source_receipt_sha256s": ("1" * 64, "1" * 64)},
        {"source_receipt_sha256s": ("2" * 64, "1" * 64)},
        {"evaluated_at_ts_ms": _T0 - 1},
    ],
)
def test_evaluation_rejects_ambiguous_sources_or_backdated_observation(changes):
    with pytest.raises(ValueError):
        _evaluation(_source(), **changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"budget_reservation_id": "other"},
        {"arm_id": "strategy-only"},
        {"attempt_started_at_ts_ms": _T0 - 1},
        {"attempt_started_at_ts_ms": _T0 + 10000},
    ],
)
def test_attempt_admission_requires_exact_scope_and_reservation(changes):
    with pytest.raises(ValueError):
        _start(**changes)


def test_timeout_terminal_is_distinct_from_unresolved_and_retains_late_clock():
    start = _start()
    failure = _failure(start, observed_at=_T0 + 20000)
    terminal = _terminal(start, failure)
    assert terminal.failure.is_late
    assert terminal.observed_at_ts_ms == _T0 + 20000
    unresolved = ResearchLLMAttemptTerminalV1(
        attempt_id=start.attempt_id,
        start_receipt_sha256=start.receipt_sha256,
        terminal_status="UNRESOLVED",
        observed_at_ts_ms=_T0 + 20000,
    )
    assert unresolved.failure is None
    with pytest.raises(ValueError, match="fabricate"):
        _terminal(start, terminal_status="UNRESOLVED")


def test_store_decode_rehashes_exact_bytes_and_revalidates_nested_content():
    source = _source()
    encoded, digest = canonical_payload(source.model_dump(mode="json"))
    row = dict(payload_json=encoded, payload_sha256=digest, receipt_sha256=source.receipt_sha256)
    assert _decode_row(row, ResearchSourceReceiptV1) == source
    for field in ("payload_sha256", "receipt_sha256"):
        with pytest.raises(MessageIdentityConflict):
            _decode_row({**row, field: "0" * 64}, ResearchSourceReceiptV1)
    source.content["close"] = 42
    with pytest.raises(MessageIdentityConflict):
        _verify_model(source, ResearchSourceReceiptV1)


def test_receipt_repository_never_accepts_runtime_or_read_only_connections():
    runtime = Database(PersistenceSettings(database_url="postgresql://test:test@localhost/kairos"))
    with pytest.raises(ValueError, match="SIMULATOR"):
        ResearchEvidenceRepository(runtime)
    simulator = Database(
        PersistenceSettings(database_url="postgresql://test:test@localhost/kairos_sim_test_unit"),
        migration_profile=MigrationProfile.SIMULATOR,
        read_only=True,
    )
    with pytest.raises(ValueError, match="writable"):
        ResearchEvidenceRepository(simulator)


def test_migration_is_additive_sim_only_with_immutable_attempt_fence():
    name = "025_simulator_research_evidence.sql"
    assert name not in Database.migration_names(MigrationProfile.RUNTIME)
    assert name == Database.migration_names(MigrationProfile.SIMULATOR)[-1]
    sql = (Path(__file__).parents[1] / "kairos_persistence" / "migrations" / name).read_text()
    assert "UNIQUE(campaign_id,arm_id,sample_id)" in sql
    assert "STARTED" not in sql  # absence of terminal is explicitly unresolved
    assert "UPDATE sim_research" not in sql
    assert "append-only" in sql
    assert "cannot adopt results or seals retrospectively" in sql


@pytest.mark.integration
async def test_real_sim_store_resolves_evidence_and_fails_closed_on_crash_duplicate_late_and_drift():
    database_url = os.getenv("KAIROS_SIM_EVIDENCE_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("explicit isolated SIM evidence test database required")
    database_name = urlsplit(database_url).path.removeprefix("/")
    if not database_name.startswith("kairos_sim_test_evidence_"):
        raise RuntimeError("refusing non-disposable independent-evidence integration target")
    require_database_target_url(database_url, database_name, local_only=True)
    database = Database(
        PersistenceSettings(database_url=database_url), migration_profile=MigrationProfile.SIMULATOR
    )
    await connect_verified_database(database, database_name, local_only=True)
    try:
        await database.migrate()
        schedule = _schedule("evidence-" + uuid4().hex[:16])
        protocol = _protocol(schedule)
        await ResearchObservationScheduleRepository(database).register(schedule)
        await ResearchAdaptiveCandidateProtocolRepository(database).register(protocol)
        repository = ResearchEvidenceRepository(database)
        assert await repository.enroll_campaign(schedule.campaign_id)
        assert not await repository.enroll_campaign(schedule.campaign_id)
        source = _source(schedule, protocol)
        assert await repository.record_source(source)
        assert not await repository.record_source(source)
        assert await repository.load_source(source.receipt_sha256) == source
        with pytest.raises(MessageIdentityConflict):
            await repository.record_source(_source(schedule, protocol, reference="other"))
        evaluation = _evaluation(source, schedule, protocol)
        assert await repository.record_evaluation(evaluation)
        assert not await repository.record_evaluation(evaluation)
        assert await repository.load_evaluation(evaluation.receipt_sha256) == evaluation
        with pytest.raises(MessageIdentityConflict, match="evaluator"):
            await repository.record_evaluation(
                _evaluation(source, schedule, protocol, evaluator_sha256="f" * 64)
            )
        late_news = _source(
            schedule,
            protocol,
            source_kind="NEWS",
            reference="news-001",
            content={"headline": "test"},
            observed_at_ts_ms=_T0 + 1,
        )
        assert await repository.record_source(late_news)
        with pytest.raises(MessageIdentityConflict, match="unavailable"):
            await repository.record_evaluation(
                _evaluation(
                    source,
                    schedule,
                    protocol,
                    source_receipt_sha256s=tuple(sorted((source.receipt_sha256, late_news.receipt_sha256))),
                )
            )
        attempt_id = schedule.campaign_id + "-attempt"
        start = _start(schedule, protocol, attempt_id=attempt_id, budget_reservation_id=attempt_id)
        assert await repository.find_attempt(start.attempt_id) is None
        assert await repository.start_attempt(start)
        assert not await repository.start_attempt(start)
        assert await repository.load_attempt(start.attempt_id) == (start, None)
        states = await repository.pending_observations(campaign_id=schedule.campaign_id)
        assert any(row["state"] == "STARTED_UNRESOLVED" for row in states)
        uncalled = _sample(schedule, protocol, evaluation, "strategy-review")
        with pytest.raises(MessageIdentityConflict, match="ambiguous"):
            await repository.record_verified_sample(uncalled)
        retry = _start(schedule, protocol, attempt_id="attempt-retry", budget_reservation_id="attempt-retry")
        with pytest.raises(MessageIdentityConflict):
            await repository.start_attempt(retry)
        failure = _failure(start)
        terminal = _terminal(start, failure)
        assert await repository.finish_attempt(terminal)
        assert not await repository.finish_attempt(terminal)
        assert await repository.load_attempt(start.attempt_id) == (start, terminal)
        with pytest.raises(MessageIdentityConflict, match="immutable"):
            await repository.finish_attempt(_terminal(start, _failure(start, observed_at=_T0 + 300)))
        for arm in schedule.arm_ids:
            sample = _sample(
                schedule, protocol, evaluation, arm, failure if arm == "strategy-review" else None
            )
            assert await repository.record_verified_sample(sample)
            assert not await repository.record_verified_sample(sample)
        qualified = await repository.seal_verified_coverage(campaign_id=schedule.campaign_id)
        assert qualified.qualification == "INDEPENDENT_SOURCE_REPLAY_ONLY"
        assert qualified.economic_qualification is False and qualified.live_orders_allowed is False
        assert await repository.seal_verified_coverage(campaign_id=schedule.campaign_id) == qualified
        for table in (
            "sim_research_source_receipts",
            "sim_research_evaluation_receipts",
            "sim_research_llm_attempt_starts",
            "sim_research_llm_attempt_terminals",
            "sim_research_source_qualified_seals",
        ):
            async with database.transaction() as connection:
                with pytest.raises(asyncpg.PostgresError, match="append-only"):
                    await connection.execute(f"DELETE FROM {table}")
        await database.migrate()  # idempotent profile, no new evidence adoption
    finally:
        await database.close()


@asynccontextmanager
async def _isolated_campaign():
    database_url = os.getenv("KAIROS_SIM_EVIDENCE_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("explicit isolated SIM evidence test database required")
    name = urlsplit(database_url).path.removeprefix("/")
    if not name.startswith("kairos_sim_test_evidence_"):
        raise RuntimeError("refusing non-disposable independent-evidence integration target")
    require_database_target_url(database_url, name, local_only=True)
    database = Database(
        PersistenceSettings(database_url=database_url), migration_profile=MigrationProfile.SIMULATOR
    )
    await connect_verified_database(database, name, local_only=True)
    try:
        await database.migrate()
        schedule = _schedule("evidence-" + uuid4().hex[:16])
        protocol = _protocol(schedule)
        await ResearchObservationScheduleRepository(database).register(schedule)
        await ResearchAdaptiveCandidateProtocolRepository(database).register(protocol)
        repository = ResearchEvidenceRepository(database)
        await repository.enroll_campaign(schedule.campaign_id)
        source = _source(schedule, protocol)
        await repository.record_source(source)
        yield database, repository, schedule, protocol, source
    finally:
        await database.close()


@pytest.mark.integration
async def test_real_sim_completed_no_intent_conflict_replays_independent_news_and_rejects_forgery():
    async with _isolated_campaign() as (database, repository, schedule, protocol, source):
        news = _source(
            schedule,
            protocol,
            source_kind="NEWS",
            reference="saved-news",
            content={"headline": "Synthetic negative news"},
        )
        await repository.record_source(news)
        evaluation = _evaluation(source, schedule, protocol)
        await repository.record_evaluation(evaluation)
        attempt_id = schedule.campaign_id + "-proposal"
        start = _start(
            schedule,
            protocol,
            arm_id="llm-proposal-research",
            arm_protocol_digest=protocol.arm_digest("llm-proposal-research"),
            attempt_id=attempt_id,
            budget_reservation_id=attempt_id,
        )
        admitted = await asyncio.gather(repository.start_attempt(start), repository.start_attempt(start))
        assert sorted(admitted) == [False, True]  # exactly one durable dispatch permission
        absent_news = _source(
            schedule,
            protocol,
            source_kind="NEWS",
            reference="absent-news",
            content={"headline": "not stored"},
        )
        with pytest.raises(MessageIdentityConflict, match="absent"):
            await repository.finish_attempt(_completed(start, absent_news))
        terminal = _completed(start, news)
        await repository.finish_attempt(terminal)
        assert await repository.find_attempt(start.attempt_id) == (start, terminal)
        planned = ScheduledResearchSampleV1(
            campaign_id=schedule.campaign_id,
            arm_id=start.arm_id,
            sample_id=start.sample_id,
            symbol=start.symbol,
            timeframe=start.timeframe,
            market_as_of_ts_ms=_T0,
            market_snapshot_sha256=_SNAPSHOT,
            strategy_id=schedule.strategy_id,
            strategy_revision=schedule.strategy_revision,
            paired_at_ts_ms=_T0 + 5000,
            sample_deadline_ts_ms=start.sample_deadline_ts_ms,
        )
        sample = build_research_decision_sample(
            planned,
            strategy_evaluation=evaluation.as_evidence(start.arm_id),
            llm_proposal=terminal.proposal,
            llm_completion=terminal.completion,
            llm_was_called=True,
        )
        sample = type(sample).model_validate(
            {
                **sample.to_payload(),
                "sample_record_id": None,
                "arm_protocol_digest": protocol.arm_digest(start.arm_id),
            }
        )
        assert sample.strategy_outcome == "NO_INTENT" and sample.llm_outcome == "SHORT_BIAS"
        forged = type(sample).model_validate(
            {**sample.to_payload(), "sample_record_id": None, "llm_completion_observed_at_ts_ms": _T0 + 201}
        )
        with pytest.raises(MessageIdentityConflict, match="exactly replay"):
            await repository.record_verified_sample(forged)
        assert await repository.record_verified_sample(sample)
        with pytest.raises(asyncpg.PostgresError, match="after sample results"):
            await repository.record_source(
                _source(
                    schedule, protocol, source_kind="MACRO", reference="after-result", content={"macro": "x"}
                )
            )
        # Enrolled campaigns cannot bypass the verified repository and suppress an evaluation.
        async with database.transaction() as connection:
            assert (
                await connection.fetchval(
                    "SELECT COUNT(*) FROM sim_research_llm_attempt_starts WHERE campaign_id=$1",
                    schedule.campaign_id,
                )
                == 1
            )


@pytest.mark.integration
async def test_real_sim_honest_unresolved_late_and_direct_sql_guards():
    async with _isolated_campaign() as (database, repository, schedule, protocol, source):
        evaluation = _evaluation(source, schedule, protocol)
        await repository.record_evaluation(evaluation)
        attempt_id = schedule.campaign_id + "-late"
        start = _start(schedule, protocol, attempt_id=attempt_id, budget_reservation_id=attempt_id)
        await repository.start_attempt(start)
        malformed = _terminal(start).model_dump(mode="json")
        malformed["failure"]["requested_model"] = "unregistered-model"
        encoded, digest = canonical_payload(malformed)
        async with database.transaction() as connection:
            with pytest.raises(asyncpg.PostgresError, match="terminal route"):
                await connection.execute(
                    "INSERT INTO sim_research_llm_attempt_terminals "
                    "(attempt_id,receipt_sha256,start_receipt_sha256,terminal_status,"
                    "observed_at_ts_ms,payload_json,payload_sha256) "
                    "VALUES($1,$2,$3,$4,$5,$6,$7)",
                    start.attempt_id,
                    malformed["receipt_sha256"],
                    start.receipt_sha256,
                    "FAILED",
                    _T0 + 200,
                    encoded,
                    digest,
                )
        late = _terminal(start, _failure(start, observed_at=_T0 + 20000))
        await repository.finish_attempt(late)
        assert (await repository.load_attempt(start.attempt_id))[1] == late
        with pytest.raises(MessageIdentityConflict, match="late attempt"):
            await repository.record_verified_sample(
                _sample(schedule, protocol, evaluation, "strategy-review")
            )
        # An ambiguous call is an immutable unresolved outcome, never NOT_CALLED or a retry.
        other_id = schedule.campaign_id + "-unresolved"
        other = _start(
            schedule,
            protocol,
            arm_id="llm-proposal-research",
            arm_protocol_digest=protocol.arm_digest("llm-proposal-research"),
            attempt_id=other_id,
            budget_reservation_id=other_id,
        )
        await repository.start_attempt(other)
        unresolved = ResearchLLMAttemptTerminalV1(
            attempt_id=other_id,
            start_receipt_sha256=other.receipt_sha256,
            terminal_status="UNRESOLVED",
            observed_at_ts_ms=_T0 + 25000,
        )
        await repository.finish_attempt(unresolved)
        with pytest.raises(MessageIdentityConflict, match="ambiguous"):
            await repository.record_verified_sample(_sample(schedule, protocol, evaluation, other.arm_id))
        with pytest.raises(MessageIdentityConflict, match="immutable"):
            await repository.finish_attempt(_terminal(other))
