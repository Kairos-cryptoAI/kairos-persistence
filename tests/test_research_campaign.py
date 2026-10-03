"""Opt-in causal campaign contracts and explicitly isolated native acceptance."""

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import asyncpg
import pytest
from kairos_core import (
    AdaptiveCandidateProtocolV1,
    EvidenceReferenceV1,
    LLMProposalAdaptiveCandidateArmV1,
    ResearchObservationScheduleV1,
    ResearchObservationWindowV1,
    StrategyOnlyAdaptiveCandidateArmV1,
    StrategyReviewAdaptiveCandidateArmV1,
    canonical_sha256,
)

from kairos_persistence import (
    Database,
    MessageIdentityConflict,
    MigrationProfile,
    PersistenceSettings,
    ResearchAdaptiveCandidateProtocolRepository,
    ResearchObservationScheduleRepository,
)
from kairos_persistence.database_target import connect_verified_database, require_database_target_url
from kairos_persistence.research_campaign import (
    ResearchArmOutcomeV1,
    ResearchCampaignPlanV1,
    ResearchCampaignRepository,
    ResearchCaptureRequirementV1,
    ResearchCausalBundleV1,
    ResearchReviewOutputV1,
    ResearchReviewReceiptV1,
    ResearchWindowClaimV1,
)


def _identity(now):
    schedule = ResearchObservationScheduleV1(
        campaign_id="native-campaign-" + uuid4().hex[:12],
        strategy_id="offline-fixture",
        strategy_revision="v1",
        source_set_sha256="a" * 64,
        evaluator_sha256="b" * 64,
        windows=(
            ResearchObservationWindowV1(
                sample_id="one",
                symbol="BTCUSDT",
                timeframe="1m",
                market_as_of_ts_ms=now + 2_000,
                paired_at_ts_ms=now + 5_000,
                sample_deadline_ts_ms=now + 10_000,
            ),
        ),
    )
    common = dict(
        candidate_id="offline-fixture",
        candidate_revision="v1",
        artifact_sha256="c" * 64,
        input_feature_sha256="d" * 64,
        decision_mapping_sha256="e" * 64,
        hypothetical_exit_sha256="f" * 64,
        cost_model_sha256="1" * 64,
    )
    llm = dict(
        provider="openai", model="test-not-a-provider-model", prompt_sha256="2" * 64, schema_sha256="3" * 64
    )
    protocol = AdaptiveCandidateProtocolV1(
        campaign_id=schedule.campaign_id,
        schedule_digest=schedule.schedule_digest,
        arms=(
            StrategyOnlyAdaptiveCandidateArmV1(**common),
            StrategyReviewAdaptiveCandidateArmV1(**common, **llm),
            LLMProposalAdaptiveCandidateArmV1(**common, **llm),
        ),
    )
    plan = ResearchCampaignPlanV1(
        campaign_id=schedule.campaign_id,
        schedule_digest=schedule.schedule_digest,
        candidate_protocol_digest=protocol.protocol_digest,
        recording_mode="OFFLINE_ENGINEERING_FIXTURE",
        scheduler_sha256="4" * 64,
        required_sources=tuple(
            ResearchCaptureRequirementV1(source_kind=kind, source_name=name, maximum_age_ms=5_000)
            for kind, name in (("MACRO", "macro"), ("MARKET_SNAPSHOT", "bars"), ("NEWS", "news"))
        ),
        maximum_windows_per_tick=1,
        maximum_clock_skew_ms=2_000,
        maximum_call_seconds=1,
    )
    return schedule, protocol, plan


def test_optin_exact_additive_manifest_preserves_old_profiles():
    runtime = Database.migration_names(MigrationProfile.RUNTIME)
    simulator = Database.migration_names(MigrationProfile.SIMULATOR)
    controlled = Database.migration_names(MigrationProfile.CONTROLLED_RUNTIME)
    campaign = Database.migration_names(MigrationProfile.RESEARCH_CAMPAIGN)
    assert len(runtime) == 17 and len(simulator) == 25
    assert controlled == runtime + ("026_operator_control.sql",)
    assert campaign == simulator + ("027_simulator_adaptive_campaign.sql",)
    assert "026_operator_control.sql" not in campaign


@pytest.mark.parametrize(
    "profile", [MigrationProfile.RUNTIME, MigrationProfile.CONTROLLED_RUNTIME, MigrationProfile.SIMULATOR]
)
def test_campaign_constructor_never_implicitly_adopts_old_profile(profile):
    name = "kairos_sim_test_fixture" if profile is MigrationProfile.SIMULATOR else "kairos_test"
    database = Database(
        PersistenceSettings(_env_file=None, database_url="postgresql://fixture@127.0.0.1/" + name),
        migration_profile=profile,
    )
    with pytest.raises(ValueError, match="explicit isolated"):
        ResearchCampaignRepository(database)


@pytest.mark.parametrize("name", ["kairos", "postgres", "kairos_sim_other/path", "kairos-sim"])
def test_campaign_never_targets_primary_or_ambiguous_database(name):
    with pytest.raises(ValueError):
        Database.require_profile_database_name(MigrationProfile.RESEARCH_CAMPAIGN, name)


def test_campaign_repository_rejects_readonly_database():
    database = Database(
        PersistenceSettings(_env_file=None, database_url="postgresql://fixture@127.0.0.1/kairos_sim_fixture"),
        migration_profile=MigrationProfile.RESEARCH_CAMPAIGN,
        read_only=True,
    )
    with pytest.raises(ValueError, match="writable"):
        ResearchCampaignRepository(database)


@pytest.mark.parametrize(
    "field,value",
    [("maximum_windows_per_tick", 65), ("maximum_call_seconds", 121), ("maximum_clock_skew_ms", 2_001)],
)
def test_preregistered_bounds_cannot_be_expanded(field, value):
    plan = _identity(1_790_064_000_000)[2]
    payload = {**plan.model_dump(mode="json"), field: value, "receipt_sha256": None}
    with pytest.raises(ValueError):
        ResearchCampaignPlanV1.model_validate(payload)


def test_review_is_distinct_from_proposal_and_must_cite_evidence():
    with pytest.raises(ValueError):
        ResearchReviewOutputV1(action="LONG_BIAS", rationale="wrong protocol", evidence_ids=("a" * 64,))
    with pytest.raises(ValueError):
        ResearchReviewOutputV1(action="ALLOW", rationale="missing provenance", evidence_ids=())


def test_campaign_sql_has_no_reclaim_no_security_definer_and_is_append_only():
    sql = (
        Path(__file__).parents[1] / "kairos_persistence/migrations/027_simulator_adaptive_campaign.sql"
    ).read_text()
    assert "SECURITY DEFINER" not in sql and "UPDATE sim_" not in sql
    assert "WHERE kind='start'" in sql and "registration_txid=txid_current()" in sql
    assert "cannot adopt past windows" in sql and "simulator_reject_independent_evidence_mutation" in sql
    assert "SCHEDULED_DENOMINATOR_ONLY" in sql and "economic_qualification" in sql


def _completion_repository(monkeypatch, *, arm="strategy-review"):
    """Typed receipt checks over SQL-shaped I/O only, not PostgreSQL evidence."""
    from tests import test_research_evidence as evidence_fixture

    schedule = evidence_fixture._schedule("campaign-clock-unit")
    protocol = evidence_fixture._protocol(schedule)
    sources = tuple(
        evidence_fixture._source(schedule, protocol, source_kind=kind, source_name=kind, reference=kind)
        for kind in ("MACRO", "MARKET_SNAPSHOT", "NEWS")
    )
    start = evidence_fixture._start(
        schedule, protocol, arm_id=arm, arm_protocol_digest=protocol.arm_digest(arm)
    )
    plan = SimpleNamespace(maximum_clock_skew_ms=2_000)
    claim = ResearchWindowClaimV1(
        campaign_id=schedule.campaign_id,
        sample_id=start.sample_id,
        plan_receipt_sha256="9" * 64,
        claim_id=canonical_sha256({"plan_receipt_sha256": "9" * 64, "sample_id": start.sample_id}),
        claimed_at_ts_ms=start.attempt_started_at_ts_ms,
    )
    bundle = ResearchCausalBundleV1(
        campaign_id=schedule.campaign_id,
        sample_id=start.sample_id,
        claim_id=claim.claim_id,
        market_snapshot_sha256=start.market_snapshot_sha256,
        source_receipt_sha256s=tuple(sorted(x.receipt_sha256 for x in sources)),
        frozen_at_ts_ms=start.attempt_started_at_ts_ms,
    )
    evaluation = SimpleNamespace(
        intent=object(),
        source_receipt_sha256s=bundle.source_receipt_sha256s,
        campaign_id=start.campaign_id,
        sample_id=start.sample_id,
    )
    state = SimpleNamespace(now=start.market_as_of_ts_ms + 200, stored=[])

    async def execute(*_args):
        return None

    async def fetchval(*_args):
        return state.now

    connection = SimpleNamespace(execute=execute, fetchval=fetchval)
    database = Database(
        PersistenceSettings(_env_file=None, database_url="postgresql://fixture@127.0.0.1/kairos_sim_fixture"),
        migration_profile=MigrationProfile.RESEARCH_CAMPAIGN,
    )

    @asynccontextmanager
    async def transaction():
        yield connection

    monkeypatch.setattr(database, "transaction", transaction)
    repository = ResearchCampaignRepository(database)

    async def found(*_args):
        return start, None

    async def context(*_args):
        return plan, schedule, protocol

    async def load_claim(*_args):
        return claim

    async def load_bundle(*_args):
        return bundle

    async def load_receipt(_connection, digest, _model):
        return next((x for x in sources if x.receipt_sha256 == digest), evaluation)

    async def absent(*_args):
        return None

    async def store(_connection, receipt, *_args, **_scope):
        state.stored.append(receipt)
        return True

    for name, value in (
        ("find_attempt", found),
        ("_context", context),
        ("_load_claim", load_claim),
        ("_bundle", load_bundle),
        ("_load_receipt", load_receipt),
        ("_row", absent),
        ("_store", store),
    ):
        monkeypatch.setattr(repository, name, value)
    evidence = EvidenceReferenceV1(
        kind=sources[0].source_kind,
        reference=sources[0].reference,
        content_sha256=sources[0].content_sha256,
        observed_at_ms=sources[0].observed_at_ts_ms,
    )
    review = ResearchReviewReceiptV1(
        campaign_id=start.campaign_id,
        sample_id=start.sample_id,
        attempt_id=start.attempt_id,
        start_receipt_sha256=start.receipt_sha256,
        bundle_receipt_sha256=bundle.receipt_sha256,
        evaluation_receipt_sha256="8" * 64,
        output=ResearchReviewOutputV1(
            action="ALLOW", rationale="Offline fixture only", evidence_ids=(canonical_sha256(evidence),)
        ),
        response_sha256="7" * 64,
        requested_model=start.requested_model,
        resolved_model=start.requested_model,
        request_id="offline-request",
        prompt_sha256=start.prompt_sha256,
        observed_at_ts_ms=state.now,
    )
    return repository, state, start, sources, review


async def test_campaign_repository_rejects_wrong_review_backend_before_store(monkeypatch):
    repository, state, _, _, review = _completion_repository(monkeypatch)
    wrong = ResearchReviewReceiptV1.model_validate(
        {**review.model_dump(mode="json"), "receipt_sha256": None, "resolved_model": "other-model"}
    )
    with pytest.raises(MessageIdentityConflict, match="frozen matched provenance"):
        await repository.record_review(wrong)
    assert not state.stored
    assert await repository.record_review(review)


@pytest.mark.parametrize("kind", ["review", "terminal"])
@pytest.mark.parametrize("offset", [-2_001, 2_001])
async def test_campaign_completion_rejects_stalled_or_future_clock(monkeypatch, kind, offset):
    from tests import test_research_evidence as evidence_fixture

    repository, state, start, _, review = _completion_repository(monkeypatch)
    state.now = start.market_as_of_ts_ms + 3_000
    observed = state.now + offset
    receipt = (
        ResearchReviewReceiptV1.model_validate(
            {**review.model_dump(mode="json"), "receipt_sha256": None, "observed_at_ts_ms": observed}
        )
        if kind == "review"
        else evidence_fixture._terminal(start, evidence_fixture._failure(start, observed_at=observed))
    )
    with pytest.raises(MessageIdentityConflict, match="actual campaign recorder time"):
        await (repository.record_review(receipt) if kind == "review" else repository.finish_attempt(receipt))
    assert not state.stored


async def test_campaign_honest_late_completion_still_saved_without_rewriting(monkeypatch):
    from tests import test_research_evidence as evidence_fixture

    repository, state, start, _, review = _completion_repository(monkeypatch)
    state.now = start.market_as_of_ts_ms + 6_000
    late = ResearchReviewReceiptV1.model_validate(
        {**review.model_dump(mode="json"), "receipt_sha256": None, "observed_at_ts_ms": state.now}
    )
    assert await repository.record_review(late)
    terminal = evidence_fixture._terminal(start, evidence_fixture._failure(start, observed_at=state.now))
    assert await repository.finish_attempt(terminal)
    assert state.stored == [late, terminal]  # SQL-shaped independent paths, not both on one real START.


@pytest.mark.parametrize("kind", ["review", "terminal"])
async def test_campaign_exact_completion_replay_survives_elapsed_clock(monkeypatch, kind):
    from kairos_persistence.runtime import canonical_payload
    from tests import test_research_evidence as evidence_fixture

    repository, state, start, _, review = _completion_repository(monkeypatch)
    receipt = (
        review
        if kind == "review"
        else evidence_fixture._terminal(start, evidence_fixture._failure(start, observed_at=state.now))
    )
    encoded, digest = canonical_payload(receipt.model_dump(mode="json"))
    saved = dict(payload_json=encoded, payload_sha256=digest, receipt_sha256=receipt.receipt_sha256)

    async def saved_row(_connection, _campaign, _sample, _arm, current_kind, _slot):
        return saved if current_kind == kind else None

    monkeypatch.setattr(repository, "_row", saved_row)
    state.now += 60_000
    method = repository.record_review if kind == "review" else repository.finish_attempt
    assert not await method(receipt)
    assert not state.stored
    changed = (
        type(receipt).model_validate(
            {
                **receipt.model_dump(mode="json"),
                "receipt_sha256": None,
                "observed_at_ts_ms": receipt.observed_at_ts_ms + 1,
            }
        )
        if kind == "review"
        else evidence_fixture._terminal(
            start, evidence_fixture._failure(start, observed_at=receipt.observed_at_ts_ms + 1)
        )
    )
    with pytest.raises(MessageIdentityConflict, match="immutable completion"):
        await method(changed)
    assert not state.stored


async def test_campaign_proposal_repository_rejects_wrong_resolved_backend(monkeypatch):
    from tests import test_research_evidence as evidence_fixture

    repository, state, start, sources, _ = _completion_repository(monkeypatch, arm="llm-proposal-research")
    good = evidence_fixture._completed(start, next(x for x in sources if x.source_kind == "MARKET_SNAPSHOT"))
    payload = good.model_dump(mode="json")
    payload["receipt_sha256"] = None
    for name in ("proposal", "completion"):
        payload[name]["model_provenance"]["resolved_model"] = "other-model"
    payload["proposal"]["proposal_id"] = None
    from kairos_core import LLMTradeProposalV1

    from kairos_persistence import ResearchLLMAttemptTerminalV1

    proposal = LLMTradeProposalV1.model_validate(payload["proposal"])
    payload["proposal"] = proposal.to_payload()
    payload["completion"]["proposal_id"] = proposal.proposal_id
    payload["completion"]["completion_receipt_id"] = None
    wrong = ResearchLLMAttemptTerminalV1.model_validate(payload)
    with pytest.raises(MessageIdentityConflict, match="frozen requested model"):
        await repository.finish_attempt(wrong)
    assert not state.stored


@pytest.mark.integration
async def test_native_campaign_preregistration_actual_capture_claim_race_and_immutable_denominator():
    url = os.getenv("KAIROS_RESEARCH_CAMPAIGN_TEST_DATABASE_URL")
    if not url:
        pytest.skip("explicit disposable RESEARCH_CAMPAIGN test DB required")
    name = urlsplit(url).path.removeprefix("/")
    prefix = "kairos_sim_test_campaign_"
    if not name.startswith(prefix) or len(name) != len(prefix) + 32:
        raise RuntimeError("refusing non-disposable campaign integration target")
    namespace = UUID(hex=name.removeprefix(prefix))
    if namespace.version != 4 or namespace.hex != name.removeprefix(prefix):
        raise RuntimeError("campaign test database must have an exact UUID4 namespace")
    require_database_target_url(url, name, local_only=True)
    database = Database(
        PersistenceSettings(
            _env_file=None, database_url=url, pool_min_size=1, pool_max_size=3, command_timeout_s=5
        ),
        migration_profile=MigrationProfile.RESEARCH_CAMPAIGN,
    )
    await connect_verified_database(database, name, local_only=True)
    try:
        await database.migrate()
        repository = ResearchCampaignRepository(database)
        now = await repository.clock()
        schedule, protocol, plan = _identity(now)
        await ResearchObservationScheduleRepository(database).register(schedule)
        await ResearchAdaptiveCandidateProtocolRepository(database).register(protocol)
        assert await repository.register(plan)
        assert not await repository.register(plan)
        sources = []
        for requirement in plan.required_sources:
            source = await repository.capture_source(
                campaign_id=plan.campaign_id,
                sample_id="one",
                source_kind=requirement.source_kind,
                source_name=requirement.source_name,
                reference=requirement.source_name,
                source_as_of_ts_ms=now - 100,
                content={"fixture": requirement.source_kind, "close": 100},
            )
            assert now <= source.observed_at_ts_ms <= await repository.clock()
            sources.append(source)
        # Bounded fixture wait, not a historical observation or scheduler retry.
        await asyncio.sleep(
            max(0, (schedule.windows[0].market_as_of_ts_ms - await repository.clock()) / 1_000)
        )
        claims = await asyncio.gather(
            repository.claim_next(plan.campaign_id), repository.claim_next(plan.campaign_id)
        )
        assert sum(x is not None for x in claims) == 1
        claim = next(x for x in claims if x is not None)
        bundle = await repository.freeze_bundle(claim)
        resolved = await repository.resolve_causal_sources(
            campaign_id=plan.campaign_id,
            sample_id="one",
            source_receipt_sha256s=bundle.source_receipt_sha256s,
        )
        assert {x.receipt_sha256 for x in sources} == {x.receipt_sha256 for x in resolved}
        evaluation = await repository.record_evaluation(claim=claim, bundle=bundle, intent=None)
        assert await repository.record_evaluation(claim=claim, bundle=bundle, intent=None) == evaluation
        with pytest.raises(MessageIdentityConflict):
            await repository.resolve_causal_sources(
                campaign_id=plan.campaign_id, sample_id="one", source_receipt_sha256s=("a" * 64,)
            )
        for arm in ("strategy-only", "strategy-review", "llm-proposal-research"):
            outcome = ResearchArmOutcomeV1(
                campaign_id=plan.campaign_id,
                sample_id="one",
                arm_id=arm,
                claim_id=claim.claim_id,
                status="NO_INTENT" if arm != "llm-proposal-research" else "BUDGET_BLOCKED",
                bundle_receipt_sha256=bundle.receipt_sha256,
                evaluation_receipt_sha256=evaluation.receipt_sha256,
                observed_at_ts_ms=await repository.clock(),
            )
            assert await repository.record_outcome(outcome)
        denominator = await repository.seal_denominator(plan.campaign_id)
        assert denominator.expected_outcomes == 3 and not denominator.economic_qualification
        assert await repository.seal_denominator(plan.campaign_id) == denominator
        async with database.transaction() as connection:
            with pytest.raises(asyncpg.PostgresError, match="append-only"):
                async with connection.transaction():
                    await connection.execute(
                        "DELETE FROM sim_adaptive_campaign_denominators WHERE campaign_id=$1",
                        plan.campaign_id,
                    )
        assert await repository.claim_next(plan.campaign_id) is None
    finally:
        await database.close()
