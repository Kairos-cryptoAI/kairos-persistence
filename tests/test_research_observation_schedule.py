"""SIM research rosters freeze before results and seal only exact matched coverage."""

from __future__ import annotations

import asyncio
import os
import re
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import asyncpg
import pytest
from kairos_core import (
    RESEARCH_ARMS,
    AdaptiveCandidateProtocolV1,
    LLMProposalAdaptiveCandidateArmV1,
    ResearchDecisionSampleV1,
    ResearchObservationScheduleV1,
    ResearchObservationWindowV1,
    StrategyOnlyAdaptiveCandidateArmV1,
    StrategyReviewAdaptiveCandidateArmV1,
)

from kairos_persistence import (
    Database,
    MessageIdentityConflict,
    MigrationProfile,
    PersistenceSettings,
    ResearchAdaptiveCandidateProtocolRepository,
    ResearchDecisionSampleRepository,
    ResearchObservationScheduleRepository,
    canonical_payload,
)
from kairos_persistence.database_target import connect_verified_database, require_database_target_url

_T0 = 1_760_000_000_000
_SIMPLE_DATABASE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,62}\Z")


def _schedule(campaign_id: str = "adaptive-matched-v1") -> ResearchObservationScheduleV1:
    return ResearchObservationScheduleV1(
        campaign_id=campaign_id,
        strategy_id="adaptive-strategy-v1",
        strategy_revision="frozen-001",
        source_set_sha256="a" * 64,
        evaluator_sha256="b" * 64,
        windows=(
            ResearchObservationWindowV1(
                sample_id="sample-001",
                symbol="BTCUSDT",
                timeframe="1m",
                market_as_of_ts_ms=_T0,
                market_snapshot_sha256="c" * 64,
                paired_at_ts_ms=_T0 + 1_000,
                sample_deadline_ts_ms=_T0 + 10_000,
            ),
            ResearchObservationWindowV1(
                sample_id="sample-002",
                symbol="ETHUSDT",
                timeframe="1m",
                market_as_of_ts_ms=_T0 + 60_000,
                market_snapshot_sha256="c" * 64,
                paired_at_ts_ms=_T0 + 61_000,
                sample_deadline_ts_ms=_T0 + 70_000,
            ),
        ),
    )


def _protocol(schedule: ResearchObservationScheduleV1, **overrides: object) -> AdaptiveCandidateProtocolV1:
    arms = (
        StrategyOnlyAdaptiveCandidateArmV1(
            candidate_id="strategy-baseline",
            candidate_revision="frozen-001",
            artifact_sha256="1" * 64,
            input_feature_sha256="2" * 64,
            decision_mapping_sha256="3" * 64,
            hypothetical_exit_sha256="4" * 64,
            cost_model_sha256="5" * 64,
        ),
        StrategyReviewAdaptiveCandidateArmV1(
            candidate_id="strategy-review",
            candidate_revision="frozen-001",
            artifact_sha256="6" * 64,
            input_feature_sha256="7" * 64,
            decision_mapping_sha256="8" * 64,
            hypothetical_exit_sha256="9" * 64,
            cost_model_sha256="a" * 64,
            provider="openai",
            model="gpt-test-model",
            prompt_sha256="b" * 64,
            schema_sha256="c" * 64,
        ),
        LLMProposalAdaptiveCandidateArmV1(
            candidate_id="llm-proposal",
            candidate_revision="frozen-001",
            artifact_sha256="d" * 64,
            input_feature_sha256="e" * 64,
            decision_mapping_sha256="f" * 64,
            hypothetical_exit_sha256="0" * 64,
            cost_model_sha256="1" * 64,
            provider="deepseek",
            model="deepseek-test-model",
            prompt_sha256="2" * 64,
            schema_sha256="3" * 64,
        ),
    )
    payload: dict[str, object] = {
        "campaign_id": schedule.campaign_id,
        "schedule_digest": schedule.schedule_digest,
        "arms": arms,
    }
    payload.update(overrides)
    return AdaptiveCandidateProtocolV1(**payload)


def _sample(
    schedule: ResearchObservationScheduleV1,
    arm_id: str,
    sample_id: str,
    protocol: AdaptiveCandidateProtocolV1 | None = None,
) -> ResearchDecisionSampleV1:
    window = next(window for window in schedule.windows if window.sample_id == sample_id)
    values: dict[str, object] = {
        "campaign_id": schedule.campaign_id,
        "arm_id": arm_id,
        "sample_id": sample_id,
        "symbol": window.symbol,
        "timeframe": window.timeframe,
        "market_as_of_ts_ms": window.market_as_of_ts_ms,
        "market_snapshot_sha256": "c" * 64,
        "paired_at_ts_ms": window.paired_at_ts_ms,
        "sample_deadline_ts_ms": window.sample_deadline_ts_ms,
        "strategy_id": schedule.strategy_id,
        "strategy_revision": schedule.strategy_revision,
        "strategy_outcome": "NO_INTENT",
        "strategy_evaluation_sha256": "d" * 64,
        "strategy_evidence_as_of_ts_ms": window.market_as_of_ts_ms,
        "strategy_market_snapshot_sha256": "c" * 64,
        "llm_outcome": "NOT_CALLED",
    }
    if protocol is not None:
        values["arm_protocol_digest"] = protocol.arm_digest(arm_id)  # type: ignore[arg-type]
    if arm_id == "strategy-review":
        values.update(
            llm_outcome="CALL_FAILED",
            llm_evidence_as_of_ts_ms=window.market_as_of_ts_ms,
            llm_market_snapshot_sha256="c" * 64,
            llm_failure_receipt_id="e" * 64,
            llm_failure_class="TIMEOUT",
            llm_failure_started_at_ts_ms=window.market_as_of_ts_ms + 100,
            llm_failure_observed_at_ts_ms=window.paired_at_ts_ms - 100,
        )
    return ResearchDecisionSampleV1(**values)


def _long_sample(
    schedule: ResearchObservationScheduleV1,
    arm_id: str,
    sample_id: str,
    protocol: AdaptiveCandidateProtocolV1 | None = None,
) -> ResearchDecisionSampleV1:
    sample = _sample(schedule, arm_id, sample_id, protocol)
    return ResearchDecisionSampleV1.model_validate(
        {
            **sample.to_payload(),
            "sample_record_id": None,
            "strategy_outcome": "LONG",
            "strategy_intent_id": "a" * 64,
            "strategy_intent_expires_at_ts_ms": sample.sample_deadline_ts_ms + 1_000,
        }
    )


_SAMPLE_COLUMNS = (
    "sample_record_id",
    "campaign_id",
    "arm_id",
    "sample_id",
    "symbol",
    "timeframe",
    "market_as_of_ts_ms",
    "paired_at_ts_ms",
    "sample_deadline_ts_ms",
    "market_snapshot_sha256",
    "strategy_id",
    "strategy_revision",
    "strategy_outcome",
    "strategy_evaluation_sha256",
    "strategy_evidence_as_of_ts_ms",
    "strategy_market_snapshot_sha256",
    "strategy_intent_id",
    "strategy_intent_expires_at_ts_ms",
    "llm_outcome",
    "llm_evidence_as_of_ts_ms",
    "llm_market_snapshot_sha256",
    "llm_proposal_id",
    "llm_proposal_expires_at_ts_ms",
    "llm_completion_receipt_id",
    "llm_completion_started_at_ts_ms",
    "llm_completion_observed_at_ts_ms",
    "llm_failure_receipt_id",
    "llm_failure_class",
    "llm_failure_started_at_ts_ms",
    "llm_failure_observed_at_ts_ms",
    "authority",
    "payload",
    "payload_sha256",
)


async def _insert_sample_with_connection(
    connection: asyncpg.Connection,
    sample: ResearchDecisionSampleV1,
    *,
    include_arm_protocol_digest: bool = True,
) -> None:
    """Insert through DB triggers on the same connection for race tests."""

    columns = _SAMPLE_COLUMNS + (("arm_protocol_digest",) if include_arm_protocol_digest else ())
    encoded, payload_sha256 = canonical_payload(sample.to_payload())
    values = [
        encoded
        if name == "payload"
        else payload_sha256
        if name == "payload_sha256"
        else getattr(sample, name)
        for name in columns
        if name not in ("arm_protocol_digest",)
    ]
    if include_arm_protocol_digest:
        values.append(sample.arm_protocol_digest)
    placeholders = [
        f"${index}" + ("::jsonb" if name == "payload" else "") for index, name in enumerate(columns, 1)
    ]
    await connection.execute(
        f"INSERT INTO sim_research_decision_samples ({','.join(columns)}) VALUES ({','.join(placeholders)})",
        *values,
    )


def _simulator_database(*, read_only: bool = False) -> Database:
    return Database(
        PersistenceSettings(
            database_url=f"postgresql://kairos:test@localhost:5432/kairos_sim_test_{uuid4().hex[:8]}"
        ),
        migration_profile=MigrationProfile.SIMULATOR,
        read_only=read_only,
    )


async def _wait_for_pending_advisory_locks(database: Database, expected: int) -> None:
    for _ in range(200):
        pending = await database.pool.fetchval(
            """SELECT count(*) FROM pg_locks
               WHERE locktype='advisory' AND NOT granted
                 AND database=(SELECT oid FROM pg_database WHERE datname=current_database())"""
        )
        if pending >= expected:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"expected at least {expected} advisory lock waiters, observed {pending}")


def test_roster_migration_is_sim_only_and_freezes_all_three_arms() -> None:
    runtime = Database.migration_names(MigrationProfile.RUNTIME)
    simulator = Database.migration_names(MigrationProfile.SIMULATOR)
    sql = (
        Path(__file__).parents[1]
        / "kairos_persistence"
        / "migrations"
        / "022_simulator_research_observation_schedule.sql"
    ).read_text(encoding="utf-8")
    lineage_sql = (
        Path(__file__).parents[1]
        / "kairos_persistence"
        / "migrations"
        / "023_simulator_research_baseline_lineage.sql"
    ).read_text(encoding="utf-8")
    protocol_sql = (
        Path(__file__).parents[1]
        / "kairos_persistence"
        / "migrations"
        / "024_simulator_adaptive_candidate_protocol.sql"
    ).read_text(encoding="utf-8")

    assert "022_simulator_research_observation_schedule.sql" not in runtime
    assert "023_simulator_research_baseline_lineage.sql" not in runtime
    assert "024_simulator_adaptive_candidate_protocol.sql" not in runtime
    assert simulator[-3:] == (
        "022_simulator_research_observation_schedule.sql",
        "023_simulator_research_baseline_lineage.sql",
        "024_simulator_adaptive_candidate_protocol.sql",
    )
    assert "sim_research_observation_schedules" in sql
    assert "sim_research_observation_windows" in sql
    assert "sim_research_coverage_seals" in sql
    assert "pg_advisory_xact_lock" in sql
    assert "txid_current()" in sql
    assert "BEFORE INSERT ON sim_research_decision_samples" in sql
    assert "BEFORE UPDATE OR DELETE" in sql
    assert "BEFORE TRUNCATE" in sql
    assert all(arm in sql for arm in RESEARCH_ARMS)
    assert "paper_" not in sql and "execution_" not in sql
    assert "CREATE OR REPLACE FUNCTION simulator_guard_scheduled_research_sample()" in lineage_sql
    for field in (
        "market_snapshot_sha256",
        "strategy_outcome",
        "strategy_evaluation_sha256",
        "strategy_evidence_as_of_ts_ms",
        "strategy_market_snapshot_sha256",
        "strategy_intent_id",
        "strategy_intent_expires_at_ts_ms",
    ):
        assert f"prior.{field} IS DISTINCT FROM NEW.{field}" in lineage_sql
    assert "paper_" not in lineage_sql and "execution_" not in lineage_sql
    assert "sim_research_adaptive_candidate_protocols" in protocol_sql
    assert "adaptive_protocol_registration_allowed" in protocol_sql
    assert "arm_protocol_digest" in protocol_sql
    assert "candidate_protocol_digest" in protocol_sql
    assert "pg_advisory_xact_lock(849621" in protocol_sql
    assert "BEFORE INSERT ON sim_research_adaptive_candidate_protocols" in protocol_sql
    assert "BEFORE INSERT ON sim_research_coverage_seals" in protocol_sql
    assert "BEFORE UPDATE OR DELETE" in protocol_sql
    assert "BEFORE TRUNCATE" in protocol_sql
    assert "REVOKE UPDATE, DELETE, TRUNCATE" in protocol_sql
    assert "paper_" not in protocol_sql and "execution_" not in protocol_sql
    assert "arm_digests - ARRAY" in protocol_sql
    assert "IS NULL" in protocol_sql and "->>'provider'" in protocol_sql


def test_repository_rejects_runtime_readonly_and_untyped_roster() -> None:
    runtime = Database(
        PersistenceSettings(database_url="postgresql://kairos:test@localhost:5432/kairos_test")
    )
    with pytest.raises(ValueError, match="SIMULATOR profile"):
        ResearchObservationScheduleRepository(runtime)
    with pytest.raises(ValueError, match="writable"):
        ResearchObservationScheduleRepository(_simulator_database(read_only=True))
    with pytest.raises(TypeError, match="explicit Database"):
        ResearchObservationScheduleRepository(object())  # type: ignore[arg-type]


def test_adaptive_protocol_repository_requires_writable_simulator_database() -> None:
    runtime = Database(
        PersistenceSettings(database_url="postgresql://kairos:test@localhost:5432/kairos_test")
    )
    with pytest.raises(ValueError, match="SIMULATOR profile"):
        ResearchAdaptiveCandidateProtocolRepository(runtime)
    with pytest.raises(ValueError, match="read-only"):
        ResearchAdaptiveCandidateProtocolRepository(_simulator_database(read_only=True))
    with pytest.raises(TypeError, match="explicit Database"):
        ResearchAdaptiveCandidateProtocolRepository(object())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_registration_checks_type_and_digest_before_connecting() -> None:
    repository = ResearchObservationScheduleRepository(_simulator_database())
    with pytest.raises(TypeError, match="ResearchObservationScheduleV1"):
        await repository.register({"campaign_id": "forged"})  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="digest"):
        await repository.register(_schedule().model_copy(update={"source_set_sha256": "f" * 64}))
    with pytest.raises(ValueError, match="campaign_id"):
        await repository.seal_coverage(campaign_id=" ")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_preregistered_sim_roster_guards_results_and_seals_exhaustive_coverage() -> None:
    database_url = os.getenv("KAIROS_SIM_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("KAIROS_SIM_TEST_DATABASE_URL is required for isolated simulator integration")
    database_name = urlsplit(database_url).path.removeprefix("/")
    if not (database_name.startswith("kairos_sim_test_") and _SIMPLE_DATABASE_NAME.fullmatch(database_name)):
        raise RuntimeError(
            "research schedule test requires a uniquely named disposable kairos_sim_test database"
        )
    require_database_target_url(database_url, database_name, local_only=True)
    database = Database(
        PersistenceSettings(database_url=database_url), migration_profile=MigrationProfile.SIMULATOR
    )
    await connect_verified_database(database, database_name, local_only=True)
    try:
        await database.migrate()
        roster = ResearchObservationScheduleRepository(database)
        samples = ResearchDecisionSampleRepository(database)
        protocols = ResearchAdaptiveCandidateProtocolRepository(database)
        schedule = _schedule(f"adaptive-{uuid4().hex[:16]}")
        assert await roster.register(schedule)
        assert not await roster.register(schedule)
        protocol = _protocol(schedule)
        arm_digests = {arm.arm_id: protocol.arm_digest(arm.arm_id) for arm in protocol.arms}
        protocol_payload, protocol_payload_sha256 = canonical_payload(protocol.to_payload())
        arm_digests_payload, _ = canonical_payload(arm_digests)
        valid_payload = protocol.to_payload()
        malformed_payloads: list[dict[str, object]] = []
        unknown_strategy_field = [dict(arm) for arm in valid_payload["arms"]]
        unknown_strategy_field[0]["provider"] = "openai"
        malformed_payloads.append({**valid_payload, "arms": unknown_strategy_field})
        unknown_llm_field = [dict(arm) for arm in valid_payload["arms"]]
        unknown_llm_field[1]["unrecognized"] = True
        malformed_payloads.append({**valid_payload, "arms": unknown_llm_field})
        missing_provider = [dict(arm) for arm in valid_payload["arms"]]
        del missing_provider[1]["provider"]
        malformed_payloads.append({**valid_payload, "arms": missing_provider})
        for malformed_payload in malformed_payloads:
            malformed_encoded, malformed_sha256 = canonical_payload(malformed_payload)
            with pytest.raises(asyncpg.PostgresError, match="fixed three-arm contract"):
                await database.pool.execute(
                    """INSERT INTO sim_research_adaptive_candidate_protocols
                       (campaign_id, schedule_digest, protocol_digest, arm_digests, authority,
                        payload, payload_sha256)
                       VALUES ($1,$2,$3,$4::jsonb,$5,$6::jsonb,$7)""",
                    protocol.campaign_id,
                    protocol.schedule_digest,
                    protocol.protocol_digest,
                    arm_digests_payload,
                    protocol.authority,
                    malformed_encoded,
                    malformed_sha256,
                )
        with pytest.raises(asyncpg.PostgresError, match="previously committed exact adaptive protocol"):
            async with database.transaction() as connection:
                await connection.execute(
                    """INSERT INTO sim_research_adaptive_candidate_protocols
                       (campaign_id, schedule_digest, protocol_digest, arm_digests, authority,
                        payload, payload_sha256)
                       VALUES ($1,$2,$3,$4::jsonb,$5,$6::jsonb,$7)""",
                    protocol.campaign_id,
                    protocol.schedule_digest,
                    protocol.protocol_digest,
                    arm_digests_payload,
                    protocol.authority,
                    protocol_payload,
                    protocol_payload_sha256,
                )
                await _insert_sample_with_connection(
                    connection, _sample(schedule, "strategy-only", "sample-001", protocol)
                )
        assert await database.pool.fetchval(
            "SELECT NOT EXISTS(SELECT 1 FROM sim_research_adaptive_candidate_protocols WHERE campaign_id=$1)",
            schedule.campaign_id,
        )

        lock_holder = await database.pool.acquire()
        try:
            await lock_holder.execute("BEGIN")
            await lock_holder.execute(
                "SELECT pg_advisory_xact_lock($1, hashtext($2))", 849621, schedule.campaign_id
            )
            first_result_task = asyncio.create_task(
                samples.record(_sample(schedule, "strategy-only", "sample-001", protocol))
            )
            await _wait_for_pending_advisory_locks(database, 1)
            protocol_task = asyncio.create_task(protocols.register(protocol))
            await _wait_for_pending_advisory_locks(database, 2)
            await lock_holder.execute("COMMIT")
            first_result, protocol_registered = await asyncio.gather(
                first_result_task, protocol_task, return_exceptions=True
            )
            assert isinstance(first_result, asyncpg.PostgresError)
            assert "previously committed exact adaptive protocol" in str(first_result)
            assert protocol_registered is True
        finally:
            if lock_holder.is_in_transaction():
                await lock_holder.execute("ROLLBACK")
            await database.pool.release(lock_holder)

        assert not await protocols.register(protocol)
        assert not await protocols.register(protocol)
        for arm in RESEARCH_ARMS:
            assert await protocols.resolve_arm_digest(
                campaign_id=schedule.campaign_id, arm_id=arm
            ) == protocol.arm_digest(
                arm  # type: ignore[arg-type]
            )
        with pytest.raises(ValueError, match="outside the fixed"):
            await protocols.resolve_arm_digest(campaign_id=schedule.campaign_id, arm_id="unknown")
        changed_schedule = ResearchObservationScheduleV1.model_validate(
            {**schedule.to_payload(), "schedule_digest": None, "source_set_sha256": "f" * 64}
        )
        with pytest.raises(MessageIdentityConflict, match="different immutable"):
            await roster.register(changed_schedule)
        with pytest.raises(ValueError, match="missing 6 scheduled arm observations"):
            await roster.seal_coverage(campaign_id=schedule.campaign_id)

        with pytest.raises(asyncpg.PostgresError, match="after schedule freeze"):
            await database.pool.execute(
                """INSERT INTO sim_research_observation_windows
                   (campaign_id,sample_id,symbol,timeframe,market_as_of_ts_ms,
                    paired_at_ts_ms,sample_deadline_ts_ms)
                   VALUES ($1,'extra','BTCUSDT','1m',$2,$3,$4)""",
                schedule.campaign_id,
                _T0 + 120_000,
                _T0 + 121_000,
                _T0 + 130_000,
            )
        with pytest.raises(asyncpg.PostgresError, match="append-only"):
            await database.pool.execute(
                "UPDATE sim_research_observation_windows SET symbol='SOLUSDT' WHERE campaign_id=$1",
                schedule.campaign_id,
            )

        altered_sample = ResearchDecisionSampleV1.model_validate(
            {
                **_sample(schedule, "strategy-only", "sample-001", protocol).to_payload(),
                "sample_record_id": None,
                "paired_at_ts_ms": _T0 + 2_000,
            }
        )
        with pytest.raises(asyncpg.PostgresError, match="frozen window"):
            await samples.record(altered_sample)
        wrong_snapshot = ResearchDecisionSampleV1.model_validate(
            {
                **_sample(schedule, "strategy-only", "sample-001", protocol).to_payload(),
                "sample_record_id": None,
                "market_snapshot_sha256": "f" * 64,
                "strategy_market_snapshot_sha256": "f" * 64,
            }
        )
        with pytest.raises(asyncpg.PostgresError, match="frozen window"):
            await samples.record(wrong_snapshot)

        with pytest.raises(asyncpg.PostgresError, match="arm digest"):
            await samples.record(_sample(schedule, "strategy-only", "sample-001"))
        wrong_protocol_digest = "f" * 64
        with pytest.raises(asyncpg.PostgresError, match="arm digest"):
            await samples.record(
                ResearchDecisionSampleV1.model_validate(
                    {
                        **_sample(schedule, "strategy-only", "sample-001").to_payload(),
                        "sample_record_id": None,
                        "arm_protocol_digest": wrong_protocol_digest,
                    }
                )
            )
        assert await samples.record(_sample(schedule, "strategy-only", "sample-001", protocol))
        changed_evaluation = ResearchDecisionSampleV1.model_validate(
            {
                **_sample(schedule, "strategy-review", "sample-001", protocol).to_payload(),
                "sample_record_id": None,
                "strategy_evaluation_sha256": "f" * 64,
            }
        )
        with pytest.raises(asyncpg.PostgresError, match="baseline strategy lineage"):
            await samples.record(changed_evaluation)
        for arm in RESEARCH_ARMS[1:]:
            assert await samples.record(_sample(schedule, arm, "sample-001", protocol))

        assert await samples.record(_long_sample(schedule, "strategy-only", "sample-002", protocol))
        changed_intent = ResearchDecisionSampleV1.model_validate(
            {
                **_long_sample(schedule, "strategy-review", "sample-002", protocol).to_payload(),
                "sample_record_id": None,
                "strategy_intent_id": "f" * 64,
            }
        )
        with pytest.raises(asyncpg.PostgresError, match="baseline strategy lineage"):
            await samples.record(changed_intent)
        for arm in RESEARCH_ARMS[1:]:
            assert await samples.record(_long_sample(schedule, arm, "sample-002", protocol))
        seal = await roster.seal_coverage(campaign_id=schedule.campaign_id)
        assert seal.expected_result_count == len(schedule.windows) * 3
        assert seal.schedule_digest == schedule.schedule_digest
        assert seal.candidate_protocol_digest == protocol.protocol_digest
        stored_seal = await database.pool.fetchrow(
            "SELECT * FROM sim_research_coverage_seals WHERE campaign_id=$1", schedule.campaign_id
        )
        assert stored_seal is not None
        assert stored_seal["candidate_protocol_digest"] == protocol.protocol_digest
        with pytest.raises(MessageIdentityConflict, match="already sealed"):
            await roster.seal_coverage(campaign_id=schedule.campaign_id)
        with pytest.raises(asyncpg.PostgresError, match="sealed SIM research campaign"):
            await samples.record(_sample(schedule, "strategy-only", "sample-001", protocol))

        late_schedule = _schedule(f"adaptive-{uuid4().hex[:16]}")
        assert await roster.register(late_schedule)
        late_protocol = _protocol(late_schedule)
        assert await protocols.register(late_protocol)
        assert await samples.record(_sample(late_schedule, "strategy-only", "sample-001", late_protocol))
        changed_protocol = _protocol(
            late_schedule,
            arms=(
                StrategyOnlyAdaptiveCandidateArmV1(
                    candidate_id="strategy-baseline-revised",
                    candidate_revision="frozen-001",
                    artifact_sha256="1" * 64,
                    input_feature_sha256="2" * 64,
                    decision_mapping_sha256="3" * 64,
                    hypothetical_exit_sha256="4" * 64,
                    cost_model_sha256="5" * 64,
                ),
                *late_protocol.arms[1:],
            ),
        )
        with pytest.raises(MessageIdentityConflict, match="different immutable adaptive protocol"):
            await protocols.register(changed_protocol)
    finally:
        await database.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_023_rejects_preexisting_mismatched_arms_even_for_a_sealed_022_campaign() -> None:
    database_url = os.getenv("KAIROS_SIM_UPGRADE_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("KAIROS_SIM_UPGRADE_TEST_DATABASE_URL is required for isolated 022 upgrade test")
    database_name = urlsplit(database_url).path.removeprefix("/")
    if not (
        database_name.startswith("kairos_sim_test_upgrade_")
        and _SIMPLE_DATABASE_NAME.fullmatch(database_name)
    ):
        raise RuntimeError("022 upgrade test requires a uniquely named disposable kairos_sim_test_upgrade DB")
    require_database_target_url(database_url, database_name, local_only=True)
    database = Database(
        PersistenceSettings(database_url=database_url), migration_profile=MigrationProfile.SIMULATOR
    )
    await connect_verified_database(database, database_name, local_only=True)
    try:
        migrations = Path(__file__).parents[1] / "kairos_persistence" / "migrations"
        old_profile = Database.migration_names(MigrationProfile.SIMULATOR)[:-2]
        assert old_profile[-1] == "022_simulator_research_observation_schedule.sql"
        async with database.transaction() as connection:
            await connection.execute("CREATE TABLE schema_migrations (version TEXT PRIMARY KEY)")
            for name in old_profile:
                await connection.execute((migrations / name).read_text(encoding="utf-8"))
                await connection.execute("INSERT INTO schema_migrations(version) VALUES ($1)", name)

        roster = ResearchObservationScheduleRepository(database)
        schedule = _schedule(f"upgrade-{uuid4().hex[:16]}")
        assert await roster.register(schedule)
        async with database.transaction() as connection:
            await _insert_sample_with_connection(
                connection,
                _sample(schedule, "strategy-only", "sample-001"),
                include_arm_protocol_digest=False,
            )
        drifted = ResearchDecisionSampleV1.model_validate(
            {
                **_sample(schedule, "strategy-review", "sample-001").to_payload(),
                "sample_record_id": None,
                "strategy_evaluation_sha256": "f" * 64,
            }
        )
        async with database.transaction() as connection:
            await _insert_sample_with_connection(connection, drifted, include_arm_protocol_digest=False)
        await database.pool.execute(
            """INSERT INTO sim_research_coverage_seals
               (campaign_id, coverage_digest, schedule_digest, expected_result_count,
                result_ids_sha256, authority, payload, payload_sha256)
               VALUES ($1,$2,$3,3,$4,'SIM_RESEARCH_ONLY',$5::jsonb,$6)""",
            schedule.campaign_id,
            "e" * 64,
            schedule.schedule_digest,
            "f" * 64,
            '{"synthetic_022_only_test": true}',
            "d" * 64,
        )

        with pytest.raises(asyncpg.PostgresError, match="preexisting scheduled SIM research arms disagree"):
            async with database.transaction() as connection:
                await connection.execute(
                    (migrations / "023_simulator_research_baseline_lineage.sql").read_text(encoding="utf-8")
                )
        rows = await database.pool.fetch("SELECT version FROM schema_migrations ORDER BY version")
        assert tuple(str(row["version"]) for row in rows) == old_profile
        old_guard = await database.pool.fetchval(
            "SELECT pg_get_functiondef('simulator_guard_scheduled_research_sample()'::regprocedure)"
        )
        assert "prior.strategy_evaluation_sha256" not in old_guard

        # Remove only this test's deliberately corrupt 022 campaign so the
        # same isolated database can prove both the fail-closed 023 upgrade
        # and a successful, evidence-preserving 022 -> 023 -> 024 upgrade.
        async with database.transaction() as connection:
            await connection.execute(
                "ALTER TABLE sim_research_coverage_seals "
                "DISABLE TRIGGER sim_research_coverage_seals_no_update_delete"
            )
            await connection.execute(
                "ALTER TABLE sim_research_decision_samples "
                "DISABLE TRIGGER sim_research_decision_samples_no_update_delete"
            )
            await connection.execute(
                "ALTER TABLE sim_research_observation_windows "
                "DISABLE TRIGGER sim_research_windows_no_update_delete"
            )
            await connection.execute(
                "ALTER TABLE sim_research_observation_schedules "
                "DISABLE TRIGGER sim_research_schedules_no_update_delete"
            )
            await connection.execute(
                "DELETE FROM sim_research_coverage_seals WHERE campaign_id=$1", schedule.campaign_id
            )
            await connection.execute(
                "DELETE FROM sim_research_decision_samples WHERE campaign_id=$1", schedule.campaign_id
            )
            await connection.execute(
                "DELETE FROM sim_research_observation_windows WHERE campaign_id=$1", schedule.campaign_id
            )
            await connection.execute(
                "DELETE FROM sim_research_observation_schedules WHERE campaign_id=$1", schedule.campaign_id
            )
            await connection.execute(
                "ALTER TABLE sim_research_observation_schedules "
                "ENABLE TRIGGER sim_research_schedules_no_update_delete"
            )
            await connection.execute(
                "ALTER TABLE sim_research_observation_windows "
                "ENABLE TRIGGER sim_research_windows_no_update_delete"
            )
            await connection.execute(
                "ALTER TABLE sim_research_decision_samples "
                "ENABLE TRIGGER sim_research_decision_samples_no_update_delete"
            )
            await connection.execute(
                "ALTER TABLE sim_research_coverage_seals "
                "ENABLE TRIGGER sim_research_coverage_seals_no_update_delete"
            )
            await connection.execute(
                (migrations / "023_simulator_research_baseline_lineage.sql").read_text(encoding="utf-8")
            )
            await connection.execute(
                "INSERT INTO schema_migrations(version) VALUES ($1)",
                "023_simulator_research_baseline_lineage.sql",
            )

        roster = ResearchObservationScheduleRepository(database)
        samples = ResearchDecisionSampleRepository(database)
        protocols = ResearchAdaptiveCandidateProtocolRepository(database)
        legacy_schedule = _schedule(f"legacy-sealed-{uuid4().hex[:16]}")
        empty_legacy_schedule = _schedule(f"legacy-empty-{uuid4().hex[:16]}")
        assert await roster.register(legacy_schedule)
        assert await roster.register(empty_legacy_schedule)
        for window in legacy_schedule.windows:
            for arm in RESEARCH_ARMS:
                async with database.transaction() as connection:
                    await _insert_sample_with_connection(
                        connection,
                        _sample(legacy_schedule, arm, window.sample_id),
                        include_arm_protocol_digest=False,
                    )
        await database.pool.execute(
            """INSERT INTO sim_research_coverage_seals
               (campaign_id, coverage_digest, schedule_digest, expected_result_count,
                result_ids_sha256, authority, payload, payload_sha256)
               VALUES ($1,$2,$3,$4,$5,'SIM_RESEARCH_ONLY',$6::jsonb,$7)""",
            legacy_schedule.campaign_id,
            "a" * 64,
            legacy_schedule.schedule_digest,
            len(legacy_schedule.windows) * len(RESEARCH_ARMS),
            "b" * 64,
            '{"legacy_022_seal": true}',
            "c" * 64,
        )
        legacy_results_before = await database.pool.fetch(
            """SELECT sample_record_id, campaign_id, arm_id, sample_id, payload, payload_sha256
               FROM sim_research_decision_samples WHERE campaign_id=$1 ORDER BY sample_id, arm_id""",
            legacy_schedule.campaign_id,
        )
        legacy_seal_before = await database.pool.fetchrow(
            "SELECT coverage_digest, schedule_digest, expected_result_count, result_ids_sha256, "
            "payload, payload_sha256 "
            "FROM sim_research_coverage_seals WHERE campaign_id=$1",
            legacy_schedule.campaign_id,
        )
        assert legacy_seal_before is not None

        await database.migrate()

        history = await database.pool.fetch("SELECT version FROM schema_migrations ORDER BY version")
        assert tuple(str(row["version"]) for row in history) == Database.migration_names(
            MigrationProfile.SIMULATOR
        )
        legacy_results_after = await database.pool.fetch(
            """SELECT sample_record_id, campaign_id, arm_id, sample_id, payload, payload_sha256,
                      arm_protocol_digest
               FROM sim_research_decision_samples WHERE campaign_id=$1 ORDER BY sample_id, arm_id""",
            legacy_schedule.campaign_id,
        )
        assert len(legacy_results_after) == len(legacy_results_before) == 6
        for before, after in zip(legacy_results_before, legacy_results_after, strict=True):
            assert tuple(before[field] for field in before.keys()) == tuple(
                after[field] for field in before.keys()
            )
            assert after["arm_protocol_digest"] is None
        legacy_seal_after = await database.pool.fetchrow(
            "SELECT coverage_digest, schedule_digest, expected_result_count, result_ids_sha256, "
            "payload, payload_sha256, "
            "candidate_protocol_digest FROM sim_research_coverage_seals WHERE campaign_id=$1",
            legacy_schedule.campaign_id,
        )
        assert legacy_seal_after is not None
        assert all(legacy_seal_after[key] == legacy_seal_before[key] for key in legacy_seal_before.keys())
        assert legacy_seal_after["candidate_protocol_digest"] is None
        assert await database.pool.fetchval(
            "SELECT NOT adaptive_protocol_registration_allowed "
            "FROM sim_research_observation_schedules WHERE campaign_id=$1",
            legacy_schedule.campaign_id,
        )
        assert await samples.load_page(campaign_id=legacy_schedule.campaign_id, arm_id="strategy-only")

        with pytest.raises(MessageIdentityConflict, match="legacy SIM schedules"):
            await protocols.register(_protocol(legacy_schedule))
        with pytest.raises(MessageIdentityConflict, match="legacy SIM schedules"):
            await protocols.register(_protocol(empty_legacy_schedule))
        with pytest.raises(asyncpg.PostgresError, match="previously committed exact adaptive protocol"):
            async with database.transaction() as connection:
                await _insert_sample_with_connection(
                    connection,
                    _sample(empty_legacy_schedule, "strategy-only", "sample-001"),
                )
        with pytest.raises(asyncpg.PostgresError, match="previously committed exact adaptive protocol"):
            await database.pool.execute(
                """INSERT INTO sim_research_coverage_seals
                   (campaign_id, coverage_digest, schedule_digest, candidate_protocol_digest,
                    expected_result_count, result_ids_sha256, authority, payload, payload_sha256)
                   VALUES ($1,$2,$3,NULL,$4,$5,'SIM_RESEARCH_ONLY',$6::jsonb,$7)""",
                empty_legacy_schedule.campaign_id,
                "d" * 64,
                empty_legacy_schedule.schedule_digest,
                len(empty_legacy_schedule.windows) * len(RESEARCH_ARMS),
                "e" * 64,
                '{"legacy_new_seal_attempt": true}',
                "f" * 64,
            )
    finally:
        await database.close()
