"""SIM research rosters freeze before results and seal only exact matched coverage."""

from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import asyncpg
import pytest
from kairos_core import (
    RESEARCH_ARMS,
    ResearchDecisionSampleV1,
    ResearchObservationScheduleV1,
    ResearchObservationWindowV1,
)

from kairos_persistence import (
    Database,
    MessageIdentityConflict,
    MigrationProfile,
    PersistenceSettings,
    ResearchDecisionSampleRepository,
    ResearchObservationScheduleRepository,
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


def _sample(
    schedule: ResearchObservationScheduleV1,
    arm_id: str,
    sample_id: str,
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
) -> ResearchDecisionSampleV1:
    sample = _sample(schedule, arm_id, sample_id)
    return ResearchDecisionSampleV1.model_validate(
        {
            **sample.to_payload(),
            "sample_record_id": None,
            "strategy_outcome": "LONG",
            "strategy_intent_id": "a" * 64,
            "strategy_intent_expires_at_ts_ms": sample.sample_deadline_ts_ms + 1_000,
        }
    )


def _simulator_database(*, read_only: bool = False) -> Database:
    return Database(
        PersistenceSettings(
            database_url=f"postgresql://kairos:test@localhost:5432/kairos_sim_test_{uuid4().hex[:8]}"
        ),
        migration_profile=MigrationProfile.SIMULATOR,
        read_only=read_only,
    )


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

    assert "022_simulator_research_observation_schedule.sql" not in runtime
    assert "023_simulator_research_baseline_lineage.sql" not in runtime
    assert simulator[-2:] == (
        "022_simulator_research_observation_schedule.sql",
        "023_simulator_research_baseline_lineage.sql",
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
        schedule = _schedule(f"adaptive-{uuid4().hex[:16]}")
        assert await roster.register(schedule)
        assert not await roster.register(schedule)
        changed_schedule = ResearchObservationScheduleV1.model_validate(
            {**schedule.to_payload(), "schedule_digest": None, "source_set_sha256": "f" * 64}
        )
        with pytest.raises(MessageIdentityConflict, match="different immutable"):
            await roster.register(changed_schedule)
        with pytest.raises(ValueError, match="missing"):
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
                **_sample(schedule, "strategy-only", "sample-001").to_payload(),
                "sample_record_id": None,
                "paired_at_ts_ms": _T0 + 2_000,
            }
        )
        with pytest.raises(asyncpg.PostgresError, match="frozen window"):
            await samples.record(altered_sample)
        wrong_snapshot = ResearchDecisionSampleV1.model_validate(
            {
                **_sample(schedule, "strategy-only", "sample-001").to_payload(),
                "sample_record_id": None,
                "market_snapshot_sha256": "f" * 64,
                "strategy_market_snapshot_sha256": "f" * 64,
            }
        )
        with pytest.raises(asyncpg.PostgresError, match="frozen window"):
            await samples.record(wrong_snapshot)

        assert await samples.record(_sample(schedule, "strategy-only", "sample-001"))
        changed_evaluation = ResearchDecisionSampleV1.model_validate(
            {
                **_sample(schedule, "strategy-review", "sample-001").to_payload(),
                "sample_record_id": None,
                "strategy_evaluation_sha256": "f" * 64,
            }
        )
        with pytest.raises(asyncpg.PostgresError, match="baseline strategy lineage"):
            await samples.record(changed_evaluation)
        for arm in RESEARCH_ARMS[1:]:
            assert await samples.record(_sample(schedule, arm, "sample-001"))

        assert await samples.record(_long_sample(schedule, "strategy-only", "sample-002"))
        changed_intent = ResearchDecisionSampleV1.model_validate(
            {
                **_long_sample(schedule, "strategy-review", "sample-002").to_payload(),
                "sample_record_id": None,
                "strategy_intent_id": "f" * 64,
            }
        )
        with pytest.raises(asyncpg.PostgresError, match="baseline strategy lineage"):
            await samples.record(changed_intent)
        for arm in RESEARCH_ARMS[1:]:
            assert await samples.record(_long_sample(schedule, arm, "sample-002"))
        seal = await roster.seal_coverage(campaign_id=schedule.campaign_id)
        assert seal.expected_result_count == len(schedule.windows) * 3
        assert seal.schedule_digest == schedule.schedule_digest
        with pytest.raises(MessageIdentityConflict, match="already sealed"):
            await roster.seal_coverage(campaign_id=schedule.campaign_id)
        with pytest.raises(asyncpg.PostgresError, match="sealed SIM research campaign"):
            await samples.record(_sample(schedule, "strategy-only", "sample-001"))

        late_schedule = _schedule(f"adaptive-{uuid4().hex[:16]}")
        assert await samples.record(_sample(late_schedule, "strategy-only", "sample-001"))
        with pytest.raises(MessageIdentityConflict, match="already has arm results"):
            await roster.register(late_schedule)
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
        old_profile = Database.migration_names(MigrationProfile.SIMULATOR)[:-1]
        assert old_profile[-1] == "022_simulator_research_observation_schedule.sql"
        async with database.transaction() as connection:
            await connection.execute("CREATE TABLE schema_migrations (version TEXT PRIMARY KEY)")
            for name in old_profile:
                await connection.execute((migrations / name).read_text(encoding="utf-8"))
                await connection.execute("INSERT INTO schema_migrations(version) VALUES ($1)", name)

        roster = ResearchObservationScheduleRepository(database)
        samples = ResearchDecisionSampleRepository(database)
        schedule = _schedule(f"upgrade-{uuid4().hex[:16]}")
        assert await roster.register(schedule)
        assert await samples.record(_sample(schedule, "strategy-only", "sample-001"))
        drifted = ResearchDecisionSampleV1.model_validate(
            {
                **_sample(schedule, "strategy-review", "sample-001").to_payload(),
                "sample_record_id": None,
                "strategy_evaluation_sha256": "f" * 64,
            }
        )
        assert await samples.record(drifted)  # 022 accepted this invalid matched baseline.
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
            await database.migrate()
        rows = await database.pool.fetch("SELECT version FROM schema_migrations ORDER BY version")
        assert tuple(str(row["version"]) for row in rows) == old_profile
        old_guard = await database.pool.fetchval(
            "SELECT pg_get_functiondef('simulator_guard_scheduled_research_sample()'::regprocedure)"
        )
        assert "prior.strategy_evaluation_sha256" not in old_guard
    finally:
        await database.close()
