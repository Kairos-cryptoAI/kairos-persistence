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

    assert "022_simulator_research_observation_schedule.sql" not in runtime
    assert simulator[-1] == "022_simulator_research_observation_schedule.sql"
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

        for window in schedule.windows:
            for arm in RESEARCH_ARMS:
                assert await samples.record(_sample(schedule, arm, window.sample_id))
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
