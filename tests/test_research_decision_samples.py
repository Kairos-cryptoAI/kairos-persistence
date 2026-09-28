"""SIM-only paired decision evidence must remain immutable and non-executable."""

from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import asyncpg
import pytest
from kairos_core import ResearchDecisionSampleV1

from kairos_persistence import (
    Database,
    MessageIdentityConflict,
    MigrationProfile,
    PersistenceSettings,
    ResearchDecisionSampleRepository,
    canonical_payload,
)
from kairos_persistence.database_target import connect_verified_database, require_database_target_url

_T0 = 1_760_000_000_000
_SIMPLE_DATABASE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,62}\Z")


def _sample(**overrides: object) -> ResearchDecisionSampleV1:
    values: dict[str, object] = {
        "campaign_id": "adaptive-campaign-v1",
        "arm_id": "strategy-vs-llm-v1",
        "sample_id": "sample-0001",
        "symbol": "BTCUSDT",
        "timeframe": "1m",
        "market_as_of_ts_ms": _T0,
        "market_snapshot_sha256": "3" * 64,
        "paired_at_ts_ms": _T0 + 1_000,
        "sample_deadline_ts_ms": _T0 + 10_000,
        "strategy_id": "adaptive-strategy-v1",
        "strategy_revision": "frozen-001",
        "strategy_outcome": "NO_INTENT",
        "strategy_evaluation_sha256": "4" * 64,
        "strategy_evidence_as_of_ts_ms": _T0,
        "strategy_market_snapshot_sha256": "3" * 64,
        "llm_outcome": "NOT_CALLED",
    }
    values.update(overrides)
    return ResearchDecisionSampleV1(**values)


def _simulator_database(*, read_only: bool = False) -> Database:
    url = f"postgresql://kairos:test@localhost:5432/kairos_sim_sample_test_{uuid4().hex[:8]}"
    return Database(
        PersistenceSettings(database_url=url),
        migration_profile=MigrationProfile.SIMULATOR,
        read_only=read_only,
    )


def test_research_decision_migration_is_simulator_only_and_append_only() -> None:
    runtime = Database.migration_names(MigrationProfile.RUNTIME)
    simulator = Database.migration_names(MigrationProfile.SIMULATOR)
    sql = (
        Path(__file__).parents[1]
        / "kairos_persistence"
        / "migrations"
        / "021_simulator_research_decision_samples.sql"
    ).read_text(encoding="utf-8")

    assert "021_simulator_research_decision_samples.sql" not in runtime
    assert simulator[-3] == "021_simulator_research_decision_samples.sql"
    assert "UNIQUE (campaign_id, arm_id, sample_id)" in sql
    assert "BEFORE UPDATE OR DELETE" in sql
    assert "BEFORE TRUNCATE" in sql
    assert "REVOKE UPDATE, DELETE, TRUNCATE" in sql
    assert "SIM_RESEARCH_ONLY" in sql
    assert "VOLATILITY_ALERT" in sql
    assert "DROP CONSTRAINT sim_llm_trade_proposals_action_check" in sql
    assert "paper_" not in sql and "execution_" not in sql


def test_repository_requires_writable_simulator_database_before_connecting() -> None:
    runtime = Database(
        PersistenceSettings(database_url="postgresql://kairos:test@localhost:5432/kairos_test")
    )
    with pytest.raises(ValueError, match="SIMULATOR profile"):
        ResearchDecisionSampleRepository(runtime)
    with pytest.raises(ValueError, match="read-only"):
        ResearchDecisionSampleRepository(_simulator_database(read_only=True))
    with pytest.raises(TypeError, match="explicit Database"):
        ResearchDecisionSampleRepository(object())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_record_rejects_untyped_input_before_connecting() -> None:
    repository = ResearchDecisionSampleRepository(_simulator_database())
    with pytest.raises(TypeError, match="ResearchDecisionSampleV1"):
        await repository.record({"sample_id": "sample-0001"})  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="ID does not match its canonical payload"):
        await repository.record(_sample().model_copy(update={"strategy_revision": "forged"}))


@pytest.mark.asyncio
async def test_page_validation_rejects_ambiguous_cursor_and_unbounded_limit() -> None:
    repository = ResearchDecisionSampleRepository(_simulator_database())
    with pytest.raises(ValueError, match="both research decision page cursor fields"):
        await repository.load_page(campaign_id="campaign", arm_id="arm", after_sample_id="sample")
    with pytest.raises(ValueError, match="non-negative integer"):
        await repository.load_page(
            campaign_id="campaign", arm_id="arm", after_market_as_of_ts_ms=True, after_sample_id="sample"
        )
    with pytest.raises(ValueError, match="1 through 1000"):
        await repository.load_page(campaign_id="campaign", arm_id="arm", limit=1_001)


def test_row_integrity_verifies_every_stored_projection_and_payload_hash() -> None:
    sample = _sample()
    encoded, payload_sha256 = canonical_payload(sample.to_payload())
    row = {
        name: getattr(sample, name)
        for name in (
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
        )
    }
    row.update(payload=encoded, payload_sha256=payload_sha256)

    assert ResearchDecisionSampleRepository._row_matches(row, sample, encoded, payload_sha256)
    assert not ResearchDecisionSampleRepository._row_matches(
        {**row, "strategy_outcome": "LONG"}, sample, encoded, payload_sha256
    )
    assert not ResearchDecisionSampleRepository._row_matches(
        {**row, "payload_sha256": "0" * 64}, sample, encoded, payload_sha256
    )


def test_wait_and_conflict_are_distinct_research_facts_without_trade_authority() -> None:
    wait_vs_llm = _sample(
        llm_outcome="SHORT_BIAS",
        llm_evidence_as_of_ts_ms=_T0,
        llm_market_snapshot_sha256="3" * 64,
        llm_proposal_id="5" * 64,
        llm_proposal_expires_at_ts_ms=_T0 + 30_000,
        llm_completion_receipt_id="a" * 64,
        llm_completion_started_at_ts_ms=_T0 + 100,
        llm_completion_observed_at_ts_ms=_T0 + 900,
    )
    long_vs_llm = _sample(
        strategy_outcome="LONG",
        strategy_intent_id="6" * 64,
        strategy_intent_expires_at_ts_ms=_T0 + 30_000,
        llm_outcome="SHORT_BIAS",
        llm_evidence_as_of_ts_ms=_T0,
        llm_market_snapshot_sha256="3" * 64,
        llm_proposal_id="5" * 64,
        llm_proposal_expires_at_ts_ms=_T0 + 30_000,
        llm_completion_receipt_id="a" * 64,
        llm_completion_started_at_ts_ms=_T0 + 100,
        llm_completion_observed_at_ts_ms=_T0 + 900,
    )

    assert wait_vs_llm.strategy_outcome == "NO_INTENT"
    assert wait_vs_llm.llm_outcome == "SHORT_BIAS"
    assert long_vs_llm.strategy_outcome == "LONG"
    assert long_vs_llm.llm_outcome == "SHORT_BIAS"
    assert wait_vs_llm.sample_record_id != long_vs_llm.sample_record_id
    assert wait_vs_llm.authority == long_vs_llm.authority == "SIM_RESEARCH_ONLY"
    assert "order" not in wait_vs_llm.to_payload()
    assert "risk_decision" not in wait_vs_llm.to_payload()


def test_volatility_alert_and_failed_call_are_distinct_non_executable_facts() -> None:
    alert = _sample(
        llm_outcome="VOLATILITY_ALERT",
        llm_evidence_as_of_ts_ms=_T0,
        llm_market_snapshot_sha256="3" * 64,
        llm_proposal_id="7" * 64,
        llm_proposal_expires_at_ts_ms=_T0 + 30_000,
        llm_completion_receipt_id="a" * 64,
        llm_completion_started_at_ts_ms=_T0 + 100,
        llm_completion_observed_at_ts_ms=_T0 + 900,
    )
    failed = _sample(
        llm_outcome="CALL_FAILED",
        llm_evidence_as_of_ts_ms=_T0,
        llm_market_snapshot_sha256="3" * 64,
        llm_failure_receipt_id="8" * 64,
        llm_failure_class="TIMEOUT",
        llm_failure_started_at_ts_ms=_T0 + 100,
        llm_failure_observed_at_ts_ms=_T0 + 900,
    )

    assert alert.llm_outcome == "VOLATILITY_ALERT"
    assert failed.llm_outcome == "CALL_FAILED"
    assert failed.llm_proposal_id is None
    assert alert.sample_record_id != failed.sample_record_id
    assert alert.authority == failed.authority == "SIM_RESEARCH_ONLY"
    assert "order" not in failed.to_payload()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_samples_are_idempotent_immutable_and_paged_only_from_simulator_database() -> None:
    database_url = os.getenv("KAIROS_SIM_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("KAIROS_SIM_TEST_DATABASE_URL is required for simulator integration tests")
    database_name = urlsplit(database_url).path.removeprefix("/")
    if not (database_name.startswith("kairos_sim_test_") and _SIMPLE_DATABASE_NAME.fullmatch(database_name)):
        raise RuntimeError(
            "research decision test requires a uniquely named disposable kairos_sim_test database"
        )
    require_database_target_url(database_url, database_name, local_only=True)
    database = Database(
        PersistenceSettings(database_url=database_url),
        migration_profile=MigrationProfile.SIMULATOR,
    )
    await connect_verified_database(database, database_name, local_only=True)
    try:
        await database.migrate()
        repository = ResearchDecisionSampleRepository(database)
        first = _sample()
        second = _sample(
            sample_id="sample-0002",
            market_as_of_ts_ms=_T0 + 60_000,
            paired_at_ts_ms=_T0 + 61_000,
            sample_deadline_ts_ms=_T0 + 70_000,
            strategy_evidence_as_of_ts_ms=_T0 + 60_000,
            market_snapshot_sha256="5" * 64,
            strategy_market_snapshot_sha256="5" * 64,
        )
        assert await repository.record(first)
        assert not await repository.record(first)
        assert await repository.record(second)
        alert = _sample(
            sample_id="sample-0003",
            market_as_of_ts_ms=_T0 + 120_000,
            paired_at_ts_ms=_T0 + 121_000,
            sample_deadline_ts_ms=_T0 + 130_000,
            market_snapshot_sha256="7" * 64,
            strategy_evidence_as_of_ts_ms=_T0 + 120_000,
            strategy_market_snapshot_sha256="7" * 64,
            llm_outcome="VOLATILITY_ALERT",
            llm_evidence_as_of_ts_ms=_T0 + 120_000,
            llm_market_snapshot_sha256="7" * 64,
            llm_proposal_id="8" * 64,
            llm_proposal_expires_at_ts_ms=_T0 + 150_000,
            llm_completion_receipt_id="b" * 64,
            llm_completion_started_at_ts_ms=_T0 + 120_100,
            llm_completion_observed_at_ts_ms=_T0 + 120_900,
        )
        failed = _sample(
            sample_id="sample-0004",
            market_as_of_ts_ms=_T0 + 180_000,
            paired_at_ts_ms=_T0 + 181_000,
            sample_deadline_ts_ms=_T0 + 190_000,
            market_snapshot_sha256="9" * 64,
            strategy_evidence_as_of_ts_ms=_T0 + 180_000,
            strategy_market_snapshot_sha256="9" * 64,
            llm_outcome="CALL_FAILED",
            llm_evidence_as_of_ts_ms=_T0 + 180_000,
            llm_market_snapshot_sha256="9" * 64,
            llm_failure_receipt_id="a" * 64,
            llm_failure_class="TIMEOUT",
            llm_failure_started_at_ts_ms=_T0 + 180_100,
            llm_failure_observed_at_ts_ms=_T0 + 180_900,
        )
        assert await repository.record(alert)
        assert await repository.record(failed)
        assert await repository.load_page(campaign_id=first.campaign_id, arm_id=first.arm_id) == (
            first,
            second,
            alert,
            failed,
        )

        changed_same_sample = _sample(strategy_evaluation_sha256="6" * 64)
        with pytest.raises(MessageIdentityConflict, match="different immutable"):
            await repository.record(changed_same_sample)

        first_page = await repository.load_page(campaign_id=first.campaign_id, arm_id=first.arm_id, limit=1)
        assert first_page == (first,)
        next_page = await repository.load_page(
            campaign_id=first.campaign_id,
            arm_id=first.arm_id,
            after_market_as_of_ts_ms=first.market_as_of_ts_ms,
            after_sample_id=first.sample_id,
            limit=1,
        )
        assert next_page == (second,)

        with pytest.raises(asyncpg.PostgresError, match="append-only"):
            await database.pool.execute(
                "UPDATE sim_research_decision_samples SET strategy_outcome='NOT_EVALUATED' "
                "WHERE sample_record_id=$1",
                first.sample_record_id,
            )
        with pytest.raises(asyncpg.PostgresError, match="append-only"):
            await database.pool.execute(
                "DELETE FROM sim_research_decision_samples WHERE sample_record_id=$1",
                first.sample_record_id,
            )
        with pytest.raises(asyncpg.PostgresError, match="append-only"):
            await database.pool.execute("TRUNCATE sim_research_decision_samples")
    finally:
        await database.close()
