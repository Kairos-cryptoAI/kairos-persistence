from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import asyncpg
import pytest
from kairos_core import (
    EvidenceReferenceV1,
    LLMProposalAction,
    LLMProposalModelProvenanceV1,
    LLMTradeProposalV1,
)

from kairos_persistence import (
    Database,
    MessageIdentityConflict,
    MigrationProfile,
    PersistenceSettings,
    SimulatorProposalRepository,
)
from kairos_persistence.database_target import connect_verified_database, require_database_target_url

_T0 = 1_760_000_000_000
_SIMPLE_DATABASE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,62}\Z")


def _proposal(**overrides: object) -> LLMTradeProposalV1:
    evidence = EvidenceReferenceV1(
        kind="closed_bar",
        reference="BTCUSDT:1m:1760000000000",
        content_sha256="4" * 64,
        observed_at_ms=_T0,
    )
    provenance = LLMProposalModelProvenanceV1(
        provider="local-test-double",
        requested_model="local-test-model",
        resolved_model="local-test-model-v1",
        request_id="request-sim-test-1",
        prompt_sha256="1" * 64,
        response_sha256="2" * 64,
        budget_reservation_id="sim-test-reservation-1",
        latency_ms=10,
        cost_usd=0.0,
    )
    values: dict[str, object] = {
        "campaign_id": "adaptive-campaign-v1",
        "arm_id": "llm-proposals-v1",
        "sample_id": "sample-0001",
        "symbol": "BTCUSDT",
        "timeframe": "1m",
        "market_as_of_ts_ms": _T0,
        "expires_at_ts_ms": _T0 + 60_000,
        "market_snapshot_sha256": "3" * 64,
        "action": LLMProposalAction.LONG_BIAS,
        "rationale": "Supplied trend evidence is directionally aligned.",
        "evidence": (evidence,),
        "model_provenance": provenance,
    }
    values.update(overrides)
    return LLMTradeProposalV1(**values)


def _simulator_database(*, read_only: bool = False) -> Database:
    url = f"postgresql://kairos:test@localhost:5432/kairos_sim_proposal_test_{uuid4().hex[:8]}"
    return Database(
        PersistenceSettings(database_url=url),
        migration_profile=MigrationProfile.SIMULATOR,
        read_only=read_only,
    )


def test_proposal_migration_is_simulator_only_and_append_only() -> None:
    runtime = Database.migration_names(MigrationProfile.RUNTIME)
    simulator = Database.migration_names(MigrationProfile.SIMULATOR)
    sql = (
        Path(__file__).parents[1] / "kairos_persistence" / "migrations" / "020_simulator_llm_proposals.sql"
    ).read_text(encoding="utf-8")

    assert "020_simulator_llm_proposals.sql" not in runtime
    assert simulator[-1] == "020_simulator_llm_proposals.sql"
    assert "UNIQUE (campaign_id, arm_id, sample_id)" in sql
    assert "BEFORE UPDATE OR DELETE" in sql
    assert "BEFORE TRUNCATE" in sql
    assert "REVOKE UPDATE, DELETE, TRUNCATE" in sql
    assert "paper_" not in sql and "execution_" not in sql


def test_repository_requires_writable_simulator_database_before_connecting() -> None:
    runtime = Database(
        PersistenceSettings(database_url="postgresql://kairos:test@localhost:5432/kairos_test")
    )
    with pytest.raises(ValueError, match="SIMULATOR profile"):
        SimulatorProposalRepository(runtime)

    with pytest.raises(ValueError, match="read-only"):
        SimulatorProposalRepository(_simulator_database(read_only=True))


def test_simulator_repository_never_falls_back_to_a_runtime_target() -> None:
    with pytest.raises(ValueError, match="kairos_sim"):
        Database(
            PersistenceSettings(database_url="postgresql://kairos:test@localhost:5432/kairos"),
            migration_profile=MigrationProfile.SIMULATOR,
        )


@pytest.mark.asyncio
async def test_page_validation_rejects_ambiguous_cursor_and_unbounded_limit() -> None:
    repository = SimulatorProposalRepository(_simulator_database())
    with pytest.raises(ValueError, match="both proposal page cursor fields"):
        await repository.load_page(campaign_id="campaign", arm_id="arm", after_sample_id="sample")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_proposals_are_idempotent_immutable_and_paged_only_from_simulator_database() -> None:
    database_url = os.getenv("KAIROS_SIM_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("KAIROS_SIM_TEST_DATABASE_URL is required for simulator integration tests")
    database_name = urlsplit(database_url).path.removeprefix("/")
    if not (database_name.startswith("kairos_sim_test_") and _SIMPLE_DATABASE_NAME.fullmatch(database_name)):
        raise RuntimeError(
            "simulator proposal test requires a uniquely named disposable kairos_sim_test database"
        )
    require_database_target_url(database_url, database_name, local_only=True)
    settings = PersistenceSettings(database_url=database_url)
    database = Database(settings, migration_profile=MigrationProfile.SIMULATOR)
    await connect_verified_database(database, database_name, local_only=True)
    try:
        await database.migrate()
        repository = SimulatorProposalRepository(database)
        proposal = _proposal()
        second = _proposal(
            sample_id="sample-0002",
            market_as_of_ts_ms=_T0 + 60_000,
            expires_at_ts_ms=_T0 + 120_000,
            market_snapshot_sha256="5" * 64,
        )
        assert await repository.record(proposal)
        assert not await repository.record(proposal)
        assert await repository.record(second)

        changed_same_sample = _proposal(rationale="A changed result for an already sealed sample.")
        with pytest.raises(MessageIdentityConflict, match="different immutable"):
            await repository.record(changed_same_sample)

        first_page = await repository.load_page(
            campaign_id=proposal.campaign_id,
            arm_id=proposal.arm_id,
            limit=1,
        )
        assert first_page == (proposal,)
        next_page = await repository.load_page(
            campaign_id=proposal.campaign_id,
            arm_id=proposal.arm_id,
            after_market_as_of_ts_ms=proposal.market_as_of_ts_ms,
            after_sample_id=proposal.sample_id,
            limit=1,
        )
        assert next_page == (second,)

        with pytest.raises(asyncpg.PostgresError, match="append-only"):
            await database.pool.execute(
                "UPDATE sim_llm_trade_proposals SET action='DEFER' WHERE proposal_id=$1",
                proposal.proposal_id,
            )
    finally:
        await database.close()
