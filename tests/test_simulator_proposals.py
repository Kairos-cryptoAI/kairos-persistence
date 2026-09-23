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
    EvidenceReferenceV1,
    LLMProposalAction,
    LLMProposalModelProvenanceV1,
    LLMTradeProposalV1,
    Topics,
)
from kairos_core.bus.base import BusEnvelope, MessageBus

from kairos_persistence import (
    SIMULATOR_PROPOSAL_CONSUMER_GROUP,
    Database,
    MessageIdentityConflict,
    MigrationProfile,
    PersistenceSettings,
    SimulatorProposalRepository,
    consume_simulator_proposals,
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


class _QueueMessageBus(MessageBus):
    def __init__(self) -> None:
        self.messages: asyncio.Queue[BusEnvelope] = asyncio.Queue()
        self.subscribed = asyncio.Event()
        self.acknowledged: list[tuple[str, str, str | None]] = []
        self.subscription: tuple[str, str | None, str | None] | None = None

    async def publish(self, topic: str, message) -> str:
        payload = self._to_payload(message)
        envelope = BusEnvelope(id=f"message-{self.messages.qsize() + 1}", topic=topic, payload=payload)
        await self.messages.put(envelope)
        return envelope.id

    async def subscribe(self, topic: str, *, group: str | None = None, consumer: str | None = None):
        self.subscription = (topic, group, consumer)
        self.subscribed.set()
        while True:
            yield await self.messages.get()

    async def ack(self, topic: str, envelope: BusEnvelope, *, group: str | None = None) -> None:
        self.acknowledged.append((topic, envelope.id, group))


class _RecordingProposalRepository(SimulatorProposalRepository):
    def __init__(self, *, fail: Exception | None = None) -> None:
        self.proposals: list[LLMTradeProposalV1] = []
        self.recorded = asyncio.Event()
        self.fail = fail

    async def record(self, proposal: LLMTradeProposalV1) -> bool:
        if self.fail is not None:
            raise self.fail
        self.proposals.append(proposal)
        self.recorded.set()
        return True


@pytest.mark.asyncio
async def test_consumer_records_advisory_proposal_before_acknowledging() -> None:
    repository = _RecordingProposalRepository()
    bus = _QueueMessageBus()
    proposal = _proposal()
    task = asyncio.create_task(consume_simulator_proposals(repository, bus, consumer="simulator-test-worker"))
    await asyncio.wait_for(bus.subscribed.wait(), timeout=1)
    message_id = await bus.publish(Topics.LLM_TRADE_PROPOSAL, proposal)
    await asyncio.wait_for(repository.recorded.wait(), timeout=1)
    await asyncio.sleep(0)

    assert bus.subscription == (
        Topics.LLM_TRADE_PROPOSAL,
        SIMULATOR_PROPOSAL_CONSUMER_GROUP,
        "simulator-test-worker",
    )
    assert repository.proposals == [proposal]
    assert bus.acknowledged == [(Topics.LLM_TRADE_PROPOSAL, message_id, SIMULATOR_PROPOSAL_CONSUMER_GROUP)]

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_consumer_fails_closed_without_ack_on_invalid_or_unpersistable_message() -> None:
    bus = _QueueMessageBus()
    invalid_repository = _RecordingProposalRepository()
    invalid_task = asyncio.create_task(
        consume_simulator_proposals(invalid_repository, bus, consumer="simulator-test-worker")
    )
    await asyncio.wait_for(bus.subscribed.wait(), timeout=1)
    await bus.publish(Topics.LLM_TRADE_PROPOSAL, {"unexpected": "payload"})
    with pytest.raises(ValueError):
        await asyncio.wait_for(invalid_task, timeout=1)
    assert bus.acknowledged == []

    bus = _QueueMessageBus()
    storage_error = RuntimeError("synthetic storage failure")
    repository = _RecordingProposalRepository(fail=storage_error)
    task = asyncio.create_task(consume_simulator_proposals(repository, bus, consumer="worker-2"))
    await asyncio.wait_for(bus.subscribed.wait(), timeout=1)
    await bus.publish(Topics.LLM_TRADE_PROPOSAL, _proposal())
    with pytest.raises(RuntimeError, match="synthetic storage failure"):
        await asyncio.wait_for(task, timeout=1)
    assert bus.acknowledged == []


def test_consumer_rejects_unsafe_identity_before_subscribing() -> None:
    repository = _RecordingProposalRepository()
    bus = _QueueMessageBus()
    with pytest.raises(ValueError, match="normalized non-empty identifier"):
        asyncio.run(consume_simulator_proposals(repository, bus, consumer=" "))
    assert bus.subscription is None


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
