"""Real TimescaleDB coverage, enabled only in the dedicated integration job."""

from __future__ import annotations

import asyncio
import hashlib
import os
from datetime import timedelta
from uuid import uuid4

import asyncpg
import pytest

from kairos_persistence import (
    AuditRepository,
    Database,
    EffectStatus,
    EffectType,
    ExecutionJournalRepository,
    MessageIdentityConflict,
    PersistenceSettings,
)

pytestmark = pytest.mark.integration


def _settings() -> PersistenceSettings:
    database_url = os.getenv("KAIROS_PERSISTENCE_DATABASE_URL")
    if not database_url:
        pytest.skip("KAIROS_PERSISTENCE_DATABASE_URL is required for integration tests")
    return PersistenceSettings(database_url=database_url)


@pytest.mark.asyncio
async def test_message_transaction_is_atomic_and_completed_duplicates_are_suppressed() -> None:
    database = Database(_settings())
    await database.connect()
    await database.migrate()
    repository = AuditRepository(database.pool)
    suffix = uuid4().hex
    consumer = f"integration-{suffix}"
    incoming_id = f"incoming-{suffix}"
    outgoing_id = f"outgoing-{suffix}"

    try:
        async with repository.message_transaction(consumer, incoming_id, "integration.input") as tx:
            assert tx.claim.claimed
            payload = '{"ok":true}'
            assert await tx.enqueue_outbox(
                outgoing_id,
                "integration.output",
                payload,
                hashlib.sha256(payload.encode()).hexdigest(),
            )
            await tx.complete({"outgoing_id": outgoing_id})

        async with repository.message_transaction(consumer, incoming_id, "integration.input") as tx:
            assert not tx.claim.claimed
            assert tx.claim.duplicate_completed

        async with database.pool.acquire() as connection:
            status = await connection.fetchval(
                "SELECT status FROM message_inbox WHERE consumer=$1 AND message_id=$2",
                consumer,
                incoming_id,
            )
            outbox_count = await connection.fetchval(
                "SELECT count(*) FROM message_outbox WHERE message_id=$1", outgoing_id
            )
        assert status == "COMPLETED"
        assert outbox_count == 1
    finally:
        async with database.pool.acquire() as connection:
            await connection.execute("DELETE FROM message_outbox WHERE message_id=$1", outgoing_id)
            await connection.execute(
                "DELETE FROM message_inbox WHERE consumer=$1 AND message_id=$2",
                consumer,
                incoming_id,
            )
        await database.close()


@pytest.mark.asyncio
async def test_message_transaction_rolls_back_side_effects_before_recording_failure() -> None:
    database = Database(_settings())
    await database.connect()
    await database.migrate()
    repository = AuditRepository(database.pool)
    suffix = uuid4().hex
    consumer = f"integration-{suffix}"
    incoming_id = f"incoming-{suffix}"
    outgoing_id = f"outgoing-{suffix}"

    try:
        with pytest.raises(asyncpg.PostgresError):
            async with repository.message_transaction(consumer, incoming_id, "integration.input") as tx:
                payload = '{"ok":true}'
                await tx.enqueue_outbox(
                    outgoing_id,
                    "integration.output",
                    payload,
                    hashlib.sha256(payload.encode()).hexdigest(),
                )
                await tx.connection.execute("SELECT 1 / 0")

        async with database.pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT status, error FROM message_inbox WHERE consumer=$1 AND message_id=$2",
                consumer,
                incoming_id,
            )
            outbox_count = await connection.fetchval(
                "SELECT count(*) FROM message_outbox WHERE message_id=$1", outgoing_id
            )
        assert row["status"] == "FAILED"
        assert "division by zero" in row["error"]
        assert outbox_count == 0
    finally:
        async with database.pool.acquire() as connection:
            await connection.execute("DELETE FROM message_outbox WHERE message_id=$1", outgoing_id)
            await connection.execute(
                "DELETE FROM message_inbox WHERE consumer=$1 AND message_id=$2",
                consumer,
                incoming_id,
            )
        await database.close()


@pytest.mark.asyncio
async def test_outbox_leases_retry_and_complete_without_two_workers_owning_a_row() -> None:
    database = Database(_settings())
    await database.connect()
    await database.migrate()
    repository = AuditRepository(database.pool)
    message_id = f"outbox-lease-{uuid4().hex}"
    payload = '{"message_id":"' + message_id + '"}'
    payload_sha256 = hashlib.sha256(payload.encode()).hexdigest()

    try:
        async with database.transaction() as connection:
            assert await repository.enqueue_outbox(
                connection,
                message_id,
                "integration.output",
                payload,
                payload_sha256,
            )

        first = await repository.claim_outbox("worker-1", limit=1)
        assert len(first) == 1
        assert first[0].message_id == message_id
        assert first[0].publish_attempts == 1
        assert await repository.claim_outbox("worker-2", limit=1) == []

        assert await repository.fail_outbox(
            first[0].id,
            "worker-1",
            "temporary transport error",
            retry_after=timedelta(0),
            max_attempts=3,
        )
        second = await repository.claim_outbox("worker-2", limit=1)
        assert len(second) == 1
        assert second[0].publish_attempts == 2
        assert await repository.mark_published(second[0].id, "worker-2")
        assert not await repository.mark_published(second[0].id, "worker-1")
    finally:
        await database.pool.execute("DELETE FROM message_outbox WHERE message_id=$1", message_id)
        await database.close()


@pytest.mark.asyncio
async def test_inbox_rejects_same_message_id_with_different_payload_fingerprint() -> None:
    database = Database(_settings())
    await database.connect()
    await database.migrate()
    repository = AuditRepository(database.pool)
    suffix = uuid4().hex
    consumer = f"identity-{suffix}"
    message_id = f"incoming-{suffix}"

    try:
        async with repository.message_transaction(
            consumer,
            message_id,
            "integration.input",
            payload_sha256="a" * 64,
        ) as transaction:
            await transaction.complete()

        with pytest.raises(MessageIdentityConflict):
            async with repository.message_transaction(
                consumer,
                message_id,
                "integration.input",
                payload_sha256="b" * 64,
            ):
                pass
    finally:
        await database.pool.execute(
            "DELETE FROM message_inbox WHERE consumer=$1 AND message_id=$2",
            consumer,
            message_id,
        )
        await database.close()


@pytest.mark.asyncio
async def test_execution_effect_journal_is_idempotent_chained_and_recoverable() -> None:
    database = Database(_settings())
    await database.connect()
    await database.migrate()
    journal = ExecutionJournalRepository(database.pool)
    effect_key = f"evedex:PLACE_ORDER:{uuid4().hex}"
    request = {"symbol": "BTCUSDT", "quantity": 0.001, "side": "BUY"}

    try:
        first, duplicate = await asyncio.gather(
            journal.prepare(
                effect_key=effect_key,
                effect_type=EffectType.PLACE_ORDER,
                exchange="evedex",
                symbol="BTCUSDT",
                client_order_id="client-1",
                request_payload=request,
            ),
            journal.prepare(
                effect_key=effect_key,
                effect_type=EffectType.PLACE_ORDER,
                exchange="evedex",
                symbol="BTCUSDT",
                client_order_id="client-1",
                request_payload=request,
            ),
        )
        assert first == duplicate
        assert first.status is EffectStatus.PREPARED
        assert [item.effect_key for item in await journal.recovery_required(exchange="evedex")] == [
            effect_key
        ]

        confirmed = await journal.confirm(
            effect_key,
            exchange_effect_id="client-1",
            response_payload={"status": "NEW"},
        )
        assert confirmed.status is EffectStatus.CONFIRMED
        reconciled = await journal.reconcile(effect_key)
        assert reconciled.status is EffectStatus.RECONCILED
        assert await journal.recovery_required(exchange="evedex") == []
        assert await journal.verify_chain(effect_key)

        with pytest.raises(MessageIdentityConflict):
            await journal.prepare(
                effect_key=effect_key,
                effect_type=EffectType.PLACE_ORDER,
                exchange="evedex",
                symbol="BTCUSDT",
                client_order_id="client-1",
                request_payload={**request, "quantity": 0.002},
            )

        await database.pool.execute(
            """UPDATE execution_effect_events SET event_payload='{"tampered":true}'::jsonb
               WHERE effect_key=$1 AND phase='CONFIRMED'""",
            effect_key,
        )
        assert not await journal.verify_chain(effect_key)
    finally:
        await database.pool.execute("DELETE FROM execution_effect_events WHERE effect_key=$1", effect_key)
        await database.pool.execute("DELETE FROM execution_effects WHERE effect_key=$1", effect_key)
        await database.close()
