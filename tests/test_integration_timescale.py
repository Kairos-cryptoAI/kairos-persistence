"""Real TimescaleDB coverage, enabled only in the dedicated integration job."""

from __future__ import annotations

import os
from uuid import uuid4

import asyncpg
import pytest

from kairos_persistence import AuditRepository, Database, PersistenceSettings

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
            assert await tx.enqueue_outbox(outgoing_id, "integration.output", '{"ok":true}')
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
                await tx.enqueue_outbox(outgoing_id, "integration.output", '{"ok":true}')
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
