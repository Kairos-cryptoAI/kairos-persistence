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
    SourceBudgetExceeded,
    SourceStateRepository,
    UsageStatus,
)
from kairos_persistence.metrics_exporter import collect_runtime_metrics

pytestmark = pytest.mark.integration


def _settings() -> PersistenceSettings:
    database_url = os.getenv("KAIROS_PERSISTENCE_DATABASE_URL")
    if not database_url:
        pytest.skip("KAIROS_PERSISTENCE_DATABASE_URL is required for integration tests")
    return PersistenceSettings(database_url=database_url)


@pytest.mark.asyncio
async def test_operations_query_covers_strict_paper_audit_and_lifecycle_tables() -> None:
    database = Database(_settings())
    await database.connect()
    try:
        await database.migrate()

        async def redis_probe(_url: str) -> bool:
            return True

        metrics = await collect_runtime_metrics(
            database.pool,
            redis_url="redis://unused",
            redis_probe=redis_probe,
        )
        assert metrics.persistence_up == 1
        assert metrics.redis_up == 1
        assert metrics.closed_bar_gaps_24h >= 0
        assert 0 <= metrics.closed_bar_minimum_coverage_ratio_24h <= 1
        assert 0 <= metrics.venue_availability_ratio_24h <= 1
        assert metrics.venue_max_book_age_ms >= 0
        assert metrics.venue_max_timestamp_skew_ms >= 0
        assert metrics.paper_unprotected_trades >= 0
        assert metrics.execution_p95_shortfall_bps >= 0
        assert metrics.api_spend_month_usd >= 0
    finally:
        await database.close()


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
    environment = "evedex-dev"
    account_id = f"paper-{uuid4().hex}"
    trade_id = hashlib.sha256(effect_key.encode()).hexdigest()

    try:
        first, duplicate = await asyncio.gather(
            journal.prepare(
                effect_key=effect_key,
                effect_type=EffectType.PLACE_ORDER,
                exchange="evedex",
                symbol="BTCUSDT",
                client_order_id="client-1",
                request_payload=request,
                environment=environment,
                account_id=account_id,
                trade_id=trade_id,
                order_role="ENTRY",
                recovery_delay=timedelta(0),
            ),
            journal.prepare(
                effect_key=effect_key,
                effect_type=EffectType.PLACE_ORDER,
                exchange="evedex",
                symbol="BTCUSDT",
                client_order_id="client-1",
                request_payload=request,
                environment=environment,
                account_id=account_id,
                trade_id=trade_id,
                order_role="ENTRY",
                recovery_delay=timedelta(0),
            ),
        )
        assert {first.created, duplicate.created} == {True, False}
        assert first.effect == duplicate.effect
        assert first.effect.status is EffectStatus.PREPARED
        assert [item.effect_key for item in await journal.recovery_required(exchange="evedex")] == [
            effect_key
        ]
        assert [
            item.effect_key
            for item in await journal.recovery_required(
                exchange="evedex",
                environment=environment,
                account_id=account_id,
            )
        ] == [effect_key]
        assert (
            await journal.recovery_required(
                exchange="evedex",
                environment=environment,
                account_id="different-paper-account",
            )
            == []
        )

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

        await database.pool.execute(
            """UPDATE execution_effects
               SET request_payload=jsonb_set(request_payload,'{quantity}','0.002'::jsonb)
               WHERE effect_key=$1""",
            effect_key,
        )
        with pytest.raises(MessageIdentityConflict, match="fingerprint"):
            await journal.get(effect_key)
        await database.pool.execute(
            """UPDATE execution_effects
               SET request_payload=jsonb_set(request_payload,'{quantity}','0.001'::jsonb)
               WHERE effect_key=$1""",
            effect_key,
        )

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


@pytest.mark.asyncio
async def test_source_cursor_and_paid_usage_survive_restart_without_budget_overshoot() -> None:
    database = Database(_settings())
    await database.connect()
    await database.migrate()
    repository = SourceStateRepository(database.pool)
    suffix = uuid4().hex
    service = f"text-integration-{suffix}"
    source = "x-api"
    cursor_key = "lookonchain"
    first_request = f"request-1-{suffix}"
    second_request = f"request-2-{suffix}"

    try:
        assert await repository.advance_cursor(service, source, cursor_key, "100")
        assert not await repository.advance_cursor(service, source, cursor_key, "100")
        with pytest.raises(ValueError, match="regression"):
            await repository.advance_cursor(service, source, cursor_key, "99")
        cursor = await SourceStateRepository(database.pool).get_cursor(service, source, cursor_key)
        assert cursor is not None
        assert cursor.cursor_value == "100"

        reservation = await repository.reserve_usage(
            service=service,
            source=source,
            reservation_id=first_request,
            reserved_units=2,
            unit_cost_microusd=5_000,
            monthly_budget_microusd=10_000,
        )
        assert reservation.status is UsageStatus.RESERVED
        with pytest.raises(SourceBudgetExceeded):
            await repository.reserve_usage(
                service=service,
                source=source,
                reservation_id=second_request,
                reserved_units=1,
                unit_cost_microusd=5_000,
                monthly_budget_microusd=10_000,
            )

        committed = await repository.commit_usage(service, source, first_request, actual_units=1)
        assert committed.status is UsageStatus.COMMITTED
        assert committed.actual_cost_microusd == 5_000
        released = await repository.reserve_usage(
            service=service,
            source=source,
            reservation_id=second_request,
            reserved_units=1,
            unit_cost_microusd=5_000,
            monthly_budget_microusd=10_000,
        )
        assert released.status is UsageStatus.RESERVED
        assert (
            await repository.release_usage(service, source, second_request)
        ).status is UsageStatus.RELEASED

        usage = await repository.monthly_usage(service, source)
        assert usage.committed_units == 1
        assert usage.committed_cost_microusd == 5_000
        assert usage.reserved_units == 0
        assert usage.budgeted_cost_microusd == 5_000
    finally:
        await database.pool.execute(
            "DELETE FROM source_usage_reservations WHERE service=$1 AND source=$2",
            service,
            source,
        )
        await database.pool.execute(
            "DELETE FROM source_cursors WHERE service=$1 AND source=$2",
            service,
            source,
        )
        await database.close()
