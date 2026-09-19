"""Offline maintenance writer never migrates or dispatches durable messages."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

import pytest

from kairos_persistence import PersistenceSettings
from kairos_persistence.offline_writer import OfflineDurableWriter, OfflineWriterError
from kairos_persistence.repository import MessageIdentityConflict

MIGRATIONS = (
    "001_audit_and_idempotency.sql",
    "002_durable_runtime.sql",
    "003_execution_effect_journal.sql",
    "004_execution_recovery_delay.sql",
    "005_source_state_and_usage.sql",
    "006_paper_trade_lifecycle.sql",
    "007_execution_runtime_health.sql",
    "008_public_execution_events.sql",
    "009_paper_canary_arms.sql",
    "010_runtime_compensation_reserve.sql",
    "011_execution_mutation_budget.sql",
    "012_outbox_producer_order.sql",
)


@dataclass
class _Store:
    audit: dict[str, dict[str, Any]] = field(default_factory=dict)
    outbox: dict[str, dict[str, Any]] = field(default_factory=dict)


class _Transaction:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection
        self.snapshot: _Store | None = None

    async def __aenter__(self) -> None:
        self.snapshot = deepcopy(self.connection.store)

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            assert self.snapshot is not None
            self.connection.store.audit = self.snapshot.audit
            self.connection.store.outbox = self.snapshot.outbox


class _Connection:
    def __init__(
        self, store: _Store, *, lock_acquired: bool = True, schema_lock_acquired: bool = True
    ) -> None:
        self.store = store
        self.lock_acquired = lock_acquired
        self.schema_lock_acquired = schema_lock_acquired
        self.lock_attempts = 0
        self.schema_lock_attempts = 0
        self.unlocked = False
        self.schema_unlocked = False

    def transaction(self) -> _Transaction:
        return _Transaction(self)

    async def fetchval(self, sql: str, *params: Any) -> bool:
        if "pg_try_advisory_lock($1)" in sql:
            self.schema_lock_attempts += 1
            assert params == (4_907_627_681_104_115_019,)
            return self.schema_lock_acquired
        if "pg_try_advisory_lock" in sql:
            self.lock_attempts += 1
            return self.lock_acquired
        raise AssertionError(f"unexpected fetchval: {sql}")

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        if "FROM event_audit WHERE message_id" not in " ".join(sql.split()):
            raise AssertionError(f"unexpected fetch: {sql}")
        message_id = params[0]
        row = self.store.audit.get(message_id)
        return [] if row is None else [deepcopy(row)]

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None:
        if "FROM message_outbox WHERE message_id" not in sql:
            raise AssertionError(f"unexpected fetchrow: {sql}")
        row = self.store.outbox.get(params[0])
        return None if row is None else deepcopy(row)

    async def execute(self, sql: str, *params: Any) -> str:
        compact = " ".join(sql.split())
        if "pg_advisory_unlock" in compact:
            if "hashtextextended" not in compact:
                self.schema_unlocked = True
            self.unlocked = True
            return "SELECT 1"
        if compact.startswith("INSERT INTO event_audit"):
            (
                _produced_at,
                message_id,
                topic,
                source,
                schema_version,
                _correlation,
                _causation,
                payload,
            ) = params
            if message_id in self.store.audit:
                return "INSERT 0 0"
            self.store.audit[message_id] = {
                "topic": topic,
                "source": source,
                "schema_version": schema_version,
                "payload": payload,
            }
            return "INSERT 0 1"
        if compact.startswith("INSERT INTO message_outbox"):
            message_id, topic, payload, payload_sha256, producer = params
            if message_id in self.store.outbox:
                return "INSERT 0 0"
            self.store.outbox[message_id] = {
                "topic": topic,
                "payload": payload,
                "payload_sha256": payload_sha256,
                "producer": producer,
            }
            return "INSERT 0 1"
        raise AssertionError(f"unexpected execute: {sql}")


class _Pool:
    def __init__(self, connection: _Connection, versions: tuple[str, ...]) -> None:
        self.connection = connection
        self.versions = versions
        self.released = False

    async def fetchval(self, sql: str) -> str:
        assert sql == "SELECT current_database()"
        return "kairos"

    async def fetch(self, sql: str) -> list[dict[str, str]]:
        assert "SELECT version FROM schema_migrations" in sql
        return [{"version": version} for version in self.versions]

    async def acquire(self) -> _Connection:
        return self.connection

    async def release(self, connection: _Connection) -> None:
        assert connection is self.connection
        self.released = True


class _Database:
    def __init__(self, pool: _Pool) -> None:
        self.pool = pool
        self.settings = PersistenceSettings(database_url="postgresql://test:test@timescaledb:5432/kairos")
        self.connect_calls = 0
        self.close_calls = 0
        self.migrate_calls = 0

    async def connect(self) -> None:
        self.connect_calls += 1

    async def close(self) -> None:
        self.close_calls += 1

    async def migrate(self) -> None:
        self.migrate_calls += 1
        raise AssertionError("offline writer must never migrate")


def _writer(
    *,
    versions: tuple[str, ...] = MIGRATIONS,
    lock_acquired: bool = True,
    schema_lock_acquired: bool = True,
) -> tuple[OfflineDurableWriter, _Database, _Store, _Connection]:
    store = _Store()
    connection = _Connection(store, lock_acquired=lock_acquired, schema_lock_acquired=schema_lock_acquired)
    database = _Database(_Pool(connection, versions))
    writer = OfflineDurableWriter(
        service_name="kairos-quant-scouts",
        expected_database_name="kairos",
        expected_schema_versions=MIGRATIONS,
        database=database,  # type: ignore[arg-type]
    )
    return writer, database, store, connection


def _payload(message_id: str, *, close: float = 101.0) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "message_id": message_id,
        "produced_at": "2026-09-12T14:00:00+00:00",
        "source": "kairos-quant-scouts",
        "symbol": "BTCUSDT",
        "close": close,
    }


@pytest.mark.asyncio
async def test_offline_writer_uses_exact_schema_without_migration_or_dispatcher():
    writer, database, store, connection = _writer()

    await writer.start()
    assert await writer.append("kairos.closed_bar.v1", _payload("bar-1")) is True
    assert await writer.append("kairos.closed_bar.v1", _payload("bar-1")) is False
    await writer.close()

    assert database.migrate_calls == 0
    assert database.connect_calls == database.close_calls == 1
    assert connection.unlocked is True
    assert connection.schema_lock_attempts == 1
    assert connection.schema_unlocked is True
    assert len(store.audit) == len(store.outbox) == 1
    assert store.outbox["bar-1"]["producer"] == "kairos-quant-scouts"
    assert not hasattr(writer, "transport")


@pytest.mark.asyncio
async def test_offline_writer_rejects_any_schema_drift_before_the_lease_or_a_write():
    writer, database, store, connection = _writer(versions=MIGRATIONS + ("013_campaign_source_budgets.sql",))

    with pytest.raises(OfflineWriterError, match="exact verified profile"):
        await writer.start()

    assert database.migrate_calls == 0
    assert database.close_calls == 1
    assert connection.schema_lock_attempts == 1
    assert connection.lock_attempts == 0
    assert connection.schema_unlocked is True
    assert store.audit == store.outbox == {}


@pytest.mark.asyncio
async def test_offline_writer_rejects_a_reused_audit_identity_with_changed_content():
    writer, _database, store, _connection = _writer()
    await writer.start()
    assert await writer.append("kairos.closed_bar.v1", _payload("bar-1")) is True

    with pytest.raises(MessageIdentityConflict, match="event audit"):
        await writer.append("kairos.closed_bar.v1", _payload("bar-1", close=102.0))
    await writer.close()

    assert len(store.audit) == len(store.outbox) == 1


@pytest.mark.asyncio
async def test_offline_writer_releases_resources_when_the_shared_producer_lease_is_held():
    writer, database, store, connection = _writer(lock_acquired=False)

    with pytest.raises(OfflineWriterError, match="already running"):
        await writer.start()

    assert database.migrate_calls == 0
    assert database.close_calls == 1
    assert connection.lock_attempts == 1
    assert connection.schema_lock_attempts == 1
    assert store.audit == store.outbox == {}
