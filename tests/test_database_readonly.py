from unittest.mock import AsyncMock

import pytest

from kairos_persistence import database, metrics_exporter
from kairos_persistence.config import PersistenceSettings
from kairos_persistence.database import Database

_DATABASE_URL = "postgresql://observer:test-only@127.0.0.1:5432/kairos"


class _Transaction:
    def __init__(self, connection, *, readonly):
        self.connection = connection
        assert readonly is True

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Connection:
    def __init__(self, versions):
        self.versions = versions
        self.queries = []

    def transaction(self, *, readonly=False):
        return _Transaction(self, readonly=readonly)

    async def fetch(self, query):
        self.queries.append(query)
        return [{"version": version} for version in self.versions]


class _Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Pool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _Acquire(self.connection)


def _settings():
    return PersistenceSettings(database_url=_DATABASE_URL)


@pytest.mark.asyncio
async def test_read_only_pool_sets_postgres_read_only_before_queries(monkeypatch):
    pool = type(
        "Pool",
        (),
        {"fetchval": AsyncMock(return_value="kairos"), "close": AsyncMock()},
    )()
    create_pool = AsyncMock(return_value=pool)
    monkeypatch.setattr(database.asyncpg, "create_pool", create_pool)

    db = Database(_settings(), read_only=True)
    await db.connect()
    try:
        assert create_pool.await_args.kwargs["server_settings"] == {"default_transaction_read_only": "on"}
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_read_only_database_rejects_migrations_before_acquiring_a_connection():
    db = Database(_settings(), read_only=True)

    with pytest.raises(RuntimeError, match="cannot apply migrations"):
        await db.migrate()


@pytest.mark.asyncio
async def test_read_only_schema_verification_uses_exact_profile_without_writes():
    expected = Database.migration_names("runtime")
    connection = _Connection(expected)
    db = Database(_settings(), read_only=True)
    db._pool = _Pool(connection)

    await db.verify_schema()

    assert connection.queries == ["SELECT version FROM schema_migrations ORDER BY version"]


@pytest.mark.asyncio
async def test_read_only_schema_verification_fails_closed_on_migration_drift():
    connection = _Connection(("001_audit_and_idempotency.sql",))
    db = Database(_settings(), read_only=True)
    db._pool = _Pool(connection)

    with pytest.raises(RuntimeError, match="does not match the exact runtime profile"):
        await db.verify_schema()


@pytest.mark.asyncio
async def test_metrics_exporter_verifies_schema_without_migrating(monkeypatch):
    calls = []

    class FakeDatabase:
        def __init__(self, settings, *, read_only):
            assert read_only is True

        async def connect(self):
            calls.append("connect")

        async def verify_schema(self):
            calls.append("verify_schema")

        async def close(self):
            calls.append("close")

    class FakeServer:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def serve_forever(self):
            return None

    async def start_server(_handler, *, host, port):
        assert (host, port) == ("127.0.0.1", 0)
        return FakeServer()

    monkeypatch.setattr(metrics_exporter, "Database", FakeDatabase)
    monkeypatch.setattr(metrics_exporter.asyncio, "start_server", start_server)

    await metrics_exporter.run_exporter(host="127.0.0.1", port=0, redis_url="redis://localhost")

    assert calls == ["connect", "verify_schema", "close"]


@pytest.mark.asyncio
async def test_metrics_exporter_does_not_listen_when_schema_verification_fails(monkeypatch):
    calls = []

    class FakeDatabase:
        def __init__(self, settings, *, read_only):
            assert read_only is True

        async def connect(self):
            calls.append("connect")

        async def verify_schema(self):
            calls.append("verify_schema")
            raise RuntimeError("schema mismatch")

        async def close(self):
            calls.append("close")

    start_server = AsyncMock()
    monkeypatch.setattr(metrics_exporter, "Database", FakeDatabase)
    monkeypatch.setattr(metrics_exporter.asyncio, "start_server", start_server)

    with pytest.raises(RuntimeError, match="schema mismatch"):
        await metrics_exporter.run_exporter(host="127.0.0.1", port=0, redis_url="redis://localhost")

    assert calls == ["connect", "verify_schema", "close"]
    start_server.assert_not_awaited()
