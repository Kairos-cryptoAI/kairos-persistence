"""Hermetic target-guard tests; no real database or adoption is performed."""

from unittest.mock import AsyncMock, Mock, call

import pytest

from kairos_persistence import Database, PersistenceSettings
from kairos_persistence import campaign_adoption as adoption
from kairos_persistence.database_target import (
    DatabaseTargetError,
    connect_verified_database,
    require_database_target_url,
)
from scripts import campaign_budget_integration as drill

NAME = "kairos_budget_test_202609120001"
DSN = f"postgresql://test:test-only@timescaledb:5432/{NAME}"


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "[::1]", "timescaledb"])
def test_explicit_local_target_is_supported(host):
    url = f"postgresql://test:password%40%3F%23@{host}:5432/{NAME}"
    assert require_database_target_url(url, NAME, local_only=True) == NAME


def test_adoption_permits_named_remote_shadow_but_drill_does_not():
    url = "postgresql://test:password@shadow-db.example:5432/kairos-shadow"
    assert require_database_target_url(url, "kairos-shadow") == "kairos-shadow"
    with pytest.raises(DatabaseTargetError, match="local"):
        require_database_target_url(url, "kairos-shadow", local_only=True)


@pytest.mark.parametrize("name", [None, "", "../kairos", "kairos?dbname=other", "a" * 64, "db\n"])
def test_requires_a_simple_explicit_database_name(name):
    with pytest.raises(DatabaseTargetError):
        require_database_target_url(DSN, name)


@pytest.mark.parametrize(
    "url",
    [
        DSN.replace(NAME, "kairos"),
        DSN + "?dbname=kairos",
        DSN + "?database=kairos",
        DSN + "?host=live.example",
        DSN + "?options=-csearch_path=public",
        DSN + "?sslmode=require",
        DSN + "?",
        DSN + "#kairos",
        DSN + "#",
        DSN + "/",
        DSN + "/../kairos",
        DSN.replace(NAME, NAME.replace("kairos", "%6bairos")),
        DSN.replace(NAME, NAME.replace("kairos", "%256bairos")),
        DSN + "%2f..%2fkairos",
        DSN + "%00",
        DSN.replace(NAME, "/" + NAME),
        DSN.replace(NAME, "../" + NAME),
        DSN.replace("timescaledb", "%74imescaledb"),
        DSN.replace("timescaledb", "localhost@timescaledb"),
        DSN.replace("timescaledb", "time\\scaledb"),
        DSN.replace("test-only", "пароль"),
        DSN.replace("test-only", "secret%GG"),
        DSN.replace("test-only", "secret%"),
        DSN.replace("test-only", "secret\n"),
        DSN.replace("timescaledb", "time\tscaledb"),
        DSN + "\n",
        " " + DSN,
        DSN.replace(":5432", ""),
        DSN.replace("5432", "65536"),
        DSN.replace("5432", "0"),
        DSN.replace("5432", "invalid"),
        DSN.replace("postgresql", "mysql"),
        f"postgresql://[::1:5432/{NAME}",
        f"postgresql://[localhost]:5432/{NAME}",
        f"postgresql:///{NAME}",
        f"dbname={NAME} host=localhost",
        "",
    ],
)
def test_overrides_encoded_targets_and_malformed_urls_are_rejected(url):
    with pytest.raises(DatabaseTargetError):
        require_database_target_url(url, NAME)


def _database(url=DSN, server_name=NAME):
    database = Mock(spec=Database)
    database.settings = PersistenceSettings(database_url=url)
    database.connect = AsyncMock()
    database.close = AsyncMock()
    database.migrate = AsyncMock()
    database.pool = Mock()
    database.pool.fetchval = AsyncMock(return_value=server_name)
    return database


async def test_bad_url_never_connects_or_prints_credentials():
    database = _database(url=DSN.replace("test-only", "private-password") + "?dbname=kairos")
    with pytest.raises(DatabaseTargetError) as caught:
        await connect_verified_database(database, NAME)
    assert "private-password" not in str(caught.value)
    assert "postgresql://" not in str(caught.value)
    database.connect.assert_not_awaited()
    database.migrate.assert_not_awaited()
    database.pool.fetchval.assert_not_awaited()


async def test_helper_only_connects_and_checks_identity_never_migrates():
    database = _database()
    await connect_verified_database(database, NAME, local_only=True)
    assert database.mock_calls == [call.connect(), call.pool.fetchval("SELECT current_database()")]


async def test_adoption_checks_server_identity_before_register(monkeypatch):
    database = _database(server_name="kairos")
    repository = Mock(side_effect=AssertionError("must not create a write repository"))
    monkeypatch.setattr(adoption, "SourceStateRepository", repository)
    with pytest.raises(DatabaseTargetError, match="server database"):
        await adoption.adopt({}, "0" * 64, database=database, expected_database_name=NAME)
    repository.assert_not_called()
    database.migrate.assert_not_awaited()
    assert database.mock_calls == [
        call.connect(),
        call.pool.fetchval("SELECT current_database()"),
        call.close(),
    ]


async def test_drill_checks_server_identity_before_any_migration_or_write(monkeypatch):
    monkeypatch.setenv("KAIROS_BUDGET_TEST_DATABASE_URL", DSN)
    monkeypatch.setenv("KAIROS_BUDGET_TEST_DATABASE_NAME", NAME)
    database = _database(server_name="kairos")
    monkeypatch.setattr(drill, "Database", lambda settings: database)
    with pytest.raises(DatabaseTargetError, match="server database"):
        await drill.run()
    database.migrate.assert_not_awaited()
    assert database.mock_calls == [
        call.connect(),
        call.pool.fetchval("SELECT current_database()"),
        call.close(),
    ]


@pytest.mark.parametrize("name", ["", "kairos", "kairos_budget_test_", "different_test", NAME + "\n"])
def test_drill_requires_separate_explicit_disposable_confirmation(monkeypatch, name):
    monkeypatch.setenv("KAIROS_BUDGET_TEST_DATABASE_URL", DSN)
    monkeypatch.setenv("KAIROS_BUDGET_TEST_DATABASE_NAME", name)
    with pytest.raises(ValueError):
        drill.test_settings()


def test_drill_literal_target_must_equal_separate_confirmation(monkeypatch):
    monkeypatch.setenv("KAIROS_BUDGET_TEST_DATABASE_URL", DSN)
    monkeypatch.setenv("KAIROS_BUDGET_TEST_DATABASE_NAME", "kairos_budget_test_other")
    with pytest.raises(DatabaseTargetError):
        drill.test_settings()


def test_drill_accepts_its_exact_confirmed_local_target(monkeypatch):
    monkeypatch.setenv("KAIROS_BUDGET_TEST_DATABASE_URL", DSN)
    monkeypatch.setenv("KAIROS_BUDGET_TEST_DATABASE_NAME", NAME)
    assert drill.test_settings().database_url == DSN
