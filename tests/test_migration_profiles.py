"""Topology manifests for persistence migrations must remain explicit."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from kairos_persistence import Database, MigrationProfile, PersistenceSettings


def _settings(database_name: str = "kairos_test") -> PersistenceSettings:
    return PersistenceSettings(database_url=f"postgresql://kairos:test@localhost:5432/{database_name}")


class _MigrationConnection:
    def __init__(self, applied: tuple[str, ...] = ()) -> None:
        self.applied = set(applied)
        self.executed: list[str] = []

    async def execute(self, sql: str, *parameters: object) -> str:
        self.executed.append(sql)
        if sql.startswith("INSERT INTO schema_migrations"):
            self.applied.add(str(parameters[0]))
        return "OK"

    async def fetch(self, sql: str) -> list[dict[str, str]]:
        assert sql == "SELECT version FROM schema_migrations ORDER BY version"
        return [{"version": name} for name in sorted(self.applied)]


class _MigrationDatabase(Database):
    def __init__(self, connection: _MigrationConnection, *, migration_profile: MigrationProfile) -> None:
        database_name = (
            "kairos_sim_migration_test" if migration_profile is MigrationProfile.SIMULATOR else "kairos_test"
        )
        super().__init__(_settings(database_name), migration_profile=migration_profile)
        self.connection = connection

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[_MigrationConnection]:
        yield self.connection


def test_runtime_profile_skips_the_simulator_only_journal_but_includes_outbox_quarantine() -> None:
    names = Database.migration_names(MigrationProfile.RUNTIME)

    assert names[-1] == "018_offline_outbox_reconciliation.sql"
    assert "017_simulator_journal.sql" not in names
    assert names == tuple(sorted(names[:16])) + ("018_offline_outbox_reconciliation.sql",)


def test_simulator_profile_is_the_only_profile_that_owns_simulator_migrations() -> None:
    names = Database.migration_names(MigrationProfile.SIMULATOR)

    assert names[-8:] == (
        "018_offline_outbox_reconciliation.sql",
        "019_simulator_book_frame_v2.sql",
        "020_simulator_llm_proposals.sql",
        "021_simulator_research_decision_samples.sql",
        "022_simulator_research_observation_schedule.sql",
        "023_simulator_research_baseline_lineage.sql",
        "024_simulator_adaptive_candidate_protocol.sql",
        "025_simulator_research_evidence.sql",
    )
    assert names[-9] == "017_simulator_journal.sql"
    assert set(Database.migration_names(MigrationProfile.RUNTIME)).issubset(set(names))


def test_database_defaults_to_the_fail_closed_runtime_profile() -> None:
    database = Database(_settings())

    assert database.migration_profile is MigrationProfile.RUNTIME


def test_simulator_profile_requires_a_dedicated_physical_database_before_connecting() -> None:
    with pytest.raises(ValueError, match="kairos_sim"):
        Database(_settings("kairos"), migration_profile=MigrationProfile.SIMULATOR)

    database = Database(_settings("kairos_sim_test_20260920"), migration_profile=MigrationProfile.SIMULATOR)

    assert database.database_name == "kairos_sim_test_20260920"


def test_runtime_profile_cannot_target_an_isolated_simulator_database_before_connecting() -> None:
    with pytest.raises(ValueError, match="cannot target"):
        Database(_settings("kairos_sim_test_20260920"), migration_profile=MigrationProfile.RUNTIME)


@pytest.mark.asyncio
async def test_runtime_migrator_never_executes_the_simulator_journal() -> None:
    connection = _MigrationConnection()
    database = _MigrationDatabase(connection, migration_profile=MigrationProfile.RUNTIME)

    await database.migrate()

    assert "017_simulator_journal.sql" not in connection.applied
    assert "018_offline_outbox_reconciliation.sql" in connection.applied
    assert not any("sim_tapes" in sql for sql in connection.executed)


@pytest.mark.asyncio
async def test_runtime_migrator_rejects_an_accidentally_mixed_simulator_history() -> None:
    connection = _MigrationConnection(("017_simulator_journal.sql",))
    database = _MigrationDatabase(connection, migration_profile=MigrationProfile.RUNTIME)

    with pytest.raises(RuntimeError, match="incompatible"):
        await database.migrate()

    assert not any(sql.startswith("INSERT INTO schema_migrations") for sql in connection.executed)


@pytest.mark.asyncio
async def test_simulator_migrator_rejects_a_runtime_history_that_already_skipped_its_journal() -> None:
    connection = _MigrationConnection(Database.migration_names(MigrationProfile.RUNTIME))
    database = _MigrationDatabase(connection, migration_profile=MigrationProfile.SIMULATOR)

    with pytest.raises(RuntimeError, match="incompatible"):
        await database.migrate()

    assert "017_simulator_journal.sql" not in connection.applied
    assert not any(sql.startswith("INSERT INTO schema_migrations") for sql in connection.executed)


@pytest.mark.asyncio
async def test_simulator_migrator_advances_an_existing_023_history_without_replaying_it() -> None:
    names = Database.migration_names(MigrationProfile.SIMULATOR)
    connection = _MigrationConnection(names[:-2])
    database = _MigrationDatabase(connection, migration_profile=MigrationProfile.SIMULATOR)

    await database.migrate()

    assert tuple(sorted(connection.applied)) == tuple(sorted(names))
    assert sum(sql.startswith("INSERT INTO schema_migrations") for sql in connection.executed) == 2
    assert any(
        sql.startswith("-- SIM-only forward migration")
        and "CREATE OR REPLACE FUNCTION simulator_guard_scheduled_research_sample()" in sql
        and "sim_research_adaptive_candidate_protocols" in sql
        for sql in connection.executed
    )
    assert not any("CREATE TABLE sim_research_observation_schedules" in sql for sql in connection.executed)


@pytest.mark.asyncio
async def test_migrator_rejects_a_history_with_a_skipped_middle_migration() -> None:
    runtime = Database.migration_names(MigrationProfile.RUNTIME)
    connection = _MigrationConnection(runtime[:12] + runtime[13:14])
    database = _MigrationDatabase(connection, migration_profile=MigrationProfile.RUNTIME)

    with pytest.raises(RuntimeError, match="incompatible"):
        await database.migrate()

    assert not any(sql.startswith("INSERT INTO schema_migrations") for sql in connection.executed)


@pytest.mark.parametrize("value", ("", "paper", "live", "SIMULATOR", object()))
def test_unknown_migration_profile_is_rejected_before_connecting(value: object) -> None:
    with pytest.raises(ValueError, match="migration_profile"):
        Database(_settings(), migration_profile=value)  # type: ignore[arg-type]
