"""Small explicit asyncpg lifecycle wrapper; no hidden ORM behavior."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from enum import StrEnum
from pathlib import Path
from urllib.parse import urlsplit

import asyncpg

from .config import PersistenceSettings


class MigrationProfile(StrEnum):
    """Explicit database topology selected before any DDL is considered."""

    RUNTIME = "runtime"
    SIMULATOR = "simulator"


# Migration 017 owns only the isolated ``kairos-sim`` journal.  It must never
# leak into the PAPER/runtime database merely because both deployments share
# the persistence package.  The order below is intentionally a manifest, not
# a glob: runtime applies 018 after 016 without silently opting into 017.
_RUNTIME_MIGRATIONS = (
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
    "013_campaign_source_budgets.sql",
    "014_bounded_canary_sessions.sql",
    "015_canary_dispatch_claims.sql",
    "016_global_canary_session_guard.sql",
    "018_offline_outbox_reconciliation.sql",
)
_SIMULATOR_MIGRATIONS = _RUNTIME_MIGRATIONS[:-1] + (
    "017_simulator_journal.sql",
    _RUNTIME_MIGRATIONS[-1],
)
_MIGRATION_MANIFESTS: dict[MigrationProfile, tuple[str, ...]] = {
    MigrationProfile.RUNTIME: _RUNTIME_MIGRATIONS,
    MigrationProfile.SIMULATOR: _SIMULATOR_MIGRATIONS,
}
_KNOWN_MIGRATIONS = frozenset(_SIMULATOR_MIGRATIONS)
_DATABASE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,62}\Z")
_SIMULATOR_DATABASE_NAME = re.compile(r"kairos_sim(?:_[A-Za-z0-9][A-Za-z0-9_-]{0,52})?\Z")


class Database:
    def __init__(
        self,
        settings: PersistenceSettings | None = None,
        *,
        migration_profile: MigrationProfile | str = MigrationProfile.RUNTIME,
    ) -> None:
        self.settings = settings or PersistenceSettings()
        try:
            self.migration_profile = MigrationProfile(migration_profile)
        except ValueError as exc:
            raise ValueError("database migration_profile must be runtime or simulator") from exc
        self.database_name = self.require_profile_database_url(
            self.migration_profile, self.settings.database_url
        )
        self._pool: asyncpg.Pool | None = None

    @classmethod
    def migration_names(cls, profile: MigrationProfile | str) -> tuple[str, ...]:
        """Return the reviewed immutable manifest for one database topology."""

        try:
            return _MIGRATION_MANIFESTS[MigrationProfile(profile)]
        except ValueError as exc:
            raise ValueError("database migration_profile must be runtime or simulator") from exc

    @classmethod
    def require_profile_database_name(cls, profile: MigrationProfile | str, database_name: str) -> str:
        """Require a physical database name compatible with one topology.

        ``SIMULATOR`` is not merely a migration ordering switch: it may only
        exist in an explicitly named ``kairos_sim`` database.  Conversely,
        normal runtime/PAPER callers cannot accidentally migrate that isolated
        database through their safe default profile.
        """

        try:
            selected = MigrationProfile(profile)
        except ValueError as exc:
            raise ValueError("database migration_profile must be runtime or simulator") from exc
        if not isinstance(database_name, str) or not _DATABASE_NAME.fullmatch(database_name):
            raise ValueError("database migration target must be a simple explicit database name")
        is_simulator = _SIMULATOR_DATABASE_NAME.fullmatch(database_name) is not None
        if selected is MigrationProfile.SIMULATOR and not is_simulator:
            raise ValueError("simulator migration_profile requires an explicit kairos_sim database")
        if selected is MigrationProfile.RUNTIME and is_simulator:
            raise ValueError("runtime migration_profile cannot target an isolated kairos_sim database")
        return database_name

    @classmethod
    def require_profile_database_url(cls, profile: MigrationProfile | str, database_url: str) -> str:
        """Extract and validate the literal database target without connecting."""

        if not isinstance(database_url, str) or not database_url.isascii():
            raise ValueError("database migration target URL must be ASCII")
        try:
            parsed = urlsplit(database_url)
        except ValueError:
            raise ValueError("database migration target URL is malformed") from None
        database_name = parsed.path.removeprefix("/")
        if parsed.path != f"/{database_name}":
            raise ValueError("database migration target URL must name one explicit database")
        return cls.require_profile_database_name(profile, database_name)

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("database is not connected")
        return self._pool

    async def connect(self) -> None:
        if self._pool is not None:
            return
        pool = await asyncpg.create_pool(
            dsn=self.settings.database_url,
            min_size=self.settings.pool_min_size,
            max_size=self.settings.pool_max_size,
            command_timeout=self.settings.command_timeout_s,
        )
        try:
            current_database = await pool.fetchval("SELECT current_database()")
            if current_database != self.database_name:
                raise RuntimeError("connected database does not match the explicit migration target")
        except BaseException:
            await pool.close()
            raise
        self._pool = pool

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[asyncpg.Connection]:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                yield connection

    async def migrate(self) -> None:
        """Apply only the immutable migrations allowed by this topology."""

        migration_paths = {
            path.name: path for path in Path(__file__).with_name("migrations").glob("[0-9][0-9][0-9]_*.sql")
        }
        if set(migration_paths) != _KNOWN_MIGRATIONS:
            raise RuntimeError("persistence migration inventory is not assigned to an explicit profile")
        expected = self.migration_names(self.migration_profile)
        # Every application service starts independently.  A transaction-scoped
        # advisory lock prevents concurrent containers from racing the same DDL.
        async with self.transaction() as connection:
            await connection.execute("SELECT pg_advisory_xact_lock($1)", 4_907_627_681_104_115_019)
            await connection.execute(
                """CREATE TABLE IF NOT EXISTS schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )"""
            )
            applied_rows = await connection.fetch("SELECT version FROM schema_migrations ORDER BY version")
            applied = tuple(str(row["version"]) for row in applied_rows)
            if applied != expected[: len(applied)]:
                raise RuntimeError(
                    "database migration history is incompatible with the "
                    f"{self.migration_profile.value} profile"
                )
            for name in expected:
                if name in applied:
                    continue
                path = migration_paths[name]
                await connection.execute(path.read_text(encoding="utf-8"))
                await connection.execute("INSERT INTO schema_migrations(version) VALUES ($1)", name)
            final_rows = await connection.fetch("SELECT version FROM schema_migrations ORDER BY version")
            if tuple(str(row["version"]) for row in final_rows) != expected:
                raise RuntimeError(
                    "database migration history changed outside the selected "
                    f"{self.migration_profile.value} profile"
                )
