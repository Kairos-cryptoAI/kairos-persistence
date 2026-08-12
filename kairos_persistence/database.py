"""Small explicit asyncpg lifecycle wrapper; no hidden ORM behavior."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg

from .config import PersistenceSettings


class Database:
    def __init__(self, settings: PersistenceSettings | None = None) -> None:
        self.settings = settings or PersistenceSettings()
        self._pool: asyncpg.Pool | None = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("database is not connected")
        return self._pool

    async def connect(self) -> None:
        if self._pool is not None:
            return
        self._pool = await asyncpg.create_pool(
            dsn=self.settings.database_url,
            min_size=self.settings.pool_min_size,
            max_size=self.settings.pool_max_size,
            command_timeout=self.settings.command_timeout_s,
        )

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
        """Apply immutable numbered SQL migrations exactly once."""
        migrations = Path(__file__).with_name("migrations")
        async with self.transaction() as connection:
            await connection.execute(
                """CREATE TABLE IF NOT EXISTS schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )"""
            )
        for path in sorted(migrations.glob("[0-9][0-9][0-9]_*.sql")):
            async with self.transaction() as connection:
                applied = await connection.fetchval(
                    "SELECT 1 FROM schema_migrations WHERE version = $1", path.name
                )
                if applied:
                    continue
                await connection.execute(path.read_text(encoding="utf-8"))
                await connection.execute("INSERT INTO schema_migrations(version) VALUES ($1)", path.name)
