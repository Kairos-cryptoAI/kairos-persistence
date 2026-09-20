"""Apply migrations once and close the pool in the same event loop."""

from __future__ import annotations

import asyncio

from kairos_persistence import Database, MigrationProfile


async def main() -> None:
    database = Database()
    await database.connect()
    try:
        await database.migrate()
        applied = tuple(
            str(row["version"])
            for row in await database.pool.fetch("SELECT version FROM schema_migrations ORDER BY version")
        )
        if applied != Database.migration_names(MigrationProfile.RUNTIME):
            raise RuntimeError("runtime migration smoke found a mixed or incomplete topology profile")
        if await database.pool.fetchval("SELECT to_regclass('public.sim_tapes')") is not None:
            raise RuntimeError("runtime migration smoke found simulator journal tables")
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
