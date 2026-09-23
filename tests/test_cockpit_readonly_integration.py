from __future__ import annotations

import os
import secrets
from urllib.parse import urlsplit
from uuid import uuid4

import asyncpg
import pytest

from kairos_persistence import Database, PersistenceSettings
from kairos_persistence.cockpit_snapshot import SYMBOLS, CockpitSnapshotRepository
from kairos_persistence.database_target import require_database_target_url

pytestmark = pytest.mark.integration
_EXPECTED_DATABASE = "kairos_cockpit_test_20260923"


@pytest.mark.asyncio
async def test_cockpit_runtime_role_is_read_only_on_an_explicit_disposable_database() -> None:
    database_url = os.getenv("KAIROS_COCKPIT_TEST_DATABASE_URL", "")
    database_name = os.getenv("KAIROS_COCKPIT_TEST_DATABASE_NAME", "")
    if not database_url or not database_name:
        pytest.skip("explicit Cockpit integration database is required")
    require_database_target_url(database_url, _EXPECTED_DATABASE, local_only=True)
    if database_name != _EXPECTED_DATABASE:
        pytest.fail("Cockpit integration is restricted to its exact disposable database")

    admin = Database(PersistenceSettings(database_url=database_url))
    await admin.connect()
    role = f"kairos_cockpit_reader_{uuid4().hex}"
    password = secrets.token_hex(32)
    reader: Database | None = None
    role_created = False
    try:
        await admin.migrate()
        async with admin.pool.acquire() as connection:
            await connection.execute(
                f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}' "
                "NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION"
            )
            role_created = True
            await connection.execute(f'GRANT CONNECT ON DATABASE "{_EXPECTED_DATABASE}" TO "{role}"')
            await connection.execute(f'GRANT USAGE ON SCHEMA public TO "{role}"')
            await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{role}"')

        parsed = urlsplit(database_url)
        authority = parsed.netloc.rsplit("@", maxsplit=1)[-1]
        reader_url = f"postgresql://{role}:{password}@{authority}/{_EXPECTED_DATABASE}"
        reader = Database(PersistenceSettings(database_url=reader_url), read_only=True)
        await reader.connect()
        await reader.verify_schema()
        repository = CockpitSnapshotRepository(reader)
        await repository.verify_read_only_access()
        snapshot = await repository.load_snapshot()

        assert [item["symbol"] for item in snapshot["markets"]] == list(SYMBOLS)
        assert snapshot["system"]["readiness"] == {
            "technical_paper_ready": False,
            "paper_qualified": False,
            "alpha_ready": False,
            "live_ready": False,
        }
        assert all(item["venue_quality"]["state"] == "UNAVAILABLE" for item in snapshot["markets"])

        async with reader.pool.acquire() as connection:
            with pytest.raises(asyncpg.PostgresError) as error:
                await connection.execute("CREATE TABLE cockpit_readonly_mutation_probe (id INTEGER)")
        assert error.value.sqlstate == "25006"
    finally:
        try:
            if reader is not None:
                await reader.close()
        finally:
            try:
                if role_created:
                    async with admin.pool.acquire() as connection:
                        await connection.execute(f'DROP OWNED BY "{role}"')
                        await connection.execute(f'DROP ROLE "{role}"')
            finally:
                await admin.close()
