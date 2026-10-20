"""Actual PostgreSQL claim race in one explicitly new disposable database only."""

import asyncio
import os
from urllib.parse import urlsplit

import pytest

from kairos_persistence import Database, MigrationProfile, NewsProcessingRepository, PersistenceSettings

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]
TEST_DATABASE = "kairos_news_test_20261020"


async def test_news_claim_race_restart_and_immutable_unknown_outcome():
    url = os.getenv("KAIROS_NEWS_TEST_DATABASE_URL")
    if not url or os.getenv("KAIROS_NEWS_TEST_DISPOSABLE_CONFIRM") != "CREATE_NEW_DISPOSABLE_NEWS_GATE":
        pytest.skip("explicit new disposable news database opt-in is required")
    parsed = urlsplit(url)
    if parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.path != f"/{TEST_DATABASE}":
        raise ValueError("news gate refuses any non-disposable target")
    database = Database(
        PersistenceSettings(database_url=url, command_timeout_s=5, pool_max_size=4),
        migration_profile=MigrationProfile.CONTROLLED_RUNTIME,
    )
    await database.connect()
    try:
        # Never adopt an existing schema or queue, even in a correctly named DB.
        assert await database.pool.fetchval("SELECT current_database()") == TEST_DATABASE
        assert (
            await database.pool.fetchval(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'"
            )
            == 0
        )
        await database.migrate()
        await database.verify_schema()
        repository = NewsProcessingRepository(database.pool)
        scope, event = "a" * 64, "b" * 64
        attempts = ("c" * 64, "d" * 64)
        outcomes = await asyncio.gather(
            *(repository.claim_batch(scope, (event,), attempt) for attempt in attempts)
        )
        assert sorted(outcomes) == [(), (event,)]
        winner = attempts[outcomes.index((event,))]
        loser = attempts[1 - outcomes.index((event,))]
        restarted = NewsProcessingRepository(database.pool)
        assert await restarted.claim_batch(scope, (event,), "e" * 64) == ()
        with pytest.raises(ValueError, match="exact owned"):
            await restarted.complete_batch(scope, (event,), loser)
        await restarted.complete_batch(scope, (event,), winner)
        await restarted.complete_batch(scope, (event,), winner)
        assert await restarted.claim_batch(scope, (event,), "f" * 64) == ()
        pending = "1" * 64
        assert await repository.claim_batch(scope, (pending,), "2" * 64) == (pending,)
        # A new process has no permission to reclaim an interrupted provider call.
        assert await NewsProcessingRepository(database.pool).claim_batch(scope, (pending,), "3" * 64) == ()
        for sql in (
            "DELETE FROM news_processing",
            "UPDATE news_processing SET attempt_id=repeat('4',64)",
            "UPDATE news_processing SET state='CLAIMED',completed_at=NULL WHERE state='COMPLETED'",
        ):
            with pytest.raises(Exception, match="immutable|cannot be deleted|only completion"):
                await database.pool.execute(sql)
        assert await database.pool.fetchval("SELECT count(*) FROM news_processing") == 2
        assert await database.pool.fetchval("SELECT count(*) FROM message_outbox") == 0
        assert await database.pool.fetchval("SELECT count(*) FROM message_inbox") == 0
    finally:
        await database.close()
