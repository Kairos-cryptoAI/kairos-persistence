from contextlib import AbstractAsyncContextManager
from unittest.mock import AsyncMock

import pytest

from kairos_persistence import Database, MigrationProfile, NewsProcessingRepository

SCOPE, FIRST, SECOND, ATTEMPT = "a" * 64, "b" * 64, "c" * 64, "d" * 64


class Context(AbstractAsyncContextManager):
    def __init__(self, value=None):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *args):
        return None


def repository():
    connection = AsyncMock()
    connection.transaction = lambda: Context()
    pool = AsyncMock()
    pool.acquire = lambda: Context(connection)
    return NewsProcessingRepository(pool), connection


def row(key, *, state="CLAIMED", attempt_id=ATTEMPT):
    return {"event_key": key, "state": state, "attempt_id": attempt_id}


def test_news_migration_is_explicit_controlled_only():
    name = "028_news_processing.sql"
    for profile in MigrationProfile:
        assert (name in Database.migration_names(profile)) == (profile is MigrationProfile.CONTROLLED_RUNTIME)


@pytest.mark.asyncio
async def test_claim_only_returns_newly_inserted_items_in_sorted_order():
    repo, connection = repository()
    connection.fetch.return_value = [row(SECOND)]
    assert await repo.claim_batch(SCOPE, (SECOND, FIRST), ATTEMPT) == (SECOND,)
    sql, scope, keys, attempt = connection.fetch.call_args.args
    assert "ON CONFLICT (scope_key,event_key) DO NOTHING" in sql
    assert (scope, keys, attempt) == (SCOPE, [FIRST, SECOND], ATTEMPT)


@pytest.mark.asyncio
async def test_existing_claim_and_completed_items_are_never_reclaimed():
    repo, connection = repository()
    connection.fetch.return_value = []
    assert await repo.claim_batch(SCOPE, (FIRST,), ATTEMPT) == ()
    assert "UPDATE" not in connection.fetch.call_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("keys", [("raw headline",), (FIRST, FIRST), (True,), [FIRST], (FIRST,) * 1001])
async def test_malformed_batch_refused_before_database(keys):
    repo, connection = repository()
    with pytest.raises(ValueError):
        await repo.claim_batch(SCOPE, keys, ATTEMPT)
    connection.fetch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["", "A" * 64, "a" * 63, True, None])
async def test_invalid_scope_and_attempt_refused_before_database(value):
    repo, connection = repository()
    with pytest.raises(ValueError):
        await repo.claim_batch(value, (FIRST,), ATTEMPT)
    with pytest.raises(ValueError):
        await repo.claim_batch(SCOPE, (FIRST,), value)
    connection.fetch.assert_not_called()


@pytest.mark.asyncio
async def test_empty_batch_never_contacts_database():
    repo, connection = repository()
    assert await repo.claim_batch(SCOPE, (), ATTEMPT) == ()
    await repo.complete_batch(SCOPE, (), ATTEMPT)
    connection.fetch.assert_not_called()


@pytest.mark.asyncio
async def test_completion_requires_owned_claims_then_exact_update_result():
    repo, connection = repository()
    connection.fetch.side_effect = [[row(FIRST), row(SECOND)], [row(SECOND), row(FIRST)]]
    await repo.complete_batch(SCOPE, (SECOND, FIRST), ATTEMPT)
    assert "FOR UPDATE" in connection.fetch.call_args_list[0].args[0]
    assert connection.fetch.call_args.args[1:] == (SCOPE, [FIRST, SECOND], ATTEMPT)


@pytest.mark.asyncio
async def test_repeating_completion_for_same_owner_is_idempotent():
    repo, connection = repository()
    connection.fetch.return_value = [row(FIRST, state="COMPLETED")]
    await repo.complete_batch(SCOPE, (FIRST,), ATTEMPT)
    assert connection.fetch.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [[], [row(FIRST, attempt_id="e" * 64)], [row(FIRST, state="RETRY")]])
async def test_absent_foreign_or_invalid_claim_cannot_be_completed(rows):
    repo, connection = repository()
    connection.fetch.return_value = rows
    with pytest.raises(ValueError, match="exact owned"):
        await repo.complete_batch(SCOPE, (FIRST,), ATTEMPT)
    assert connection.fetch.await_count == 1


@pytest.mark.asyncio
async def test_unconfirmed_completion_does_not_become_success():
    repo, connection = repository()
    connection.fetch.side_effect = [[row(FIRST)], []]
    with pytest.raises(RuntimeError, match="not confirmed"):
        await repo.complete_batch(SCOPE, (FIRST,), ATTEMPT)


@pytest.mark.asyncio
async def test_unconfirmed_claim_propagates_without_automatic_retry():
    repo, connection = repository()
    connection.fetch.side_effect = TimeoutError
    with pytest.raises(TimeoutError):
        await repo.claim_batch(SCOPE, (FIRST,), ATTEMPT)
    assert connection.fetch.await_count == 1
