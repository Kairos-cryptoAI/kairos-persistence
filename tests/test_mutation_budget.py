from __future__ import annotations

import os

import pytest

from kairos_persistence import (
    Database,
    ExecutionMutationBudgetRepository,
    PersistenceSettings,
)


@pytest.mark.asyncio
async def test_mutation_budget_rejects_weaker_than_safe_configuration() -> None:
    repository = ExecutionMutationBudgetRepository(None)  # type: ignore[arg-type]
    kwargs = {
        "environment": "evedex-dev",
        "account_id": "paper-test",
        "exchange": "evedex",
        "effect_id": "effect-1",
        "operation": "place-entry",
        "compensation": False,
    }
    with pytest.raises(ValueError, match=r"\[1, 30\]"):
        await repository.reserve(**kwargs, capacity=31)
    with pytest.raises(ValueError, match="at least 60000"):
        await repository.reserve(**kwargs, window_ms=59_999)
    with pytest.raises(ValueError, match="at least 4"):
        await repository.reserve(**kwargs, compensation_reserve=3)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mutation_budget_is_durable_idempotent_and_holds_compensation_slots() -> None:
    database_url = os.getenv("KAIROS_PERSISTENCE_DATABASE_URL")
    if not database_url:
        pytest.skip("KAIROS_PERSISTENCE_DATABASE_URL is required for integration tests")
    database = Database(PersistenceSettings(database_url=database_url))
    await database.connect()
    await database.migrate()
    repository = ExecutionMutationBudgetRepository(database.pool)
    scope = {
        "environment": "evedex-dev",
        "account_id": "paper-mutation-budget-test",
        "exchange": "evedex",
        "capacity": 5,
        "window_ms": 60_000,
        "compensation_reserve": 4,
    }
    try:
        await database.pool.execute(
            """DELETE FROM execution_mutation_reservations
                WHERE environment=$1 AND account_id=$2 AND exchange=$3""",
            scope["environment"],
            scope["account_id"],
            scope["exchange"],
        )
        await database.pool.execute(
            """DELETE FROM execution_mutation_budget_scopes
                WHERE environment=$1 AND account_id=$2 AND exchange=$3""",
            scope["environment"],
            scope["account_id"],
            scope["exchange"],
        )

        first = await repository.reserve(
            **scope,
            effect_id="entry-1",
            operation="place-entry",
            compensation=False,
        )
        assert first.granted and not first.replay and first.remaining == 4
        replay = await repository.reserve(
            **scope,
            effect_id="entry-1",
            operation="place-entry",
            compensation=False,
        )
        assert replay.granted and replay.replay and replay.occurred_at == first.occurred_at
        denied = await repository.reserve(
            **scope,
            effect_id="entry-2",
            operation="place-entry",
            compensation=False,
        )
        assert not denied.granted and denied.remaining == 4

        for index, expected_remaining in enumerate((3, 2, 1, 0), start=1):
            compensation = await repository.reserve(
                **scope,
                effect_id=f"close-{index}",
                operation="emergency-close",
                compensation=True,
            )
            assert compensation.granted and compensation.remaining == expected_remaining
        exhausted = await repository.reserve(
            **scope,
            effect_id="close-5",
            operation="emergency-close",
            compensation=True,
        )
        assert not exhausted.granted and exhausted.remaining == 0
        with pytest.raises(ValueError, match="different mutation semantics"):
            await repository.reserve(
                **scope,
                effect_id="entry-1",
                operation="cancel-entry",
                compensation=False,
            )
    finally:
        await database.pool.execute(
            """DELETE FROM execution_mutation_reservations
                WHERE environment=$1 AND account_id=$2 AND exchange=$3""",
            scope["environment"],
            scope["account_id"],
            scope["exchange"],
        )
        await database.pool.execute(
            """DELETE FROM execution_mutation_budget_scopes
                WHERE environment=$1 AND account_id=$2 AND exchange=$3""",
            scope["environment"],
            scope["account_id"],
            scope["exchange"],
        )
        await database.close()
