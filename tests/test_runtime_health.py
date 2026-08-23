from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kairos_persistence import ExecutionRuntimeHealth, ExecutionRuntimeHealthRepository


def _health(**updates: object) -> ExecutionRuntimeHealth:
    values: dict[str, object] = {
        "environment": "paper",
        "account_id": "remote-dev-account",
        "exchange": "evedex",
        "observed_at": datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
        "auth_age_ms": 10_000,
        "auth_expires_in_ms": 290_000,
        "local_mutation_reserve": 28,
        "local_mutation_capacity": 30,
        "local_mutation_compensation_reserve": 4,
        "local_mutation_window_ms": 60_000,
        "venue_rate_limit_observable": False,
        "venue_rate_limit_reserve": None,
    }
    values.update(updates)
    return ExecutionRuntimeHealth(**values)  # type: ignore[arg-type]


def test_runtime_health_rejects_invalid_or_secret_like_state() -> None:
    _health().validate()
    with pytest.raises(ValueError, match="cannot exceed"):
        _health(local_mutation_reserve=31).validate()
    with pytest.raises(ValueError, match="timezone-aware"):
        _health(observed_at=datetime(2026, 8, 23, 12, 0)).validate()
    with pytest.raises(ValueError, match="non-negative"):
        _health(auth_expires_in_ms=-1).validate()
    with pytest.raises(ValueError, match="boolean"):
        _health(venue_rate_limit_observable=1).validate()


class _Pool:
    def __init__(self, row=None, status: str = "INSERT 0 1") -> None:
        self.row = row
        self.status = status
        self.execute_args = None

    async def execute(self, *args):
        self.execute_args = args
        return self.status

    async def fetchrow(self, *_args):
        return self.row


@pytest.mark.asyncio
async def test_runtime_health_upsert_is_monotonic_and_round_trips() -> None:
    health = _health()
    pool = _Pool(row=health.__dict__ if hasattr(health, "__dict__") else None)
    repository = ExecutionRuntimeHealthRepository(pool)
    assert await repository.record(health)
    assert "EXCLUDED.observed_at >=" in pool.execute_args[0]

    row = {
        field: getattr(health, field)
        for field in (
            "environment",
            "account_id",
            "exchange",
            "observed_at",
            "auth_age_ms",
            "auth_expires_in_ms",
            "local_mutation_reserve",
            "local_mutation_capacity",
            "local_mutation_compensation_reserve",
            "local_mutation_window_ms",
            "venue_rate_limit_observable",
            "venue_rate_limit_reserve",
        )
    }
    pool.row = row
    assert (
        await repository.latest(
            environment="paper",
            account_id="remote-dev-account",
            exchange="evedex",
        )
        == health
    )
