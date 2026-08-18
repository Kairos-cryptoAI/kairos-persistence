from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from datetime import UTC, date, datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest

from kairos_persistence import SourceBudgetExceeded, SourceStateRepository, UsageStatus


class _Context(AbstractAsyncContextManager[Any]):
    def __init__(self, value: Any = None) -> None:
        self.value = value

    async def __aenter__(self) -> Any:
        return self.value

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _Pool:
    def __init__(self, connection: Any) -> None:
        self.connection = connection
        self.fetchrow = AsyncMock()

    def acquire(self) -> _Context:
        return _Context(self.connection)


def _reservation_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "service": "text-scouts",
        "source": "x-api",
        "reservation_id": "request-1",
        "billing_month": date(2026, 8, 1),
        "reserved_units": 10,
        "unit_cost_microusd": 5_000,
        "status": "RESERVED",
        "actual_units": None,
    }
    row.update(overrides)
    return row


@pytest.mark.asyncio
async def test_reservation_is_budgeted_before_a_paid_request() -> None:
    connection = AsyncMock()
    connection.transaction = lambda: _Context()
    connection.fetchrow.side_effect = [None, _reservation_row()]
    connection.fetchval.return_value = 0
    repository = SourceStateRepository(_Pool(connection))  # type: ignore[arg-type]

    reservation = await repository.reserve_usage(
        service="text-scouts",
        source="x-api",
        reservation_id="request-1",
        reserved_units=10,
        unit_cost_microusd=5_000,
        monthly_budget_microusd=10_000_000,
        requested_at=datetime(2026, 8, 18, tzinfo=UTC),
    )

    assert reservation.status is UsageStatus.RESERVED
    assert reservation.reserved_cost_microusd == 50_000
    assert connection.execute.await_count == 1
    assert connection.fetchrow.await_count == 2


@pytest.mark.asyncio
async def test_reservation_fails_closed_at_monthly_budget() -> None:
    connection = AsyncMock()
    connection.transaction = lambda: _Context()
    connection.fetchrow.return_value = None
    connection.fetchval.return_value = 9_980_000
    repository = SourceStateRepository(_Pool(connection))  # type: ignore[arg-type]

    with pytest.raises(SourceBudgetExceeded, match="budget"):
        await repository.reserve_usage(
            service="text-scouts",
            source="x-api",
            reservation_id="request-2",
            reserved_units=10,
            unit_cost_microusd=5_000,
            monthly_budget_microusd=10_000_000,
            requested_at=datetime(2026, 8, 18, tzinfo=UTC),
        )

    assert connection.fetchrow.await_count == 1


@pytest.mark.asyncio
async def test_cursor_and_usage_inputs_are_strict_and_canonical() -> None:
    repository = SourceStateRepository(_Pool(AsyncMock()))  # type: ignore[arg-type]

    for value in ("", "01", "-1", "1.0", "１２", str(2**64)):
        with pytest.raises(ValueError):
            await repository.advance_cursor("text-scouts", "x-api", "account", value)

    with pytest.raises(ValueError, match="timezone-aware"):
        await repository.reserve_usage(
            service="text-scouts",
            source="x-api",
            reservation_id="request-3",
            reserved_units=1,
            unit_cost_microusd=5_000,
            monthly_budget_microusd=10_000,
            requested_at=datetime(2026, 8, 18),
        )
    with pytest.raises(ValueError, match="positive integer"):
        await repository.reserve_usage(
            service="text-scouts",
            source="x-api",
            reservation_id="request-4",
            reserved_units=True,
            unit_cost_microusd=5_000,
            monthly_budget_microusd=10_000,
        )


@pytest.mark.asyncio
async def test_monthly_usage_keeps_reserved_and_committed_cost_separate() -> None:
    pool = _Pool(AsyncMock())
    pool.fetchrow.return_value = {
        "committed_units": 25,
        "committed_cost_microusd": 125_000,
        "reserved_units": 10,
        "reserved_cost_microusd": 50_000,
    }
    repository = SourceStateRepository(pool)  # type: ignore[arg-type]

    usage = await repository.monthly_usage("text-scouts", "x-api", datetime(2026, 8, 31, 23, 59, tzinfo=UTC))

    assert usage.billing_month == date(2026, 8, 1)
    assert usage.budgeted_cost_microusd == 175_000


def test_usage_reservation_reports_exact_integer_cost() -> None:
    reservation = SourceStateRepository._reservation(
        _reservation_row(status="COMMITTED", actual_units=7)  # type: ignore[arg-type]
    )
    assert reservation.actual_cost_microusd == 35_000
