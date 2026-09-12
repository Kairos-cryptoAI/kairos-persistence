from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from test_source_state import _Context, _Pool, _reservation_row

from kairos_persistence import QUALIFICATION_CAMPAIGN_ID, SourceBudgetExceeded, SourceStateRepository
from kairos_persistence.repository import MessageIdentityConflict


def _campaign(**changes):
    return {
        "source": "x",
        "campaign_id": QUALIFICATION_CAMPAIGN_ID,
        "budget_microusd": 2_000_000,
        "historical_cost_microusd": 100_000,
        "historical_evidence_sha256": "a" * 64,
        **changes,
    }


def _repository():
    connection = AsyncMock()
    connection.transaction = lambda: _Context()
    return SourceStateRepository(_Pool(connection), campaign_id=QUALIFICATION_CAMPAIGN_ID), connection


@pytest.mark.asyncio
async def test_unregistered_campaign_blocks_before_reservation():
    repository, connection = _repository()
    connection.fetchrow.return_value = None
    with pytest.raises(SourceBudgetExceeded, match="adoption"):
        await repository.reserve_usage(
            service="qualification",
            source="x",
            reservation_id="r1",
            reserved_units=1,
            unit_cost_microusd=5000,
            monthly_budget_microusd=2_000_000,
        )
    assert connection.fetchrow.await_count == 1
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("month", [8, 9, 10])
async def test_cumulative_cap_includes_other_services_months_and_off_ledger_spend(month):
    repository, connection = _repository()
    connection.fetchrow.side_effect = [_campaign(), None]
    connection.fetchval.return_value = 1_899_000
    with pytest.raises(SourceBudgetExceeded, match="cumulative"):
        await repository.reserve_usage(
            service="fresh-service",
            source="x",
            reservation_id="fresh-id",
            reserved_units=1,
            unit_cost_microusd=5000,
            monthly_budget_microusd=2_000_000,
            requested_at=datetime(2026, month, 1, tzinfo=UTC),
        )
    query, sources = connection.fetchval.call_args.args
    assert "billing_month" not in query and "service=" not in query
    assert sources == ["x", "x-api", "x_api"]
    assert connection.execute.call_args.args[1] == "source-budget:x"


@pytest.mark.asyncio
async def test_campaign_idempotent_reservation_can_cross_calendar_month():
    repository, connection = _repository()
    connection.fetchrow.side_effect = [_campaign(), _reservation_row()]
    result = await repository.reserve_usage(
        service="text-scouts",
        source="x-api",
        reservation_id="request-1",
        reserved_units=10,
        unit_cost_microusd=5000,
        monthly_budget_microusd=2_000_000,
        requested_at=datetime(2026, 9, 1, tzinfo=UTC),
    )
    assert result.reservation_id == "request-1"
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_registered_provider_cannot_bypass_campaign_via_legacy_monthly_api():
    repository, connection = _repository()
    repository.campaign_id = None
    connection.fetchrow.side_effect = [_campaign(), None]
    connection.fetchval.return_value = 2_000_000
    with pytest.raises(SourceBudgetExceeded, match="cumulative"):
        await repository.reserve_usage(
            service="legacy",
            source="x",
            reservation_id="r1",
            reserved_units=1,
            unit_cost_microusd=5000,
            monthly_budget_microusd=99_000_000,
        )


@pytest.mark.asyncio
async def test_historical_adoption_is_idempotent_and_refuses_receipt_or_budget_reset():
    repository, connection = _repository()
    connection.fetchrow.return_value = _campaign()
    values = dict(
        source="x",
        budget_microusd=2_000_000,
        historical_cost_microusd=100_000,
        historical_evidence_sha256="a" * 64,
    )
    await repository.register_campaign(**values)
    assert connection.execute.await_count == 1  # lock only; never copies historical reservations
    for replacement in (
        {"historical_cost_microusd": 0},
        {"historical_evidence_sha256": "b" * 64},
        {"budget_microusd": 1_000_000},
    ):
        with pytest.raises(MessageIdentityConflict, match="reset"):
            await repository.register_campaign(**(values | replacement))


@pytest.mark.asyncio
async def test_campaign_registration_is_explicit_and_cap_bounded():
    repository, connection = _repository()
    connection.fetchrow.return_value = None
    await repository.register_campaign(
        source="deepseek",
        budget_microusd=1_000_000,
        historical_cost_microusd=200_000,
        historical_evidence_sha256="a" * 64,
    )
    assert "INSERT INTO campaign_source_budgets" in connection.execute.call_args.args[0]
    with pytest.raises(ValueError, match="ceiling"):
        await repository.register_campaign(
            source="deepseek",
            budget_microusd=1_000_001,
            historical_cost_microusd=0,
            historical_evidence_sha256="a" * 64,
        )
    with pytest.raises(ValueError, match="campaign"):
        SourceStateRepository(_Pool(connection), campaign_id="reset-september")
