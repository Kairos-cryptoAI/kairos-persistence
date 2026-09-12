"""Real PostgreSQL campaign-budget drill, exclusively in a named test database."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from datetime import UTC, datetime

from kairos_persistence import (
    QUALIFICATION_CAMPAIGN_ID,
    Database,
    MessageIdentityConflict,
    PersistenceSettings,
    SourceBudgetExceeded,
    SourceStateRepository,
)
from kairos_persistence.database_target import connect_verified_database, require_database_target_url


def test_settings() -> PersistenceSettings:
    url = os.environ["KAIROS_BUDGET_TEST_DATABASE_URL"]
    database = os.getenv("KAIROS_BUDGET_TEST_DATABASE_NAME", "")
    if not re.fullmatch(r"kairos_budget_test_[a-z0-9_]{1,32}", database):
        raise ValueError("campaign integration requires an explicitly named isolated test database")
    require_database_target_url(url, database, local_only=True)
    return PersistenceSettings(database_url=url)


async def run() -> dict[str, object]:
    database = Database(test_settings())
    expected_name = os.environ["KAIROS_BUDGET_TEST_DATABASE_NAME"]
    await connect_verified_database(database, expected_name, local_only=True)
    try:
        await database.migrate()
        # Refuse a reused test DB: never erase even prior drill evidence.
        count = await database.pool.fetchval("SELECT count(*) FROM source_usage_reservations")
        registered = await database.pool.fetchval("SELECT count(*) FROM campaign_source_budgets")
        if count or registered:
            raise ValueError("test database contains evidence; use a new empty named test database")
        legacy = SourceStateRepository(database.pool)
        campaign = SourceStateRepository(database.pool, campaign_id=QUALIFICATION_CAMPAIGN_ID)
        august = datetime(2026, 8, 31, 23, 59, tzinfo=UTC)
        september = datetime(2026, 9, 1, tzinfo=UTC)

        # Existing unknown ACK remains RESERVED; it must not disappear on adoption.
        for service, source, identity in (
            ("runtime", "x", "old-committed"),
            ("probe", "x-api", "old-unknown"),
        ):
            await legacy.reserve_usage(
                service=service,
                source=source,
                reservation_id=identity,
                reserved_units=1,
                unit_cost_microusd=400_000,
                monthly_budget_microusd=2_000_000,
                requested_at=august,
            )
        await legacy.commit_usage("runtime", "x", "old-committed", 1)
        adoption = dict(
            source="x",
            budget_microusd=2_000_000,
            historical_cost_microusd=200_000,
            historical_evidence_sha256=hashlib.sha256(b"synthetic off-ledger drill evidence").hexdigest(),
        )
        await campaign.register_campaign(**adoption)
        await campaign.register_campaign(**adoption)
        try:
            await campaign.register_campaign(**(adoption | {"historical_cost_microusd": 0}))
        except MessageIdentityConflict:
            pass
        else:
            raise AssertionError("campaign adoption debt was reset")
        assert (await campaign.campaign_usage("x")).budgeted_cost_microusd == 1_000_000

        async def reserve(index: int):
            try:
                return await campaign.reserve_usage(
                    service=f"concurrent-{index}",
                    source="x_api",
                    reservation_id=f"race-{index}",
                    reserved_units=1,
                    unit_cost_microusd=400_000,
                    monthly_budget_microusd=2_000_000,
                    requested_at=september,
                )
            except SourceBudgetExceeded:
                return None

        reservations = await asyncio.gather(*(reserve(index) for index in range(8)))
        winners = [item for item in reservations if item is not None]
        assert len(winners) == 2  # 1M outstanding/history + two 400k requests, never a third.
        assert (await campaign.campaign_usage("x")).budgeted_cost_microusd == 1_800_000
        await database.close()
        await connect_verified_database(database, expected_name, local_only=True)
        campaign = SourceStateRepository(database.pool, campaign_id=QUALIFICATION_CAMPAIGN_ID)
        assert (await campaign.campaign_usage("x")).budgeted_cost_microusd == 1_800_000
        winner = winners[0]
        replay = await campaign.reserve_usage(
            service=winner.service,
            source=winner.source,
            reservation_id=winner.reservation_id,
            reserved_units=1,
            unit_cost_microusd=400_000,
            monthly_budget_microusd=2_000_000,
            requested_at=datetime(2026, 10, 1, tzinfo=UTC),
        )
        assert replay == winner
        await campaign.commit_usage(winner.service, winner.source, winner.reservation_id, 1)
        await campaign.commit_usage(winner.service, winner.source, winner.reservation_id, 1)
        # Even a caller of the old monthly interface cannot regain allowance.
        try:
            await SourceStateRepository(database.pool).reserve_usage(
                service="new-month",
                source="x",
                reservation_id="bypass",
                reserved_units=1,
                unit_cost_microusd=400_000,
                monthly_budget_microusd=99_000_000,
                requested_at=datetime(2027, 1, 1, tzinfo=UTC),
            )
        except SourceBudgetExceeded:
            pass
        else:
            raise AssertionError("monthly interface bypassed the campaign")
        rows = await database.pool.fetchval("SELECT count(*) FROM source_usage_reservations")
        assert rows == 4
        return {
            "state": "PASS",
            "rows": rows,
            "race_winners": len(winners),
            "budgeted_cost_microusd": 1_800_000,
            "restart": True,
            "month_reset_blocked": True,
            "historical_adoption_idempotent": True,
        }
    finally:
        await database.close()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(run()), sort_keys=True))
