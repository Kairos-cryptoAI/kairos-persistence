from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from kairos_core.contracts import SentimentSignal
from kairos_core.enums import ImpactDirection

from kairos_persistence.repository import AuditRepository


@pytest.mark.asyncio
async def test_append_event_uses_parameterized_insert_and_is_idempotent():
    pool = AsyncMock()
    pool.execute.return_value = "INSERT 0 1"
    repo = AuditRepository(pool)
    message = SentimentSignal(
        source="text-scouts", correlation_id="trace-1", topic="ETF",
        sentiment=0.8, impact=ImpactDirection.BULLISH,
    )

    inserted = await repo.append_event("kairos.sentiment", message)

    assert inserted is True
    sql, *params = pool.execute.await_args.args
    assert "ON CONFLICT" in sql
    assert message.message_id in params
    assert "trace-1" in params


@pytest.mark.asyncio
async def test_complete_and_fail_are_bounded_by_processing_state():
    pool = AsyncMock()
    repo = AuditRepository(pool)
    await repo.complete_message("execution", "message-1", {"order": "x"})
    complete_sql = pool.execute.await_args.args[0]
    assert "status='PROCESSING'" in complete_sql

    await repo.fail_message("execution", "message-1", "x" * 5000)
    args = pool.execute.await_args.args
    assert "status='FAILED'" in args[0]
    assert len(args[3]) == 4000


def test_default_lease_is_operationally_bounded():
    default = AuditRepository.claim_message.__defaults__[0]
    assert default == timedelta(minutes=2)
