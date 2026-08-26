from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest
from kairos_core.contracts import SentimentSignal
from kairos_core.enums import ImpactDirection

from kairos_persistence.repository import AuditRepository


def _sql_kind(sql: str) -> str:
    compact = " ".join(sql.split())
    if compact.startswith("INSERT INTO message_inbox"):
        return "claim"
    if "SET status='COMPLETED'" in compact:
        return "complete"
    if "SET status='FAILED'" in compact:
        return "fail"
    if compact.startswith("INSERT INTO message_outbox"):
        return "outbox"
    if compact.startswith("INSERT INTO event_audit"):
        return "business"
    raise AssertionError(f"unexpected SQL: {compact}")


class _FakeTransaction:
    def __init__(self, connection: _FakeConnection) -> None:
        self.connection = connection
        self.snapshot: dict[str, Any] | None = None

    async def __aenter__(self) -> None:
        self.snapshot = self.connection.snapshot()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            assert self.snapshot is not None
            self.connection.restore(self.snapshot)


class _FakeConnection:
    def __init__(self) -> None:
        self.inbox: dict[tuple[str, str], dict[str, Any]] = {}
        self.outbox: dict[str, dict[str, Any]] = {}
        self.business: list[tuple[str, tuple[Any, ...]]] = []
        self.calls: list[tuple[str, str, tuple[Any, ...]]] = []

    def snapshot(self) -> dict[str, Any]:
        return {
            "inbox": {key: value.copy() for key, value in self.inbox.items()},
            "outbox": {key: value.copy() for key, value in self.outbox.items()},
            "business": self.business.copy(),
        }

    def restore(self, state: dict[str, Any]) -> None:
        self.inbox = state["inbox"]
        self.outbox = state["outbox"]
        self.business = state["business"]

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None:
        compact = " ".join(sql.split())
        if compact.startswith("SELECT status, topic, payload_sha256 FROM message_inbox"):
            row = self.inbox.get((params[0], params[1]))
            return None if row is None else row.copy()
        if compact.startswith("SELECT topic, payload, payload_sha256 FROM message_outbox"):
            row = self.outbox.get(params[0])
            return None if row is None else row.copy()
        kind = _sql_kind(sql)
        self.calls.append(("fetchrow", kind, params))
        assert kind == "claim"
        consumer, message_id, topic, _lease, payload_sha256 = params
        key = (consumer, message_id)
        row = self.inbox.get(key)
        if row is None:
            self.inbox[key] = {
                "topic": topic,
                "status": "PROCESSING",
                "attempts": 1,
                "payload_sha256": payload_sha256,
            }
            return self.inbox[key].copy()
        if row["status"] == "FAILED":
            row.update(status="PROCESSING", attempts=row["attempts"] + 1)
            return row.copy()
        return None

    async def fetchval(self, sql: str, *params: Any) -> str | None:
        self.calls.append(("fetchval", "status", params))
        row = self.inbox.get((params[0], params[1]))
        return row["status"] if row else None

    async def execute(self, sql: str, *params: Any) -> str:
        kind = _sql_kind(sql)
        self.calls.append(("execute", kind, params))
        if kind == "business":
            self.business.append((sql, params))
            return "INSERT 0 1"
        if kind == "outbox":
            message_id, topic, payload, payload_sha256 = params
            if message_id in self.outbox:
                return "INSERT 0 0"
            self.outbox[message_id] = {
                "topic": topic,
                "payload": payload,
                "payload_sha256": payload_sha256,
            }
            return "INSERT 0 1"

        consumer, message_id = params[:2]
        row = self.inbox.get((consumer, message_id))
        if row is None or row["status"] != "PROCESSING":
            return "UPDATE 0"
        if kind == "complete":
            row.update(status="COMPLETED", result=params[2])
        elif kind == "fail":
            row.update(status="FAILED", error=params[2])
        return "UPDATE 1"


class _FakePool:
    def __init__(self, connection: _FakeConnection) -> None:
        self.connection = connection

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[_FakeConnection]:
        yield self.connection


class _ClaimConnection:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.sql = ""

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)  # type: ignore[arg-type]

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        self.sql = " ".join(sql.split())
        return self.rows

    def snapshot(self) -> dict[str, Any]:
        return {}

    def restore(self, state: dict[str, Any]) -> None:
        del state


@pytest.mark.asyncio
async def test_append_event_uses_parameterized_insert_and_is_idempotent():
    pool = AsyncMock()
    pool.execute.return_value = "INSERT 0 1"
    repo = AuditRepository(pool)
    message = SentimentSignal(
        source="text-scouts",
        correlation_id="trace-1",
        topic="ETF",
        sentiment=0.8,
        impact=ImpactDirection.BULLISH,
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
    pool.execute.return_value = "UPDATE 1"
    repo = AuditRepository(pool)
    assert await repo.complete_message("execution", "message-1", {"order": "x"}) is True
    complete_sql = pool.execute.await_args.args[0]
    assert "status='PROCESSING'" in complete_sql
    assert pool.execute.await_args.args[3] == '{"order":"x"}'

    assert await repo.fail_message("execution", "message-1", "x" * 5000) is True
    args = pool.execute.await_args.args
    assert "status='FAILED'" in args[0]
    assert len(args[3]) == 4000


def test_default_lease_is_operationally_bounded():
    default = AuditRepository.claim_message.__defaults__[0]
    assert default == timedelta(minutes=2)


@pytest.mark.asyncio
async def test_message_transaction_commits_business_outbox_and_completion_together():
    connection = _FakeConnection()
    repo = AuditRepository(_FakePool(connection))  # type: ignore[arg-type]

    async with repo.message_transaction("execution", "incoming-1", "orders") as tx:
        assert tx.claim.claimed is True
        await tx.connection.execute("INSERT INTO event_audit VALUES (...) ", "business-row")
        assert await tx.enqueue_outbox("outgoing-1", "reports", '{"ok":true}', "0" * 64) is True
        await tx.complete({"order": "exchange-1"})

    assert connection.inbox[("execution", "incoming-1")]["status"] == "COMPLETED"
    assert list(connection.outbox) == ["outgoing-1"]
    assert len(connection.business) == 1
    assert {kind for _method, kind, _args in connection.calls} >= {
        "claim",
        "business",
        "outbox",
        "complete",
    }


@pytest.mark.asyncio
async def test_message_transaction_rolls_back_business_and_outbox_then_records_failure():
    connection = _FakeConnection()
    repo = AuditRepository(_FakePool(connection))  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="exchange unavailable"):
        async with repo.message_transaction("execution", "incoming-1", "orders") as tx:
            await tx.connection.execute("INSERT INTO event_audit VALUES (...) ", "business-row")
            await tx.enqueue_outbox("outgoing-1", "reports", "{}", "0" * 64)
            raise RuntimeError("exchange unavailable")

    row = connection.inbox[("execution", "incoming-1")]
    assert row["status"] == "FAILED"
    assert row["error"] == "exchange unavailable"
    assert connection.business == []
    assert connection.outbox == {}


@pytest.mark.asyncio
async def test_completed_duplicate_is_read_only_and_does_not_repeat_side_effects():
    connection = _FakeConnection()
    connection.inbox[("execution", "incoming-1")] = {
        "topic": "orders",
        "status": "COMPLETED",
        "attempts": 1,
        "payload_sha256": None,
    }
    repo = AuditRepository(_FakePool(connection))  # type: ignore[arg-type]

    async with repo.message_transaction("execution", "incoming-1", "orders") as tx:
        assert tx.claim.claimed is False
        assert tx.claim.duplicate_completed is True
        with pytest.raises(RuntimeError, match="duplicate"):
            await tx.enqueue_outbox("outgoing-1", "reports", "{}", "0" * 64)

    assert connection.inbox[("execution", "incoming-1")]["status"] == "COMPLETED"
    assert connection.outbox == {}


@pytest.mark.asyncio
async def test_missing_complete_rolls_back_and_marks_message_failed():
    connection = _FakeConnection()
    repo = AuditRepository(_FakePool(connection))  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="without complete"):
        async with repo.message_transaction("execution", "incoming-1", "orders") as tx:
            await tx.enqueue_outbox("outgoing-1", "reports", "{}", "0" * 64)

    assert connection.inbox[("execution", "incoming-1")]["status"] == "FAILED"
    assert connection.outbox == {}


@pytest.mark.asyncio
async def test_claim_outbox_preserves_causal_id_order() -> None:
    rows = [
        {
            "id": 12,
            "message_id": "later",
            "topic": "bars",
            "payload": {"message_id": "later"},
            "payload_sha256": "b" * 64,
            "publish_attempts": 1,
        },
        {
            "id": 11,
            "message_id": "earlier",
            "topic": "bars",
            "payload": {"message_id": "earlier"},
            "payload_sha256": "a" * 64,
            "publish_attempts": 1,
        },
    ]
    connection = _ClaimConnection(rows)
    repo = AuditRepository(_FakePool(connection))  # type: ignore[arg-type]

    claimed = await repo.claim_outbox("worker", limit=2)

    assert [record.id for record in claimed] == [11, 12]
    assert "SELECT * FROM claimed ORDER BY id" in connection.sql
