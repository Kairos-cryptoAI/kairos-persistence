from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock

import pytest
from kairos_core.bus import BusEnvelope, MessageBus
from kairos_core.bus.base import Publishable

from kairos_persistence import DurableMessageBus, InboxClaim, PersistenceSettings, canonical_payload
from kairos_persistence.database import MigrationProfile
from kairos_persistence.repository import OutboxRecord


class _Transport(MessageBus):
    def __init__(self, envelopes: list[BusEnvelope]) -> None:
        self.envelopes = envelopes
        self.published: list[tuple[str, dict[str, Any]]] = []
        self.acked: list[tuple[str, str, str | None]] = []
        self.closed = False

    async def publish(self, topic: str, message: Publishable) -> str:
        payload = self._to_payload(message)
        self.published.append((topic, payload))
        return f"transport-{len(self.published)}"

    async def _subscribe(self) -> AsyncIterator[BusEnvelope]:
        for envelope in self.envelopes:
            yield envelope

    def subscribe(
        self,
        topic: str,
        *,
        group: str | None = None,
        consumer: str | None = None,
    ) -> AsyncIterator[BusEnvelope]:
        return self._subscribe()

    async def ack(self, topic: str, envelope: BusEnvelope, *, group: str | None = None) -> None:
        self.acked.append((topic, envelope.id, group))

    async def close(self) -> None:
        self.closed = True


class _Database:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[object]:
        self.events.append("standalone-begin")
        yield object()
        self.events.append("standalone-commit")


@dataclass
class _Transaction:
    repository: _Repository
    claim: InboxClaim
    connection: object
    completed: bool = False

    async def enqueue_outbox(self, message_id: str, topic: str, payload: str, payload_sha256: str) -> bool:
        self.repository.events.append(f"outbox:{message_id}")
        self.repository.outbox.append((message_id, topic, payload, payload_sha256))
        return True

    async def complete(self, result: dict[str, Any] | None = None) -> None:
        self.completed = True
        self.repository.events.append("inbox-complete")


class _Repository:
    def __init__(self, events: list[str], claim: InboxClaim | None = None) -> None:
        self.events = events
        self.claim = claim or InboxClaim(claimed=True)
        self.outbox: list[tuple[str, str, str, str]] = []
        self.audit: list[tuple[str, dict[str, Any]]] = []

    @asynccontextmanager
    async def message_transaction(self, *args: Any, **kwargs: Any) -> AsyncIterator[_Transaction]:
        self.events.append("inbox-begin")
        tx = _Transaction(self, self.claim, object())
        try:
            yield tx
        except Exception:
            self.events.append("inbox-failed")
            raise
        else:
            self.events.append("inbox-commit")

    async def append_payload(self, topic: str, payload: dict[str, Any], *, connection: object) -> bool:
        self.audit.append((topic, payload))
        return True

    async def enqueue_outbox(
        self,
        connection: object,
        message_id: str,
        topic: str,
        payload: str,
        payload_sha256: str,
        producer: str,
    ) -> bool:
        self.events.append(f"producer:{producer}")
        self.outbox.append((message_id, topic, payload, payload_sha256))
        return True


class _DispatchRepository:
    def __init__(
        self,
        record: OutboxRecord,
        *,
        ack: bool | None = True,
        ack_error: Exception | None = None,
        claim_error: Exception | None = None,
    ) -> None:
        self.record = record
        self.ack = ack
        self.ack_error = ack_error
        self.claim_error = claim_error
        self.calls: list[tuple[str, Any]] = []
        self.runtime: DurableMessageBus | None = None

    async def claim_outbox(self, *args: Any, **kwargs: Any) -> list[OutboxRecord]:
        self.calls.append(("claim-committed", kwargs))
        if self.claim_error is not None:
            raise self.claim_error
        assert self.runtime is not None
        self.runtime._closing = True
        return [self.record]

    async def mark_published(self, record: OutboxRecord, worker_id: str) -> bool:
        self.calls.append(("ack", (record, worker_id)))
        if self.ack_error is not None:
            raise self.ack_error
        return bool(self.ack)

    async def mark_outbox_publish_outcome_unknown(
        self, record: OutboxRecord, worker_id: str, *, reason_code: str
    ) -> bool:
        self.calls.append(("unknown", (record, worker_id, reason_code)))
        return True

    async def fail_outbox(self, *args: Any, **kwargs: Any) -> bool:
        self.calls.append(("unsafe-retry", args))
        return True


def _outbox_record(*, payload: dict[str, Any] | None = None, sha256: str | None = None) -> OutboxRecord:
    selected_payload = _payload("outgoing-dispatch-1") if payload is None else payload
    digest = canonical_payload(selected_payload)[1] if sha256 is None else sha256
    return OutboxRecord(
        id=71,
        producer="test-service",
        message_id="outgoing-dispatch-1",
        topic="output",
        payload=selected_payload,
        payload_sha256=digest,
        publish_attempts=4,
        reconciliation_id="dispatch:stable-test-id",
    )


def _dispatch_runtime(transport: _Transport, repository: _DispatchRepository) -> DurableMessageBus:
    runtime = DurableMessageBus(
        transport,
        service_name="test-service",
        settings=PersistenceSettings(shutdown_timeout_s=0),
        database=_Database([]),  # type: ignore[arg-type]
    )
    runtime.repository = repository  # type: ignore[assignment]
    repository.runtime = runtime
    return runtime


class _DispatchTransport(_Transport):
    def __init__(self, *, publish_error: BaseException | None = None) -> None:
        super().__init__([])
        self.publish_error = publish_error
        self.events: list[str] = []

    async def publish(self, topic: str, message: Publishable) -> str:
        self.events.append("publish")
        if self.publish_error is not None:
            raise self.publish_error
        return await super().publish(topic, message)


def _payload(message_id: str) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "message_id": message_id,
        "produced_at": "2026-08-18T12:00:00Z",
        "source": "test",
        "value": 1,
    }


def _runtime(transport: _Transport, repository: _Repository, events: list[str]) -> DurableMessageBus:
    runtime = DurableMessageBus(
        transport,
        service_name="test-service",
        settings=PersistenceSettings(shutdown_timeout_s=0),
        database=_Database(events),  # type: ignore[arg-type]
    )
    runtime.repository = repository
    runtime._started = True
    return runtime


def test_canonical_payload_is_order_independent_and_content_sensitive() -> None:
    encoded_a, digest_a = canonical_payload({"b": 2, "a": 1})
    encoded_b, digest_b = canonical_payload({"a": 1, "b": 2})
    assert encoded_a == encoded_b == '{"a":1,"b":2}'
    assert digest_a == digest_b
    assert canonical_payload({"a": 2})[1] != digest_a


@pytest.mark.asyncio
async def test_dispatch_claim_is_committed_before_publish_and_ack_uses_claim_record() -> None:
    transport = _DispatchTransport()
    repository = _DispatchRepository(_outbox_record())
    runtime = _dispatch_runtime(transport, repository)

    await runtime._dispatch_outbox()

    assert repository.calls[0][0] == "claim-committed"
    assert transport.events == ["publish"]
    assert [call[0] for call in repository.calls] == ["claim-committed", "ack"]
    record, worker_id = repository.calls[1][1]
    assert record.reconciliation_id == "dispatch:stable-test-id"
    assert worker_id == runtime._worker_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("publish_error", "ack", "ack_error", "expected_reason"),
    [
        (RuntimeError("secret-like token=must-not-persist"), True, None, "PUBLISH_EXCEPTION"),
        (None, False, None, "ACK_NOT_APPLIED"),
        (None, True, RuntimeError("database details must-not-persist"), "ACK_EXCEPTION"),
    ],
)
async def test_dispatch_ambiguous_publish_or_ack_never_retries(
    publish_error: BaseException | None,
    ack: bool | None,
    ack_error: Exception | None,
    expected_reason: str,
) -> None:
    transport = _DispatchTransport(publish_error=publish_error)
    repository = _DispatchRepository(_outbox_record(), ack=ack, ack_error=ack_error)
    runtime = _dispatch_runtime(transport, repository)

    await runtime._dispatch_outbox()

    expected_calls = (
        ["claim-committed", "unknown"]
        if expected_reason == "PUBLISH_EXCEPTION"
        else ["claim-committed", "ack", "unknown"]
    )
    assert [call[0] for call in repository.calls] == expected_calls
    assert repository.calls[-1][1][2] == expected_reason
    assert "unsafe-retry" not in [call[0] for call in repository.calls]
    if expected_reason == "PUBLISH_EXCEPTION":
        assert transport.published == []
    else:
        assert len(transport.published) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("publish_error", "expected_reason"),
    [
        (asyncio.CancelledError(), "PUBLISH_CANCELLED"),
        (None, "ACK_CANCELLED"),
    ],
)
async def test_dispatch_cancellation_after_claim_quarantines_and_propagates(
    publish_error: BaseException | None,
    expected_reason: str,
) -> None:
    transport = _DispatchTransport(publish_error=publish_error)
    repository = _DispatchRepository(
        _outbox_record(), ack_error=asyncio.CancelledError() if publish_error is None else None
    )
    runtime = _dispatch_runtime(transport, repository)

    with pytest.raises(asyncio.CancelledError):
        await runtime._dispatch_outbox()

    expected_calls = (
        ["claim-committed", "unknown"]
        if expected_reason == "PUBLISH_CANCELLED"
        else ["claim-committed", "ack", "unknown"]
    )
    assert [call[0] for call in repository.calls] == expected_calls
    assert repository.calls[-1][1][2] == expected_reason
    assert "unsafe-retry" not in [call[0] for call in repository.calls]


@pytest.mark.asyncio
async def test_task_cancel_during_transport_wait_persists_unknown_marker() -> None:
    entered_publish = asyncio.Event()

    class _BlockingTransport(_DispatchTransport):
        async def publish(self, topic: str, message: Publishable) -> str:
            self.events.append("publish")
            entered_publish.set()
            await asyncio.Event().wait()
            return "unreachable"

    transport = _BlockingTransport()
    repository = _DispatchRepository(_outbox_record())
    runtime = _dispatch_runtime(transport, repository)
    task = asyncio.create_task(runtime._dispatch_outbox())
    await entered_publish.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert [call[0] for call in repository.calls] == ["claim-committed", "unknown"]
    assert repository.calls[-1][1][2] == "PUBLISH_CANCELLED"


@pytest.mark.asyncio
async def test_dispatch_claim_failure_never_reaches_publisher() -> None:
    transport = _DispatchTransport()
    repository = _DispatchRepository(_outbox_record(), claim_error=RuntimeError("claim rollback"))
    runtime = _dispatch_runtime(transport, repository)

    with pytest.raises(RuntimeError, match="claim rollback"):
        await runtime._dispatch_outbox()

    assert transport.events == []


@pytest.mark.asyncio
async def test_dispatch_hash_mismatch_is_quarantined_before_publisher_call() -> None:
    transport = _DispatchTransport()
    repository = _DispatchRepository(_outbox_record(sha256="0" * 64))
    runtime = _dispatch_runtime(transport, repository)

    await runtime._dispatch_outbox()

    assert transport.events == []
    assert [call[0] for call in repository.calls] == ["claim-committed", "unknown"]
    assert repository.calls[-1][1][2] == "PRE_PUBLISH_HASH_MISMATCH"


@pytest.mark.asyncio
async def test_delivery_commits_inbox_and_outbox_before_transport_ack() -> None:
    events: list[str] = []
    envelope = BusEnvelope(id="redis-1", topic="input", payload=_payload("incoming-1"))
    transport = _Transport([envelope])
    repository = _Repository(events)
    runtime = _runtime(transport, repository, events)

    stream = runtime.subscribe("input", group="workers", consumer="one")
    received = await anext(stream)
    await runtime.publish("output", _payload("outgoing-1"))
    await runtime.ack("input", received, group="workers")
    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    assert events == ["inbox-begin", "outbox:outgoing-1", "inbox-complete", "inbox-commit"]
    assert transport.acked == [("input", "redis-1", "workers")]
    assert repository.outbox[0][0] == "outgoing-1"


@pytest.mark.asyncio
async def test_completed_duplicate_is_acked_without_being_delivered() -> None:
    events: list[str] = []
    envelope = BusEnvelope(id="redis-1", topic="input", payload=_payload("incoming-1"))
    transport = _Transport([envelope])
    repository = _Repository(events, InboxClaim(claimed=False, duplicate_completed=True))
    runtime = _runtime(transport, repository, events)

    with pytest.raises(StopAsyncIteration):
        await anext(runtime.subscribe("input", group="workers"))

    assert transport.acked == [("input", "redis-1", "workers")]
    assert repository.audit == []


@pytest.mark.asyncio
async def test_handler_without_ack_is_failed_and_transport_remains_pending() -> None:
    events: list[str] = []
    envelope = BusEnvelope(id="redis-1", topic="input", payload=_payload("incoming-1"))
    transport = _Transport([envelope])
    repository = _Repository(events)
    runtime = _runtime(transport, repository, events)

    stream = runtime.subscribe("input", group="workers")
    assert await anext(stream) is envelope
    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    assert events == ["inbox-begin", "inbox-failed"]
    assert transport.acked == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "verify,profile", [(True, MigrationProfile.RUNTIME), (False, MigrationProfile.CONTROLLED_RUNTIME)]
)
async def test_verified_runtime_start_never_migrates_or_starts_outbox_after_schema_failure(verify, profile):
    database = type(
        "Database",
        (),
        {
            "connect": AsyncMock(),
            "migrate": AsyncMock(),
            "verify_schema": AsyncMock(side_effect=RuntimeError("schema differs")),
            "migration_profile": profile,
        },
    )()
    runtime = DurableMessageBus(
        _Transport([]), service_name="test", database=database, verify_schema_only=verify
    )
    with pytest.raises(RuntimeError, match="schema differs"):
        await runtime.start()
    database.migrate.assert_not_called()
    database.verify_schema.assert_awaited_once()
    assert not runtime._started and runtime._dispatcher is None


@pytest.mark.asyncio
async def test_paper_default_profile_refuses_before_connection_or_outbox():
    database = type(
        "Database",
        (),
        {
            "connect": AsyncMock(),
            "migrate": AsyncMock(),
            "verify_schema": AsyncMock(),
            "migration_profile": MigrationProfile.RUNTIME,
        },
    )()
    runtime = DurableMessageBus(
        _Transport([]),
        service_name="paper",
        database=database,
        verify_schema_only=True,
        required_migration_profile=MigrationProfile.CONTROLLED_RUNTIME,
    )
    with pytest.raises(RuntimeError, match="explicit.*controlled-runtime"):
        await runtime.start()
    database.connect.assert_not_called()
    database.migrate.assert_not_called()
    database.verify_schema.assert_not_called()
    assert not runtime._started and runtime._dispatcher is None


@pytest.mark.asyncio
async def test_controlled_profile_rejects_unsafe_runtime_before_outbox(monkeypatch):
    from kairos_persistence.operator_control import OperatorControlRepository

    probe = AsyncMock(side_effect=PermissionError("roles not independently separated"))
    monkeypatch.setattr(OperatorControlRepository, "verify_runtime_access", probe)
    database = type(
        "Database",
        (),
        {
            "connect": AsyncMock(),
            "migrate": AsyncMock(),
            "verify_schema": AsyncMock(),
            "migration_profile": MigrationProfile.CONTROLLED_RUNTIME,
            "pool": object(),
        },
    )()
    runtime = DurableMessageBus(_Transport([]), service_name="paper", database=database)
    with pytest.raises(PermissionError, match="roles not independently separated"):
        await runtime.start()
    probe.assert_awaited_once()
    database.migrate.assert_not_called()
    assert not runtime._started and runtime._dispatcher is None
