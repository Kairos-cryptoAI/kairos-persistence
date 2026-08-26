from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import pytest
from kairos_core.bus import BusEnvelope, MessageBus
from kairos_core.bus.base import Publishable

from kairos_persistence import DurableMessageBus, InboxClaim, PersistenceSettings, canonical_payload


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
