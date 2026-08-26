"""Crash-safe bridge between the at-least-once bus and PostgreSQL inbox/outbox."""

from __future__ import annotations

import asyncio
import hashlib
import json
import socket
from collections.abc import AsyncIterator
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import uuid4

from kairos_core.bus import BusEnvelope, MessageBus
from kairos_core.bus.base import Publishable
from kairos_core.contracts.base import KairosMessage
from kairos_core.logging import get_logger

from .config import PersistenceSettings
from .database import Database
from .repository import AuditRepository, InboxTransaction, MessageIdentityConflict

log = get_logger("durable-runtime")


def canonical_payload(payload: dict[str, Any]) -> tuple[str, str]:
    """Return canonical JSON and its content identity."""
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _publishable_payload(message: Publishable) -> dict[str, Any]:
    if isinstance(message, KairosMessage):
        return message.to_payload()
    if isinstance(message, dict):
        return message
    raise TypeError(f"cannot publish object of type {type(message)!r}")


def _message_id(payload: dict[str, Any]) -> str:
    value = payload.get("message_id")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("durable messages require a non-empty message_id")
    return value


@dataclass
class _Delivery:
    transaction: InboxTransaction
    envelope: BusEnvelope
    group: str
    ack_requested: bool = False


class DurableMessageBus(MessageBus):
    """MessageBus that persists input completion and output intent before ACK.

    Existing consumers keep their normal ``subscribe``/``publish``/``ack`` API.
    While a subscribed handler is active, publishes are inserted into the same
    PostgreSQL transaction as inbox completion.  ``ack`` only records intent;
    the Redis ACK is sent after that transaction commits.
    """

    def __init__(
        self,
        transport: MessageBus,
        *,
        service_name: str,
        settings: PersistenceSettings | None = None,
        database: Database | None = None,
    ) -> None:
        if not service_name.strip():
            raise ValueError("service_name must not be empty")
        self.transport = transport
        self.service_name = service_name.strip()
        self.settings = settings or PersistenceSettings()
        self.database = database or Database(self.settings)
        self.repository: AuditRepository | None = None
        self._delivery: ContextVar[_Delivery | None] = ContextVar(f"kairos_delivery_{id(self)}", default=None)
        self._start_lock = asyncio.Lock()
        self._started = False
        self._closing = False
        self._wake_dispatcher = asyncio.Event()
        self._dispatcher: asyncio.Task[None] | None = None
        self._worker_id = f"{socket.gethostname()}:{self.service_name}:{uuid4().hex}"

    async def _ensure_started(self) -> None:
        if self._started:
            return
        async with self._start_lock:
            if self._started:
                return
            if self._closing:
                raise RuntimeError("durable bus is closing")
            await self.database.connect()
            await self.database.migrate()
            self.repository = AuditRepository(self.database.pool)
            self._started = True
            self._dispatcher = asyncio.create_task(
                self._dispatch_outbox(), name=f"{self.service_name}-outbox"
            )

    async def start(self) -> None:
        """Connect, migrate and start dispatch before another runtime component uses the pool."""
        await self._ensure_started()

    def _repository(self) -> AuditRepository:
        if self.repository is None:
            raise RuntimeError("durable bus is not started")
        return self.repository

    async def publish(self, topic: str, message: Publishable) -> str:
        await self._ensure_started()
        payload = _publishable_payload(message)
        message_id = _message_id(payload)
        encoded, payload_sha256 = canonical_payload(payload)
        delivery = self._delivery.get()
        repository = self._repository()
        if delivery is not None:
            await repository.append_payload(topic, payload, connection=delivery.transaction.connection)
            await delivery.transaction.enqueue_outbox(message_id, topic, encoded, payload_sha256)
        else:
            async with self.database.transaction() as connection:
                await repository.append_payload(topic, payload, connection=connection)
                await repository.enqueue_outbox(
                    connection,
                    message_id=message_id,
                    topic=topic,
                    payload=encoded,
                    payload_sha256=payload_sha256,
                    producer=self.service_name,
                )
        self._wake_dispatcher.set()
        return message_id

    def subscribe(
        self,
        topic: str,
        *,
        group: str | None = None,
        consumer: str | None = None,
    ) -> AsyncIterator[BusEnvelope]:
        return self._subscribe(topic, group=group, consumer=consumer)

    async def _subscribe(
        self,
        topic: str,
        *,
        group: str | None,
        consumer: str | None,
    ) -> AsyncIterator[BusEnvelope]:
        await self._ensure_started()
        logical_group = group or "default"
        durable_consumer = f"{self.service_name}:{logical_group}"
        repository = self._repository()
        async for envelope in self.transport.subscribe(topic, group=logical_group, consumer=consumer):
            payload = envelope.payload
            message_id = _message_id(payload)
            _encoded, payload_sha256 = canonical_payload(payload)
            should_ack = False
            try:
                async with repository.message_transaction(
                    durable_consumer,
                    message_id,
                    topic,
                    lease=timedelta(seconds=self.settings.inbox_lease_s),
                    payload_sha256=payload_sha256,
                    outbox_producer=self.service_name,
                ) as transaction:
                    if not transaction.claim.claimed:
                        should_ack = transaction.claim.duplicate_completed
                    else:
                        await repository.append_payload(topic, payload, connection=transaction.connection)
                        delivery = _Delivery(transaction, envelope, logical_group)
                        token: Token[_Delivery | None] = self._delivery.set(delivery)
                        try:
                            yield envelope
                        finally:
                            self._delivery.reset(token)
                        if delivery.ack_requested:
                            await transaction.complete({"transport_id": envelope.id})
                            should_ack = True
                        else:
                            raise RuntimeError("handler returned without requesting ACK")
            except MessageIdentityConflict:
                log.exception(
                    "durable.inbox_identity_conflict",
                    service=self.service_name,
                    topic=topic,
                    envelope_id=envelope.id,
                    message_id=message_id,
                )
                raise
            except RuntimeError as exc:
                if str(exc) != "handler returned without requesting ACK":
                    raise
                log.warning(
                    "durable.delivery_not_acked",
                    service=self.service_name,
                    topic=topic,
                    envelope_id=envelope.id,
                    message_id=message_id,
                )
            if should_ack:
                await self.transport.ack(topic, envelope, group=logical_group)
                self._wake_dispatcher.set()

    async def ack(self, topic: str, envelope: BusEnvelope, *, group: str | None = None) -> None:
        delivery = self._delivery.get()
        if delivery is None:
            raise RuntimeError("durable ACK must occur inside an active subscription delivery")
        effective_group = group or delivery.group
        if (
            topic != envelope.topic
            or envelope.id != delivery.envelope.id
            or effective_group != delivery.group
        ):
            raise ValueError("ACK does not match the active durable delivery")
        if delivery.ack_requested:
            raise RuntimeError("durable delivery was already acknowledged")
        delivery.ack_requested = True

    async def _dispatch_outbox(self) -> None:
        repository = self._repository()
        while not self._closing:
            records = await repository.claim_outbox(
                self._worker_id,
                producer=self.service_name,
                limit=self.settings.outbox_batch_size,
                lease=timedelta(seconds=self.settings.outbox_lease_s),
            )
            if not records:
                self._wake_dispatcher.clear()
                try:
                    await asyncio.wait_for(self._wake_dispatcher.wait(), timeout=self.settings.outbox_poll_s)
                except TimeoutError:
                    pass
                continue
            for record in records:
                try:
                    _encoded, actual_sha256 = canonical_payload(record.payload)
                    if record.payload_sha256 is not None and actual_sha256 != record.payload_sha256:
                        raise MessageIdentityConflict(
                            f"outbox row {record.id} payload does not match its fingerprint"
                        )
                    await self.transport.publish(record.topic, record.payload)
                    if not await repository.mark_published(record.id, self._worker_id):
                        raise RuntimeError(f"outbox lease lost for row {record.id}")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    retry_s = min(
                        self.settings.outbox_retry_max_s,
                        self.settings.outbox_retry_base_s * (2 ** min(record.publish_attempts - 1, 16)),
                    )
                    await repository.fail_outbox(
                        record.id,
                        self._worker_id,
                        str(exc),
                        retry_after=timedelta(seconds=retry_s),
                        max_attempts=self.settings.outbox_max_attempts,
                    )
                    log.exception(
                        "durable.outbox_publish_failed",
                        service=self.service_name,
                        row_id=record.id,
                        message_id=record.message_id,
                        attempt=record.publish_attempts,
                    )

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._wake_dispatcher.set()
        if self._dispatcher is not None:
            try:
                await asyncio.wait_for(self._dispatcher, timeout=self.settings.shutdown_timeout_s)
            except TimeoutError:
                self._dispatcher.cancel()
                await asyncio.gather(self._dispatcher, return_exceptions=True)
        if self._started:
            await self.database.close()
        await self.transport.close()
