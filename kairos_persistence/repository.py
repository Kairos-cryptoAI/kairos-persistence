"""Explicit SQL repositories for audit, inbox/outbox and execution state."""

from __future__ import annotations

import json
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Any

import asyncpg
from kairos_core.contracts.base import KairosMessage


@dataclass(frozen=True)
class InboxClaim:
    claimed: bool
    duplicate_completed: bool = False
    attempts: int = 0


@dataclass(frozen=True)
class OutboxRecord:
    id: int
    message_id: str
    topic: str
    payload: dict[str, Any]
    payload_sha256: str | None
    publish_attempts: int


class MessageIdentityConflict(RuntimeError):
    """A stable message ID was reused with different immutable content."""


@dataclass
class InboxTransaction:
    """One claimed inbox message and its atomic business transaction.

    Business writes made through :attr:`connection`, outbox inserts and the
    final inbox completion all use one PostgreSQL connection.  The repository
    wraps them in a savepoint so a processing failure rolls back business and
    outbox writes before recording the inbox row as ``FAILED``.
    """

    repository: AuditRepository
    claim: InboxClaim
    consumer: str
    message_id: str
    _connection: asyncpg.Connection
    _completed: bool = False

    @property
    def connection(self) -> asyncpg.Connection:
        self._require_claimed()
        return self._connection

    @property
    def completed(self) -> bool:
        return self._completed

    def _require_claimed(self) -> None:
        if not self.claim.claimed:
            raise RuntimeError("cannot process a duplicate or currently leased inbox message")

    async def append_event(self, topic: str, message: KairosMessage) -> bool:
        self._require_claimed()
        return await self.repository.append_event(topic, message, connection=self._connection)

    async def enqueue_outbox(self, message_id: str, topic: str, payload: str, payload_sha256: str) -> bool:
        self._require_claimed()
        return await self.repository.enqueue_outbox(
            self._connection,
            message_id=message_id,
            topic=topic,
            payload=payload,
            payload_sha256=payload_sha256,
        )

    async def complete(self, result: dict[str, Any] | None = None) -> None:
        self._require_claimed()
        if self._completed:
            raise RuntimeError("inbox message is already completed")
        updated = await self.repository.complete_message(
            self.consumer,
            self.message_id,
            result,
            connection=self._connection,
        )
        if not updated:
            raise RuntimeError("inbox claim was lost before completion")
        self._completed = True


class AuditRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def append_event(
        self,
        topic: str,
        message: KairosMessage,
        *,
        connection: asyncpg.Connection | None = None,
    ) -> bool:
        executor = connection or self.pool
        result = await executor.execute(
            """INSERT INTO event_audit
               (produced_at, message_id, topic, source, schema_version,
                correlation_id, causation_id, payload)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
               ON CONFLICT (produced_at, message_id) DO NOTHING""",
            message.produced_at,
            message.message_id,
            topic,
            message.source,
            message.schema_version,
            message.correlation_id,
            message.causation_id,
            message.model_dump_json(),
        )
        return result.endswith("1")

    async def append_payload(
        self,
        topic: str,
        payload: dict[str, Any],
        *,
        connection: asyncpg.Connection | None = None,
    ) -> bool:
        """Persist a validated wire payload without knowing its concrete contract type."""
        message_id = payload.get("message_id")
        source = payload.get("source")
        schema_version = payload.get("schema_version")
        produced_at = payload.get("produced_at")
        if not all(isinstance(value, str) and value for value in (message_id, source, schema_version)):
            raise ValueError("durable payload requires message_id, source and schema_version strings")
        if not isinstance(produced_at, str):
            raise ValueError("durable payload requires an ISO-8601 produced_at string")
        try:
            parsed_at = datetime.fromisoformat(produced_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("durable payload produced_at is not valid ISO-8601") from exc
        if parsed_at.utcoffset() is None:
            raise ValueError("durable payload produced_at must be timezone-aware")
        parsed_at = parsed_at.astimezone(UTC)
        executor = connection or self.pool
        result = await executor.execute(
            """INSERT INTO event_audit
               (produced_at, message_id, topic, source, schema_version,
                correlation_id, causation_id, payload)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
               ON CONFLICT (produced_at, message_id) DO NOTHING""",
            parsed_at,
            message_id,
            topic,
            source,
            schema_version,
            payload.get("correlation_id"),
            payload.get("causation_id"),
            json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        )
        return result.endswith("1")

    async def claim_message(
        self,
        consumer: str,
        message_id: str,
        topic: str,
        lease: timedelta = timedelta(minutes=2),
        payload_sha256: str | None = None,
        *,
        connection: asyncpg.Connection | None = None,
    ) -> InboxClaim:
        """Claim once, or reclaim an expired PROCESSING/FAILED record atomically."""
        if connection is not None:
            return await self._claim_message(connection, consumer, message_id, topic, lease, payload_sha256)
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                return await self._claim_message(
                    connection, consumer, message_id, topic, lease, payload_sha256
                )

    @staticmethod
    async def _claim_message(
        connection: asyncpg.Connection,
        consumer: str,
        message_id: str,
        topic: str,
        lease: timedelta,
        payload_sha256: str | None,
    ) -> InboxClaim:
        row = await connection.fetchrow(
            """INSERT INTO message_inbox
               (consumer, message_id, topic, status, lease_until, payload_sha256)
               VALUES ($1,$2,$3,'PROCESSING',now()+$4::interval,$5)
               ON CONFLICT (consumer, message_id) DO UPDATE SET
                 status='PROCESSING', attempts=message_inbox.attempts+1,
                 lease_until=now()+$4::interval, updated_at=now(), error=NULL
               WHERE message_inbox.status='FAILED'
                  OR (message_inbox.status='PROCESSING' AND message_inbox.lease_until < now())
               RETURNING attempts, topic, payload_sha256""",
            consumer,
            message_id,
            topic,
            lease,
            payload_sha256,
        )
        if row is not None:
            if row["topic"] != topic or row["payload_sha256"] != payload_sha256:
                raise MessageIdentityConflict(
                    f"message_id {message_id!r} was reused with different topic or payload"
                )
            return InboxClaim(claimed=True, attempts=row["attempts"])
        existing = await connection.fetchrow(
            """SELECT status, topic, payload_sha256 FROM message_inbox
               WHERE consumer=$1 AND message_id=$2""",
            consumer,
            message_id,
        )
        if existing is None:
            return InboxClaim(claimed=False)
        if existing["topic"] != topic or existing["payload_sha256"] != payload_sha256:
            raise MessageIdentityConflict(
                f"message_id {message_id!r} was reused with different topic or payload"
            )
        return InboxClaim(claimed=False, duplicate_completed=existing["status"] == "COMPLETED")

    async def complete_message(
        self,
        consumer: str,
        message_id: str,
        result: dict[str, Any] | None = None,
        *,
        connection: asyncpg.Connection | None = None,
    ) -> bool:
        executor = connection or self.pool
        result_json = json.dumps(result, separators=(",", ":")) if result is not None else None
        status = await executor.execute(
            """UPDATE message_inbox SET status='COMPLETED', result=$3::jsonb,
               updated_at=now() WHERE consumer=$1 AND message_id=$2 AND status='PROCESSING'""",
            consumer,
            message_id,
            result_json,
        )
        return status.endswith("1")

    async def fail_message(
        self,
        consumer: str,
        message_id: str,
        error: str,
        *,
        connection: asyncpg.Connection | None = None,
    ) -> bool:
        executor = connection or self.pool
        status = await executor.execute(
            """UPDATE message_inbox SET status='FAILED', error=$3,
               updated_at=now() WHERE consumer=$1 AND message_id=$2 AND status='PROCESSING'""",
            consumer,
            message_id,
            error[:4000],
        )
        return status.endswith("1")

    @asynccontextmanager
    async def message_transaction(
        self,
        consumer: str,
        message_id: str,
        topic: str,
        lease: timedelta = timedelta(minutes=2),
        payload_sha256: str | None = None,
    ) -> AsyncIterator[InboxTransaction]:
        """Claim and process one message with atomic inbox/business/outbox writes.

        Callers must invoke :meth:`InboxTransaction.complete` after all business
        and outbox writes.  Exceptions roll those writes back to a savepoint,
        persist ``FAILED`` in the outer transaction and are then re-raised.
        Completed duplicates are yielded read-only so consumers can ACK them.
        """
        failure: Exception | None = None
        failure_traceback: TracebackType | None = None

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                claim = await self.claim_message(
                    consumer,
                    message_id,
                    topic,
                    lease,
                    payload_sha256,
                    connection=connection,
                )
                unit = InboxTransaction(self, claim, consumer, message_id, connection)

                if not claim.claimed:
                    yield unit
                    return

                try:
                    # asyncpg implements nested transactions as savepoints.
                    async with connection.transaction():
                        yield unit
                        if not unit.completed:
                            raise RuntimeError("claimed inbox message left transaction without complete()")
                except Exception as exc:
                    failure = exc
                    failure_traceback = sys.exc_info()[2]
                    failed = await self.fail_message(
                        consumer,
                        message_id,
                        str(exc),
                        connection=connection,
                    )
                    if not failed:
                        raise RuntimeError("inbox claim was lost while recording failure") from exc

        if failure is not None:
            raise failure.with_traceback(failure_traceback)

    async def enqueue_outbox(
        self,
        connection: asyncpg.Connection,
        message_id: str,
        topic: str,
        payload: str,
        payload_sha256: str,
    ) -> bool:
        status = await connection.execute(
            """INSERT INTO message_outbox(message_id, topic, payload, payload_sha256)
               VALUES ($1,$2,$3::jsonb,$4) ON CONFLICT (message_id) DO NOTHING""",
            message_id,
            topic,
            payload,
            payload_sha256,
        )
        if status.endswith("1"):
            return True
        existing = await connection.fetchrow(
            "SELECT topic, payload, payload_sha256 FROM message_outbox WHERE message_id=$1",
            message_id,
        )
        existing_payload = None if existing is None else existing["payload"]
        if isinstance(existing_payload, str):
            existing_payload = json.loads(existing_payload)
        existing_encoded = (
            None
            if existing_payload is None
            else json.dumps(existing_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        )
        if (
            existing is None
            or existing["topic"] != topic
            or existing_encoded != payload
            or existing["payload_sha256"] not in (None, payload_sha256)
        ):
            raise MessageIdentityConflict(
                f"outbox message_id {message_id!r} was reused with different topic or payload"
            )
        if existing["payload_sha256"] is None:
            await connection.execute(
                "UPDATE message_outbox SET payload_sha256=$2 WHERE message_id=$1",
                message_id,
                payload_sha256,
            )
        return False

    async def pending_outbox(self, limit: int = 100) -> list[asyncpg.Record]:
        return await self.pool.fetch(
            """SELECT id, message_id, topic, payload FROM message_outbox
               WHERE published_at IS NULL ORDER BY id LIMIT $1""",
            limit,
        )

    async def claim_outbox(
        self,
        worker_id: str,
        *,
        limit: int = 100,
        lease: timedelta = timedelta(seconds=30),
    ) -> list[OutboxRecord]:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(
                    """WITH candidates AS (
                           SELECT id FROM message_outbox
                           WHERE published_at IS NULL AND dead_lettered_at IS NULL
                             AND available_at <= now()
                             AND (lease_until IS NULL OR lease_until < now())
                           ORDER BY id
                           FOR UPDATE SKIP LOCKED
                           LIMIT $2
                       )
                       UPDATE message_outbox AS outbox
                       SET lease_owner=$1, lease_until=now()+$3::interval,
                           publish_attempts=publish_attempts+1
                       FROM candidates
                       WHERE outbox.id=candidates.id
                       RETURNING outbox.id, outbox.message_id, outbox.topic, outbox.payload,
                                 outbox.payload_sha256, outbox.publish_attempts""",
                    worker_id,
                    limit,
                    lease,
                )
        return [
            OutboxRecord(
                id=row["id"],
                message_id=row["message_id"],
                topic=row["topic"],
                payload=(json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]),
                payload_sha256=row["payload_sha256"],
                publish_attempts=row["publish_attempts"],
            )
            for row in rows
        ]

    async def mark_published(self, row_id: int, worker_id: str) -> bool:
        status = await self.pool.execute(
            """UPDATE message_outbox SET published_at=now(), lease_until=NULL, lease_owner=NULL,
               last_error=NULL WHERE id=$1 AND lease_owner=$2 AND published_at IS NULL""",
            row_id,
            worker_id,
        )
        return status.endswith("1")

    async def fail_outbox(
        self,
        row_id: int,
        worker_id: str,
        error: str,
        *,
        retry_after: timedelta,
        max_attempts: int,
    ) -> bool:
        status = await self.pool.execute(
            """UPDATE message_outbox SET
                 last_error=$3, lease_until=NULL, lease_owner=NULL,
                 available_at=now()+$4::interval,
                 dead_lettered_at=CASE WHEN publish_attempts >= $5 THEN now() ELSE NULL END
               WHERE id=$1 AND lease_owner=$2 AND published_at IS NULL""",
            row_id,
            worker_id,
            error[:4000],
            retry_after,
            max_attempts,
        )
        return status.endswith("1")
