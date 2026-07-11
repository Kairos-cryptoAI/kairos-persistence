"""Explicit SQL repositories for audit, inbox/outbox and execution state."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import asyncpg
from kairos_core.contracts.base import KairosMessage


@dataclass(frozen=True)
class InboxClaim:
    claimed: bool
    duplicate_completed: bool = False
    attempts: int = 0


class AuditRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def append_event(self, topic: str, message: KairosMessage) -> bool:
        result = await self.pool.execute(
            """INSERT INTO event_audit
               (produced_at, message_id, topic, source, schema_version,
                correlation_id, causation_id, payload)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
               ON CONFLICT (produced_at, message_id) DO NOTHING""",
            message.produced_at, message.message_id, topic, message.source,
            message.schema_version, message.correlation_id, message.causation_id,
            message.model_dump_json(),
        )
        return result.endswith("1")

    async def claim_message(
        self, consumer: str, message_id: str, topic: str, lease: timedelta = timedelta(minutes=2)
    ) -> InboxClaim:
        """Claim once, or reclaim an expired PROCESSING/FAILED record atomically."""
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """INSERT INTO message_inbox
                       (consumer, message_id, topic, status, lease_until)
                       VALUES ($1,$2,$3,'PROCESSING',now()+$4::interval)
                       ON CONFLICT (consumer, message_id) DO UPDATE SET
                         status='PROCESSING', attempts=message_inbox.attempts+1,
                         lease_until=now()+$4::interval, updated_at=now(), error=NULL
                       WHERE message_inbox.status='FAILED'
                          OR (message_inbox.status='PROCESSING' AND message_inbox.lease_until < now())
                       RETURNING attempts""",
                    consumer, message_id, topic, lease,
                )
                if row is not None:
                    return InboxClaim(claimed=True, attempts=row["attempts"])
                status = await connection.fetchval(
                    "SELECT status FROM message_inbox WHERE consumer=$1 AND message_id=$2",
                    consumer, message_id,
                )
                return InboxClaim(claimed=False, duplicate_completed=status == "COMPLETED")

    async def complete_message(
        self, consumer: str, message_id: str, result: dict[str, Any] | None = None
    ) -> None:
        await self.pool.execute(
            """UPDATE message_inbox SET status='COMPLETED', result=$3::jsonb,
               updated_at=now() WHERE consumer=$1 AND message_id=$2 AND status='PROCESSING'""",
            consumer, message_id, result,
        )

    async def fail_message(self, consumer: str, message_id: str, error: str) -> None:
        await self.pool.execute(
            """UPDATE message_inbox SET status='FAILED', error=$3,
               updated_at=now() WHERE consumer=$1 AND message_id=$2 AND status='PROCESSING'""",
            consumer, message_id, error[:4000],
        )

    async def enqueue_outbox(
        self, connection: asyncpg.Connection, message_id: str, topic: str, payload: str
    ) -> bool:
        status = await connection.execute(
            """INSERT INTO message_outbox(message_id, topic, payload)
               VALUES ($1,$2,$3::jsonb) ON CONFLICT (message_id) DO NOTHING""",
            message_id, topic, payload,
        )
        return status.endswith("1")

    async def pending_outbox(self, limit: int = 100) -> list[asyncpg.Record]:
        return await self.pool.fetch(
            """SELECT id, message_id, topic, payload FROM message_outbox
               WHERE published_at IS NULL ORDER BY id LIMIT $1""", limit
        )

    async def mark_published(self, row_id: int) -> None:
        await self.pool.execute(
            """UPDATE message_outbox SET published_at=now(), publish_attempts=publish_attempts+1,
               last_error=NULL WHERE id=$1""", row_id
        )
