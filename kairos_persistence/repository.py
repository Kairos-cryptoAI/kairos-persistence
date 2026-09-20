"""Explicit SQL repositories for audit, inbox/outbox and execution state."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from types import TracebackType
from typing import Any

import asyncpg
from kairos_core.contracts.base import KairosMessage

OFFLINE_OUTBOX_RECONCILIATION_MIGRATION = "018_offline_outbox_reconciliation.sql"


@dataclass(frozen=True)
class InboxClaim:
    claimed: bool
    duplicate_completed: bool = False
    attempts: int = 0


@dataclass(frozen=True)
class OutboxRecord:
    id: int
    producer: str
    message_id: str
    topic: str
    payload: dict[str, Any]
    payload_sha256: str | None
    publish_attempts: int


class OfflineOutboxClaimState(StrEnum):
    """Result of a deliberately narrow offline outbox claim."""

    CLAIMED = "CLAIMED"
    REJECTED = "REJECTED"


class OfflineOutboxClaimRejection(StrEnum):
    """Fail-closed reasons an exact offline outbox claim was not made."""

    NOT_FOUND = "NOT_FOUND"
    IDENTITY_MISMATCH = "IDENTITY_MISMATCH"
    ALREADY_PUBLISHED = "ALREADY_PUBLISHED"
    DEAD_LETTERED = "DEAD_LETTERED"
    LEASE_NOT_EXPIRED = "LEASE_NOT_EXPIRED"
    LEASE_PRESENT = "LEASE_PRESENT"
    NOT_YET_AVAILABLE = "NOT_YET_AVAILABLE"
    RECONCILIATION_NOT_CLEAR = "RECONCILIATION_NOT_CLEAR"
    EARLIER_UNPUBLISHED_PREDECESSOR = "EARLIER_UNPUBLISHED_PREDECESSOR"
    PAYLOAD_HASH_MISMATCH = "PAYLOAD_HASH_MISMATCH"
    AUDIT_MISMATCH = "AUDIT_MISMATCH"
    RACE_LOST = "RACE_LOST"


class OfflineOutboxQuarantineState(StrEnum):
    """Terminal result of a DB-only expired-outbox quarantine attempt."""

    QUARANTINED = "QUARANTINED"
    ALREADY_QUARANTINED = "ALREADY_QUARANTINED"
    REJECTED = "REJECTED"


class OfflineOutboxQuarantineRejection(StrEnum):
    """Fail-closed reasons an expired effect was not quarantined."""

    MIGRATION_018_REQUIRED = "MIGRATION_018_REQUIRED"
    NOT_FOUND = "NOT_FOUND"
    IDENTITY_MISMATCH = "IDENTITY_MISMATCH"
    ALREADY_PUBLISHED = "ALREADY_PUBLISHED"
    DEAD_LETTERED = "DEAD_LETTERED"
    LEASE_NOT_EXPIRED = "LEASE_NOT_EXPIRED"
    LEASE_IDENTITY_MISMATCH = "LEASE_IDENTITY_MISMATCH"
    NOT_YET_AVAILABLE = "NOT_YET_AVAILABLE"
    RECONCILIATION_NOT_CLEAR = "RECONCILIATION_NOT_CLEAR"
    PAYLOAD_HASH_MISMATCH = "PAYLOAD_HASH_MISMATCH"
    AUDIT_MISMATCH = "AUDIT_MISMATCH"
    RACE_LOST = "RACE_LOST"


@dataclass(frozen=True)
class OfflineOutboxIdentity:
    """Every immutable field an offline recovery operator must pre-commit to."""

    id: int
    producer: str
    message_id: str
    topic: str
    payload_sha256: str
    publish_attempts: int

    def __post_init__(self) -> None:
        if not isinstance(self.id, int) or isinstance(self.id, bool) or self.id <= 0:
            raise ValueError("offline outbox identity id must be a positive integer")
        for name, value in (
            ("producer", self.producer),
            ("message_id", self.message_id),
            ("topic", self.topic),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"offline outbox identity {name} must be a non-empty string")
        if (
            not isinstance(self.payload_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", self.payload_sha256) is None
        ):
            raise ValueError("offline outbox identity payload_sha256 must be a lowercase SHA-256")
        if (
            not isinstance(self.publish_attempts, int)
            or isinstance(self.publish_attempts, bool)
            or self.publish_attempts < 0
        ):
            raise ValueError("offline outbox identity publish_attempts must be a non-negative integer")


@dataclass(frozen=True)
class OfflineOutboxExpiredLease:
    """Immutable lease values copied from the inspected expired-effect receipt."""

    owner: str
    until: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.owner, str) or not self.owner.strip():
            raise ValueError("offline expired lease owner must be a non-empty string")
        if (
            not isinstance(self.until, datetime)
            or self.until.tzinfo is None
            or self.until.utcoffset() is None
        ):
            raise ValueError("offline expired lease until must be a timezone-aware datetime")


@dataclass(frozen=True)
class OfflineOutboxClaim:
    """A single persisted row leased for one explicit offline reconciliation."""

    identity: OfflineOutboxIdentity
    payload: dict[str, Any]
    reconciliation_id: str
    claimed_publish_attempts: int


@dataclass(frozen=True)
class OfflineOutboxClaimResult:
    """A claim or a concrete reason it was fail-closed before publishing."""

    state: OfflineOutboxClaimState
    claim: OfflineOutboxClaim | None = None
    rejection: OfflineOutboxClaimRejection | None = None

    def __post_init__(self) -> None:
        if self.state is OfflineOutboxClaimState.CLAIMED:
            if self.claim is None or self.rejection is not None:
                raise ValueError("claimed offline outbox result must contain only a claim")
        elif self.claim is not None or self.rejection is None:
            raise ValueError("rejected offline outbox result must contain only a rejection")


@dataclass(frozen=True)
class OfflineOutboxQuarantineReceipt:
    """Durable state returned for a new or idempotently repeated quarantine."""

    reconciliation_id: str
    reason: str
    legacy_lease: OfflineOutboxExpiredLease
    started_at: datetime
    outcome_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.reconciliation_id, str) or not self.reconciliation_id.strip():
            raise ValueError("offline quarantine receipt reconciliation_id must be non-empty")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("offline quarantine receipt reason must be non-empty")
        for name, value in (("started_at", self.started_at), ("outcome_at", self.outcome_at)):
            if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"offline quarantine receipt {name} must be timezone-aware")
        if self.outcome_at < self.started_at:
            raise ValueError("offline quarantine receipt outcome_at must not precede started_at")


@dataclass(frozen=True)
class OfflineOutboxQuarantineResult:
    """One exact expired effect was quarantined, already quarantined, or unchanged."""

    state: OfflineOutboxQuarantineState
    identity: OfflineOutboxIdentity
    receipt: OfflineOutboxQuarantineReceipt | None = None
    rejection: OfflineOutboxQuarantineRejection | None = None

    def __post_init__(self) -> None:
        terminal_states = {
            OfflineOutboxQuarantineState.QUARANTINED,
            OfflineOutboxQuarantineState.ALREADY_QUARANTINED,
        }
        if self.state in terminal_states:
            if self.rejection is not None or self.receipt is None:
                raise ValueError("quarantined offline outbox result requires only a durable receipt")
        elif self.receipt is not None or self.rejection is None:
            raise ValueError("rejected offline outbox result requires only a rejection")


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
    outbox_producer: str
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
            producer=self.outbox_producer,
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

    @staticmethod
    def _require_matching_audit_identity(
        rows: list[asyncpg.Record] | list[dict[str, Any]],
        *,
        message_id: str,
        topic: str,
        canonical_json: str,
    ) -> None:
        """Reject a stable ID that does not name exactly one immutable audit payload."""
        if len(rows) != 1:
            raise MessageIdentityConflict(
                f"event audit message_id {message_id!r} does not identify exactly one immutable payload"
            )
        row = rows[0]
        stored_payload = row["payload"]
        if isinstance(stored_payload, str):
            try:
                stored_payload = json.loads(stored_payload)
            except json.JSONDecodeError as exc:
                raise MessageIdentityConflict(
                    f"event audit message_id {message_id!r} has an unreadable payload"
                ) from exc
        try:
            stored_json = json.dumps(
                stored_payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise MessageIdentityConflict(
                f"event audit message_id {message_id!r} has a non-canonical payload"
            ) from exc
        if row["topic"] != topic or stored_json != canonical_json:
            raise MessageIdentityConflict(
                f"event audit message_id {message_id!r} was reused with different topic or payload"
            )

    async def append_payload_strict(
        self,
        topic: str,
        payload: dict[str, Any],
        canonical_json: str,
        payload_sha256: str,
        *,
        connection: asyncpg.Connection,
    ) -> bool:
        """Append a payload only when its globally stable ID has one exact meaning.

        ``event_audit`` historically keys rows by ``(produced_at, message_id)``.
        Offline repair needs a stronger invariant: one deterministic message ID
        must map to one topic and canonical payload across the whole audit log.
        The caller holds its producer lease and one transaction while it pairs
        this fact with the matching outbox row.
        """
        message_id = payload.get("message_id")
        if not isinstance(message_id, str) or not message_id.strip():
            raise ValueError("durable payload requires a non-empty message_id")
        if (
            not isinstance(canonical_json, str)
            or not isinstance(payload_sha256, str)
            or hashlib.sha256(canonical_json.encode("utf-8")).hexdigest() != payload_sha256
        ):
            raise ValueError("durable payload canonical identity is invalid")
        rows = await connection.fetch(
            """SELECT topic, payload FROM event_audit
                 WHERE message_id=$1 FOR UPDATE""",
            message_id,
        )
        if rows:
            self._require_matching_audit_identity(
                rows, message_id=message_id, topic=topic, canonical_json=canonical_json
            )
            return False
        inserted = await self.append_payload(topic, payload, connection=connection)
        if inserted:
            return True
        # A direct writer may have inserted the composite primary key between
        # the identity read and insert. Never assume that conflict is benign.
        rows = await connection.fetch(
            """SELECT topic, payload FROM event_audit
                 WHERE message_id=$1 FOR UPDATE""",
            message_id,
        )
        self._require_matching_audit_identity(
            rows, message_id=message_id, topic=topic, canonical_json=canonical_json
        )
        return False

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
        *,
        outbox_producer: str | None = None,
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
                unit = InboxTransaction(
                    self,
                    claim,
                    consumer,
                    message_id,
                    outbox_producer or consumer,
                    connection,
                )

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
        producer: str,
    ) -> bool:
        if not producer.strip():
            raise ValueError("outbox producer must not be empty")
        status = await connection.execute(
            """INSERT INTO message_outbox(message_id, topic, payload, payload_sha256, producer)
               VALUES ($1,$2,$3::jsonb,$4,$5) ON CONFLICT (message_id) DO NOTHING""",
            message_id,
            topic,
            payload,
            payload_sha256,
            producer,
        )
        if status.endswith("1"):
            return True
        existing = await connection.fetchrow(
            """SELECT topic, payload, payload_sha256, producer
                 FROM message_outbox WHERE message_id=$1""",
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
            or existing["producer"] != producer
            or existing_encoded != payload
            or existing["payload_sha256"] not in (None, payload_sha256)
        ):
            raise MessageIdentityConflict(
                f"outbox message_id {message_id!r} was reused with different topic, payload, or producer"
            )
        if existing["payload_sha256"] is None:
            await connection.execute(
                "UPDATE message_outbox SET payload_sha256=$2 WHERE message_id=$1",
                message_id,
                payload_sha256,
            )
        return False

    @staticmethod
    def _canonical_outbox_payload(payload: Any) -> tuple[dict[str, Any], str]:
        """Return the only accepted JSON representation of an outbox payload."""
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError as exc:
                raise ValueError("outbox payload is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("outbox payload must be a JSON object")
        try:
            canonical = json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("outbox payload is not canonicalizable") from exc
        return json.loads(canonical), hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @classmethod
    def _validate_exact_offline_outbox(
        cls,
        row: asyncpg.Record | dict[str, Any] | None,
        *,
        identity: OfflineOutboxIdentity,
        audit_rows: list[asyncpg.Record] | list[dict[str, Any]],
        has_unpublished_predecessor: bool,
        lease_requirement: str = "expired",
        require_reconciliation_clear: bool = True,
        require_no_unpublished_predecessor: bool = True,
        require_available: bool = True,
    ) -> OfflineOutboxClaimRejection | None:
        """Validate all recovery invariants before an external publish is possible."""
        if lease_requirement not in {"expired", "clear", "ignored"}:
            raise ValueError("offline outbox lease requirement must be expired, clear, or ignored")
        if row is None:
            return OfflineOutboxClaimRejection.NOT_FOUND
        if any(
            row[name] != expected
            for name, expected in (
                ("id", identity.id),
                ("producer", identity.producer),
                ("message_id", identity.message_id),
                ("topic", identity.topic),
                ("payload_sha256", identity.payload_sha256),
                ("publish_attempts", identity.publish_attempts),
            )
        ):
            return OfflineOutboxClaimRejection.IDENTITY_MISMATCH
        if row["published_at"] is not None:
            return OfflineOutboxClaimRejection.ALREADY_PUBLISHED
        if row["dead_lettered_at"] is not None:
            return OfflineOutboxClaimRejection.DEAD_LETTERED
        if lease_requirement == "expired" and not row["lease_expired"]:
            return OfflineOutboxClaimRejection.LEASE_NOT_EXPIRED
        if lease_requirement == "clear" and not row["lease_clear"]:
            return OfflineOutboxClaimRejection.LEASE_PRESENT
        if require_available and not row["available"]:
            return OfflineOutboxClaimRejection.NOT_YET_AVAILABLE
        if require_reconciliation_clear and row["reconciliation_state"] != "NONE":
            return OfflineOutboxClaimRejection.RECONCILIATION_NOT_CLEAR
        if require_no_unpublished_predecessor and has_unpublished_predecessor:
            return OfflineOutboxClaimRejection.EARLIER_UNPUBLISHED_PREDECESSOR
        try:
            payload, actual_sha256 = cls._canonical_outbox_payload(row["payload"])
        except ValueError:
            return OfflineOutboxClaimRejection.PAYLOAD_HASH_MISMATCH
        if actual_sha256 != identity.payload_sha256 or payload.get("message_id") != identity.message_id:
            return OfflineOutboxClaimRejection.PAYLOAD_HASH_MISMATCH
        if len(audit_rows) != 1:
            return OfflineOutboxClaimRejection.AUDIT_MISMATCH
        audit = audit_rows[0]
        try:
            audit_payload, audit_sha256 = cls._canonical_outbox_payload(audit["payload"])
        except ValueError:
            return OfflineOutboxClaimRejection.AUDIT_MISMATCH
        if (
            audit["topic"] != identity.topic
            or audit_sha256 != identity.payload_sha256
            or audit_payload != payload
            or audit_payload.get("message_id") != identity.message_id
        ):
            return OfflineOutboxClaimRejection.AUDIT_MISMATCH
        return None

    @staticmethod
    def _require_reconciliation_id(reconciliation_id: str) -> str:
        if (
            not isinstance(reconciliation_id, str)
            or not reconciliation_id.strip()
            or len(reconciliation_id) > 200
        ):
            raise ValueError("offline reconciliation_id must be a non-empty string of at most 200 characters")
        return reconciliation_id.strip()

    @staticmethod
    def _require_quarantine_reason(reason: str) -> str:
        if not isinstance(reason, str) or not reason.strip() or len(reason.strip()) > 3500:
            raise ValueError(
                "offline quarantine reason must be a non-empty string of at most 3500 characters"
            )
        return reason.strip()

    @staticmethod
    def _quarantine_evidence(reason: str, expired_lease: OfflineOutboxExpiredLease) -> str:
        """Canonical durable evidence for an otherwise-cleared historical lease."""
        lease_until = expired_lease.until.astimezone(UTC).isoformat(timespec="microseconds")
        evidence = json.dumps(
            {
                "expired_lease_owner_sha256": hashlib.sha256(expired_lease.owner.encode("utf-8")).hexdigest(),
                "expired_lease_until": lease_until,
                "reason": reason,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        if len(evidence) > 4000:
            raise ValueError("offline quarantine evidence exceeds the durable error limit")
        return evidence

    @staticmethod
    def _as_quarantine_rejection(
        rejection: OfflineOutboxClaimRejection,
    ) -> OfflineOutboxQuarantineRejection:
        """Keep the direct-quarantine API distinct without weakening its checks."""
        return OfflineOutboxQuarantineRejection(rejection.value)

    @staticmethod
    def _quarantine_receipt(
        *,
        reconciliation_id: str,
        reason: str,
        legacy_lease: OfflineOutboxExpiredLease,
        started_at: Any,
        outcome_at: Any,
    ) -> OfflineOutboxQuarantineReceipt | None:
        if (
            not isinstance(started_at, datetime)
            or started_at.tzinfo is None
            or started_at.utcoffset() is None
            or not isinstance(outcome_at, datetime)
            or outcome_at.tzinfo is None
            or outcome_at.utcoffset() is None
            or outcome_at < started_at
        ):
            return None
        return OfflineOutboxQuarantineReceipt(
            reconciliation_id=reconciliation_id,
            reason=reason,
            legacy_lease=legacy_lease,
            started_at=started_at,
            outcome_at=outcome_at,
        )

    async def quarantine_expired_outbox_exact(
        self,
        identity: OfflineOutboxIdentity,
        *,
        expired_lease: OfflineOutboxExpiredLease,
        reconciliation_id: str,
        reason: str,
    ) -> OfflineOutboxQuarantineResult:
        """Atomically quarantine one exact expired effect without a transport call.

        This is a DB-only operator primitive for a known expired lease.  It is
        valid only once migration 018 is present, locks the exact outbox and
        audit identity in one transaction, and binds the expired lease values
        captured in the inspected receipt. It never claims, publishes, retries,
        or increments ``publish_attempts``. A rejection performs no outbox
        mutation. ``last_error`` stores canonical reason plus lease hash/timestamp
        evidence, so the same durable outcome can be read idempotently only with
        the same exact reconciliation id, reason, and lease identity.
        """
        if not isinstance(expired_lease, OfflineOutboxExpiredLease):
            raise ValueError("offline quarantine requires an explicit expired lease identity")
        reconciliation_id = self._require_reconciliation_id(reconciliation_id)
        reason = self._require_quarantine_reason(reason)
        evidence = self._quarantine_evidence(reason, expired_lease)

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                migration_table_exists = await connection.fetchval(
                    "SELECT to_regclass('schema_migrations') IS NOT NULL"
                )
                if not migration_table_exists:
                    return OfflineOutboxQuarantineResult(
                        state=OfflineOutboxQuarantineState.REJECTED,
                        identity=identity,
                        rejection=OfflineOutboxQuarantineRejection.MIGRATION_018_REQUIRED,
                    )
                migration_applied = await connection.fetchval(
                    "SELECT 1 FROM schema_migrations WHERE version=$1",
                    OFFLINE_OUTBOX_RECONCILIATION_MIGRATION,
                )
                if not migration_applied:
                    return OfflineOutboxQuarantineResult(
                        state=OfflineOutboxQuarantineState.REJECTED,
                        identity=identity,
                        rejection=OfflineOutboxQuarantineRejection.MIGRATION_018_REQUIRED,
                    )

                # This makes separately deployed operator tools serialize
                # before reading or mutating the same immutable effect.
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    f"offline-outbox-quarantine:{identity.id}",
                )
                row = await connection.fetchrow(
                    """SELECT id, producer, message_id, topic, payload, payload_sha256,
                              publish_attempts, published_at, dead_lettered_at,
                              lease_owner, lease_until, last_error,
                              (lease_until IS NOT NULL AND lease_until < now()) AS lease_expired,
                              (available_at <= now()) AS available, reconciliation_state,
                              reconciliation_id, reconciliation_started_at,
                              reconciliation_outcome_at
                         FROM message_outbox
                        WHERE id=$1
                        FOR UPDATE""",
                    identity.id,
                )
                audit_rows = await connection.fetch(
                    """SELECT topic, payload
                         FROM event_audit
                        WHERE message_id=$1
                        FOR UPDATE""",
                    identity.message_id,
                )
                rejection = self._validate_exact_offline_outbox(
                    row,
                    identity=identity,
                    audit_rows=audit_rows,
                    # A direct quarantine only freezes this ambiguous effect;
                    # historical same-producer backlog belongs in the signed
                    # inspection receipt, not in a condition that could stop
                    # the safety transition.
                    has_unpublished_predecessor=False,
                    lease_requirement="ignored",
                    require_reconciliation_clear=False,
                    require_no_unpublished_predecessor=False,
                    require_available=False,
                )
                if rejection is not None:
                    return OfflineOutboxQuarantineResult(
                        state=OfflineOutboxQuarantineState.REJECTED,
                        identity=identity,
                        rejection=self._as_quarantine_rejection(rejection),
                    )

                if row is None:
                    raise RuntimeError("validated offline outbox row unexpectedly disappeared")
                if row["reconciliation_state"] == "PUBLISH_OUTCOME_UNKNOWN":
                    receipt = self._quarantine_receipt(
                        reconciliation_id=reconciliation_id,
                        reason=reason,
                        legacy_lease=expired_lease,
                        started_at=row["reconciliation_started_at"],
                        outcome_at=row["reconciliation_outcome_at"],
                    )
                    if (
                        receipt is not None
                        and row["reconciliation_id"] == reconciliation_id
                        and row["last_error"] == evidence
                        and row["lease_owner"] is None
                        and row["lease_until"] is None
                    ):
                        return OfflineOutboxQuarantineResult(
                            state=OfflineOutboxQuarantineState.ALREADY_QUARANTINED,
                            identity=identity,
                            receipt=receipt,
                        )
                    return OfflineOutboxQuarantineResult(
                        state=OfflineOutboxQuarantineState.REJECTED,
                        identity=identity,
                        rejection=OfflineOutboxQuarantineRejection.RECONCILIATION_NOT_CLEAR,
                    )
                if row["reconciliation_state"] != "NONE":
                    return OfflineOutboxQuarantineResult(
                        state=OfflineOutboxQuarantineState.REJECTED,
                        identity=identity,
                        rejection=OfflineOutboxQuarantineRejection.RECONCILIATION_NOT_CLEAR,
                    )
                if not row["available"]:
                    return OfflineOutboxQuarantineResult(
                        state=OfflineOutboxQuarantineState.REJECTED,
                        identity=identity,
                        rejection=OfflineOutboxQuarantineRejection.NOT_YET_AVAILABLE,
                    )
                if not row["lease_expired"]:
                    return OfflineOutboxQuarantineResult(
                        state=OfflineOutboxQuarantineState.REJECTED,
                        identity=identity,
                        rejection=OfflineOutboxQuarantineRejection.LEASE_NOT_EXPIRED,
                    )
                if row["lease_owner"] != expired_lease.owner or row["lease_until"] != expired_lease.until:
                    return OfflineOutboxQuarantineResult(
                        state=OfflineOutboxQuarantineState.REJECTED,
                        identity=identity,
                        rejection=OfflineOutboxQuarantineRejection.LEASE_IDENTITY_MISMATCH,
                    )

                quarantined = await connection.fetchrow(
                    """UPDATE message_outbox
                           SET lease_until=NULL,
                               lease_owner=NULL,
                               last_error=$9,
                               reconciliation_state='PUBLISH_OUTCOME_UNKNOWN',
                               reconciliation_id=$10,
                               reconciliation_started_at=now(),
                               reconciliation_outcome_at=now()
                         WHERE id=$1
                           AND producer=$2
                           AND message_id=$3
                           AND topic=$4
                           AND payload_sha256=$5
                           AND publish_attempts=$6
                           AND lease_owner=$7
                           AND lease_until=$8
                           AND published_at IS NULL
                           AND dead_lettered_at IS NULL
                           AND available_at <= now()
                           AND lease_until < now()
                           AND reconciliation_state='NONE'
                     RETURNING reconciliation_id, last_error,
                               reconciliation_started_at, reconciliation_outcome_at""",
                    identity.id,
                    identity.producer,
                    identity.message_id,
                    identity.topic,
                    identity.payload_sha256,
                    identity.publish_attempts,
                    expired_lease.owner,
                    expired_lease.until,
                    evidence,
                    reconciliation_id,
                )
                if quarantined is None:
                    return OfflineOutboxQuarantineResult(
                        state=OfflineOutboxQuarantineState.REJECTED,
                        identity=identity,
                        rejection=OfflineOutboxQuarantineRejection.RACE_LOST,
                    )
                receipt = self._quarantine_receipt(
                    reconciliation_id=quarantined["reconciliation_id"],
                    reason=reason,
                    legacy_lease=expired_lease,
                    started_at=quarantined["reconciliation_started_at"],
                    outcome_at=quarantined["reconciliation_outcome_at"],
                )
                if receipt is None:
                    raise RuntimeError("quarantine update returned an incomplete durable receipt")
                if (
                    quarantined["reconciliation_id"] != reconciliation_id
                    or quarantined["last_error"] != evidence
                ):
                    raise RuntimeError("quarantine update returned mismatched durable evidence")
                return OfflineOutboxQuarantineResult(
                    state=OfflineOutboxQuarantineState.QUARANTINED,
                    identity=identity,
                    receipt=receipt,
                )

    async def claim_expired_outbox_exact(
        self,
        identity: OfflineOutboxIdentity,
        *,
        reconciliation_id: str,
        lease: timedelta = timedelta(minutes=5),
    ) -> OfflineOutboxClaimResult:
        """Lease exactly one expired row after exhaustive, immutable verification.

        This is intentionally incompatible with the normal dispatcher: it does
        not select a producer head or retry a queue. The caller must know every
        immutable field of one expired row ahead of time, and the durable
        ``PUBLISHING`` state blocks automatic dispatch after this method commits.
        """
        reconciliation_id = self._require_reconciliation_id(reconciliation_id)
        if not isinstance(lease, timedelta) or not timedelta(0) < lease <= timedelta(minutes=5):
            raise ValueError("offline exact outbox lease must be greater than zero and at most five minutes")

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # The row lock protects the record itself; this named advisory
                # lock makes independently deployed recovery tools serialize
                # before they can inspect or mutate the same row.
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    f"offline-outbox-reconciliation:{identity.id}",
                )
                row = await connection.fetchrow(
                    """SELECT id, producer, message_id, topic, payload, payload_sha256,
                              publish_attempts, published_at, dead_lettered_at,
                              (lease_until IS NOT NULL AND lease_until < now()) AS lease_expired,
                              (available_at <= now()) AS available, reconciliation_state
                         FROM message_outbox
                        WHERE id=$1
                        FOR UPDATE""",
                    identity.id,
                )
                audit_rows = await connection.fetch(
                    """SELECT topic, payload
                         FROM event_audit
                        WHERE message_id=$1
                        FOR UPDATE""",
                    identity.message_id,
                )
                predecessor = await connection.fetchrow(
                    """SELECT id
                         FROM message_outbox
                        WHERE producer=$1
                          AND id < $2
                          AND published_at IS NULL
                          AND dead_lettered_at IS NULL
                        ORDER BY id
                        LIMIT 1
                        FOR UPDATE""",
                    identity.producer,
                    identity.id,
                )
                rejection = self._validate_exact_offline_outbox(
                    row,
                    identity=identity,
                    audit_rows=audit_rows,
                    has_unpublished_predecessor=predecessor is not None,
                )
                if rejection is not None:
                    return OfflineOutboxClaimResult(
                        state=OfflineOutboxClaimState.REJECTED,
                        rejection=rejection,
                    )
                claimed = await connection.fetchrow(
                    """UPDATE message_outbox
                           SET lease_owner=$1,
                               lease_until=now()+$2::interval,
                               publish_attempts=publish_attempts+1,
                               reconciliation_state='PUBLISHING',
                               reconciliation_id=$1,
                               reconciliation_started_at=now(),
                               reconciliation_outcome_at=NULL,
                               last_error=NULL
                         WHERE id=$3
                           AND producer=$4
                           AND message_id=$5
                           AND topic=$6
                           AND payload_sha256=$7
                           AND publish_attempts=$8
                           AND published_at IS NULL
                           AND dead_lettered_at IS NULL
                           AND available_at <= now()
                           AND lease_until < now()
                           AND reconciliation_state='NONE'
                     RETURNING publish_attempts""",
                    reconciliation_id,
                    lease,
                    identity.id,
                    identity.producer,
                    identity.message_id,
                    identity.topic,
                    identity.payload_sha256,
                    identity.publish_attempts,
                )
                if claimed is None:
                    return OfflineOutboxClaimResult(
                        state=OfflineOutboxClaimState.REJECTED,
                        rejection=OfflineOutboxClaimRejection.RACE_LOST,
                    )
                payload, _sha256 = self._canonical_outbox_payload(row["payload"])
                return OfflineOutboxClaimResult(
                    state=OfflineOutboxClaimState.CLAIMED,
                    claim=OfflineOutboxClaim(
                        identity=identity,
                        payload=payload,
                        reconciliation_id=reconciliation_id,
                        claimed_publish_attempts=claimed["publish_attempts"],
                    ),
                )

    async def claim_ready_outbox_exact(
        self,
        identity: OfflineOutboxIdentity,
        *,
        reconciliation_id: str,
        lease: timedelta = timedelta(minutes=5),
    ) -> OfflineOutboxClaimResult:
        """Claim one exact, unleased producer head for a signed prefix drain.

        This deliberately does *not* reclaim an expired ordinary-dispatch
        lease.  An expired lease has an ambiguous delivery history and must go
        through the separately authorized one-row reconciliation procedure.
        The claimed row enters ``PUBLISHING`` before the caller can reach its
        transport; a crash or uncertain ACK therefore remains fail-closed
        rather than returning to the normal dispatcher.
        """
        reconciliation_id = self._require_reconciliation_id(reconciliation_id)
        if not isinstance(lease, timedelta) or not timedelta(0) < lease <= timedelta(minutes=5):
            raise ValueError("offline exact outbox lease must be greater than zero and at most five minutes")

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    f"offline-outbox-reconciliation:{identity.id}",
                )
                row = await connection.fetchrow(
                    """SELECT id, producer, message_id, topic, payload, payload_sha256,
                              publish_attempts, published_at, dead_lettered_at,
                              (lease_until IS NULL) AS lease_clear,
                              (available_at <= now()) AS available, reconciliation_state
                         FROM message_outbox
                        WHERE id=$1
                        FOR UPDATE""",
                    identity.id,
                )
                audit_rows = await connection.fetch(
                    """SELECT topic, payload
                         FROM event_audit
                        WHERE message_id=$1
                        FOR UPDATE""",
                    identity.message_id,
                )
                predecessor = await connection.fetchrow(
                    """SELECT id
                         FROM message_outbox
                        WHERE producer=$1
                          AND id < $2
                          AND published_at IS NULL
                          AND dead_lettered_at IS NULL
                        ORDER BY id
                        LIMIT 1
                        FOR UPDATE""",
                    identity.producer,
                    identity.id,
                )
                rejection = self._validate_exact_offline_outbox(
                    row,
                    identity=identity,
                    audit_rows=audit_rows,
                    has_unpublished_predecessor=predecessor is not None,
                    lease_requirement="clear",
                )
                if rejection is not None:
                    return OfflineOutboxClaimResult(
                        state=OfflineOutboxClaimState.REJECTED,
                        rejection=rejection,
                    )
                claimed = await connection.fetchrow(
                    """UPDATE message_outbox
                           SET lease_owner=$1,
                               lease_until=now()+$2::interval,
                               publish_attempts=publish_attempts+1,
                               reconciliation_state='PUBLISHING',
                               reconciliation_id=$1,
                               reconciliation_started_at=now(),
                               reconciliation_outcome_at=NULL,
                               last_error=NULL
                         WHERE id=$3
                           AND producer=$4
                           AND message_id=$5
                           AND topic=$6
                           AND payload_sha256=$7
                           AND publish_attempts=$8
                           AND published_at IS NULL
                           AND dead_lettered_at IS NULL
                           AND available_at <= now()
                           AND lease_until IS NULL
                           AND reconciliation_state='NONE'
                     RETURNING publish_attempts""",
                    reconciliation_id,
                    lease,
                    identity.id,
                    identity.producer,
                    identity.message_id,
                    identity.topic,
                    identity.payload_sha256,
                    identity.publish_attempts,
                )
                if claimed is None:
                    return OfflineOutboxClaimResult(
                        state=OfflineOutboxClaimState.REJECTED,
                        rejection=OfflineOutboxClaimRejection.RACE_LOST,
                    )
                payload, _sha256 = self._canonical_outbox_payload(row["payload"])
                return OfflineOutboxClaimResult(
                    state=OfflineOutboxClaimState.CLAIMED,
                    claim=OfflineOutboxClaim(
                        identity=identity,
                        payload=payload,
                        reconciliation_id=reconciliation_id,
                        claimed_publish_attempts=claimed["publish_attempts"],
                    ),
                )

    async def acknowledge_exact_outbox_publish(
        self,
        identity: OfflineOutboxIdentity,
        *,
        reconciliation_id: str,
    ) -> bool:
        """Durably ACK one already-published exact recovery row, never a queue batch."""
        reconciliation_id = self._require_reconciliation_id(reconciliation_id)
        status = await self.pool.execute(
            """UPDATE message_outbox
                   SET published_at=now(),
                       lease_until=NULL,
                       lease_owner=NULL,
                       last_error=NULL,
                       reconciliation_state='ACKNOWLEDGED',
                       reconciliation_outcome_at=now()
                 WHERE id=$1
                   AND producer=$2
                   AND message_id=$3
                   AND topic=$4
                   AND payload_sha256=$5
                   AND publish_attempts=$6
                   AND reconciliation_state='PUBLISHING'
                   AND reconciliation_id=$7
                   AND published_at IS NULL
                   AND dead_lettered_at IS NULL""",
            identity.id,
            identity.producer,
            identity.message_id,
            identity.topic,
            identity.payload_sha256,
            identity.publish_attempts + 1,
            reconciliation_id,
        )
        return status.endswith("1")

    async def mark_exact_outbox_publish_outcome_unknown(
        self,
        identity: OfflineOutboxIdentity,
        *,
        reconciliation_id: str,
        reason: str,
    ) -> bool:
        """Permanently block automatic redispatch when a publish result is ambiguous."""
        reconciliation_id = self._require_reconciliation_id(reconciliation_id)
        status = await self.pool.execute(
            """UPDATE message_outbox
                   SET lease_until=NULL,
                       lease_owner=NULL,
                       last_error=$8,
                       reconciliation_state='PUBLISH_OUTCOME_UNKNOWN',
                       reconciliation_outcome_at=now()
                 WHERE id=$1
                   AND producer=$2
                   AND message_id=$3
                   AND topic=$4
                   AND payload_sha256=$5
                   AND publish_attempts=$6
                   AND reconciliation_state='PUBLISHING'
                   AND reconciliation_id=$7
                   AND published_at IS NULL
                   AND dead_lettered_at IS NULL""",
            identity.id,
            identity.producer,
            identity.message_id,
            identity.topic,
            identity.payload_sha256,
            identity.publish_attempts + 1,
            reconciliation_id,
            reason[:4000],
        )
        return status.endswith("1")

    async def pending_outbox(self, limit: int = 100) -> list[asyncpg.Record]:
        return await self.pool.fetch(
            """SELECT id, producer, message_id, topic, payload FROM message_outbox
               WHERE published_at IS NULL ORDER BY id LIMIT $1""",
            limit,
        )

    async def claim_outbox(
        self,
        worker_id: str,
        *,
        producer: str,
        limit: int = 100,
        lease: timedelta = timedelta(seconds=30),
    ) -> list[OutboxRecord]:
        if not producer.strip():
            raise ValueError("outbox producer must not be empty")
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(
                    """WITH candidates AS (
                           SELECT outbox.id
                             FROM message_outbox AS outbox
                            WHERE outbox.producer=$4
                              AND outbox.published_at IS NULL
                              AND outbox.dead_lettered_at IS NULL
                              AND outbox.reconciliation_state='NONE'
                              AND outbox.available_at <= now()
                              AND (outbox.lease_until IS NULL OR outbox.lease_until < now())
                              AND NOT EXISTS (
                                  SELECT 1
                                    FROM message_outbox AS prior
                                   WHERE prior.producer=outbox.producer
                                     AND prior.id < outbox.id
                                     AND prior.published_at IS NULL
                                     AND prior.dead_lettered_at IS NULL
                              )
                            ORDER BY outbox.id
                            FOR UPDATE OF outbox SKIP LOCKED
                            LIMIT LEAST($2, 1)
                       ), claimed AS (
                           UPDATE message_outbox AS outbox
                           SET lease_owner=$1, lease_until=now()+$3::interval,
                               publish_attempts=publish_attempts+1
                           FROM candidates
                           WHERE outbox.id=candidates.id
                           RETURNING outbox.id, outbox.producer, outbox.message_id,
                                     outbox.topic, outbox.payload, outbox.payload_sha256,
                                     outbox.publish_attempts
                       )
                       SELECT * FROM claimed ORDER BY id""",
                    worker_id,
                    limit,
                    lease,
                    producer,
                )
        # One producer head is leased at a time.  An earlier retry therefore
        # cannot be overtaken, and two replicas cannot publish adjacent causal
        # messages concurrently.
        return [
            OutboxRecord(
                id=row["id"],
                producer=row["producer"],
                message_id=row["message_id"],
                topic=row["topic"],
                payload=(json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]),
                payload_sha256=row["payload_sha256"],
                publish_attempts=row["publish_attempts"],
            )
            for row in sorted(rows, key=lambda row: row["id"])
        ]

    async def mark_published(self, row_id: int, worker_id: str) -> bool:
        status = await self.pool.execute(
            """UPDATE message_outbox SET published_at=now(), lease_until=NULL, lease_owner=NULL,
               last_error=NULL WHERE id=$1 AND lease_owner=$2 AND published_at IS NULL
               AND reconciliation_state='NONE'""",
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
               WHERE id=$1 AND lease_owner=$2 AND published_at IS NULL
                 AND reconciliation_state='NONE'""",
            row_id,
            worker_id,
            error[:4000],
            retry_after,
            max_attempts,
        )
        return status.endswith("1")
