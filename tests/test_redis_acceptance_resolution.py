"""Payload-free Redis acceptance resolution contracts; no database or Redis I/O."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import pytest

from kairos_persistence.redis_acceptance_evidence import (
    RedisAcceptanceEvidenceError,
    RedisAcceptanceEvidenceV1,
)
from kairos_persistence.repository import (
    OFFLINE_OUTBOX_RECONCILIATION_MIGRATION,
    AuditRepository,
    OfflineOutboxIdentity,
    OfflineOutboxTransportResolutionRejection,
    OfflineOutboxTransportResolutionState,
)

_PAYLOAD = {"message_id": "logical-effect-42", "value": 1}
_CANONICAL_PAYLOAD = '{"message_id":"logical-effect-42","value":1}'
_PAYLOAD_SHA = hashlib.sha256(_CANONICAL_PAYLOAD.encode("utf-8")).hexdigest()
_STARTED = datetime(2026, 10, 10, 12, 0, tzinfo=UTC)
_OUTCOME = datetime(2026, 10, 10, 12, 1, tzinfo=UTC)
_PUBLISHED = datetime(2026, 10, 10, 12, 2, tzinfo=UTC)
_LEGACY_ERROR = json.dumps(
    {
        "expired_lease_owner_sha256": "a" * 64,
        "expired_lease_until": "2026-10-10T11:59:00.000000+00:00",
        "reason": "operator quarantine",
    },
    sort_keys=True,
    separators=(",", ":"),
)


def _identity() -> OfflineOutboxIdentity:
    return OfflineOutboxIdentity(
        id=42,
        producer="quant",
        message_id="logical-effect-42",
        topic="kairos.market.closed_bar.v1",
        payload_sha256=_PAYLOAD_SHA,
        publish_attempts=3,
    )


def _evidence(
    *,
    match_count: int = 1,
    stream_ids: tuple[str, ...] = ("1791624000000-0",),
    **updates: Any,
) -> RedisAcceptanceEvidenceV1:
    values: dict[str, Any] = {
        "redis_server_run_id": "b" * 40,
        "probe_run_id": "probe-20261010-a",
        "probed_at_utc": _PUBLISHED,
        "topic": _identity().topic,
        "message_id": _identity().message_id,
        "canonical_payload_sha256": _PAYLOAD_SHA,
        "match_count": match_count,
        "stream_ids": stream_ids,
    }
    values.update(updates)
    return RedisAcceptanceEvidenceV1(**values)


def _row(**updates: Any) -> dict[str, Any]:
    identity = _identity()
    row: dict[str, Any] = {
        "id": identity.id,
        "producer": identity.producer,
        "message_id": identity.message_id,
        "topic": identity.topic,
        "payload": _PAYLOAD,
        "payload_sha256": identity.payload_sha256,
        "publish_attempts": identity.publish_attempts,
        "published_at": None,
        "dead_lettered_at": None,
        "lease_owner": None,
        "lease_until": None,
        "last_error": _LEGACY_ERROR,
        "reconciliation_state": "PUBLISH_OUTCOME_UNKNOWN",
        "reconciliation_id": "reconcile-42",
        "reconciliation_started_at": _STARTED,
        "reconciliation_outcome_at": _OUTCOME,
    }
    row.update(updates)
    return row


class _Transaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


class _Connection:
    def __init__(
        self,
        *,
        migration_applied: bool = True,
        row: dict[str, Any] | None = None,
        audit_rows: list[dict[str, Any]] | None = None,
        update_result: dict[str, Any] | None | object = ...,
    ) -> None:
        self.migration_applied = migration_applied
        self.row = _row() if row is None else row
        self.audit_rows = (
            [{"topic": _identity().topic, "payload": _PAYLOAD}] if audit_rows is None else audit_rows
        )
        self.update_result = update_result
        self.calls: list[tuple[str, str, tuple[Any, ...]]] = []
        self.updates = 0

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def fetchval(self, sql: str, *args: Any) -> bool | int | None:
        compact = " ".join(sql.split())
        self.calls.append(("fetchval", compact, args))
        if "to_regclass('schema_migrations')" in compact:
            return True
        if "FROM schema_migrations" in compact:
            assert args == (OFFLINE_OUTBOX_RECONCILIATION_MIGRATION,)
            return 1 if self.migration_applied else None
        raise AssertionError(compact)

    async def execute(self, sql: str, *args: Any) -> str:
        self.calls.append(("execute", " ".join(sql.split()), args))
        return "SELECT 1"

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        compact = " ".join(sql.split())
        self.calls.append(("fetchrow", compact, args))
        if compact.startswith("UPDATE message_outbox"):
            self.updates += 1
            if self.update_result is not ...:
                return self.update_result  # type: ignore[return-value]
            self.row["published_at"] = _PUBLISHED
            self.row["reconciliation_state"] = "ACKNOWLEDGED"
            self.row["last_error"] = args[6]
            return {"published_at": _PUBLISHED}
        if "FROM message_outbox" in compact:
            return self.row
        raise AssertionError(compact)

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        compact = " ".join(sql.split())
        self.calls.append(("fetch", compact, args))
        assert "FROM event_audit" in compact
        return self.audit_rows


class _Pool:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection
        self.acquisitions = 0

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[_Connection]:
        self.acquisitions += 1
        yield self.connection


@pytest.mark.asyncio
async def test_unique_positive_match_resolves_unknown_and_preserves_attempts_and_quarantine_metadata():
    connection = _Connection()
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]

    result = await repository.resolve_unknown_outbox_from_redis_evidence(
        _identity(), reconciliation_id="reconcile-42", evidence=_evidence()
    )

    assert result.state is OfflineOutboxTransportResolutionState.RESOLVED
    assert result.receipt is not None
    assert result.receipt.evidence == _evidence()
    assert result.receipt.resolved_at == _PUBLISHED
    assert connection.updates == 1
    assert connection.row["published_at"] == _PUBLISHED
    assert connection.row["reconciliation_state"] == "ACKNOWLEDGED"
    assert connection.row["publish_attempts"] == 3
    assert connection.row["reconciliation_started_at"] == _STARTED
    assert connection.row["reconciliation_outcome_at"] == _OUTCOME
    stored = json.loads(connection.row["last_error"])
    assert stored["legacy_last_error"] == _LEGACY_ERROR
    assert stored["evidence"] == _evidence().document()
    assert stored["evidence_sha256"] == _evidence().sha256
    assert "payload" not in stored and "credentials" not in stored
    update = next(sql for _, sql, _ in connection.calls if sql.startswith("UPDATE message_outbox"))
    assert "publish_attempts=publish_attempts+1" not in update
    assert "reconciliation_outcome_at" not in update


@pytest.mark.asyncio
async def test_same_positive_evidence_is_idempotent_but_changed_evidence_is_rejected():
    connection = _Connection()
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]
    await repository.resolve_unknown_outbox_from_redis_evidence(
        _identity(), reconciliation_id="reconcile-42", evidence=_evidence()
    )

    repeated = await repository.resolve_unknown_outbox_from_redis_evidence(
        _identity(), reconciliation_id="reconcile-42", evidence=_evidence()
    )
    changed = await repository.resolve_unknown_outbox_from_redis_evidence(
        _identity(),
        reconciliation_id="reconcile-42",
        evidence=_evidence(stream_ids=("1791624000001-0",)),
    )

    assert repeated.state is OfflineOutboxTransportResolutionState.ALREADY_RESOLVED
    assert repeated.receipt is not None and repeated.receipt.resolved_at == _PUBLISHED
    assert changed.state is OfflineOutboxTransportResolutionState.REJECTED
    assert changed.rejection is OfflineOutboxTransportResolutionRejection.EVIDENCE_MISMATCH
    assert connection.updates == 1


@pytest.mark.asyncio
async def test_zero_match_remains_unknown_without_database_access_or_retry():
    pool = _Pool(_Connection())
    repository = AuditRepository(pool)  # type: ignore[arg-type]
    evidence = _evidence(match_count=0, stream_ids=())

    result = await repository.resolve_unknown_outbox_from_redis_evidence(
        _identity(), reconciliation_id="reconcile-42", evidence=evidence
    )

    assert result.state is OfflineOutboxTransportResolutionState.NO_MATCH_REMAINS_UNKNOWN
    assert result.receipt is None
    assert pool.acquisitions == 0


@pytest.mark.asyncio
async def test_multiple_matches_are_explicit_conflict_without_database_access_or_replay():
    pool = _Pool(_Connection())
    repository = AuditRepository(pool)  # type: ignore[arg-type]
    evidence = _evidence(match_count=2, stream_ids=("1791624000000-0", "1791624000001-0"))

    result = await repository.resolve_unknown_outbox_from_redis_evidence(
        _identity(), reconciliation_id="reconcile-42", evidence=evidence
    )

    assert result.state is OfflineOutboxTransportResolutionState.MULTIPLE_MATCHES_CONFLICT
    assert result.receipt is None
    assert pool.acquisitions == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row_updates", "audit_rows", "reconciliation_id", "expected"),
    [
        (
            {"producer": "other"},
            None,
            "reconcile-42",
            OfflineOutboxTransportResolutionRejection.IDENTITY_MISMATCH,
        ),
        (
            {"reconciliation_id": "other"},
            None,
            "reconcile-42",
            OfflineOutboxTransportResolutionRejection.RECONCILIATION_ID_MISMATCH,
        ),
        (
            {"reconciliation_state": "NONE"},
            None,
            "reconcile-42",
            OfflineOutboxTransportResolutionRejection.RECONCILIATION_NOT_UNKNOWN,
        ),
        (
            {"published_at": _PUBLISHED},
            None,
            "reconcile-42",
            OfflineOutboxTransportResolutionRejection.INCONSISTENT_PUBLISHED_STATE,
        ),
        ({}, [], "reconcile-42", OfflineOutboxTransportResolutionRejection.AUDIT_MISMATCH),
        (
            {},
            [{"topic": "other", "payload": _PAYLOAD}],
            "reconcile-42",
            OfflineOutboxTransportResolutionRejection.AUDIT_MISMATCH,
        ),
        (
            {"payload": {"message_id": "logical-effect-42", "value": 2}},
            None,
            "reconcile-42",
            OfflineOutboxTransportResolutionRejection.PAYLOAD_HASH_MISMATCH,
        ),
        ({}, None, "wrong-id", OfflineOutboxTransportResolutionRejection.RECONCILIATION_ID_MISMATCH),
    ],
)
async def test_positive_match_rejects_changed_row_audit_or_reconciliation_state_without_update(
    row_updates: dict[str, Any],
    audit_rows: list[dict[str, Any]] | None,
    reconciliation_id: str,
    expected: OfflineOutboxTransportResolutionRejection,
) -> None:
    connection = _Connection(
        row=_row(**row_updates),
        audit_rows=None if audit_rows is None else audit_rows,
    )
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]

    result = await repository.resolve_unknown_outbox_from_redis_evidence(
        _identity(), reconciliation_id=reconciliation_id, evidence=_evidence()
    )

    assert result.state is OfflineOutboxTransportResolutionState.REJECTED
    assert result.rejection is expected
    assert connection.updates == 0


@pytest.mark.asyncio
async def test_missing_018_never_reads_or_changes_outbox():
    connection = _Connection(migration_applied=False)
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]

    result = await repository.resolve_unknown_outbox_from_redis_evidence(
        _identity(), reconciliation_id="reconcile-42", evidence=_evidence()
    )

    assert result.rejection is OfflineOutboxTransportResolutionRejection.MIGRATION_018_REQUIRED
    assert connection.updates == 0
    assert not any(
        "FROM message_outbox" in sql or "FROM event_audit" in sql for _, sql, _ in connection.calls
    )


def test_evidence_is_bounded_literal_and_never_contains_a_payload():
    evidence = _evidence()
    assert set(evidence.document()) == {
        "schema",
        "redis_server_run_id",
        "probe_run_id",
        "probed_at_utc",
        "topic",
        "message_id",
        "canonical_payload_sha256",
        "match_count",
        "stream_ids",
    }
    assert _CANONICAL_PAYLOAD not in evidence.canonical_json()
    for updates in (
        {"redis_server_run_id": "not-a-redis-run-id"},
        {"probe_run_id": "redis://user:password@host/0"},
        {"message_id": ""},
        {"canonical_payload_sha256": "A" * 64},
        {"probed_at_utc": datetime(2026, 10, 10, 12, 0)},
        {"match_count": 0, "stream_ids": ("1-0",)},
        {"match_count": 2, "stream_ids": ("1-0",)},
        {"match_count": 1, "stream_ids": ("bad",)},
        {"match_count": 1, "stream_ids": (["1-0"],)},
    ):
        with pytest.raises(RedisAcceptanceEvidenceError):
            _evidence(**updates)


@pytest.mark.parametrize(
    "row_updates",
    [
        {"dead_lettered_at": datetime(2026, 10, 10, 12, 0, tzinfo=UTC)},
        {"lease_owner": "stale-worker"},
        {"lease_until": datetime(2026, 10, 10, 12, 0, tzinfo=UTC)},
    ],
)
@pytest.mark.asyncio
async def test_acknowledged_rows_with_conflicting_terminal_metadata_are_rejected(row_updates):
    connection = _Connection()
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]
    evidence = _evidence()
    resolved = await repository.resolve_unknown_outbox_from_redis_evidence(
        _identity(), reconciliation_id="reconcile-42", evidence=evidence
    )
    assert resolved.state is OfflineOutboxTransportResolutionState.RESOLVED
    connection.row.update(row_updates)

    result = await repository.resolve_unknown_outbox_from_redis_evidence(
        _identity(), reconciliation_id="reconcile-42", evidence=evidence
    )

    assert result.state is OfflineOutboxTransportResolutionState.REJECTED
    assert result.rejection is OfflineOutboxTransportResolutionRejection.INCONSISTENT_PUBLISHED_STATE
    assert connection.updates == 1
