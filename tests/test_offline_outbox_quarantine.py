from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import pytest

from kairos_persistence.repository import (
    OFFLINE_OUTBOX_RECONCILIATION_MIGRATION,
    AuditRepository,
    OfflineOutboxExpiredLease,
    OfflineOutboxIdentity,
    OfflineOutboxQuarantineRejection,
    OfflineOutboxQuarantineState,
)

_DEFAULT = object()
_EXPIRED_AT = datetime(2026, 9, 20, 8, 0, tzinfo=UTC)
_QUARANTINED_AT = datetime(2026, 9, 20, 9, 0, tzinfo=UTC)


def _payload() -> dict[str, Any]:
    return {"message_id": "expired-effect-42", "value": 1}


def _identity() -> OfflineOutboxIdentity:
    encoded = '{"message_id":"expired-effect-42","value":1}'
    return OfflineOutboxIdentity(
        id=42,
        producer="recovery-producer",
        message_id="expired-effect-42",
        topic="kairos.recovery.v1",
        payload_sha256=hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
        publish_attempts=3,
    )


def _expired_lease() -> OfflineOutboxExpiredLease:
    return OfflineOutboxExpiredLease(owner="historical-worker-7", until=_EXPIRED_AT)


def _row(**overrides: Any) -> dict[str, Any]:
    identity = _identity()
    row: dict[str, Any] = {
        "id": identity.id,
        "producer": identity.producer,
        "message_id": identity.message_id,
        "topic": identity.topic,
        "payload": _payload(),
        "payload_sha256": identity.payload_sha256,
        "publish_attempts": identity.publish_attempts,
        "published_at": None,
        "dead_lettered_at": None,
        "lease_owner": _expired_lease().owner,
        "lease_until": _expired_lease().until,
        "last_error": None,
        "lease_expired": True,
        "available": True,
        "reconciliation_state": "NONE",
        "reconciliation_id": None,
        "reconciliation_started_at": None,
        "reconciliation_outcome_at": None,
    }
    row.update(overrides)
    return row


def _audit(**overrides: Any) -> dict[str, Any]:
    audit: dict[str, Any] = {"topic": _identity().topic, "payload": _payload()}
    audit.update(overrides)
    return audit


def _already_quarantined_row(
    *,
    reconciliation_id: str = "recovery-quarantine-1",
    reason: str = "manual safety quarantine",
) -> dict[str, Any]:
    return _row(
        lease_owner=None,
        lease_until=None,
        lease_expired=False,
        last_error=AuditRepository._quarantine_evidence(reason, _expired_lease()),
        reconciliation_state="PUBLISH_OUTCOME_UNKNOWN",
        reconciliation_id=reconciliation_id,
        reconciliation_started_at=_QUARANTINED_AT,
        reconciliation_outcome_at=_QUARANTINED_AT,
    )


class _Transaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


class _QuarantineConnection:
    def __init__(
        self,
        *,
        migration_table_exists: bool = True,
        migration_applied: bool = True,
        row: dict[str, Any] | None | object = _DEFAULT,
        audit_rows: list[dict[str, Any]] | None = None,
        update_result: dict[str, Any] | None | object = _DEFAULT,
    ) -> None:
        self.migration_table_exists = migration_table_exists
        self.migration_applied = migration_applied
        self.row = _row() if row is _DEFAULT else row
        self.audit_rows = [_audit()] if audit_rows is None else audit_rows
        self.update_result = update_result
        self.calls: list[tuple[str, str, tuple[Any, ...]]] = []

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def fetchval(self, sql: str, *params: Any) -> bool | int | None:
        compact = " ".join(sql.split())
        self.calls.append(("fetchval", compact, params))
        if "to_regclass('schema_migrations')" in compact:
            return self.migration_table_exists
        if "FROM schema_migrations" in compact:
            assert params == (OFFLINE_OUTBOX_RECONCILIATION_MIGRATION,)
            return 1 if self.migration_applied else None
        raise AssertionError(f"unexpected fetchval query: {compact}")

    async def execute(self, sql: str, *params: Any) -> str:
        self.calls.append(("execute", " ".join(sql.split()), params))
        return "SELECT 1"

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None:
        compact = " ".join(sql.split())
        self.calls.append(("fetchrow", compact, params))
        if compact.startswith("UPDATE message_outbox"):
            if self.update_result is not _DEFAULT:
                return self.update_result  # type: ignore[return-value]
            return {
                "reconciliation_id": params[9],
                "last_error": params[8],
                "reconciliation_started_at": _QUARANTINED_AT,
                "reconciliation_outcome_at": _QUARANTINED_AT,
            }
        if "FROM message_outbox" in compact:
            return self.row  # type: ignore[return-value]
        raise AssertionError(f"unexpected fetchrow query: {compact}")

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        compact = " ".join(sql.split())
        self.calls.append(("fetch", compact, params))
        assert "FROM event_audit" in compact
        return self.audit_rows


class _Pool:
    def __init__(self, connection: _QuarantineConnection) -> None:
        self.connection = connection

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[_QuarantineConnection]:
        yield self.connection


@pytest.mark.asyncio
async def test_exact_expired_outbox_is_quarantined_without_a_publish_or_attempt_increment() -> None:
    connection = _QuarantineConnection()
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]

    result = await repository.quarantine_expired_outbox_exact(
        _identity(),
        expired_lease=_expired_lease(),
        reconciliation_id="recovery-quarantine-1",
        reason="operator stopped after an ambiguous historical lease",
    )

    assert result.state is OfflineOutboxQuarantineState.QUARANTINED
    assert result.receipt is not None
    assert result.receipt.reconciliation_id == "recovery-quarantine-1"
    assert result.receipt.legacy_lease == _expired_lease()
    assert result.rejection is None
    calls = "\n".join(sql for _method, sql, _params in connection.calls)
    assert "pg_advisory_xact_lock(hashtextextended($1, 0))" in calls
    assert "WHERE id=$1 FOR UPDATE" in calls
    assert "FROM event_audit" in calls and "FOR UPDATE" in calls
    update = next(
        sql for method, sql, _params in connection.calls if method == "fetchrow" and sql.startswith("UPDATE")
    )
    assert "reconciliation_state='PUBLISH_OUTCOME_UNKNOWN'" in update
    assert "reconciliation_id=$10" in update
    assert "reconciliation_started_at=now()" in update
    assert "reconciliation_outcome_at=now()" in update
    assert "lease_until=NULL" in update and "lease_owner=NULL" in update
    assert "publish_attempts=publish_attempts+1" not in update
    assert "available_at=now()+" not in update
    assert "PUBLISHING" not in update
    assert "lease_owner=$7" in update and "lease_until=$8" in update
    assert not any("SELECT id FROM message_outbox" in sql for _method, sql, _params in connection.calls)
    update_params = next(
        params
        for method, sql, params in connection.calls
        if method == "fetchrow" and sql.startswith("UPDATE")
    )
    assert update_params[6:8] == (_expired_lease().owner, _expired_lease().until)
    assert json.loads(update_params[8]) == {
        "expired_lease_owner_sha256": hashlib.sha256(_expired_lease().owner.encode("utf-8")).hexdigest(),
        "expired_lease_until": "2026-09-20T08:00:00.000000+00:00",
        "reason": "operator stopped after an ambiguous historical lease",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row", "audit_rows", "expected"),
    [
        (None, None, OfflineOutboxQuarantineRejection.NOT_FOUND),
        (_row(producer="other"), None, OfflineOutboxQuarantineRejection.IDENTITY_MISMATCH),
        (_row(published_at="already"), None, OfflineOutboxQuarantineRejection.ALREADY_PUBLISHED),
        (_row(dead_lettered_at="dead"), None, OfflineOutboxQuarantineRejection.DEAD_LETTERED),
        (_row(lease_expired=False), None, OfflineOutboxQuarantineRejection.LEASE_NOT_EXPIRED),
        (_row(available=False), None, OfflineOutboxQuarantineRejection.NOT_YET_AVAILABLE),
        (
            _row(reconciliation_state="PUBLISHING"),
            None,
            OfflineOutboxQuarantineRejection.RECONCILIATION_NOT_CLEAR,
        ),
        (
            _row(lease_owner="other-worker"),
            None,
            OfflineOutboxQuarantineRejection.LEASE_IDENTITY_MISMATCH,
        ),
        (
            _row(lease_until=_EXPIRED_AT.replace(microsecond=1)),
            None,
            OfflineOutboxQuarantineRejection.LEASE_IDENTITY_MISMATCH,
        ),
        (_row(), [], OfflineOutboxQuarantineRejection.AUDIT_MISMATCH),
    ],
)
async def test_rejected_expired_outbox_quarantine_never_mutates(
    row: dict[str, Any] | None,
    audit_rows: list[dict[str, Any]] | None,
    expected: OfflineOutboxQuarantineRejection,
) -> None:
    connection = _QuarantineConnection(row=row, audit_rows=audit_rows)
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]

    result = await repository.quarantine_expired_outbox_exact(
        _identity(),
        expired_lease=_expired_lease(),
        reconciliation_id="recovery-quarantine-1",
        reason="manual safety quarantine",
    )

    assert result.state is OfflineOutboxQuarantineState.REJECTED
    assert result.rejection is expected
    assert not any(method == "fetchrow" and sql.startswith("UPDATE") for method, sql, _ in connection.calls)


@pytest.mark.asyncio
async def test_same_exact_reconciliation_id_returns_a_terminal_idempotent_quarantine() -> None:
    row = _already_quarantined_row()
    row["available"] = False  # Terminal reads do not depend on initial availability.
    connection = _QuarantineConnection(row=row)
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]

    result = await repository.quarantine_expired_outbox_exact(
        _identity(),
        expired_lease=_expired_lease(),
        reconciliation_id="recovery-quarantine-1",
        reason="manual safety quarantine",
    )

    assert result.state is OfflineOutboxQuarantineState.ALREADY_QUARANTINED
    assert result.receipt is not None
    assert result.receipt.reconciliation_id == "recovery-quarantine-1"
    assert result.receipt.reason == "manual safety quarantine"
    assert not any(method == "fetchrow" and sql.startswith("UPDATE") for method, sql, _ in connection.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reconciliation_id", "reason"),
    [
        ("different-reconciliation", "manual safety quarantine"),
        ("recovery-quarantine-1", "different reason"),
    ],
)
async def test_quarantine_idempotency_rejects_a_different_id_or_reason_without_mutation(
    reconciliation_id: str,
    reason: str,
) -> None:
    connection = _QuarantineConnection(row=_already_quarantined_row())
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]

    result = await repository.quarantine_expired_outbox_exact(
        _identity(),
        expired_lease=_expired_lease(),
        reconciliation_id=reconciliation_id,
        reason=reason,
    )

    assert result.state is OfflineOutboxQuarantineState.REJECTED
    assert result.rejection is OfflineOutboxQuarantineRejection.RECONCILIATION_NOT_CLEAR
    assert not any(method == "fetchrow" and sql.startswith("UPDATE") for method, sql, _ in connection.calls)


@pytest.mark.asyncio
async def test_quarantine_idempotency_rejects_changed_expired_lease_evidence_without_mutation() -> None:
    connection = _QuarantineConnection(row=_already_quarantined_row())
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]
    changed_lease = OfflineOutboxExpiredLease(owner="different-worker", until=_EXPIRED_AT)

    result = await repository.quarantine_expired_outbox_exact(
        _identity(),
        expired_lease=changed_lease,
        reconciliation_id="recovery-quarantine-1",
        reason="manual safety quarantine",
    )

    assert result.state is OfflineOutboxQuarantineState.REJECTED
    assert result.rejection is OfflineOutboxQuarantineRejection.RECONCILIATION_NOT_CLEAR
    assert not any(method == "fetchrow" and sql.startswith("UPDATE") for method, sql, _ in connection.calls)


@pytest.mark.asyncio
async def test_quarantine_requires_migration_018_before_reading_or_mutating_an_effect() -> None:
    connection = _QuarantineConnection(migration_applied=False)
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]

    result = await repository.quarantine_expired_outbox_exact(
        _identity(),
        expired_lease=_expired_lease(),
        reconciliation_id="recovery-quarantine-1",
        reason="manual safety quarantine",
    )

    assert result.state is OfflineOutboxQuarantineState.REJECTED
    assert result.rejection is OfflineOutboxQuarantineRejection.MIGRATION_018_REQUIRED
    assert all(
        "message_outbox" not in sql and "event_audit" not in sql for _method, sql, _ in connection.calls
    )


@pytest.mark.asyncio
async def test_quarantine_reports_a_lost_exact_update_race_without_claiming_or_retrying() -> None:
    connection = _QuarantineConnection(update_result=None)
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]

    result = await repository.quarantine_expired_outbox_exact(
        _identity(),
        expired_lease=_expired_lease(),
        reconciliation_id="recovery-quarantine-1",
        reason="manual safety quarantine",
    )

    assert result.state is OfflineOutboxQuarantineState.REJECTED
    assert result.rejection is OfflineOutboxQuarantineRejection.RACE_LOST
    update = next(
        sql for method, sql, _params in connection.calls if method == "fetchrow" and sql.startswith("UPDATE")
    )
    assert "PUBLISHING" not in update
    assert "publish_attempts=publish_attempts+1" not in update


def test_quarantine_input_and_implementation_are_db_only_and_bounded() -> None:
    repository = AuditRepository(_Pool(_QuarantineConnection()))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="non-empty"):
        repository._require_quarantine_reason(" ")
    with pytest.raises(ValueError, match="at most 3500"):
        repository._require_quarantine_reason("x" * 3501)

    source = inspect.getsource(AuditRepository.quarantine_expired_outbox_exact)
    assert "OFFLINE_OUTBOX_RECONCILIATION_MIGRATION" in source
    assert "publisher" not in source
    assert "claim_expired_outbox_exact" not in source
    assert "publish_attempts=publish_attempts+1" not in source
