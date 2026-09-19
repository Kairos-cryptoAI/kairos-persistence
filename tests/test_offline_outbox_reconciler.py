from __future__ import annotations

import hashlib
import inspect
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from kairos_persistence.offline_outbox_reconciler import (
    OfflineOutboxReconciler,
    OfflineOutboxReconciliationState,
)
from kairos_persistence.repository import (
    AuditRepository,
    OfflineOutboxClaimRejection,
    OfflineOutboxClaimResult,
    OfflineOutboxClaimState,
    OfflineOutboxIdentity,
)


def _payload() -> dict[str, Any]:
    return {"message_id": "outbox-message-1", "value": 1}


def _identity() -> OfflineOutboxIdentity:
    payload = _payload()
    encoded = '{"message_id":"outbox-message-1","value":1}'
    return OfflineOutboxIdentity(
        id=42,
        producer="recovery-producer",
        message_id=payload["message_id"],
        topic="kairos.recovery.v1",
        payload_sha256=hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
        publish_attempts=3,
    )


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
        "lease_expired": True,
        "lease_clear": False,
        "available": True,
        "reconciliation_state": "NONE",
    }
    row.update(overrides)
    return row


def _audit(**overrides: Any) -> dict[str, Any]:
    row = {"topic": _identity().topic, "payload": _payload()}
    row.update(overrides)
    return row


class _Transaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


class _ExactClaimConnection:
    def __init__(
        self,
        *,
        row: dict[str, Any] | None = None,
        audit_rows: list[dict[str, Any]] | None = None,
        predecessor: dict[str, Any] | None = None,
        update_result: dict[str, Any] | None | object = ...,
    ) -> None:
        self.row = _row() if row is None else row
        self.audit_rows = [_audit()] if audit_rows is None else audit_rows
        self.predecessor = predecessor
        self.update_result = {"publish_attempts": 4} if update_result is ... else update_result
        self.calls: list[tuple[str, str, tuple[Any, ...]]] = []

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def execute(self, sql: str, *params: Any) -> str:
        self.calls.append(("execute", " ".join(sql.split()), params))
        return "SELECT 1"

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None:
        compact = " ".join(sql.split())
        self.calls.append(("fetchrow", compact, params))
        if compact.startswith("UPDATE message_outbox"):
            return self.update_result
        if compact.startswith("SELECT id FROM message_outbox"):
            return self.predecessor
        if "FROM message_outbox" in compact:
            return self.row
        raise AssertionError(f"unexpected fetchrow query: {compact}")

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        compact = " ".join(sql.split())
        self.calls.append(("fetch", compact, params))
        assert "FROM event_audit" in compact
        return self.audit_rows


class _Pool:
    def __init__(self, connection: _ExactClaimConnection) -> None:
        self.connection = connection

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[_ExactClaimConnection]:
        yield self.connection


def _validate(
    row: dict[str, Any] | None,
    audit_rows: list[dict[str, Any]] | None = None,
    *,
    predecessor: bool = False,
) -> OfflineOutboxClaimRejection | None:
    return AuditRepository._validate_exact_offline_outbox(
        row,
        identity=_identity(),
        audit_rows=[_audit()] if audit_rows is None else audit_rows,
        has_unpublished_predecessor=predecessor,
    )


def test_exact_identity_rejects_invalid_or_ambiguous_operator_input() -> None:
    with pytest.raises(ValueError, match="positive"):
        OfflineOutboxIdentity(0, "producer", "message", "topic", "a" * 64, 0)
    with pytest.raises(ValueError, match="SHA-256"):
        OfflineOutboxIdentity(1, "producer", "message", "topic", "A" * 64, 0)
    with pytest.raises(ValueError, match="non-negative"):
        OfflineOutboxIdentity(1, "producer", "message", "topic", "a" * 64, -1)


def test_validator_rejects_every_mutated_immutable_identity_field() -> None:
    identity = _identity()
    mutations = {
        "id": identity.id + 1,
        "producer": "other-producer",
        "message_id": "other-message",
        "topic": "other-topic",
        "payload_sha256": "f" * 64,
        "publish_attempts": identity.publish_attempts + 1,
    }
    for field, mutated_value in mutations.items():
        assert _validate(_row(**{field: mutated_value})) is OfflineOutboxClaimRejection.IDENTITY_MISMATCH, (
            field
        )


def test_validator_hashes_canonical_payload_not_incidental_object_key_order() -> None:
    reversed_payload = {"value": 1, "message_id": "outbox-message-1"}

    assert _validate(_row(payload=reversed_payload), [_audit(payload=reversed_payload)]) is None
    assert (
        _validate(_row(payload={"message_id": "outbox-message-1", "value": float("nan")}))
        is OfflineOutboxClaimRejection.PAYLOAD_HASH_MISMATCH
    )


@pytest.mark.parametrize(
    ("row", "audit_rows", "predecessor", "expected"),
    [
        (None, None, False, OfflineOutboxClaimRejection.NOT_FOUND),
        (_row(published_at="already"), None, False, OfflineOutboxClaimRejection.ALREADY_PUBLISHED),
        (_row(dead_lettered_at="dead"), None, False, OfflineOutboxClaimRejection.DEAD_LETTERED),
        (_row(lease_expired=False), None, False, OfflineOutboxClaimRejection.LEASE_NOT_EXPIRED),
        (_row(available=False), None, False, OfflineOutboxClaimRejection.NOT_YET_AVAILABLE),
        (
            _row(reconciliation_state="PUBLISHING"),
            None,
            False,
            OfflineOutboxClaimRejection.RECONCILIATION_NOT_CLEAR,
        ),
        (_row(), None, True, OfflineOutboxClaimRejection.EARLIER_UNPUBLISHED_PREDECESSOR),
        (
            _row(payload={"message_id": "outbox-message-1", "value": 2}),
            None,
            False,
            OfflineOutboxClaimRejection.PAYLOAD_HASH_MISMATCH,
        ),
        (_row(), [], False, OfflineOutboxClaimRejection.AUDIT_MISMATCH),
        (_row(), [_audit(), _audit()], False, OfflineOutboxClaimRejection.AUDIT_MISMATCH),
        (
            _row(),
            [_audit(topic="different-topic")],
            False,
            OfflineOutboxClaimRejection.AUDIT_MISMATCH,
        ),
    ],
)
def test_validator_is_fail_closed_for_every_recovery_precondition(
    row: dict[str, Any] | None,
    audit_rows: list[dict[str, Any]] | None,
    predecessor: bool,
    expected: OfflineOutboxClaimRejection,
) -> None:
    assert _validate(row, audit_rows, predecessor=predecessor) is expected


@pytest.mark.asyncio
async def test_exact_claim_serializes_then_locks_and_binds_every_identity_field() -> None:
    connection = _ExactClaimConnection()
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]

    result = await repository.claim_expired_outbox_exact(_identity(), reconciliation_id="recovery-approval-1")

    assert result.state is OfflineOutboxClaimState.CLAIMED
    assert result.claim is not None
    assert result.claim.payload == _payload()
    assert result.claim.claimed_publish_attempts == 4
    calls = "\n".join(sql for _method, sql, _params in connection.calls)
    assert "pg_advisory_xact_lock(hashtextextended($1, 0))" in calls
    assert "WHERE id=$1 FOR UPDATE" in calls
    assert "FROM event_audit" in calls and "FOR UPDATE" in calls
    assert "reconciliation_state='PUBLISHING'" in calls
    assert "payload_sha256=$7" in calls
    assert "publish_attempts=$8" in calls


@pytest.mark.asyncio
async def test_exact_claim_rejects_a_lost_update_race_without_publishing() -> None:
    connection = _ExactClaimConnection(update_result=None)
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]

    result = await repository.claim_expired_outbox_exact(_identity(), reconciliation_id="recovery-race-1")

    assert result.state is OfflineOutboxClaimState.REJECTED
    assert result.rejection is OfflineOutboxClaimRejection.RACE_LOST


@pytest.mark.asyncio
async def test_ready_exact_claim_requires_an_unleased_verified_producer_head() -> None:
    connection = _ExactClaimConnection(row=_row(lease_expired=False, lease_clear=True))
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]

    result = await repository.claim_ready_outbox_exact(_identity(), reconciliation_id="drain-approval-1")

    assert result.state is OfflineOutboxClaimState.CLAIMED
    assert result.claim is not None
    assert result.claim.claimed_publish_attempts == 4
    calls = "\n".join(sql for _method, sql, _params in connection.calls)
    assert "(lease_until IS NULL) AS lease_clear" in calls
    assert "lease_until IS NULL" in calls
    assert "reconciliation_state='PUBLISHING'" in calls


@pytest.mark.asyncio
async def test_ready_exact_claim_refuses_an_active_or_expired_lease() -> None:
    connection = _ExactClaimConnection(row=_row(lease_clear=False))
    repository = AuditRepository(_Pool(connection))  # type: ignore[arg-type]

    result = await repository.claim_ready_outbox_exact(_identity(), reconciliation_id="drain-approval-1")

    assert result.state is OfflineOutboxClaimState.REJECTED
    assert result.rejection is OfflineOutboxClaimRejection.LEASE_PRESENT
    assert not any(method == "fetchrow" and sql.startswith("UPDATE") for method, sql, _ in connection.calls)


def test_normal_dispatcher_cannot_claim_reconciliation_rows() -> None:
    source = inspect.getsource(AuditRepository.claim_outbox)
    assert "outbox.reconciliation_state='NONE'" in source
    assert "LIMIT LEAST($2, 1)" in source
    migration = (
        Path(__file__).parents[1]
        / "kairos_persistence"
        / "migrations"
        / "018_offline_outbox_reconciliation.sql"
    ).read_text(encoding="utf-8")
    assert "PUBLISH_OUTCOME_UNKNOWN" in migration
    assert "message_outbox_reconciliation_state" in migration
    assert "reconciliation_id IS NOT NULL" in migration
    for method in (
        AuditRepository.acknowledge_exact_outbox_publish,
        AuditRepository.mark_exact_outbox_publish_outcome_unknown,
    ):
        source = inspect.getsource(method)
        assert "publish_attempts=$6" in source
        assert "reconciliation_state='PUBLISHING'" in source
        assert "reconciliation_id=$7" in source


class _ReconciliationRepository:
    def __init__(
        self,
        claim_result: OfflineOutboxClaimResult,
        *,
        acknowledge: bool = True,
        quarantine: bool = True,
        acknowledge_error: Exception | None = None,
        quarantine_error: Exception | None = None,
    ) -> None:
        self.claim_result = claim_result
        self.acknowledge = acknowledge
        self.quarantine = quarantine
        self.acknowledge_error = acknowledge_error
        self.quarantine_error = quarantine_error
        self.calls: list[str] = []

    async def claim_expired_outbox_exact(self, *args: Any, **kwargs: Any) -> OfflineOutboxClaimResult:
        self.calls.append("claim")
        return self.claim_result

    async def acknowledge_exact_outbox_publish(self, *args: Any, **kwargs: Any) -> bool:
        self.calls.append("acknowledge")
        if self.acknowledge_error is not None:
            raise self.acknowledge_error
        return self.acknowledge

    async def mark_exact_outbox_publish_outcome_unknown(self, *args: Any, **kwargs: Any) -> bool:
        self.calls.append("quarantine")
        if self.quarantine_error is not None:
            raise self.quarantine_error
        return self.quarantine


def _claimed_result() -> OfflineOutboxClaimResult:
    identity = _identity()
    from kairos_persistence.repository import OfflineOutboxClaim

    return OfflineOutboxClaimResult(
        state=OfflineOutboxClaimState.CLAIMED,
        claim=OfflineOutboxClaim(
            identity=identity,
            payload=_payload(),
            reconciliation_id="recovery-publish-1",
            claimed_publish_attempts=identity.publish_attempts + 1,
        ),
    )


@pytest.mark.asyncio
async def test_reconciler_publishes_once_then_acknowledges() -> None:
    repository = _ReconciliationRepository(_claimed_result())
    reconciler = OfflineOutboxReconciler(repository)  # type: ignore[arg-type]
    published: list[tuple[str, dict[str, Any]]] = []

    async def publisher(topic: str, payload: dict[str, Any]) -> None:
        published.append((topic, payload))

    result = await reconciler.reconcile(
        _identity(), reconciliation_id="recovery-publish-1", publisher=publisher
    )

    assert result.state is OfflineOutboxReconciliationState.PUBLISH_ACKNOWLEDGED
    assert published == [(_identity().topic, _payload())]
    assert repository.calls == ["claim", "acknowledge"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("publisher_raises", "acknowledge", "acknowledge_error", "quarantine", "expected_failure"),
    [
        (True, True, None, True, "publisher_exception:RuntimeError"),
        (False, False, None, True, "database_ack_not_applied"),
        (False, True, RuntimeError("connection lost"), False, "database_ack_exception:RuntimeError"),
    ],
)
async def test_reconciler_never_retries_an_ambiguous_publish_boundary(
    publisher_raises: bool,
    acknowledge: bool,
    acknowledge_error: Exception | None,
    quarantine: bool,
    expected_failure: str,
) -> None:
    repository = _ReconciliationRepository(
        _claimed_result(),
        acknowledge=acknowledge,
        acknowledge_error=acknowledge_error,
        quarantine=quarantine,
    )
    reconciler = OfflineOutboxReconciler(repository)  # type: ignore[arg-type]
    calls = 0

    async def publisher(_topic: str, _payload: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        if publisher_raises:
            raise RuntimeError("transport may have accepted the message")

    result = await reconciler.reconcile(
        _identity(), reconciliation_id="recovery-publish-1", publisher=publisher
    )

    assert result.state is OfflineOutboxReconciliationState.PUBLISH_OUTCOME_UNKNOWN
    assert result.failure_kind == expected_failure
    assert result.unknown_quarantined is quarantine
    assert calls == 1
    assert repository.calls.count("quarantine") == 1


@pytest.mark.asyncio
async def test_reconciler_does_not_invoke_publisher_when_exact_claim_rejects() -> None:
    rejected = OfflineOutboxClaimResult(
        state=OfflineOutboxClaimState.REJECTED,
        rejection=OfflineOutboxClaimRejection.LEASE_NOT_EXPIRED,
    )
    repository = _ReconciliationRepository(rejected)
    reconciler = OfflineOutboxReconciler(repository)  # type: ignore[arg-type]

    async def publisher(_topic: str, _payload: dict[str, Any]) -> None:
        raise AssertionError("publisher must not run for a rejected exact claim")

    result = await reconciler.reconcile(
        _identity(), reconciliation_id="recovery-reject-1", publisher=publisher
    )

    assert result.state is OfflineOutboxReconciliationState.CLAIM_REJECTED
    assert result.rejection is OfflineOutboxClaimRejection.LEASE_NOT_EXPIRED
    assert repository.calls == ["claim"]


@pytest.mark.asyncio
async def test_exact_claim_lease_is_bounded() -> None:
    repository = AuditRepository(_Pool(_ExactClaimConnection()))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="at most five minutes"):
        await repository.claim_expired_outbox_exact(
            _identity(), reconciliation_id="recovery-lease-1", lease=timedelta(minutes=5, seconds=1)
        )
