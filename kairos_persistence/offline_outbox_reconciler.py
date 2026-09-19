"""One-row, fail-closed reconciliation for an explicitly identified outbox effect.

This module is intentionally not a durable message bus and has no retry loop.
It exists for a human-approved recovery procedure after the ordinary dispatcher
has been stopped.  A transport send and the database acknowledgement cannot be
one atomic operation; every failure from that boundary is therefore reported as
``PUBLISH_OUTCOME_UNKNOWN`` and the row remains durably blocked from automatic
redispatch.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .repository import (
    AuditRepository,
    OfflineOutboxClaim,
    OfflineOutboxClaimRejection,
    OfflineOutboxClaimState,
    OfflineOutboxIdentity,
)

OutboxPublisher = Callable[[str, dict[str, Any]], Awaitable[None]]


class OfflineOutboxReconciliationState(StrEnum):
    """Terminal result of one manually invoked exact-row reconciliation."""

    CLAIM_REJECTED = "CLAIM_REJECTED"
    PUBLISH_ACKNOWLEDGED = "PUBLISH_ACKNOWLEDGED"
    PUBLISH_OUTCOME_UNKNOWN = "PUBLISH_OUTCOME_UNKNOWN"


@dataclass(frozen=True)
class OfflineOutboxReconciliationResult:
    """A result that never implies safe retry after an attempted publish."""

    state: OfflineOutboxReconciliationState
    identity: OfflineOutboxIdentity
    rejection: OfflineOutboxClaimRejection | None = None
    unknown_quarantined: bool | None = None
    failure_kind: str | None = None

    def __post_init__(self) -> None:
        if self.state is OfflineOutboxReconciliationState.CLAIM_REJECTED:
            if self.rejection is None or self.unknown_quarantined is not None:
                raise ValueError("claim rejection must contain only a concrete rejection reason")
        elif self.state is OfflineOutboxReconciliationState.PUBLISH_ACKNOWLEDGED:
            if self.rejection is not None or self.unknown_quarantined is not None:
                raise ValueError("acknowledged publish must not carry rejection or quarantine state")
        elif self.rejection is not None or self.unknown_quarantined is None:
            raise ValueError("unknown publish outcome must state whether durable quarantine succeeded")


class OfflineOutboxReconciler:
    """Execute exactly one explicitly identified offline publish attempt.

    The publisher is injected so this class owns neither a network connection
    nor credentials.  It deliberately treats both a publisher exception and a
    lost database ACK as ambiguous: either event may occur after the transport
    accepted the message.  It never invokes the publisher a second time.
    """

    def __init__(self, repository: AuditRepository) -> None:
        self.repository = repository

    async def reconcile(
        self,
        identity: OfflineOutboxIdentity,
        *,
        reconciliation_id: str,
        publisher: OutboxPublisher,
    ) -> OfflineOutboxReconciliationResult:
        claim_result = await self.repository.claim_expired_outbox_exact(
            identity,
            reconciliation_id=reconciliation_id,
        )
        if claim_result.state is OfflineOutboxClaimState.REJECTED:
            if claim_result.rejection is None:
                raise RuntimeError("offline outbox repository returned a rejected claim without a reason")
            return OfflineOutboxReconciliationResult(
                state=OfflineOutboxReconciliationState.CLAIM_REJECTED,
                identity=identity,
                rejection=claim_result.rejection,
            )
        if claim_result.claim is None:
            raise RuntimeError("offline outbox repository returned a claimed result without a row")
        claim = claim_result.claim
        try:
            await publisher(claim.identity.topic, claim.payload)
        except asyncio.CancelledError:
            await self._quarantine_unknown(claim, "publisher_cancelled")
            raise
        except Exception as exc:
            return await self._unknown_result(claim, f"publisher_exception:{type(exc).__name__}")

        try:
            acknowledged = await self.repository.acknowledge_exact_outbox_publish(
                identity,
                reconciliation_id=reconciliation_id,
            )
        except asyncio.CancelledError:
            await self._quarantine_unknown(claim, "database_ack_cancelled")
            raise
        except Exception as exc:
            return await self._unknown_result(claim, f"database_ack_exception:{type(exc).__name__}")
        if acknowledged:
            return OfflineOutboxReconciliationResult(
                state=OfflineOutboxReconciliationState.PUBLISH_ACKNOWLEDGED,
                identity=identity,
            )
        return await self._unknown_result(claim, "database_ack_not_applied")

    async def _unknown_result(
        self,
        claim: OfflineOutboxClaim,
        failure_kind: str,
    ) -> OfflineOutboxReconciliationResult:
        quarantined = await self._quarantine_unknown(claim, failure_kind)
        return OfflineOutboxReconciliationResult(
            state=OfflineOutboxReconciliationState.PUBLISH_OUTCOME_UNKNOWN,
            identity=claim.identity,
            unknown_quarantined=quarantined,
            failure_kind=failure_kind,
        )

    async def _quarantine_unknown(self, claim: OfflineOutboxClaim, failure_kind: str) -> bool:
        """Best-effort persistence never changes the fail-closed caller outcome."""
        try:
            return await self.repository.mark_exact_outbox_publish_outcome_unknown(
                claim.identity,
                reconciliation_id=claim.reconciliation_id,
                reason=f"offline reconciliation {failure_kind}",
            )
        except Exception:
            # A process must not retry after a possible publish even if its
            # durable quarantine write itself is unavailable.  ``PUBLISHING``
            # is already excluded from the ordinary dispatcher after a crash.
            return False
