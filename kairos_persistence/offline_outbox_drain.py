"""One-row primitive for a signed, bounded offline outbox prefix drain.

This module deliberately does not select a queue, construct a transport, or
own a retry loop.  A higher-level operator tool must supply every immutable
identity in a previously inspected, signed prefix.  The primitive claims one
already-selected, unleased producer head, performs one publish attempt, and
either durably acknowledges it or quarantines the ambiguous outcome.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum

from .offline_outbox_reconciler import OutboxPublisher
from .repository import (
    AuditRepository,
    OfflineOutboxClaim,
    OfflineOutboxClaimRejection,
    OfflineOutboxClaimState,
    OfflineOutboxIdentity,
)


class OfflineOutboxDrainState(StrEnum):
    """Terminal state for one exact member of a bounded signed prefix."""

    CLAIM_REJECTED = "CLAIM_REJECTED"
    PUBLISH_ACKNOWLEDGED = "PUBLISH_ACKNOWLEDGED"
    PUBLISH_OUTCOME_UNKNOWN = "PUBLISH_OUTCOME_UNKNOWN"


@dataclass(frozen=True)
class OfflineOutboxDrainResult:
    """A one-shot result that never permits an automatic retry."""

    state: OfflineOutboxDrainState
    identity: OfflineOutboxIdentity
    rejection: OfflineOutboxClaimRejection | None = None
    unknown_quarantined: bool | None = None
    failure_kind: str | None = None

    def __post_init__(self) -> None:
        if self.state is OfflineOutboxDrainState.CLAIM_REJECTED:
            if self.rejection is None or self.unknown_quarantined is not None:
                raise ValueError("claim rejection must contain only a concrete rejection reason")
        elif self.state is OfflineOutboxDrainState.PUBLISH_ACKNOWLEDGED:
            if self.rejection is not None or self.unknown_quarantined is not None:
                raise ValueError("acknowledged publish must not carry rejection or quarantine state")
        elif self.rejection is not None or self.unknown_quarantined is None:
            raise ValueError("unknown publish outcome must state whether durable quarantine succeeded")


class OfflineOutboxPrefixDrainer:
    """Publish exactly one precommitted ready row from an offline drain prefix.

    The repository moves the row to ``PUBLISHING`` before ``publisher`` is
    called.  No retry is possible after that transport boundary: an exception,
    cancellation, or failed database acknowledgement produces a durable
    ``PUBLISH_OUTCOME_UNKNOWN`` quarantine whenever the database is reachable.
    """

    def __init__(self, repository: AuditRepository) -> None:
        self.repository = repository

    async def drain_exact(
        self,
        identity: OfflineOutboxIdentity,
        *,
        drain_id: str,
        publisher: OutboxPublisher,
    ) -> OfflineOutboxDrainResult:
        claim_result = await self.repository.claim_ready_outbox_exact(
            identity,
            reconciliation_id=drain_id,
        )
        if claim_result.state is OfflineOutboxClaimState.REJECTED:
            if claim_result.rejection is None:
                raise RuntimeError("offline outbox repository returned a rejected claim without a reason")
            return OfflineOutboxDrainResult(
                state=OfflineOutboxDrainState.CLAIM_REJECTED,
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
                reconciliation_id=drain_id,
            )
        except asyncio.CancelledError:
            await self._quarantine_unknown(claim, "database_ack_cancelled")
            raise
        except Exception as exc:
            return await self._unknown_result(claim, f"database_ack_exception:{type(exc).__name__}")
        if acknowledged:
            return OfflineOutboxDrainResult(
                state=OfflineOutboxDrainState.PUBLISH_ACKNOWLEDGED,
                identity=identity,
            )
        return await self._unknown_result(claim, "database_ack_not_applied")

    async def _unknown_result(
        self,
        claim: OfflineOutboxClaim,
        failure_kind: str,
    ) -> OfflineOutboxDrainResult:
        quarantined = await self._quarantine_unknown(claim, failure_kind)
        return OfflineOutboxDrainResult(
            state=OfflineOutboxDrainState.PUBLISH_OUTCOME_UNKNOWN,
            identity=claim.identity,
            unknown_quarantined=quarantined,
            failure_kind=failure_kind,
        )

    async def _quarantine_unknown(self, claim: OfflineOutboxClaim, failure_kind: str) -> bool:
        """Best-effort durable quarantine never changes the fail-closed result."""
        try:
            return await self.repository.mark_exact_outbox_publish_outcome_unknown(
                claim.identity,
                reconciliation_id=claim.reconciliation_id,
                reason=f"offline prefix drain {failure_kind}",
            )
        except Exception:
            # ``PUBLISHING`` already excludes a crash-recovered row from the
            # normal dispatcher, even when this durable explanatory update is
            # itself unavailable.
            return False
