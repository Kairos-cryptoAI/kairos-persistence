from __future__ import annotations

import hashlib
from typing import Any

import pytest

from kairos_persistence.offline_outbox_drain import (
    OfflineOutboxDrainState,
    OfflineOutboxPrefixDrainer,
)
from kairos_persistence.repository import (
    OfflineOutboxClaim,
    OfflineOutboxClaimRejection,
    OfflineOutboxClaimResult,
    OfflineOutboxClaimState,
    OfflineOutboxIdentity,
)


def _payload() -> dict[str, Any]:
    return {"message_id": "bounded-drain-message-1", "value": 1}


def _identity() -> OfflineOutboxIdentity:
    payload = _payload()
    encoded = '{"message_id":"bounded-drain-message-1","value":1}'
    return OfflineOutboxIdentity(
        id=19,
        producer="kairos-quant-scouts",
        message_id=str(payload["message_id"]),
        topic="kairos.market.closed_bar.v1",
        payload_sha256=hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
        publish_attempts=2,
    )


def _claimed() -> OfflineOutboxClaimResult:
    identity = _identity()
    return OfflineOutboxClaimResult(
        state=OfflineOutboxClaimState.CLAIMED,
        claim=OfflineOutboxClaim(
            identity=identity,
            payload=_payload(),
            reconciliation_id="signed-prefix-19",
            claimed_publish_attempts=identity.publish_attempts + 1,
        ),
    )


class _Repository:
    def __init__(
        self,
        claim: OfflineOutboxClaimResult,
        *,
        acknowledged: bool = True,
        acknowledge_error: Exception | None = None,
        quarantine: bool = True,
    ) -> None:
        self.claim = claim
        self.acknowledged = acknowledged
        self.acknowledge_error = acknowledge_error
        self.quarantine = quarantine
        self.calls: list[str] = []

    async def claim_ready_outbox_exact(self, *args: Any, **kwargs: Any) -> OfflineOutboxClaimResult:
        self.calls.append("claim")
        return self.claim

    async def acknowledge_exact_outbox_publish(self, *args: Any, **kwargs: Any) -> bool:
        self.calls.append("acknowledge")
        if self.acknowledge_error is not None:
            raise self.acknowledge_error
        return self.acknowledged

    async def mark_exact_outbox_publish_outcome_unknown(self, *args: Any, **kwargs: Any) -> bool:
        self.calls.append("quarantine")
        return self.quarantine


@pytest.mark.asyncio
async def test_prefix_drainer_publishes_one_verified_row_then_acknowledges() -> None:
    repository = _Repository(_claimed())
    drainer = OfflineOutboxPrefixDrainer(repository)  # type: ignore[arg-type]
    published: list[tuple[str, dict[str, Any]]] = []

    async def publisher(topic: str, payload: dict[str, Any]) -> None:
        published.append((topic, payload))

    result = await drainer.drain_exact(_identity(), drain_id="signed-prefix-19", publisher=publisher)

    assert result.state is OfflineOutboxDrainState.PUBLISH_ACKNOWLEDGED
    assert published == [(_identity().topic, _payload())]
    assert repository.calls == ["claim", "acknowledge"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("publisher_error", "acknowledged", "acknowledge_error", "expected_failure"),
    [
        (RuntimeError("transport may have accepted"), True, None, "publisher_exception:RuntimeError"),
        (None, False, None, "database_ack_not_applied"),
        (None, True, RuntimeError("database unavailable"), "database_ack_exception:RuntimeError"),
    ],
)
async def test_prefix_drainer_never_retries_an_ambiguous_transport_boundary(
    publisher_error: Exception | None,
    acknowledged: bool,
    acknowledge_error: Exception | None,
    expected_failure: str,
) -> None:
    repository = _Repository(
        _claimed(),
        acknowledged=acknowledged,
        acknowledge_error=acknowledge_error,
    )
    drainer = OfflineOutboxPrefixDrainer(repository)  # type: ignore[arg-type]
    calls = 0

    async def publisher(_topic: str, _payload: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        if publisher_error is not None:
            raise publisher_error

    result = await drainer.drain_exact(_identity(), drain_id="signed-prefix-19", publisher=publisher)

    assert result.state is OfflineOutboxDrainState.PUBLISH_OUTCOME_UNKNOWN
    assert result.failure_kind == expected_failure
    assert result.unknown_quarantined is True
    assert calls == 1
    assert repository.calls.count("quarantine") == 1


@pytest.mark.asyncio
async def test_prefix_drainer_does_not_publish_when_the_signed_identity_no_longer_claims() -> None:
    rejected = OfflineOutboxClaimResult(
        state=OfflineOutboxClaimState.REJECTED,
        rejection=OfflineOutboxClaimRejection.EARLIER_UNPUBLISHED_PREDECESSOR,
    )
    repository = _Repository(rejected)
    drainer = OfflineOutboxPrefixDrainer(repository)  # type: ignore[arg-type]

    async def publisher(_topic: str, _payload: dict[str, Any]) -> None:
        raise AssertionError("publisher must not run after a rejected prefix claim")

    result = await drainer.drain_exact(_identity(), drain_id="signed-prefix-19", publisher=publisher)

    assert result.state is OfflineOutboxDrainState.CLAIM_REJECTED
    assert result.rejection is OfflineOutboxClaimRejection.EARLIER_UNPUBLISHED_PREDECESSOR
    assert repository.calls == ["claim"]
