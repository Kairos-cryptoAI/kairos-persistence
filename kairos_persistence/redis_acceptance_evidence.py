"""Bounded, payload-free evidence for an already accepted Redis Stream entry."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime

REDIS_ACCEPTANCE_EVIDENCE_SCHEMA = "kairos.redis-acceptance-evidence.v1"
REDIS_ACCEPTANCE_RESOLUTION_SCHEMA = "kairos.redis-acceptance-resolution.v1"
MAX_STREAM_IDS = 16
MAX_EVIDENCE_BYTES = 2048


class RedisAcceptanceEvidenceError(ValueError):
    """Malformed or oversized evidence cannot be used for reconciliation."""


@dataclass(frozen=True)
class RedisAcceptanceEvidenceV1:
    """A controller-observed Redis match, without retaining the message payload.

    The authenticated operator/controller must obtain these values from a
    read-only Redis probe over the exact stream. ``canonical_payload_sha256``
    is computed from the decoded entry's canonical JSON payload. This type is
    evidence metadata only; it does not query Redis or publish anything.
    """

    redis_server_run_id: str
    probe_run_id: str
    probed_at_utc: datetime
    topic: str
    message_id: str
    canonical_payload_sha256: str
    match_count: int
    stream_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.redis_server_run_id, str) or not re.fullmatch(
            r"[0-9a-f]{40}", self.redis_server_run_id
        ):
            raise RedisAcceptanceEvidenceError("Redis server run_id must be 40 lowercase hex characters")
        if not isinstance(self.probe_run_id, str) or not re.fullmatch(
            r"[A-Za-z0-9._-]{1,128}", self.probe_run_id
        ):
            raise RedisAcceptanceEvidenceError("Redis probe run_id must be a bounded literal identifier")
        if (
            not isinstance(self.probed_at_utc, datetime)
            or self.probed_at_utc.tzinfo is None
            or self.probed_at_utc.utcoffset() != UTC.utcoffset(self.probed_at_utc)
        ):
            raise RedisAcceptanceEvidenceError("Redis probe timestamp must be timezone-aware UTC")
        for name, value, maximum in (
            ("topic", self.topic, 250),
            ("message_id", self.message_id, 512),
        ):
            if (
                not isinstance(value, str)
                or not value
                or len(value) > maximum
                or any(ord(character) < 32 for character in value)
            ):
                raise RedisAcceptanceEvidenceError(f"Redis evidence {name} is invalid or oversized")
        if not isinstance(self.canonical_payload_sha256, str) or not re.fullmatch(
            r"[0-9a-f]{64}", self.canonical_payload_sha256
        ):
            raise RedisAcceptanceEvidenceError("Redis evidence payload hash must be lowercase SHA-256")
        if (
            not isinstance(self.match_count, int)
            or isinstance(self.match_count, bool)
            or not 0 <= self.match_count <= 10_000
        ):
            raise RedisAcceptanceEvidenceError("Redis match_count must be between zero and 10000")
        if not isinstance(self.stream_ids, tuple) or len(self.stream_ids) > MAX_STREAM_IDS:
            raise RedisAcceptanceEvidenceError("Redis stream ID evidence exceeds its bounded limit")
        if any(
            not isinstance(stream_id, str) or re.fullmatch(r"[0-9]{1,20}-[0-9]{1,10}", stream_id) is None
            for stream_id in self.stream_ids
        ):
            raise RedisAcceptanceEvidenceError("Redis stream ID is malformed")
        if len(set(self.stream_ids)) != len(self.stream_ids):
            raise RedisAcceptanceEvidenceError("Redis stream IDs must be unique")
        if self.match_count == 0 and self.stream_ids:
            raise RedisAcceptanceEvidenceError("zero Redis matches cannot contain stream IDs")
        if self.match_count == 1 and len(self.stream_ids) != 1:
            raise RedisAcceptanceEvidenceError("one Redis match requires its exact stream ID")
        if self.match_count > 1 and len(self.stream_ids) < 2:
            raise RedisAcceptanceEvidenceError("multiple Redis matches require at least two stream IDs")
        if len(self.stream_ids) != min(self.match_count, MAX_STREAM_IDS):
            raise RedisAcceptanceEvidenceError("Redis stream ID sample does not match its bounded count")
        if len(self.canonical_json().encode("utf-8")) > MAX_EVIDENCE_BYTES:
            raise RedisAcceptanceEvidenceError("Redis acceptance evidence exceeds its byte limit")

    def document(self) -> dict[str, object]:
        """Return safe, payload-free fields suitable for a receipt or audit hash."""

        return {
            "schema": REDIS_ACCEPTANCE_EVIDENCE_SCHEMA,
            "redis_server_run_id": self.redis_server_run_id,
            "probe_run_id": self.probe_run_id,
            "probed_at_utc": self.probed_at_utc.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "topic": self.topic,
            "message_id": self.message_id,
            "canonical_payload_sha256": self.canonical_payload_sha256,
            "match_count": self.match_count,
            "stream_ids": list(self.stream_ids),
        }

    def canonical_json(self) -> str:
        return json.dumps(self.document(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


__all__ = [
    "MAX_EVIDENCE_BYTES",
    "MAX_STREAM_IDS",
    "REDIS_ACCEPTANCE_EVIDENCE_SCHEMA",
    "REDIS_ACCEPTANCE_RESOLUTION_SCHEMA",
    "RedisAcceptanceEvidenceError",
    "RedisAcceptanceEvidenceV1",
]
