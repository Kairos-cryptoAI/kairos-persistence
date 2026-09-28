"""Append-only adaptive candidate rosters for isolated SIM research."""

from __future__ import annotations

from typing import Any

from kairos_core import AdaptiveCandidateProtocolV1, canonical_sha256
from pydantic import ValidationError

from .database import Database, MigrationProfile
from .repository import MessageIdentityConflict
from .research_observation_schedule import (
    ResearchObservationScheduleRepository,
    _decode_payload,
    _schedule_from_row,
)
from .runtime import canonical_payload

_RESEARCH_LOCK_NAMESPACE = 849621
_RESEARCH_ARMS = ("strategy-only", "strategy-review", "llm-proposal-research")


class ResearchAdaptiveCandidateProtocolRepository:
    """Register one immutable candidate roster before scheduled results."""

    def __init__(self, database: Database) -> None:
        if not isinstance(database, Database):
            raise TypeError("adaptive candidate protocol storage requires an explicit Database")
        if database.migration_profile is not MigrationProfile.SIMULATOR:
            raise ValueError("adaptive candidate protocol storage requires the isolated SIMULATOR profile")
        if database.read_only:
            raise ValueError("adaptive candidate protocol storage cannot use a read-only database")
        self._database = database

    async def register(self, protocol: AdaptiveCandidateProtocolV1) -> bool:
        """Atomically bind a protocol to its exact committed schedule.

        Returns ``False`` for the exact same protocol. A schedule that already
        has any results or a coverage seal is intentionally not retroactively
        adopted, including pre-024 legacy schedules.
        """

        if type(protocol) is not AdaptiveCandidateProtocolV1:
            raise TypeError("adaptive protocol storage accepts only AdaptiveCandidateProtocolV1")
        if protocol.protocol_digest != canonical_sha256(protocol.identity_payload()):
            raise ValueError("adaptive candidate protocol digest does not match its canonical roster")
        if protocol.campaign_id != protocol.campaign_id.strip():
            raise ValueError("adaptive candidate protocol campaign_id must be normalized")
        arm_digests = _arm_digests(protocol)
        encoded, payload_sha256 = canonical_payload(protocol.to_payload())
        arm_digests_json, _ = canonical_payload(arm_digests)

        async with self._database.transaction() as connection:
            await connection.execute(
                "SELECT pg_advisory_xact_lock($1, hashtext($2))",
                _RESEARCH_LOCK_NAMESPACE,
                protocol.campaign_id,
            )
            schedule_row = await connection.fetchrow(
                "SELECT * FROM sim_research_observation_schedules WHERE campaign_id=$1 FOR UPDATE",
                protocol.campaign_id,
            )
            if schedule_row is None:
                raise ValueError("adaptive candidate protocol requires an existing SIM research schedule")
            schedule = _schedule_from_row(schedule_row)
            schedule_encoded, schedule_sha256 = canonical_payload(schedule.to_payload())
            await ResearchObservationScheduleRepository._verify_schedule(
                connection, schedule_row, schedule, schedule_encoded, schedule_sha256
            )
            if (
                protocol.campaign_id != schedule.campaign_id
                or protocol.schedule_digest != schedule.schedule_digest
            ):
                raise MessageIdentityConflict("adaptive candidate protocol differs from the frozen schedule")

            existing = await connection.fetchrow(
                "SELECT * FROM sim_research_adaptive_candidate_protocols WHERE campaign_id=$1 FOR SHARE",
                protocol.campaign_id,
            )
            if existing is not None:
                stored = self._verify_row(existing)
                stored_arm_digests = _decode_payload(existing["arm_digests"])
                stored_encoded, stored_sha256 = canonical_payload(stored.to_payload())
                if (
                    stored.protocol_digest != protocol.protocol_digest
                    or stored_encoded != encoded
                    or stored_sha256 != payload_sha256
                    or stored_arm_digests != arm_digests
                ):
                    raise MessageIdentityConflict(
                        "campaign already has a different immutable adaptive protocol"
                    )
                return False

            if not bool(schedule_row["adaptive_protocol_registration_allowed"]):
                raise MessageIdentityConflict(
                    "legacy SIM schedules cannot receive a retroactive adaptive protocol"
                )
            if await connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM sim_research_decision_samples WHERE campaign_id=$1)",
                protocol.campaign_id,
            ):
                raise MessageIdentityConflict("cannot register an adaptive protocol after campaign results")
            if await connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM sim_research_coverage_seals WHERE campaign_id=$1)",
                protocol.campaign_id,
            ):
                raise MessageIdentityConflict("cannot register an adaptive protocol after a coverage seal")

            await connection.execute(
                """INSERT INTO sim_research_adaptive_candidate_protocols
                   (campaign_id, schedule_digest, protocol_digest, arm_digests, authority,
                    payload, payload_sha256)
                   VALUES ($1,$2,$3,$4::jsonb,$5,$6::jsonb,$7)""",
                protocol.campaign_id,
                protocol.schedule_digest,
                protocol.protocol_digest,
                arm_digests_json,
                protocol.authority,
                encoded,
                payload_sha256,
            )
            return True

    async def resolve_arm_digest(self, *, campaign_id: str, arm_id: str) -> str:
        """Resolve one frozen arm digest for the SIM sample builder."""

        if not isinstance(campaign_id, str) or not campaign_id or campaign_id != campaign_id.strip():
            raise ValueError("campaign_id must be a non-empty normalized identifier")
        if arm_id not in _RESEARCH_ARMS:
            raise ValueError("arm_id is outside the fixed adaptive candidate roster")
        async with self._database.pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT * FROM sim_research_adaptive_candidate_protocols WHERE campaign_id=$1",
                campaign_id,
            )
        if row is None:
            raise ValueError("campaign has no registered adaptive candidate protocol")
        protocol = self._verify_row(row)
        return protocol.arm_digest(arm_id)  # type: ignore[arg-type]

    @staticmethod
    def _verify_row(row: Any) -> AdaptiveCandidateProtocolV1:
        try:
            payload = _decode_payload(row["payload"])
            protocol = AdaptiveCandidateProtocolV1.model_validate(payload)
            encoded, payload_sha256 = canonical_payload(protocol.to_payload())
            arm_digests = _arm_digests(protocol)
            stored_arm_digests = _decode_payload(row["arm_digests"])
        except (TypeError, ValueError, ValidationError, KeyError) as exc:
            raise MessageIdentityConflict("stored adaptive candidate protocol is invalid") from exc
        if not isinstance(stored_arm_digests, dict) or not (
            row["campaign_id"] == protocol.campaign_id
            and row["schedule_digest"] == protocol.schedule_digest
            and row["protocol_digest"] == protocol.protocol_digest
            and row["authority"] == protocol.authority
            and stored_arm_digests == arm_digests
            and row["payload_sha256"] == payload_sha256
            and canonical_payload(payload)[0] == encoded
        ):
            raise MessageIdentityConflict("stored adaptive candidate protocol failed integrity verification")
        return protocol


def _arm_digests(protocol: AdaptiveCandidateProtocolV1) -> dict[str, str]:
    return {arm.arm_id: protocol.arm_digest(arm.arm_id) for arm in protocol.arms}  # type: ignore[arg-type]
