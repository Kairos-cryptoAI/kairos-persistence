"""Atomic SIM-only preregistration and sealed matched-arm coverage.

Never applies runtime migrations, calls a provider, evaluates PnL or creates
trade authority. The simulator DB trigger also guards inserts made outside
this repository so an unscheduled result cannot be silently accepted.
"""

from __future__ import annotations

import json
from typing import Any, cast

from kairos_core import (
    RESEARCH_ARMS,
    ResearchCoverageSealV1,
    ResearchObservationScheduleV1,
    canonical_sha256,
)
from kairos_core.contracts.research_schedule import ResearchArm
from kairos_core.research_schedule import evaluate_research_coverage
from pydantic import ValidationError

from .database import Database, MigrationProfile
from .repository import MessageIdentityConflict
from .research_decision_samples import ResearchDecisionSampleRepository, _sample_from_row
from .runtime import canonical_payload

_RESEARCH_LOCK_NAMESPACE = 849621


class ResearchObservationScheduleRepository:
    """Freeze one campaign roster before results, then seal exhaustive coverage."""

    def __init__(self, database: Database) -> None:
        if not isinstance(database, Database):
            raise TypeError("research schedule requires an explicit Database")
        if database.migration_profile is not MigrationProfile.SIMULATOR:
            raise ValueError("research schedule requires the isolated SIMULATOR profile")
        if database.read_only:
            raise ValueError("research schedule requires a writable simulator database")
        self._database = database

    async def register(self, schedule: ResearchObservationScheduleV1) -> bool:
        """Atomically preregister the complete roster; refuse historical adoption."""

        if type(schedule) is not ResearchObservationScheduleV1:
            raise TypeError("research roster accepts only ResearchObservationScheduleV1")
        if schedule.schedule_digest != canonical_sha256(schedule.identity_payload()):
            raise ValueError("research schedule digest does not match its canonical roster")
        encoded, payload_sha256 = canonical_payload(schedule.to_payload())
        async with self._database.transaction() as connection:
            await connection.execute(
                "SELECT pg_advisory_xact_lock($1, hashtext($2))",
                _RESEARCH_LOCK_NAMESPACE,
                schedule.campaign_id,
            )
            row = await connection.fetchrow(
                "SELECT * FROM sim_research_observation_schedules WHERE campaign_id=$1 FOR SHARE",
                schedule.campaign_id,
            )
            if row is not None:
                await self._verify_schedule(connection, row, schedule, encoded, payload_sha256)
                return False
            if await connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM sim_research_decision_samples WHERE campaign_id=$1)",
                schedule.campaign_id,
            ):
                raise MessageIdentityConflict("cannot preregister after the campaign already has arm results")
            await connection.execute(
                """INSERT INTO sim_research_observation_schedules
                   (campaign_id, schedule_digest, strategy_id, strategy_revision, window_count,
                    authority, payload, payload_sha256)
                   VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)""",
                schedule.campaign_id,
                schedule.schedule_digest,
                schedule.strategy_id,
                schedule.strategy_revision,
                len(schedule.windows),
                schedule.authority,
                encoded,
                payload_sha256,
            )
            await connection.executemany(
                """INSERT INTO sim_research_observation_windows
                   (campaign_id, sample_id, symbol, timeframe, market_as_of_ts_ms,
                    market_snapshot_sha256, paired_at_ts_ms, sample_deadline_ts_ms)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8)""",
                [
                    (
                        schedule.campaign_id,
                        window.sample_id,
                        window.symbol,
                        window.timeframe,
                        window.market_as_of_ts_ms,
                        window.market_snapshot_sha256,
                        window.paired_at_ts_ms,
                        window.sample_deadline_ts_ms,
                    )
                    for window in schedule.windows
                ],
            )
            return True

    async def seal_coverage(self, *, campaign_id: str) -> ResearchCoverageSealV1:
        """Seal only an exact three-arm matrix; later inserts are DB-rejected."""

        if not isinstance(campaign_id, str) or not campaign_id or campaign_id != campaign_id.strip():
            raise ValueError("campaign_id must be a normalized non-empty identifier")
        async with self._database.transaction() as connection:
            await connection.execute(
                "SELECT pg_advisory_xact_lock($1, hashtext($2))", _RESEARCH_LOCK_NAMESPACE, campaign_id
            )
            row = await connection.fetchrow(
                "SELECT * FROM sim_research_observation_schedules WHERE campaign_id=$1 FOR UPDATE",
                campaign_id,
            )
            if row is None:
                raise ValueError("campaign has no preregistered SIM research schedule")
            if await connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM sim_research_coverage_seals WHERE campaign_id=$1)", campaign_id
            ):
                raise MessageIdentityConflict("research coverage was already sealed")
            schedule = _schedule_from_row(row)
            encoded, payload_sha256 = canonical_payload(schedule.to_payload())
            await self._verify_schedule(connection, row, schedule, encoded, payload_sha256)
            from .adaptive_candidate_protocols import ResearchAdaptiveCandidateProtocolRepository

            protocol_row = await connection.fetchrow(
                "SELECT * FROM sim_research_adaptive_candidate_protocols WHERE campaign_id=$1 FOR SHARE",
                campaign_id,
            )
            if protocol_row is None:
                raise ValueError("campaign has no registered adaptive candidate protocol")
            protocol = ResearchAdaptiveCandidateProtocolRepository._verify_row(protocol_row)
            if (
                protocol_row["freeze_txid"] == await connection.fetchval("SELECT txid_current()")
                or protocol.campaign_id != schedule.campaign_id
                or protocol.schedule_digest != schedule.schedule_digest
            ):
                raise MessageIdentityConflict(
                    "coverage requires a previously committed exact adaptive protocol"
                )
            sample_rows = await connection.fetch(
                "SELECT * FROM sim_research_decision_samples WHERE campaign_id=$1 "
                "ORDER BY sample_id, arm_id FOR SHARE",
                campaign_id,
            )
            samples = []
            for sample_row in sample_rows:
                sample = _sample_from_row(sample_row)
                sample_encoded, sample_sha256 = canonical_payload(sample.to_payload())
                if not ResearchDecisionSampleRepository._row_matches(
                    sample_row, sample, sample_encoded, sample_sha256
                ):
                    raise MessageIdentityConflict("stored research arm result failed integrity verification")
                if sample.arm_id not in RESEARCH_ARMS:
                    raise MessageIdentityConflict("stored research result uses an unknown protocol arm")
                arm_id = cast(ResearchArm, sample.arm_id)
                if sample.arm_protocol_digest != protocol.arm_digest(arm_id):
                    raise MessageIdentityConflict(
                        "stored research arm result differs from its frozen protocol arm"
                    )
                if sample_row["recorded_at"] <= row["frozen_at"]:
                    raise MessageIdentityConflict("research arm result predates its frozen schedule")
                samples.append(sample)
            evaluated_seal = evaluate_research_coverage(schedule, samples)
            seal = ResearchCoverageSealV1(
                campaign_id=evaluated_seal.campaign_id,
                schedule_digest=evaluated_seal.schedule_digest,
                candidate_protocol_digest=protocol.protocol_digest,
                expected_result_count=evaluated_seal.expected_result_count,
                result_ids_sha256=evaluated_seal.result_ids_sha256,
            )
            encoded_seal, seal_payload_sha256 = canonical_payload(seal.to_payload())
            await connection.execute(
                """INSERT INTO sim_research_coverage_seals
                   (campaign_id, coverage_digest, schedule_digest, candidate_protocol_digest,
                    expected_result_count,
                    result_ids_sha256, authority, payload, payload_sha256)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)""",
                seal.campaign_id,
                seal.coverage_digest,
                seal.schedule_digest,
                seal.candidate_protocol_digest,
                seal.expected_result_count,
                seal.result_ids_sha256,
                seal.authority,
                encoded_seal,
                seal_payload_sha256,
            )
            return seal

    @staticmethod
    async def _verify_schedule(
        connection: Any,
        row: Any,
        schedule: ResearchObservationScheduleV1,
        encoded: str,
        payload_sha256: str,
    ) -> None:
        try:
            stored_encoded, stored_sha256 = canonical_payload(_decode_payload(row["payload"]))
        except (TypeError, ValueError) as exc:
            raise MessageIdentityConflict("stored research roster payload is invalid") from exc
        if not (
            row["campaign_id"] == schedule.campaign_id
            and row["schedule_digest"] == schedule.schedule_digest
            and row["strategy_id"] == schedule.strategy_id
            and row["strategy_revision"] == schedule.strategy_revision
            and row["window_count"] == len(schedule.windows)
            and row["authority"] == schedule.authority
            and row["payload_sha256"] == payload_sha256 == stored_sha256
            and stored_encoded == encoded
        ):
            raise MessageIdentityConflict("campaign already has a different immutable research schedule")
        rows = await connection.fetch(
            """SELECT sample_id, symbol, timeframe, market_as_of_ts_ms, market_snapshot_sha256,
                      paired_at_ts_ms, sample_deadline_ts_ms
               FROM sim_research_observation_windows
               WHERE campaign_id=$1 ORDER BY market_as_of_ts_ms, symbol, timeframe, sample_id""",
            schedule.campaign_id,
        )
        expected = [
            (
                window.sample_id,
                window.symbol,
                window.timeframe,
                window.market_as_of_ts_ms,
                window.market_snapshot_sha256,
                window.paired_at_ts_ms,
                window.sample_deadline_ts_ms,
            )
            for window in schedule.windows
        ]
        actual = [
            tuple(
                row[field]
                for field in (
                    "sample_id",
                    "symbol",
                    "timeframe",
                    "market_as_of_ts_ms",
                    "market_snapshot_sha256",
                    "paired_at_ts_ms",
                    "sample_deadline_ts_ms",
                )
            )
            for row in rows
        ]
        if actual != expected:
            raise MessageIdentityConflict("stored research windows differ from the frozen roster")


def _decode_payload(value: object) -> dict[str, Any]:
    try:
        decoded = json.loads(value) if isinstance(value, str) else value
    except (TypeError, ValueError) as exc:
        raise MessageIdentityConflict("stored research roster is not valid JSON") from exc
    if not isinstance(decoded, dict):
        raise MessageIdentityConflict("stored research roster payload must be an object")
    return decoded


def _schedule_from_row(row: Any) -> ResearchObservationScheduleV1:
    try:
        return ResearchObservationScheduleV1.model_validate(_decode_payload(row["payload"]))
    except (TypeError, ValueError, ValidationError) as exc:
        raise MessageIdentityConflict("stored research roster violates its typed contract") from exc
