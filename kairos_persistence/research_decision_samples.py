"""Append-only matched strategy/LLM decision observations in the isolated SIM DB.

This is evidence storage, not a candidate-to-order promotion path.  No runtime
or PAPER migration owns this table, and the repository accepts only the typed
research sample contract in a writable, explicitly named simulator database.
"""

from __future__ import annotations

import json
from typing import Any

from kairos_core import ResearchDecisionSampleV1, canonical_sha256
from pydantic import ValidationError

from .database import Database, MigrationProfile
from .repository import MessageIdentityConflict
from .runtime import canonical_payload


class ResearchDecisionSampleRepository:
    """Store one immutable paired decision per campaign, arm and sample clock."""

    def __init__(self, database: Database) -> None:
        if not isinstance(database, Database):
            raise TypeError("research decision storage requires an explicit Database")
        if database.migration_profile is not MigrationProfile.SIMULATOR:
            raise ValueError("research decision storage requires the isolated SIMULATOR profile")
        if database.read_only:
            raise ValueError("research decision storage cannot use a read-only database")
        self._database = database

    async def record(self, sample: ResearchDecisionSampleV1) -> bool:
        """Append once, accept an exact replay, reject any logical-key identity drift."""

        if type(sample) is not ResearchDecisionSampleV1:
            raise TypeError("research decision storage accepts only ResearchDecisionSampleV1")
        if sample.sample_record_id is None:  # guaranteed by the contract validator
            raise ValueError("research decision sample is missing its canonical ID")
        if sample.sample_record_id != canonical_sha256(sample.identity_payload()):
            raise ValueError("research decision sample ID does not match its canonical payload")
        encoded, payload_sha256 = canonical_payload(sample.to_payload())

        async with self._database.transaction() as connection:
            inserted = await connection.fetchrow(
                """INSERT INTO sim_research_decision_samples
                       (sample_record_id, campaign_id, arm_id, sample_id, symbol, timeframe,
                        market_as_of_ts_ms, paired_at_ts_ms, sample_deadline_ts_ms,
                        market_snapshot_sha256, strategy_id, strategy_revision, strategy_outcome,
                        strategy_evaluation_sha256, strategy_evidence_as_of_ts_ms,
                        strategy_market_snapshot_sha256, strategy_intent_id,
                        strategy_intent_expires_at_ts_ms, llm_outcome, llm_evidence_as_of_ts_ms,
                        llm_market_snapshot_sha256, llm_proposal_id,
                        llm_proposal_expires_at_ts_ms, llm_completion_receipt_id,
                        llm_completion_started_at_ts_ms, llm_completion_observed_at_ts_ms,
                        llm_failure_receipt_id,
                        llm_failure_class, llm_failure_started_at_ts_ms,
                        llm_failure_observed_at_ts_ms, authority, payload, payload_sha256)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
                           $19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32::jsonb,$33)
                   ON CONFLICT DO NOTHING RETURNING sample_record_id""",
                sample.sample_record_id,
                sample.campaign_id,
                sample.arm_id,
                sample.sample_id,
                sample.symbol,
                sample.timeframe,
                sample.market_as_of_ts_ms,
                sample.paired_at_ts_ms,
                sample.sample_deadline_ts_ms,
                sample.market_snapshot_sha256,
                sample.strategy_id,
                sample.strategy_revision,
                sample.strategy_outcome,
                sample.strategy_evaluation_sha256,
                sample.strategy_evidence_as_of_ts_ms,
                sample.strategy_market_snapshot_sha256,
                sample.strategy_intent_id,
                sample.strategy_intent_expires_at_ts_ms,
                sample.llm_outcome,
                sample.llm_evidence_as_of_ts_ms,
                sample.llm_market_snapshot_sha256,
                sample.llm_proposal_id,
                sample.llm_proposal_expires_at_ts_ms,
                sample.llm_completion_receipt_id,
                sample.llm_completion_started_at_ts_ms,
                sample.llm_completion_observed_at_ts_ms,
                sample.llm_failure_receipt_id,
                sample.llm_failure_class,
                sample.llm_failure_started_at_ts_ms,
                sample.llm_failure_observed_at_ts_ms,
                sample.authority,
                encoded,
                payload_sha256,
            )
            if inserted is not None:
                return True

            rows = await connection.fetch(
                """SELECT * FROM sim_research_decision_samples
                   WHERE sample_record_id=$1 OR (campaign_id=$2 AND arm_id=$3 AND sample_id=$4)
                   FOR SHARE""",
                sample.sample_record_id,
                sample.campaign_id,
                sample.arm_id,
                sample.sample_id,
            )
            if len(rows) != 1:
                raise MessageIdentityConflict("research decision sample identity is ambiguous or missing")
            if not self._row_matches(rows[0], sample, encoded, payload_sha256):
                raise MessageIdentityConflict(
                    "campaign/arm/sample already contains a different immutable research decision"
                )
            return False

    async def load_page(
        self,
        *,
        campaign_id: str,
        arm_id: str,
        after_market_as_of_ts_ms: int | None = None,
        after_sample_id: str | None = None,
        limit: int = 500,
    ) -> tuple[ResearchDecisionSampleV1, ...]:
        """Return a bounded, integrity-checked page in sample-time order."""

        _require_identifier("campaign_id", campaign_id)
        _require_identifier("arm_id", arm_id)
        if (after_market_as_of_ts_ms is None) != (after_sample_id is None):
            raise ValueError("both research decision page cursor fields must be set together")
        if after_market_as_of_ts_ms is not None and (
            isinstance(after_market_as_of_ts_ms, bool)
            or not isinstance(after_market_as_of_ts_ms, int)
            or after_market_as_of_ts_ms < 0
        ):
            raise ValueError("research decision page cursor timestamp must be a non-negative integer")
        if after_sample_id is not None:
            _require_identifier("after_sample_id", after_sample_id)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1_000:
            raise ValueError("research decision page limit must be an integer from 1 through 1000")

        async with self._database.pool.acquire() as connection:
            rows = await connection.fetch(
                """SELECT * FROM sim_research_decision_samples
                   WHERE campaign_id=$1 AND arm_id=$2
                     AND ($3::BIGINT IS NULL OR (market_as_of_ts_ms, sample_id) > ($3, $4))
                   ORDER BY market_as_of_ts_ms, sample_id
                   LIMIT $5""",
                campaign_id,
                arm_id,
                after_market_as_of_ts_ms,
                after_sample_id,
                limit,
            )

        samples: list[ResearchDecisionSampleV1] = []
        for row in rows:
            sample = _sample_from_row(row)
            encoded, payload_sha256 = canonical_payload(sample.to_payload())
            if not self._row_matches(row, sample, encoded, payload_sha256):
                raise MessageIdentityConflict("stored research decision sample failed integrity verification")
            samples.append(sample)
        return tuple(samples)

    @staticmethod
    def _row_matches(
        row: Any,
        sample: ResearchDecisionSampleV1,
        encoded: str,
        payload_sha256: str,
    ) -> bool:
        stored_payload = _decode_payload(row["payload"])
        try:
            stored_encoded, stored_sha256 = canonical_payload(stored_payload)
        except (TypeError, ValueError):
            return False
        fields = (
            "sample_record_id",
            "campaign_id",
            "arm_id",
            "sample_id",
            "symbol",
            "timeframe",
            "market_as_of_ts_ms",
            "paired_at_ts_ms",
            "sample_deadline_ts_ms",
            "market_snapshot_sha256",
            "strategy_id",
            "strategy_revision",
            "strategy_outcome",
            "strategy_evaluation_sha256",
            "strategy_evidence_as_of_ts_ms",
            "strategy_market_snapshot_sha256",
            "strategy_intent_id",
            "strategy_intent_expires_at_ts_ms",
            "llm_outcome",
            "llm_evidence_as_of_ts_ms",
            "llm_market_snapshot_sha256",
            "llm_proposal_id",
            "llm_proposal_expires_at_ts_ms",
            "llm_completion_receipt_id",
            "llm_completion_started_at_ts_ms",
            "llm_completion_observed_at_ts_ms",
            "llm_failure_receipt_id",
            "llm_failure_class",
            "llm_failure_started_at_ts_ms",
            "llm_failure_observed_at_ts_ms",
            "authority",
        )
        return (
            all(row[field] == getattr(sample, field) for field in fields)
            and row["payload_sha256"] == payload_sha256 == stored_sha256
            and stored_encoded == encoded
        )


def _decode_payload(value: object) -> dict[str, Any]:
    try:
        decoded = json.loads(value) if isinstance(value, str) else value
    except (TypeError, ValueError) as exc:
        raise MessageIdentityConflict("stored research decision sample is not valid JSON") from exc
    if not isinstance(decoded, dict):
        raise MessageIdentityConflict("stored research decision sample payload must be a JSON object")
    return decoded


def _sample_from_row(row: Any) -> ResearchDecisionSampleV1:
    try:
        return ResearchDecisionSampleV1.model_validate(_decode_payload(row["payload"]))
    except (TypeError, ValueError, ValidationError) as exc:
        raise MessageIdentityConflict("stored research decision sample violates its typed contract") from exc


def _require_identifier(name: str, value: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty normalized identifier")
