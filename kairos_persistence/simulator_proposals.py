"""Append-only storage for research-only LLM proposals in the isolated SIM DB."""

from __future__ import annotations

import json
from typing import Any

from kairos_core import LLMTradeProposalV1

from .database import Database, MigrationProfile
from .repository import MessageIdentityConflict
from .runtime import canonical_payload


class SimulatorProposalRepository:
    """Persist typed proposals only in a writable ``kairos_sim`` database.

    This repository accepts no strategy intent, risk decision, venue, or order
    contract. Its table belongs only to ``MigrationProfile.SIMULATOR`` and is
    append-only at the database layer; runtime/PAPER profiles never create it.
    """

    def __init__(self, database: Database) -> None:
        if not isinstance(database, Database):
            raise TypeError("simulator proposal storage requires an explicit Database")
        if database.migration_profile is not MigrationProfile.SIMULATOR:
            raise ValueError("simulator proposal storage requires the isolated SIMULATOR profile")
        if database.read_only:
            raise ValueError("simulator proposal storage cannot use a read-only database")
        self._database = database

    async def record(self, proposal: LLMTradeProposalV1) -> bool:
        """Append one proposal; exact replay is idempotent, identity drift fails closed."""

        if type(proposal) is not LLMTradeProposalV1:
            raise TypeError("simulator proposal storage accepts only LLMTradeProposalV1")
        proposal_id = proposal.proposal_id
        if proposal_id is None:  # impossible after contract validation
            raise ValueError("LLM proposal is missing its canonical ID")
        payload = proposal.to_payload()
        encoded, payload_sha256 = canonical_payload(payload)

        async with self._database.transaction() as connection:
            inserted = await connection.fetchrow(
                """INSERT INTO sim_llm_trade_proposals
                       (proposal_id, campaign_id, arm_id, sample_id, symbol, timeframe,
                        market_as_of_ts_ms, expires_at_ts_ms, market_snapshot_sha256,
                        action, payload, payload_sha256)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12)
                   ON CONFLICT DO NOTHING
                   RETURNING proposal_id""",
                proposal_id,
                proposal.campaign_id,
                proposal.arm_id,
                proposal.sample_id,
                proposal.symbol,
                proposal.timeframe,
                proposal.market_as_of_ts_ms,
                proposal.expires_at_ts_ms,
                proposal.market_snapshot_sha256,
                proposal.action.value,
                encoded,
                payload_sha256,
            )
            if inserted is not None:
                return True

            rows = await connection.fetch(
                """SELECT proposal_id, campaign_id, arm_id, sample_id, symbol, timeframe,
                          market_as_of_ts_ms, expires_at_ts_ms, market_snapshot_sha256,
                          action, payload, payload_sha256
                   FROM sim_llm_trade_proposals
                   WHERE proposal_id=$1 OR (campaign_id=$2 AND arm_id=$3 AND sample_id=$4)
                   FOR SHARE""",
                proposal_id,
                proposal.campaign_id,
                proposal.arm_id,
                proposal.sample_id,
            )
            if len(rows) != 1:
                raise MessageIdentityConflict("simulator proposal identity is ambiguous or missing")
            row = rows[0]
            if not self._row_matches(row, proposal, encoded, payload_sha256):
                raise MessageIdentityConflict(
                    "campaign/arm/sample already contains a different immutable LLM proposal"
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
    ) -> tuple[LLMTradeProposalV1, ...]:
        """Load a bounded, integrity-checked page in causal timestamp order."""

        _require_identifier("campaign_id", campaign_id)
        _require_identifier("arm_id", arm_id)
        if (after_market_as_of_ts_ms is None) != (after_sample_id is None):
            raise ValueError("both proposal page cursor fields must be set together")
        if after_market_as_of_ts_ms is not None and (
            isinstance(after_market_as_of_ts_ms, bool)
            or not isinstance(after_market_as_of_ts_ms, int)
            or after_market_as_of_ts_ms < 0
        ):
            raise ValueError("proposal page cursor timestamp must be a non-negative integer")
        if after_sample_id is not None:
            _require_identifier("after_sample_id", after_sample_id)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1_000:
            raise ValueError("proposal page limit must be an integer from 1 through 1000")

        async with self._database.pool.acquire() as connection:
            rows = await connection.fetch(
                """SELECT proposal_id, campaign_id, arm_id, sample_id, symbol, timeframe,
                          market_as_of_ts_ms, expires_at_ts_ms, market_snapshot_sha256,
                          action, payload, payload_sha256
                   FROM sim_llm_trade_proposals
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

        proposals: list[LLMTradeProposalV1] = []
        for row in rows:
            proposal = _proposal_from_row(row)
            encoded, payload_sha256 = canonical_payload(proposal.to_payload())
            if not self._row_matches(row, proposal, encoded, payload_sha256):
                raise MessageIdentityConflict("stored simulator LLM proposal failed integrity verification")
            proposals.append(proposal)
        return tuple(proposals)

    @staticmethod
    def _row_matches(
        row: Any,
        proposal: LLMTradeProposalV1,
        encoded: str,
        payload_sha256: str,
    ) -> bool:
        stored_payload = _decode_payload(row["payload"])
        try:
            stored_encoded, stored_sha256 = canonical_payload(stored_payload)
        except (TypeError, ValueError):
            return False
        return (
            str(row["proposal_id"]) == proposal.proposal_id
            and str(row["campaign_id"]) == proposal.campaign_id
            and str(row["arm_id"]) == proposal.arm_id
            and str(row["sample_id"]) == proposal.sample_id
            and str(row["symbol"]) == proposal.symbol
            and str(row["timeframe"]) == proposal.timeframe
            and int(row["market_as_of_ts_ms"]) == proposal.market_as_of_ts_ms
            and int(row["expires_at_ts_ms"]) == proposal.expires_at_ts_ms
            and str(row["market_snapshot_sha256"]) == proposal.market_snapshot_sha256
            and str(row["action"]) == proposal.action.value
            and str(row["payload_sha256"]) == payload_sha256 == stored_sha256
            and stored_encoded == encoded
        )


def _decode_payload(value: object) -> dict[str, Any]:
    try:
        decoded = json.loads(value) if isinstance(value, str) else value
    except (TypeError, ValueError) as exc:
        raise MessageIdentityConflict("stored simulator LLM proposal is not valid JSON") from exc
    if not isinstance(decoded, dict):
        raise MessageIdentityConflict("stored simulator LLM proposal payload must be a JSON object")
    return decoded


def _proposal_from_row(row: Any) -> LLMTradeProposalV1:
    proposal = LLMTradeProposalV1.model_validate(_decode_payload(row["payload"]))
    if proposal.proposal_id != str(row["proposal_id"]):
        raise MessageIdentityConflict("stored simulator LLM proposal ID does not match its payload")
    return proposal


def _require_identifier(name: str, value: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty normalized identifier")
