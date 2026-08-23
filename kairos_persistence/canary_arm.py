"""Single-use durable authorization for manually armed technical canaries."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import asyncpg
from kairos_core.contracts import CandidateReviewV1, StrategicAllocation
from kairos_core.enums import MarketRegime, ReviewDecision, Side

from .repository import AuditRepository, MessageIdentityConflict
from .runtime import canonical_payload

CANARY_REVIEW_TOPIC = "kairos.aggregator.review.v1"
CANARY_SOURCE = "kairos-paper-canary"
CANARY_STRATEGY_ID = "technical-canary"
CANARY_STRATEGY_REVISION = "1"
CANARY_WEIGHT = 0.0025
CANARY_REASON = "TECHNICAL_CANARY_MANUAL_POLICY"
CANARY_RATIONALE = "Manually armed EVEDEX DEV technical canary; no alpha or LLM claim."
CANARY_VENUE_SYMBOLS = {
    "BTCUSDT": "BTCUSD:DEV",
    "ETHUSDT": "ETHUSD:DEV",
    "SOLUSDT": "SOLUSD:DEV",
    "BNBUSDT": "BNBUSD:DEV",
    "XRPUSDT": "XRPUSD:DEV",
}


@dataclass(frozen=True, slots=True)
class PaperCanaryArm:
    arm_id: str
    account_id: str
    review: CandidateReviewV1
    allocation: StrategicAllocation
    status: str
    expires_at: datetime
    decided_at_ms: int | None


class PaperCanaryArmRepository:
    """Atomically arm, enqueue and consume one exact canary review."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def arm(
        self,
        *,
        account_id: str,
        review: CandidateReviewV1,
        allocation: StrategicAllocation,
    ) -> PaperCanaryArm:
        self._validate_account(account_id)
        self._validate_binding(review, allocation)
        self._validate_account_binding(account_id, review)
        review_payload = review.model_dump(mode="json")
        allocation_payload = allocation.model_dump(mode="json")
        review_json, review_sha = canonical_payload(review_payload)
        allocation_json, allocation_sha = canonical_payload(allocation_payload)
        arm_json, _ = canonical_payload(
            {
                "account_id": account_id,
                "allocation": allocation_payload,
                "domain": "kairos.paper-canary-arm.v1",
                "review": review_payload,
            }
        )
        arm_id = hashlib.sha256(arm_json.encode("utf-8")).hexdigest()
        expires_at = datetime.fromtimestamp(
            review.intent.entry_expires_ts_ms / 1_000,
            tz=UTC,
        )
        audit = AuditRepository(self.pool)
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await self._account_xact_lock(connection, account_id)
                await connection.execute(
                    """UPDATE paper_canary_arms
                          SET status='EXPIRED'
                        WHERE account_id=$1 AND status='ARMED' AND expires_at < now()""",
                    account_id,
                )
                active_arm_id = await connection.fetchval(
                    """SELECT arm_id FROM paper_canary_arms
                        WHERE account_id=$1 AND status='ARMED' FOR UPDATE""",
                    account_id,
                )
                existing = await connection.fetchrow(
                    "SELECT * FROM paper_canary_arms WHERE arm_id=$1 FOR UPDATE",
                    arm_id,
                )
                if active_arm_id is not None and active_arm_id != arm_id:
                    raise ValueError("another technical canary session is already armed for this account")
                if existing is None:
                    row = await connection.fetchrow(
                        """INSERT INTO paper_canary_arms
                           (arm_id,review_id,intent_id,account_id,symbol,side,
                            review_payload,review_sha256,allocation_payload,allocation_sha256,
                            status,expires_at)
                           SELECT $1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9::jsonb,$10,'ARMED',$11
                            WHERE $11 >= now()
                           RETURNING *""",
                        arm_id,
                        review.review_id,
                        review.intent.intent_id,
                        account_id,
                        review.intent.symbol,
                        review.intent.side.value,
                        review_json,
                        review_sha,
                        allocation_json,
                        allocation_sha,
                        expires_at,
                    )
                    if row is None:
                        raise ValueError("cannot arm an expired technical canary")
                else:
                    row = existing
                    self._validate_row_identity(
                        row,
                        account_id=account_id,
                        review_sha=review_sha,
                        allocation_sha=allocation_sha,
                    )
                await audit.append_event(CANARY_REVIEW_TOPIC, review, connection=connection)
                await audit.enqueue_outbox(
                    connection,
                    review.message_id,
                    CANARY_REVIEW_TOPIC,
                    review_json,
                    review_sha,
                )
        return self._record(row)

    async def consume(
        self,
        *,
        account_id: str,
        review: CandidateReviewV1,
    ) -> PaperCanaryArm | None:
        """Consume or replay the exact stored authorization for ``review``."""
        self._validate_account(account_id)
        self._validate_account_binding(account_id, review)
        review_json, review_sha = canonical_payload(review.model_dump(mode="json"))
        del review_json
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await self._account_xact_lock(connection, account_id)
                row = await connection.fetchrow(
                    """SELECT * FROM paper_canary_arms
                        WHERE account_id=$1 AND review_id=$2 FOR UPDATE""",
                    account_id,
                    review.review_id,
                )
                if row is None:
                    return None
                if row["review_sha256"] != review_sha:
                    raise MessageIdentityConflict("manual canary review_id was reused with different content")
                if row["status"] == "EXPIRED":
                    return None
                database_now = await connection.fetchval("SELECT clock_timestamp()")
                if row["status"] == "ARMED" and row["expires_at"] < database_now:
                    await connection.execute(
                        "UPDATE paper_canary_arms SET status='EXPIRED' WHERE arm_id=$1",
                        row["arm_id"],
                    )
                    return None
                if row["status"] == "ARMED":
                    decided_at_ms = max(
                        review.reviewed_at_ms,
                        review.intent.entry_eligible_ts_ms,
                        int(database_now.astimezone(UTC).timestamp() * 1_000),
                    )
                    row = await connection.fetchrow(
                        """UPDATE paper_canary_arms
                              SET status='CONSUMED', consumed_at=now(), decided_at_ms=$2
                            WHERE arm_id=$1 AND status='ARMED'
                            RETURNING *""",
                        row["arm_id"],
                        decided_at_ms,
                    )
                    if row is None:
                        raise RuntimeError("manual canary authorization lost its row lock")
                return self._record(row)

    async def get(self, arm_id: str) -> PaperCanaryArm | None:
        if not arm_id or arm_id != arm_id.strip():
            raise ValueError("arm_id must be a normalized non-empty string")
        row = await self.pool.fetchrow(
            "SELECT * FROM paper_canary_arms WHERE arm_id=$1",
            arm_id,
        )
        return None if row is None else self._record(row)

    @staticmethod
    async def _account_xact_lock(
        connection: asyncpg.Connection,
        account_id: str,
    ) -> None:
        await connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            f"kairos.paper-canary-arm:{account_id}",
        )

    @staticmethod
    def _validate_account(account_id: str) -> None:
        if not account_id or account_id != account_id.strip():
            raise ValueError("account_id must be a normalized non-empty string")
        if account_id.casefold() in {"primary", "prod", "production", "live"}:
            raise ValueError("technical canary requires a dedicated non-production account")

    @staticmethod
    def _validate_binding(
        review: CandidateReviewV1,
        allocation: StrategicAllocation,
    ) -> None:
        intent = review.intent
        route = review.route
        if (
            review.source != CANARY_SOURCE
            or route.source != CANARY_SOURCE
            or intent.source != CANARY_SOURCE
            or allocation.source != CANARY_SOURCE
        ):
            raise ValueError("manual canary binding requires the fixed canary source")
        if (
            review.decision is not ReviewDecision.ALLOW
            or review.reviewer != "DETERMINISTIC"
            or review.model_provenance is not None
            or review.priority != 0
            or review.reason_codes != (CANARY_REASON,)
        ):
            raise ValueError("manual canary arm requires a deterministic ALLOW review")
        if intent.strategy_id != CANARY_STRATEGY_ID or intent.strategy_revision != CANARY_STRATEGY_REVISION:
            raise ValueError("manual canary arm requires technical-canary@1")
        if (
            review.message_id != review.review_id
            or review.correlation_id != intent.intent_id
            or review.causation_id != route.message_id
            or route.message_id != route.route_id
            or route.correlation_id != intent.intent_id
            or route.causation_id != intent.message_id
            or intent.message_id != intent.intent_id
            or intent.correlation_id != intent.intent_id
            or intent.causation_id is not None
        ):
            raise ValueError("manual canary envelopes must preserve exact deterministic lineage")
        if (
            route.intent != intent
            or review.evidence != intent.evidence
            or route.review_tier.value != "NORMAL"
            or route.requested_reasoning_effort.value != "medium"
            or route.conflict_rationale is not None
            or route.routed_at_ms != intent.decision_ts_ms
            or route.review_deadline_ms != intent.entry_expires_ts_ms
            or review.reviewed_at_ms != intent.entry_eligible_ts_ms
        ):
            raise ValueError("manual canary review must preserve the exact fixed route")
        if (
            intent.signal_strength != 0.0
            or intent.entry_eligible_ts_ms != intent.decision_ts_ms + 1
            or len(intent.evidence) != 1
            or intent.evidence[0].kind != "closed_bar"
            or intent.evidence[0].content_sha256 is None
            or route.evidence_ids != (intent.evidence[0].content_sha256,)
        ):
            raise ValueError("manual canary intent must be bound to one closed bar")
        metadata = dict(intent.metadata)
        expected_venue_symbol = CANARY_VENUE_SYMBOLS.get(intent.symbol)
        if expected_venue_symbol is None or metadata != {
            "account_id": metadata.get("account_id"),
            "alpha_claim": "false",
            "entry_policy": "NEXT_BAR_MARKET",
            "purpose": "technical_execution_canary",
            "venue_symbol": expected_venue_symbol,
        }:
            raise ValueError("manual canary intent metadata is not the fixed DEV policy")
        if not metadata["account_id"]:
            raise ValueError("manual canary intent requires an account binding")
        if set(allocation.strategy_weights) != {CANARY_STRATEGY_ID} or not math.isclose(
            allocation.strategy_weights[CANARY_STRATEGY_ID],
            CANARY_WEIGHT,
            rel_tol=0,
            abs_tol=1e-12,
        ):
            raise ValueError("manual canary allocation must reserve exactly 0.25% for the canary")
        if not math.isclose(
            allocation.stable_reserve_pct,
            1 - CANARY_WEIGHT,
            rel_tol=0,
            abs_tol=1e-12,
        ):
            raise ValueError("manual canary allocation must keep 99.75% in reserve")
        if not math.isclose(allocation.max_gross_leverage, 1.0, rel_tol=0, abs_tol=1e-12):
            raise ValueError("manual canary allocation must be limited to 1x")
        expected_regime = MarketRegime.BULL if intent.side is Side.LONG else MarketRegime.BEAR
        if intent.side not in {Side.LONG, Side.SHORT} or allocation.regime is not expected_regime:
            raise ValueError("manual canary allocation must be direction-compatible")
        if allocation.correlation_id != intent.intent_id:
            raise ValueError("manual canary allocation must be bound to the exact intent")
        produced_at_ms = int(allocation.produced_at.timestamp() * 1_000)
        if (
            allocation.causation_id != intent.message_id
            or produced_at_ms != intent.entry_eligible_ts_ms
            or allocation.triggered_by.value != "schedule"
            or allocation.rationale != CANARY_RATIONALE
        ):
            raise ValueError("manual canary allocation must preserve fixed lineage and policy")
        allocation_identity = {
            "causation_id": intent.message_id,
            "contract_version": "technical-canary-allocation.v1",
            "correlation_id": intent.intent_id,
            "max_gross_leverage": 1.0,
            "produced_at_ms": intent.entry_eligible_ts_ms,
            "regime": expected_regime.value,
            "rationale": CANARY_RATIONALE,
            "schema_version": "1.0",
            "source": CANARY_SOURCE,
            "stable_reserve_pct": 1.0 - CANARY_WEIGHT,
            "strategy_weights": {CANARY_STRATEGY_ID: CANARY_WEIGHT},
            "triggered_by": "schedule",
        }
        if allocation.message_id != canonical_payload(allocation_identity)[1]:
            raise ValueError("manual canary allocation message_id is not deterministic")

    @staticmethod
    def _validate_account_binding(account_id: str, review: CandidateReviewV1) -> None:
        if dict(review.intent.metadata).get("account_id") != account_id:
            raise ValueError("manual canary intent account does not match repository scope")

    @staticmethod
    def _validate_row_identity(
        row,
        *,
        account_id: str,
        review_sha: str,
        allocation_sha: str,
    ) -> None:
        if (
            row["account_id"] != account_id
            or row["review_sha256"] != review_sha
            or row["allocation_sha256"] != allocation_sha
        ):
            raise MessageIdentityConflict("manual canary arm identity was reused with different content")

    @staticmethod
    def _record(row) -> PaperCanaryArm:
        review_payload = PaperCanaryArmRepository._object(row["review_payload"])
        allocation_payload = PaperCanaryArmRepository._object(row["allocation_payload"])
        if canonical_payload(review_payload)[1] != row["review_sha256"]:
            raise MessageIdentityConflict("stored canary review fingerprint does not match")
        if canonical_payload(allocation_payload)[1] != row["allocation_sha256"]:
            raise MessageIdentityConflict("stored canary allocation fingerprint does not match")
        return PaperCanaryArm(
            arm_id=row["arm_id"],
            account_id=row["account_id"],
            review=CandidateReviewV1.model_validate(review_payload),
            allocation=StrategicAllocation.model_validate(allocation_payload),
            status=row["status"],
            expires_at=row["expires_at"],
            decided_at_ms=row["decided_at_ms"],
        )

    @staticmethod
    def _object(value: Any) -> dict[str, Any]:
        if isinstance(value, str):
            value = json.loads(value)
        if not isinstance(value, dict):
            raise MessageIdentityConflict("stored canary payload is not a JSON object")
        return value
