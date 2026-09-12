"""Bounded DEV canary admission; operational evidence is not an alpha claim.

Receipts are derived from database-timed, hash-chained observations, never an
operator's ``accepted=true`` JSON. The recorder is a trusted internal component;
this module cannot establish that an arbitrary caller actually queried a venue.
No method calls an exchange, paid API or legacy synthetic execution path.
"""

from __future__ import annotations

import asyncio
import json
import math
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Annotated, Any, Literal

import asyncpg
from kairos_core.contracts import CandidateReviewV1, RiskTradeDecisionV1
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .repository import MessageIdentityConflict
from .runtime import canonical_payload

SHA = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT")
READONLY_DURATION_MS = 86_400_000
MAX_RECEIPT_AGE_MS = 3_600_000
ZERO_SHA = "0" * 64


class CanaryAdmissionError(ValueError):
    """Safe, non-secret-bearing refusal of a new canary admission."""


@dataclass(frozen=True, slots=True)
class CanaryEntryBinding:
    session_id: str
    attempt_id: str
    effect_id: str
    risk_decision_id: str
    trade_id: str
    entry_deadline_at: datetime
    dispatch_claimed: bool


class OperationalModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class CanaryScope(OperationalModel):
    project: Literal["kairos-paper-gate"] = "kairos-paper-gate"
    environment: Literal["paper", "paper-dev"]
    account_id: Annotated[str, Field(pattern=r"^kairos-paper-dev-[a-z0-9][a-z0-9-]{0,63}$")]
    remote_account_id: Annotated[str, Field(min_length=1, max_length=128)]
    exchange_url: Literal["https://trading-api.evedex.tech"] = "https://trading-api.evedex.tech"
    auth_url: Literal["https://auth-api.evedex.tech"] = "https://auth-api.evedex.tech"
    chain_id: Literal[16182] = 16182
    sdk_version: Literal["1.2.11"] = "1.2.11"
    config_sha256: SHA
    recorder_code_sha256: SHA

    @field_validator("remote_account_id")
    @classmethod
    def normalized_remote_account(cls, value: str) -> str:
        if value != value.strip() or value.casefold() in {"prod", "production", "live", "not_configured"}:
            raise ValueError("a normalized configured DEV remote account is required")
        return value


class SymbolObservation(OperationalModel):
    symbol: str
    available: bool
    basis_bps: float | None = None
    spread_bps: Annotated[float, Field(ge=0)] | None = None
    slippage_bps: Annotated[float, Field(ge=0)] | None = None
    book_age_ms: Annotated[int, Field(ge=0, strict=True)] | None = None
    timestamp_skew_ms: Annotated[int, Field(ge=0, strict=True)] | None = None
    book_nonempty: bool = False

    @model_validator(mode="after")
    def validate_available(self) -> SymbolObservation:
        if self.symbol not in SYMBOLS:
            raise ValueError("read-only evidence requires the fixed five symbols")
        fields = (
            self.basis_bps,
            self.spread_bps,
            self.slippage_bps,
            self.book_age_ms,
            self.timestamp_skew_ms,
        )
        if self.available and (any(value is None for value in fields) or not self.book_nonempty):
            raise ValueError("available evidence requires a real nonempty book and all measurements")
        return self


class ReadonlyObservation(OperationalModel):
    observed_at_ms: Annotated[int, Field(gt=0, strict=True)]
    symbols: tuple[SymbolObservation, ...]
    unresolved_bar_gaps: Annotated[int, Field(ge=0, strict=True)]
    reconciliation_drift: bool
    entry_mutations: Annotated[int, Field(ge=0, strict=True)]

    @field_validator("symbols")
    @classmethod
    def exact_symbols(cls, value: tuple[SymbolObservation, ...]) -> tuple[SymbolObservation, ...]:
        if len(value) != 5 or {item.symbol for item in value} != set(SYMBOLS):
            raise ValueError("each observation must cover all five symbols, including failures")
        return tuple(sorted(value, key=lambda item: item.symbol))


class CanarySlot(OperationalModel):
    slot_id: Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9-]{0,63}$")]
    symbol: str
    side: Literal["LONG", "SHORT"]
    scenario: Literal["ROUND_TRIP", "STOP", "TARGET", "TIMEOUT", "RESTART", "ENTRY_CANCEL"]
    stop_distance_bps: Annotated[float, Field(ge=25, le=100)] = 50.0
    target_distance_bps: Annotated[float, Field(ge=25, le=150)] = 75.0
    max_holding_ms: Annotated[int, Field(ge=60_000, le=900_000, strict=True)] = 300_000
    entry_window_ms: Annotated[int, Field(ge=5_000, le=30_000, strict=True)] = 30_000

    @model_validator(mode="after")
    def validate_slot(self) -> CanarySlot:
        if self.symbol not in SYMBOLS or self.target_distance_bps > 2 * self.stop_distance_bps:
            raise ValueError("invalid fixed DEV symbol or target/stop ratio")
        return self

    def intent_config(self) -> dict[str, object]:
        return {
            "entry_window_ms": self.entry_window_ms,
            "max_holding_ms": self.max_holding_ms,
            "side": self.side,
            "stop_distance_bps": self.stop_distance_bps,
            "strategy_ref": "technical-canary@1",
            "symbol": self.symbol,
            "target_distance_bps": self.target_distance_bps,
        }


class BoundedCanaryPlan(OperationalModel):
    version: Literal["bounded-canary-session.v1"] = "bounded-canary-session.v1"
    slots: tuple[CanarySlot, ...]
    max_attempts: Annotated[int, Field(ge=5, le=10, strict=True)] = 10
    duration_ms: Annotated[int, Field(gt=0, le=7_200_000, strict=True)] = 7_200_000

    @model_validator(mode="after")
    def complete_plan(self) -> BoundedCanaryPlan:
        if not 5 <= len(self.slots) <= self.max_attempts or len({s.slot_id for s in self.slots}) != len(
            self.slots
        ):
            raise ValueError("plan needs 5..10 unique slots within the attempt cap")
        if {s.symbol for s in self.slots} != set(SYMBOLS):
            raise ValueError("plan must preserve all five required symbols")
        if not {"STOP", "TARGET", "TIMEOUT", "RESTART", "ENTRY_CANCEL"} <= {s.scenario for s in self.slots}:
            raise ValueError("plan must request stop/target/timeout/restart/cancel coverage")
        return self


def payload(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise MessageIdentityConflict("stored operational evidence is not an object")
    return value


def digest(value: dict[str, Any]) -> str:
    return canonical_payload(value)[1]


def millis(value: datetime) -> int:
    return int(value.astimezone(UTC).timestamp() * 1_000)


def sample_identity(run_id: str, seq: int, received_at: datetime, previous: str, data: dict) -> str:
    return digest(
        {
            "domain": "kairos.readonly-sample.v1",
            "run_id": run_id,
            "seq": seq,
            "received_at": received_at.isoformat(),
            "previous": previous,
            "observation": data,
        }
    )


def validate_slot_review(slot: CanarySlot, review: CandidateReviewV1) -> None:
    intent = review.intent
    if (
        slot.symbol != intent.symbol
        or slot.side != intent.side.value
        or digest(slot.intent_config()) != intent.provenance.config_sha256
    ):
        raise CanaryAdmissionError("review does not match the exact frozen session slot")
    reference = Decimal(str(intent.reference_price))
    sign = Decimal(1) if slot.side == "LONG" else Decimal(-1)
    stop = reference * (1 - sign * Decimal(str(slot.stop_distance_bps)) / 10_000)
    target = reference * (1 + sign * Decimal(str(slot.target_distance_bps)) / 10_000)
    quantum = Decimal("0.00000001")
    if (
        intent.exit_plan.stop_price != float(stop.quantize(quantum, rounding=ROUND_HALF_EVEN))
        or intent.exit_plan.target_price != float(target.quantize(quantum, rounding=ROUND_HALF_EVEN))
        or intent.exit_plan.max_holding_ms != slot.max_holding_ms
        or intent.entry_expires_ts_ms - intent.entry_eligible_ts_ms != slot.entry_window_ms
    ):
        raise CanaryAdmissionError("canary review changed the exact session exit/entry plan")


def verify_readonly_evidence(run: Any, samples: list[Any], ended_at: datetime) -> dict[str, Any]:
    """Pure verifier over persisted rows. Failure/missing intervals count against availability."""
    scope = CanaryScope.model_validate(payload(run["scope"]))
    if digest(scope.model_dump(mode="json")) != run["scope_sha256"]:
        raise MessageIdentityConflict("read-only scope fingerprint mismatch")
    start_ms, end_ms = millis(run["started_at"]), millis(ended_at)
    duration = end_ms - start_ms
    period = run["sample_period_ms"]
    if duration < READONLY_DURATION_MS or not 5_000 <= period <= 60_000:
        raise CanaryAdmissionError("a real complete 24-hour read-only window is required")
    if len(samples) != run["sample_count"] or not samples:
        raise MessageIdentityConflict("read-only sample count mismatch or empty evidence")
    head = ZERO_SHA
    previous_ms = start_ms - 1
    available_ms = {symbol: 0 for symbol in SYMBOLS}
    measures: dict[str, dict[str, list[float]]] = {
        symbol: {metric: [] for metric in ("basis_bps", "spread_bps", "slippage_bps")} for symbol in SYMBOLS
    }
    for seq, row in enumerate(samples, start=1):
        data = payload(row["payload"])
        sample = ReadonlyObservation.model_validate(data)
        received_ms = millis(row["received_at"])
        expected = sample_identity(run["run_id"], seq, row["received_at"], head, data)
        if row["seq"] != seq or row["previous_sha256"] != head or row["sample_sha256"] != expected:
            raise MessageIdentityConflict("read-only observation hash chain is invalid")
        if not start_ms <= received_ms <= end_ms or received_ms <= previous_ms:
            raise CanaryAdmissionError("read-only observation timestamps are reordered or outside the run")
        if abs(sample.observed_at_ms - received_ms) > 2_000:
            raise CanaryAdmissionError("read-only observation is backdated or clock-skewed")
        if sample.unresolved_bar_gaps or sample.reconciliation_drift or sample.entry_mutations:
            raise CanaryAdmissionError("read-only gap, reconciliation drift or mutation evidence")
        next_ms = millis(samples[seq]["received_at"]) if seq < len(samples) else end_ms
        weight = max(0, min(period, next_ms - received_ms))
        for item in sample.symbols:
            if not item.available:
                continue
            if item.book_age_ms is None or item.book_age_ms > 5_000:
                raise CanaryAdmissionError("read-only book age exceeds five seconds")
            if item.timestamp_skew_ms is None or item.timestamp_skew_ms > 2_000:
                raise CanaryAdmissionError("read-only timestamp skew exceeds two seconds")
            available_ms[item.symbol] += weight
            for metric in measures[item.symbol]:
                value = getattr(item, metric)
                measures[item.symbol][metric].append(abs(value))
        head, previous_ms = expected, received_ms
    if head != run["head_sha256"]:
        raise MessageIdentityConflict("read-only terminal chain fingerprint mismatch")
    if end_ms - previous_ms > period or not all(item.available for item in sample.symbols):
        raise CanaryAdmissionError("read-only window must end with fresh nonempty books for all five symbols")
    metrics: dict[str, Any] = {}
    for symbol in SYMBOLS:
        availability = available_ms[symbol] / duration
        if availability < 0.99:
            raise CanaryAdmissionError("read-only availability below 99% for a required symbol")
        metrics[symbol] = {"availability": availability}
        for metric, values in measures[symbol].items():
            ordered = sorted(values)
            p95 = ordered[math.ceil(len(ordered) * 0.95) - 1]
            if p95 > 25:
                raise CanaryAdmissionError("read-only p95 basis/spread/slippage exceeds 25 bps")
            metrics[symbol][f"p95_{metric}"] = p95
    return {
        "domain": "kairos.readonly-receipt.v1",
        "run_id": run["run_id"],
        "scope_sha256": run["scope_sha256"],
        "database_instance_id": str(run["database_instance_id"]),
        "started_at": run["started_at"].isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_ms": duration,
        "sample_count": len(samples),
        "head_sha256": head,
        "sample_period_ms": period,
        "metrics": metrics,
    }


class CanarySessionRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def begin_readonly(self, scope: CanaryScope, *, sample_period_ms: int = 15_000) -> str:
        scope = CanaryScope.model_validate(scope.model_dump(mode="json"))
        if type(sample_period_ms) is not int or not 5_000 <= sample_period_ms <= 60_000:
            raise ValueError("read-only cadence must be 5..60 seconds")
        data = scope.model_dump(mode="json")
        run_id = digest({"domain": "kairos.readonly-run.v1", "scope": data, "nonce": secrets.token_hex(32)})
        await self.pool.execute(
            """INSERT INTO paper_readonly_runs
               (run_id,scope,scope_sha256,database_instance_id,sample_period_ms,head_sha256)
               SELECT $1,$2::jsonb,$3,instance_id,$4,$5 FROM paper_canary_database_identity""",
            run_id,
            canonical_payload(data)[0],
            digest(data),
            sample_period_ms,
            ZERO_SHA,
        )
        return run_id

    async def append_observation(self, run_id: str, observation: ReadonlyObservation) -> int:
        observation = ReadonlyObservation.model_validate(observation.model_dump(mode="json"))
        data = observation.model_dump(mode="json")
        async with self.pool.acquire() as connection, connection.transaction():
            run = await connection.fetchrow(
                "SELECT * FROM paper_readonly_runs WHERE run_id=$1 FOR UPDATE", run_id
            )
            if run is None or run["state"] != "RECORDING":
                raise CanaryAdmissionError("read-only run is missing or sealed")
            now = await connection.fetchval("SELECT clock_timestamp()")
            seq = run["sample_count"] + 1
            if seq > 1:
                last = await connection.fetchrow(
                    "SELECT * FROM paper_readonly_samples WHERE run_id=$1 AND seq=$2",
                    run_id,
                    seq - 1,
                )
                prior_data = payload(last["payload"])
                if prior_data["observed_at_ms"] == observation.observed_at_ms:
                    if prior_data != data:
                        raise MessageIdentityConflict(
                            "read-only observation timestamp was reused with different bytes"
                        )
                    return seq - 1
                if millis(now) - millis(last["received_at"]) < run["sample_period_ms"] - 2_000:
                    raise CanaryAdmissionError("read-only samples cannot oversample the fixed cadence")
            if abs(millis(now) - observation.observed_at_ms) > 2_000:
                raise CanaryAdmissionError("cannot append a backdated read-only observation")
            sha = sample_identity(run_id, seq, now, run["head_sha256"], data)
            await connection.execute(
                """INSERT INTO paper_readonly_samples
                   (run_id,seq,received_at,payload,previous_sha256,sample_sha256)
                   VALUES($1,$2,$3,$4::jsonb,$5,$6)""",
                run_id,
                seq,
                now,
                canonical_payload(data)[0],
                run["head_sha256"],
                sha,
            )
            await connection.execute(
                "UPDATE paper_readonly_runs SET sample_count=$2,head_sha256=$3 WHERE run_id=$1",
                run_id,
                seq,
                sha,
            )
            return seq

    async def certify_readonly(self, run_id: str, *, expected_scope: CanaryScope) -> str:
        async with self.pool.acquire() as connection, connection.transaction():
            run = await connection.fetchrow(
                "SELECT * FROM paper_readonly_runs WHERE run_id=$1 FOR UPDATE", run_id
            )
            if run is None or run["scope_sha256"] != digest(expected_scope.model_dump(mode="json")):
                raise CanaryAdmissionError("read-only receipt scope/config/account mismatch")
            existing = await connection.fetchrow(
                "SELECT * FROM paper_readonly_receipts WHERE run_id=$1", run_id
            )
            now = await connection.fetchval("SELECT clock_timestamp()")
            ended = datetime.fromisoformat(payload(existing["payload"])["ended_at"]) if existing else now
            samples = await connection.fetch(
                "SELECT * FROM paper_readonly_samples WHERE run_id=$1 ORDER BY seq", run_id
            )
            proof = verify_readonly_evidence(run, list(samples), ended)
            await self._assert_readonly_mutations(connection, expected_scope, run["started_at"], ended)
            receipt_id = digest(proof)
            if existing and (receipt_id != existing["receipt_id"] or payload(existing["payload"]) != proof):
                raise MessageIdentityConflict("stored read-only receipt differs from its evidence")
            await connection.execute(
                """INSERT INTO paper_readonly_receipts(receipt_id,run_id,payload)
                   VALUES($1,$2,$3::jsonb) ON CONFLICT(receipt_id) DO NOTHING""",
                receipt_id,
                run_id,
                canonical_payload(proof)[0],
            )
            await connection.execute(
                "UPDATE paper_readonly_runs SET state='CERTIFIED' WHERE run_id=$1", run_id
            )
            return receipt_id

    async def arm_session(
        self,
        *,
        receipt_id: str,
        scope: CanaryScope,
        plan: BoundedCanaryPlan,
        operator_nonce: str,
        receipt_max_age_ms: int = MAX_RECEIPT_AGE_MS,
    ) -> dict[str, Any]:
        if type(receipt_max_age_ms) is not int or not 0 < receipt_max_age_ms <= MAX_RECEIPT_AGE_MS:
            raise CanaryAdmissionError("receipt freshness may only tighten the one-hour default")
        if not operator_nonce or operator_nonce != operator_nonce.strip() or len(operator_nonce) > 128:
            raise CanaryAdmissionError("a normalized manual session nonce is required")
        scope = CanaryScope.model_validate(scope.model_dump(mode="json"))
        plan = BoundedCanaryPlan.model_validate(plan.model_dump(mode="json"))
        scope_data, plan_data = scope.model_dump(mode="json"), plan.model_dump(mode="json")
        session_id = digest(
            {
                "domain": "kairos.canary-session.v1",
                "receipt_id": receipt_id,
                "scope": scope_data,
                "plan": plan_data,
                "operator_nonce": operator_nonce,
            }
        )
        async with self.pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"kairos.canary-session-project:{scope.project}",
            )
            existing = await connection.fetchrow(
                "SELECT * FROM paper_canary_sessions WHERE session_id=$1", session_id
            )
            if existing:
                return self._session(existing)  # Idempotent restart never resets deadline/counter/state.
            receipt = await connection.fetchrow(
                "SELECT * FROM paper_readonly_receipts WHERE receipt_id=$1", receipt_id
            )
            if receipt is None:
                raise CanaryAdmissionError("a persisted verified read-only receipt is required")
            proof = payload(receipt["payload"])
            run = await connection.fetchrow(
                "SELECT * FROM paper_readonly_runs WHERE run_id=$1 FOR UPDATE", receipt["run_id"]
            )
            samples = await connection.fetch(
                "SELECT * FROM paper_readonly_samples WHERE run_id=$1 ORDER BY seq", receipt["run_id"]
            )
            ended = datetime.fromisoformat(proof["ended_at"])
            verified = verify_readonly_evidence(run, list(samples), ended)
            await self._assert_readonly_mutations(connection, scope, run["started_at"], ended)
            instance = await connection.fetchval("SELECT instance_id FROM paper_canary_database_identity")
            now = await connection.fetchval("SELECT clock_timestamp()")
            if digest(verified) != receipt_id or proof != verified:
                raise MessageIdentityConflict("receipt does not match its persisted evidence")
            if (
                proof["scope_sha256"] != digest(scope_data)
                or proof["database_instance_id"] != str(instance)
                or not 0 <= millis(now) - millis(ended) <= receipt_max_age_ms
            ):
                raise CanaryAdmissionError(
                    "read-only receipt is stale or belongs to another deployment/account/config"
                )
            active = await connection.fetchval(
                """SELECT session_id FROM paper_canary_sessions WHERE scope->>'project'=$1
                   AND state IN ('ARMED','RUNNING','DRAINING')""",
                scope.project,
            )
            if active:
                raise CanaryAdmissionError("another DEV session is active or still draining")
            open_trade = await connection.fetchval(
                """SELECT 1 FROM execution_trades WHERE trading_mode='PAPER' AND profile='DEV'
                   AND state NOT IN ('FLAT','CANCELLED') LIMIT 1"""
            )
            if open_trade:
                raise CanaryAdmissionError("a nonterminal DEV trade globally blocks a new canary session")
            used = await connection.fetchval(
                "SELECT session_id FROM paper_canary_sessions WHERE receipt_id=$1", receipt_id
            )
            if used:
                raise CanaryAdmissionError("read-only receipt already armed its one permitted session")
            row = await connection.fetchrow(
                """INSERT INTO paper_canary_sessions
                   (session_id,receipt_id,scope,scope_sha256,remote_account_id,account_id,plan,plan_sha256,
                    operator_nonce,state,armed_at,entry_deadline_at,max_attempts)
                   VALUES($1,$2,$3::jsonb,$4,$5,$6,$7::jsonb,$8,$9,'ARMED',$10,$11,$12) RETURNING *""",
                session_id,
                receipt_id,
                canonical_payload(scope_data)[0],
                digest(scope_data),
                scope.remote_account_id,
                scope.account_id,
                canonical_payload(plan_data)[0],
                digest(plan_data),
                operator_nonce,
                now,
                now + timedelta(milliseconds=plan.duration_ms),
                plan.max_attempts,
            )
            return self._session(row)

    async def status(self, session_id: str) -> dict[str, Any]:
        row = await self.pool.fetchrow("SELECT * FROM paper_canary_sessions WHERE session_id=$1", session_id)
        if row is None:
            raise CanaryAdmissionError("canary session does not exist")
        result = self._session(row)
        attempts = await self.pool.fetch(
            "SELECT * FROM paper_canary_attempts WHERE session_id=$1 ORDER BY ordinal", session_id
        )
        result["attempts"] = [dict(item) for item in attempts]
        return result

    async def refresh(self, session_id: str) -> dict[str, Any]:
        """Observe persisted completion/deadline; never invent a successful scenario."""
        async with self.pool.acquire() as connection, connection.transaction():
            account = await connection.fetchval(
                "SELECT account_id FROM paper_canary_sessions WHERE session_id=$1", session_id
            )
            if account is None:
                raise CanaryAdmissionError("canary session does not exist")
            # Same lock order as arm/consume: account before session.
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", f"kairos.paper-canary-arm:{account}"
            )
            row = await connection.fetchrow(
                "SELECT * FROM paper_canary_sessions WHERE session_id=$1 FOR UPDATE", session_id
            )
            scope = CanaryScope.model_validate(self._session(row)["scope"])
            await self.refresh_attempts(connection, session_id, scope)
            now = await connection.fetchval("SELECT clock_timestamp()")
            if row["state"] in {"ARMED", "RUNNING"} and now >= row["entry_deadline_at"]:
                await connection.execute(
                    """UPDATE paper_canary_sessions SET state='DRAINING',stop_reason='SESSION_DEADLINE',
                       last_progress_at=clock_timestamp() WHERE session_id=$1""",
                    session_id,
                )
            pending = await connection.fetchval(
                "SELECT 1 FROM paper_canary_attempts WHERE session_id=$1 AND state <> 'TERMINAL'", session_id
            )
            if not pending:
                await connection.execute(
                    """UPDATE paper_canary_sessions SET state=CASE WHEN stop_reason='OPERATOR_STOP'
                       THEN 'ABORTED' ELSE 'INCOMPLETE' END,last_progress_at=clock_timestamp()
                       WHERE session_id=$1 AND (state='DRAINING' OR attempts_reserved >= max_attempts
                           OR attempts_reserved >= jsonb_array_length(plan->'slots'))""",
                    session_id,
                )
        return await self.status(session_id)

    @staticmethod
    async def _assert_readonly_mutations(
        connection: asyncpg.Connection, scope: CanaryScope, started_at: datetime, ended_at: datetime
    ) -> None:
        exists = await connection.fetchval(
            """SELECT 1 FROM execution_effects WHERE exchange='evedex'
               AND account_id=ANY($1::text[]) AND prepared_at BETWEEN $2 AND $3 LIMIT 1""",
            [scope.account_id, scope.remote_account_id],
            started_at,
            ended_at,
        )
        if exists:
            raise CanaryAdmissionError("read-only window contains an execution mutation attempt")

    async def stop(self, session_id: str, *, reason: str = "OPERATOR_STOP") -> dict[str, Any]:
        if reason not in {
            "OPERATOR_STOP",
            "UNPROTECTED_EXPOSURE",
            "UNKNOWN_MUTATION",
            "RECONCILIATION_DRIFT",
        }:
            raise CanaryAdmissionError("unsupported non-secret stop reason")
        async with self.pool.acquire() as connection, connection.transaction():
            # Compatible with the session-level lock retained by final_dispatch.
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", self._dispatch_lock_key(session_id)
            )
            row = await connection.fetchrow(
                "SELECT * FROM paper_canary_sessions WHERE session_id=$1 FOR UPDATE", session_id
            )
            if row is None:
                raise CanaryAdmissionError("canary session does not exist")
            if row["state"] in {"ARMED", "RUNNING"}:
                row = await connection.fetchrow(
                    """UPDATE paper_canary_sessions SET state='DRAINING',stop_reason=$2,
                       last_progress_at=clock_timestamp() WHERE session_id=$1 RETURNING *""",
                    session_id,
                    reason,
                )
            return self._session(row)

    @staticmethod
    def _dispatch_lock_key(session_id: str) -> str:
        return f"kairos.canary-dispatch:{session_id}"

    async def bind_entry(
        self, *, decision: RiskTradeDecisionV1, expected_scope: CanaryScope, effect_id: str
    ) -> CanaryEntryBinding:
        """Pre-PREPARED exact binding. Idempotent, but never a venue mutation lease."""
        async with self.pool.acquire() as connection, connection.transaction():
            return await self._entry_binding(connection, decision, expected_scope, effect_id, active=True)

    async def recovery_binding(
        self, *, decision: RiskTradeDecisionV1, expected_scope: CanaryScope, effect_id: str
    ) -> CanaryEntryBinding:
        """Read-only exact evidence lookup after stop/expiry; never grants re-dispatch."""
        async with self.pool.acquire() as connection, connection.transaction(readonly=True):
            return await self._entry_binding(connection, decision, expected_scope, effect_id, active=False)

    @asynccontextmanager
    async def final_dispatch(
        self,
        *,
        decision: RiskTradeDecisionV1,
        expected_scope: CanaryScope,
        effect_id: str,
        max_hold_s: float = 30.0,
    ) -> AsyncIterator[CanaryEntryBinding]:
        """One committed claim, then a bounded caller-owned venue call under a session lock.

        Caller lock order: execution account -> trade -> this dispatch lock ->
        short session-row transaction. This helper never acquires account/trade
        locks. stop acquires only this dispatch lock, then the session row.
        Claim COMMIT precedes yield, so process loss cannot erase dispatch intent.
        A crash before send may sacrifice a slot; it never licenses a blind retry.
        """
        if isinstance(max_hold_s, bool) or not math.isfinite(max_hold_s) or not 0 < max_hold_s <= 30:
            raise CanaryAdmissionError("dispatch lock hold must be positive and at most 30 seconds")
        async with self.pool.acquire() as connection:
            session_id = await connection.fetchval(
                "SELECT session_id FROM paper_canary_attempts WHERE review_id=$1", decision.review.review_id
            )
            if session_id is None:
                raise CanaryAdmissionError("entry has no bounded canary attempt")
            key = self._dispatch_lock_key(session_id)
            await connection.execute("SELECT pg_advisory_lock(hashtextextended($1,0))", key)
            try:
                async with connection.transaction():
                    binding = await self._entry_binding(
                        connection, decision, expected_scope, effect_id, active=True
                    )
                    if binding.dispatch_claimed:
                        raise CanaryAdmissionError(
                            "entry dispatch was already claimed; reconcile without resubmitting"
                        )
                    await self._assert_prepared_effect(connection, decision, expected_scope, effect_id)
                    await connection.execute(
                        """INSERT INTO paper_canary_dispatch_claims
                           (effect_id,attempt_id,session_id,risk_decision_id,trade_id,scope_sha256)
                           VALUES($1,$2,$3,$4,$5,$6)""",
                        effect_id,
                        binding.attempt_id,
                        binding.session_id,
                        binding.risk_decision_id,
                        binding.trade_id,
                        digest(expected_scope.model_dump(mode="json")),
                    )
                    await connection.execute(
                        """UPDATE paper_canary_sessions SET last_progress_at=clock_timestamp()
                           WHERE session_id=$1""",
                        binding.session_id,
                    )
                # No caller/venue code can execute before the short transaction commits.
                now = await connection.fetchval("SELECT clock_timestamp()")
                if now >= binding.entry_deadline_at or millis(now) > decision.venue_quality.expires_at_ms:
                    raise CanaryAdmissionError(
                        "entry expired during dispatch claim; reconcile without resubmitting"
                    )
                async with asyncio.timeout(max_hold_s):
                    yield CanaryEntryBinding(
                        session_id=binding.session_id,
                        attempt_id=binding.attempt_id,
                        effect_id=effect_id,
                        risk_decision_id=binding.risk_decision_id,
                        trade_id=binding.trade_id,
                        entry_deadline_at=binding.entry_deadline_at,
                        dispatch_claimed=True,
                    )
            finally:
                await asyncio.shield(
                    connection.execute("SELECT pg_advisory_unlock(hashtextextended($1,0))", key)
                )

    @staticmethod
    async def _assert_prepared_effect(
        connection: asyncpg.Connection, decision: RiskTradeDecisionV1, scope: CanaryScope, effect_id: str
    ) -> None:
        effect = await connection.fetchrow("SELECT * FROM execution_effects WHERE effect_key=$1", effect_id)
        if (
            effect is None
            or effect["status"] != "PREPARED"
            or effect["effect_type"] != "PLACE_ORDER"
            or effect["environment"] != f"{scope.environment}:EVEDEX:DEV:PAPER"
            or effect["account_id"] != scope.account_id
            or effect["trade_id"] != decision.trade_id
            or effect["exchange"] != "evedex"
            or effect["symbol"] != decision.intent.symbol
            or effect["order_role"] != "ENTRY"
        ):
            raise CanaryAdmissionError("final dispatch requires the exact durable PREPARED entry effect")
        request = payload(effect["request_payload"])
        expected = {
            "trade_id": decision.trade_id,
            "intent_id": decision.intent.intent_id,
            "client_order_id": effect["client_order_id"],
            "venue_symbol": decision.venue_symbol,
            "side": "BUY" if decision.intent.side.value == "LONG" else "SELL",
            "quantity_hex": float(decision.quantity).hex(),
            "limit_price_hex": float(decision.worst_entry_price).hex(),
            "leverage_hex": float(decision.leverage).hex(),
        }
        if (
            request != expected
            or digest(request) != effect["request_sha256"]
            or not effect["client_order_id"]
        ):
            raise MessageIdentityConflict("prepared canary entry request differs from the immutable decision")

    @staticmethod
    async def _entry_binding(
        connection: asyncpg.Connection,
        decision: RiskTradeDecisionV1,
        expected_scope: CanaryScope,
        effect_id: str,
        *,
        active: bool,
    ) -> CanaryEntryBinding:
        decision = RiskTradeDecisionV1.model_validate(decision.model_dump(mode="json"))
        scope = CanaryScope.model_validate(expected_scope.model_dump(mode="json"))
        if decision.decision_id is None or decision.trade_id is None:
            raise CanaryAdmissionError("entry decision has no canonical lineage")
        if not effect_id or effect_id != effect_id.strip() or len(effect_id) > 512:
            raise CanaryAdmissionError("a normalized entry effect identity is required")
        if (
            not decision.approved
            or decision.trading_mode.value != "PAPER"
            or decision.evedex_profile.value != "DEV"
            or decision.account_id != scope.account_id
        ):
            raise CanaryAdmissionError("entry binding requires a risk-approved exact DEV account decision")
        quantity_text = dict(decision.intent.metadata).get("canary_quantity")
        if (
            decision.leverage != 1.0
            or quantity_text is None
            or Decimal(str(decision.quantity)) != Decimal(quantity_text)
        ):
            raise CanaryAdmissionError("entry requires exactly 1x and the bound venue-minimum quantity")
        query = """SELECT a.*,s.scope,s.scope_sha256,s.plan,s.plan_sha256,s.receipt_id,
                   s.entry_deadline_at,s.state AS session_state,arm.status AS arm_status,
                   arm.review_payload,arm.review_sha256,arm.decided_at_ms
                   FROM paper_canary_attempts a JOIN paper_canary_sessions s USING(session_id)
                   JOIN paper_canary_arms arm USING(arm_id) WHERE a.review_id=$1"""
        if active:
            query += " FOR UPDATE OF s,a"
        row = await connection.fetchrow(query, decision.review.review_id)
        if row is None or row["scope_sha256"] != digest(scope.model_dump(mode="json")):
            raise CanaryAdmissionError("entry session scope/account/config is missing or mismatched")
        checked = CanarySessionRepository._session(row)
        receipt = await connection.fetchrow(
            """SELECT receipt.payload,run.state,run.head_sha256,run.sample_count,
               identity.instance_id FROM paper_readonly_receipts receipt
               JOIN paper_readonly_runs run USING(run_id)
               CROSS JOIN paper_canary_database_identity identity WHERE receipt.receipt_id=$1""",
            row["receipt_id"],
        )
        if receipt is None:
            raise CanaryAdmissionError("entry session has no persisted read-only receipt")
        proof = payload(receipt["payload"])
        if (
            digest(proof) != row["receipt_id"]
            or proof["scope_sha256"] != row["scope_sha256"]
            or proof["database_instance_id"] != str(receipt["instance_id"])
            or proof["head_sha256"] != receipt["head_sha256"]
            or proof["sample_count"] != receipt["sample_count"]
            or receipt["state"] != "CERTIFIED"
        ):
            raise MessageIdentityConflict("entry read-only receipt binding is inconsistent")
        if (
            checked["scope"] != scope.model_dump(mode="json")
            or row["intent_id"] != decision.intent.intent_id
            or row["arm_status"] != "CONSUMED"
            or row["decided_at_ms"] != decision.decided_at_ms
        ):
            raise CanaryAdmissionError("entry decision is not the exact consumed canary authorization")
        stored_review = payload(row["review_payload"])
        if (
            stored_review != decision.review.model_dump(mode="json")
            or digest(stored_review) != row["review_sha256"]
        ):
            raise MessageIdentityConflict("entry canary review payload differs from its arm")
        plan = BoundedCanaryPlan.model_validate(checked["plan"])
        slot = next((item for item in plan.slots if item.slot_id == row["slot_id"]), None)
        if slot is None:
            raise CanaryAdmissionError("entry session slot is missing")
        validate_slot_review(slot, decision.review)
        deadline = min(
            row["entry_deadline_at"], datetime.fromtimestamp(decision.intent.entry_expires_ts_ms / 1000, UTC)
        )
        now = await connection.fetchval("SELECT clock_timestamp()")
        if active and (
            row["session_state"] != "RUNNING"
            or row["state"] != "CONSUMED"
            or now >= deadline
            or millis(now) < decision.intent.entry_eligible_ts_ms
            or millis(now) > decision.venue_quality.expires_at_ms
        ):
            raise CanaryAdmissionError("entry session/attempt is stopped, expired or not currently eligible")
        for field, expected in (
            ("risk_decision_id", decision.decision_id),
            ("trade_id", decision.trade_id),
            ("entry_effect_id", effect_id),
        ):
            if row[field] is not None and row[field] != expected:
                raise MessageIdentityConflict("canary attempt already binds another decision/trade/effect")
            if not active and row[field] is None:
                raise CanaryAdmissionError("recovery cannot create a missing entry binding")
        if active:
            if row["entry_effect_id"] is None and await connection.fetchval(
                "SELECT 1 FROM execution_effects WHERE effect_key=$1", effect_id
            ):
                raise CanaryAdmissionError(
                    "entry must be bound before PREPARED; existing effects require recovery"
                )
            await connection.execute(
                """UPDATE paper_canary_attempts SET risk_decision_id=$2,trade_id=$3,entry_effect_id=$4
                   WHERE attempt_id=$1""",
                row["attempt_id"],
                decision.decision_id,
                decision.trade_id,
                effect_id,
            )
        claim = await connection.fetchrow(
            "SELECT * FROM paper_canary_dispatch_claims WHERE attempt_id=$1", row["attempt_id"]
        )
        if claim and (
            claim["effect_id"] != effect_id
            or claim["risk_decision_id"] != decision.decision_id
            or claim["trade_id"] != decision.trade_id
            or claim["scope_sha256"] != row["scope_sha256"]
        ):
            raise MessageIdentityConflict("durable dispatch claim binding is inconsistent")
        return CanaryEntryBinding(
            session_id=row["session_id"],
            attempt_id=row["attempt_id"],
            effect_id=effect_id,
            risk_decision_id=decision.decision_id,
            trade_id=decision.trade_id,
            entry_deadline_at=deadline,
            dispatch_claimed=claim is not None,
        )

    @staticmethod
    def _session(row: Any) -> dict[str, Any]:
        result = dict(row)
        for key in ("scope", "plan"):
            result[key] = payload(row[key])
            if digest(result[key]) != row[f"{key}_sha256"]:
                raise MessageIdentityConflict("stored canary session fingerprint mismatch")
        return result

    @staticmethod
    async def reserve_attempt(
        connection: asyncpg.Connection,
        *,
        session_id: str,
        slot_id: str,
        account_id: str,
        arm_id: str,
        review: CandidateReviewV1,
    ) -> dict[str, Any]:
        """Caller holds the existing arm transaction. No refund, including Risk rejection."""
        session = await connection.fetchrow(
            "SELECT * FROM paper_canary_sessions WHERE session_id=$1 FOR UPDATE", session_id
        )
        if session is None or session["account_id"] != account_id:
            raise CanaryAdmissionError("canary session/account binding is missing")
        checked = CanarySessionRepository._session(session)
        scope = CanaryScope.model_validate(checked["scope"])
        plan = BoundedCanaryPlan.model_validate(checked["plan"])
        slot = next((item for item in plan.slots if item.slot_id == slot_id), None)
        if slot is None:
            raise CanaryAdmissionError("review does not match the exact frozen session slot")
        validate_slot_review(slot, review)
        existing = await connection.fetchrow(
            "SELECT * FROM paper_canary_attempts WHERE session_id=$1 AND slot_id=$2", session_id, slot_id
        )
        if existing:
            if existing["arm_id"] != arm_id or existing["review_id"] != review.review_id:
                raise MessageIdentityConflict("canary slot was already reserved with different bytes")
            return dict(existing)
        now = await connection.fetchval("SELECT clock_timestamp()")
        if session["state"] not in {"ARMED", "RUNNING"} or now >= session["entry_deadline_at"]:
            raise CanaryAdmissionError("canary session stopped or expired; recovery remains permitted")
        if review.intent.entry_expires_ts_ms > millis(session["entry_deadline_at"]):
            raise CanaryAdmissionError("candidate entry deadline exceeds the session deadline")
        await CanarySessionRepository.refresh_attempts(connection, session_id, scope)
        pending = await connection.fetchval(
            "SELECT 1 FROM paper_canary_attempts WHERE session_id=$1 AND state <> 'TERMINAL'", session_id
        )
        if pending:
            raise CanaryAdmissionError("previous canary attempt is not authoritatively terminal")
        ordinal = session["attempts_reserved"] + 1
        if ordinal > session["max_attempts"] or ordinal > len(plan.slots):
            raise CanaryAdmissionError("canary session attempt cap exhausted")
        if plan.slots[ordinal - 1].slot_id != slot_id:
            raise CanaryAdmissionError("canary slots must run in their fixed order")
        attempt_id = digest(
            {
                "domain": "kairos.canary-attempt.v1",
                "session_id": session_id,
                "slot_id": slot_id,
                "arm_id": arm_id,
            }
        )
        # The arm row is inserted by the caller before this function; rollback is shared.
        row = await connection.fetchrow(
            """INSERT INTO paper_canary_attempts
               (attempt_id,session_id,ordinal,slot_id,arm_id,review_id,intent_id,symbol,state)
               VALUES($1,$2,$3,$4,$5,$6,$7,$8,'ARMED') RETURNING *""",
            attempt_id,
            session_id,
            ordinal,
            slot_id,
            arm_id,
            review.review_id,
            review.intent.intent_id,
            slot.symbol,
        )
        await connection.execute(
            """UPDATE paper_canary_sessions SET attempts_reserved=$2,state='RUNNING',
               last_progress_at=clock_timestamp() WHERE session_id=$1""",
            session_id,
            ordinal,
        )
        return dict(row)

    @staticmethod
    async def refresh_attempts(connection: asyncpg.Connection, session_id: str, scope: CanaryScope) -> None:
        """Only exact durable Risk refusal or reconciled terminal journal closes an attempt."""
        rows = await connection.fetch(
            "SELECT * FROM paper_canary_attempts WHERE session_id=$1 AND state IN ('ARMED','CONSUMED')",
            session_id,
        )
        for row in rows:
            expired = await connection.fetchrow(
                """UPDATE paper_canary_arms SET status='EXPIRED'
                   WHERE arm_id=$1 AND status IN ('ARMED','EXPIRED')
                   AND expires_at < clock_timestamp() RETURNING arm_id""",
                row["arm_id"],
            )
            if expired:
                await connection.execute(
                    """UPDATE paper_canary_attempts SET state='TERMINAL',terminal_at=clock_timestamp(),
                       terminal_reason='ENTRY_EXPIRED' WHERE attempt_id=$1""",
                    row["attempt_id"],
                )
                continue
            decisions = await connection.fetch(
                """SELECT payload FROM event_audit WHERE topic='kairos.risk.trade_decision.v1'
                   AND payload->'review'->>'review_id'=$1 ORDER BY persisted_at""",
                row["review_id"],
            )
            unique = {digest(payload(item["payload"])): payload(item["payload"]) for item in decisions}
            if len(unique) != 1:
                continue
            decision = RiskTradeDecisionV1.model_validate(next(iter(unique.values())))
            if (
                decision.account_id != scope.account_id
                or decision.intent.intent_id != row["intent_id"]
                or decision.trading_mode.value != "PAPER"
                or decision.evedex_profile.value != "DEV"
            ):
                raise MessageIdentityConflict("canary Risk decision lineage mismatch")
            reason = None
            trade_id = None
            if not decision.approved:
                reason = "RISK_REJECTED"
            else:
                trade = await connection.fetchrow(
                    """SELECT * FROM execution_trades WHERE risk_decision_id=$1
                       AND environment=$2 AND account_id=$3 AND exchange='evedex'""",
                    decision.decision_id,
                    f"{scope.environment}:EVEDEX:DEV:PAPER",
                    scope.account_id,
                )
                if (
                    trade
                    and trade["state"] in {"FLAT", "CANCELLED"}
                    and trade["last_reconciled_at"] is not None
                ):
                    unresolved = await connection.fetchval(
                        """SELECT 1 FROM execution_effects
                           WHERE trade_id=$1 AND status IN ('PREPARED','FAILED')""",
                        trade["trade_id"],
                    )
                    if not unresolved:
                        reason, trade_id = trade["state"], trade["trade_id"]
            if reason:
                await connection.execute(
                    """UPDATE paper_canary_attempts SET state='TERMINAL',terminal_at=clock_timestamp(),
                       terminal_reason=$2,risk_decision_id=$3,trade_id=$4 WHERE attempt_id=$1""",
                    row["attempt_id"],
                    reason,
                    decision.decision_id,
                    trade_id,
                )
                await connection.execute(
                    "UPDATE paper_canary_sessions SET last_progress_at=clock_timestamp() WHERE session_id=$1",
                    session_id,
                )
