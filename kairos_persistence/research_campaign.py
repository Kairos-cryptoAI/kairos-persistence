"""Opt-in durable causal campaign journal; not a scientific or trading gate.

Only the separately selected RESEARCH_CAMPAIGN topology owns these tables.
Legacy SIM25 evidence and its exact-clock rules are never changed or adopted.
Window claims have no lease/reclaim operation: a crash preserves uncertainty,
and a second process may account for it but may never dispatch that window again.
"""

from __future__ import annotations

from collections import Counter
from typing import Annotated, Any, Literal, Self

from kairos_core import (
    RESEARCH_ARMS,
    ResearchDecisionSampleV1,
    ResearchObservationScheduleV1,
    StrategyIntentV1,
    canonical_sha256,
)
from kairos_core.contracts.base import StrictValueModel
from pydantic import Field, StrictInt, StrictStr, model_validator

from .adaptive_candidate_protocols import ResearchAdaptiveCandidateProtocolRepository
from .database import Database, MigrationProfile
from .repository import MessageIdentityConflict
from .research_evidence import (
    ResearchLLMAttemptStartV1,
    ResearchLLMAttemptTerminalV1,
    ResearchSourceReceiptV1,
    ResearchStrategyEvaluationReceiptV1,
    _decode_row,
    _Receipt,
    _verify_model,
)
from .research_observation_schedule import ResearchObservationScheduleRepository, _schedule_from_row
from .runtime import canonical_payload

_ID = Annotated[StrictStr, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")]
_SHA = Annotated[StrictStr, Field(pattern=r"^[0-9a-f]{64}$")]
_MS = Annotated[StrictInt, Field(ge=0, le=253_402_300_799_999)]
Arm = Literal["strategy-only", "strategy-review", "llm-proposal-research"]
_LOCK = 849621
_CLOCK = "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::BIGINT"


class ResearchCaptureRequirementV1(StrictValueModel):
    source_kind: Literal["MARKET_SNAPSHOT", "NEWS", "MACRO"]
    source_name: _ID
    maximum_age_ms: Annotated[StrictInt, Field(ge=1, le=604_800_000)]


class ResearchCampaignPlanV1(_Receipt):
    contract_version: Literal["adaptive-causal-campaign-plan.v1"] = "adaptive-causal-campaign-plan.v1"
    campaign_id: _ID
    schedule_digest: _SHA
    candidate_protocol_digest: _SHA
    capture_policy: Literal["db-observed-before-decision-cutoff.v1"] = "db-observed-before-decision-cutoff.v1"
    recording_mode: Literal["CAUSAL_OBSERVATION", "OFFLINE_ENGINEERING_FIXTURE"]
    scheduler_sha256: _SHA
    required_sources: tuple[ResearchCaptureRequirementV1, ...] = Field(min_length=3, max_length=16)
    maximum_windows_per_tick: Annotated[StrictInt, Field(ge=1, le=64)]
    maximum_clock_skew_ms: Annotated[StrictInt, Field(ge=0, le=2_000)]
    maximum_call_seconds: Annotated[StrictInt, Field(ge=1, le=120)]

    @model_validator(mode="after")
    def capture_identity(self) -> Self:
        keys = [(item.source_kind, item.source_name) for item in self.required_sources]
        if keys != sorted(set(keys)):
            raise ValueError("capture requirements must be distinct and sorted")
        kinds = Counter(item.source_kind for item in self.required_sources)
        if kinds["MARKET_SNAPSHOT"] != 1 or kinds["NEWS"] < 1 or kinds["MACRO"] < 1:
            raise ValueError("causal campaign requires exactly one market plus news and macro")
        return self


class ResearchWindowClaimV1(_Receipt):
    contract_version: Literal["adaptive-window-claim.v1"] = "adaptive-window-claim.v1"
    campaign_id: _ID
    sample_id: _ID
    plan_receipt_sha256: _SHA
    claim_id: _SHA
    claimed_at_ts_ms: _MS

    @model_validator(mode="after")
    def claim_identity(self) -> Self:
        if self.claim_id != canonical_sha256(
            {"plan_receipt_sha256": self.plan_receipt_sha256, "sample_id": self.sample_id}
        ):
            raise ValueError("window claim differs from its frozen campaign/sample identity")
        return self


class ResearchCausalBundleV1(_Receipt):
    contract_version: Literal["adaptive-causal-bundle.v1"] = "adaptive-causal-bundle.v1"
    campaign_id: _ID
    sample_id: _ID
    claim_id: _SHA
    market_snapshot_sha256: _SHA
    source_receipt_sha256s: tuple[_SHA, ...] = Field(min_length=3, max_length=16)
    frozen_at_ts_ms: _MS

    @model_validator(mode="after")
    def source_roster(self) -> Self:
        if self.source_receipt_sha256s != tuple(sorted(set(self.source_receipt_sha256s))):
            raise ValueError("causal bundle sources must be distinct and sorted")
        return self


class ResearchReviewOutputV1(StrictValueModel):
    contract_version: Literal["adaptive-review-output.v1"] = "adaptive-review-output.v1"
    action: Literal["ALLOW", "VETO", "DEFER"]
    rationale: StrictStr = Field(min_length=1, max_length=1_024)
    evidence_ids: tuple[_SHA, ...] = Field(min_length=1, max_length=16)

    @model_validator(mode="after")
    def distinct_evidence(self) -> Self:
        if len(set(self.evidence_ids)) != len(self.evidence_ids) or not self.rationale.strip():
            raise ValueError("review requires distinct cited evidence and a nonempty rationale")
        return self


class ResearchReviewReceiptV1(_Receipt):
    contract_version: Literal["adaptive-review-completion.v1"] = "adaptive-review-completion.v1"
    campaign_id: _ID
    sample_id: _ID
    attempt_id: _ID
    start_receipt_sha256: _SHA
    bundle_receipt_sha256: _SHA
    evaluation_receipt_sha256: _SHA
    output: ResearchReviewOutputV1
    response_sha256: _SHA
    requested_model: _ID
    resolved_model: _ID
    request_id: _ID
    prompt_sha256: _SHA
    observed_at_ts_ms: _MS


class ResearchCostReceiptV1(_Receipt):
    """Captured by the budget adapter, never supplied in the model JSON."""

    contract_version: Literal["adaptive-budget-observation.v1"] = "adaptive-budget-observation.v1"
    campaign_id: _ID
    sample_id: _ID
    arm_id: Literal["strategy-review", "llm-proposal-research"]
    attempt_id: _ID
    stage: Literal["RESERVATION_REQUESTED", "RESERVATION_DENIED", "RESERVED", "COMMIT_REQUESTED", "COMMITTED"]
    amount_microusd: Annotated[StrictInt, Field(ge=0, le=12_000_000)]
    observed_at_ts_ms: _MS


class ResearchArmOutcomeV1(_Receipt):
    contract_version: Literal["adaptive-scheduled-arm-outcome.v1"] = "adaptive-scheduled-arm-outcome.v1"
    campaign_id: _ID
    sample_id: _ID
    arm_id: Arm
    claim_id: _SHA
    status: Literal[
        "BASELINE",
        "NO_INTENT",
        "ALLOW",
        "VETO",
        "DEFER",
        "PROPOSAL",
        "CALL_FAILED",
        "SOURCE_MISSING",
        "EVALUATOR_FAILED",
        "MISSED",
        "LATE",
        "UNKNOWN",
        "BUDGET_BLOCKED",
    ]
    bundle_receipt_sha256: _SHA | None = None
    evaluation_receipt_sha256: _SHA | None = None
    attempt_id: _ID | None = None
    decision_receipt_sha256: _SHA | None = None
    causal_sample_receipt_sha256: _SHA | None = None
    observed_at_ts_ms: _MS


class ResearchDenominatorReceiptV1(_Receipt):
    contract_version: Literal["adaptive-scheduled-denominator.v1"] = "adaptive-scheduled-denominator.v1"
    qualification: Literal["SCHEDULED_DENOMINATOR_ONLY"] = "SCHEDULED_DENOMINATOR_ONLY"
    economic_qualification: Literal[False] = False
    live_orders_allowed: Literal[False] = False
    campaign_id: _ID
    plan_receipt_sha256: _SHA
    recording_mode: Literal["CAUSAL_OBSERVATION", "OFFLINE_ENGINEERING_FIXTURE"]
    expected_outcomes: Annotated[StrictInt, Field(ge=3, le=150_000)]
    outcome_ids_sha256: _SHA
    status_counts: dict[str, StrictInt]
    committed_cost_microusd: Annotated[StrictInt, Field(ge=0)]
    outstanding_reservation_microusd: Annotated[StrictInt, Field(ge=0)]
    unknown_attempt_count: Annotated[StrictInt, Field(ge=0)]
    unknown_budget_operation_count: Annotated[StrictInt, Field(ge=0)]


class ResearchCampaignRepository:
    """A real PostgreSQL recorder and once-only window/attempt journal."""

    def __init__(self, database: Database) -> None:
        if (
            not isinstance(database, Database)
            or database.migration_profile is not MigrationProfile.RESEARCH_CAMPAIGN
        ):
            raise ValueError("adaptive campaign requires the explicit isolated RESEARCH_CAMPAIGN profile")
        if database.read_only:
            raise ValueError("adaptive campaign recorder requires a writable isolated database")
        self._database = database

    async def clock(self) -> int:
        async with self._database.pool.acquire() as connection:
            return int(await connection.fetchval(_CLOCK))

    async def register(self, plan: ResearchCampaignPlanV1) -> bool:
        _verify_model(plan, ResearchCampaignPlanV1)
        async with self._database.transaction() as connection:
            await self._lock(connection, plan.campaign_id)
            schedule, protocol = await self._identity(connection, plan.campaign_id)
            if (
                plan.schedule_digest != schedule.schedule_digest
                or plan.candidate_protocol_digest != protocol.protocol_digest
            ):
                raise MessageIdentityConflict("campaign plan differs from frozen schedule/protocol")
            old = await connection.fetchrow(
                "SELECT * FROM sim_adaptive_campaign_plans WHERE campaign_id=$1", plan.campaign_id
            )
            if old is not None:
                if _decode_row(old, ResearchCampaignPlanV1) != plan:
                    raise MessageIdentityConflict("campaign already has a different immutable plan")
                return False
            if schedule.windows[0].market_as_of_ts_ms <= int(await connection.fetchval(_CLOCK)):
                raise MessageIdentityConflict("campaign cannot preregister past decision windows")
            encoded, digest = canonical_payload(plan.model_dump(mode="json"))
            await connection.execute(
                "INSERT INTO sim_adaptive_campaign_plans"
                "(campaign_id,receipt_sha256,payload_json,payload_sha256) "
                "VALUES($1,$2,$3,$4)",
                plan.campaign_id,
                plan.receipt_sha256,
                encoded,
                digest,
            )
            return True

    async def load_campaign(self, campaign_id: str):
        async with self._database.transaction() as connection:
            await self._lock(connection, campaign_id)
            return await self._context(connection, campaign_id)

    async def capture_source(
        self,
        *,
        campaign_id: str,
        sample_id: str,
        source_kind: Literal["MARKET_SNAPSHOT", "NEWS", "MACRO"],
        source_name: str,
        reference: str,
        source_as_of_ts_ms: int,
        content: dict[str, Any],
    ) -> ResearchSourceReceiptV1:
        """Stamp actual database observation time; late input is retained, never backdated."""
        async with self._database.transaction() as connection:
            await self._lock(connection, campaign_id)
            plan, schedule, protocol = await self._context(connection, campaign_id)
            self._window(schedule, sample_id)
            if (source_kind, source_name) not in {
                (x.source_kind, x.source_name) for x in plan.required_sources
            }:
                raise MessageIdentityConflict("source is not in the preregistered capture policy")
            if await self._row(connection, campaign_id, sample_id, "all", "bundle", "one") is not None:
                raise MessageIdentityConflict("frozen causal bundle cannot receive new source content")
            slot = f"{source_kind}:{source_name}"
            old = await self._row(connection, campaign_id, sample_id, "all", "source", slot)
            if old is not None:
                receipt = _decode_row(old, ResearchSourceReceiptV1)
                if (receipt.reference, receipt.source_as_of_ts_ms, receipt.content) != (
                    reference,
                    source_as_of_ts_ms,
                    content,
                ):
                    raise MessageIdentityConflict(
                        "source capture cannot replace its independently saved bytes"
                    )
                return receipt
            receipt = ResearchSourceReceiptV1(
                campaign_id=campaign_id,
                sample_id=sample_id,
                schedule_digest=schedule.schedule_digest,
                candidate_protocol_digest=protocol.protocol_digest,
                source_kind=source_kind,
                source_name=source_name,
                reference=reference,
                source_as_of_ts_ms=source_as_of_ts_ms,
                observed_at_ts_ms=int(await connection.fetchval(_CLOCK)),
                content=content,
            )
            await self._store(connection, receipt, "source", "all", slot)
            return receipt

    async def claim_next(self, campaign_id: str) -> ResearchWindowClaimV1 | None:
        """Durable once-only claim. Never reclaims an unfinished window after restart."""
        async with self._database.transaction() as connection:
            await self._lock(connection, campaign_id)
            plan, _, _ = await self._context(connection, campaign_id)
            now = int(await connection.fetchval(_CLOCK))
            row = await connection.fetchrow(
                """SELECT w.sample_id FROM sim_research_observation_windows w
                LEFT JOIN sim_adaptive_window_claims c
                  ON c.campaign_id=w.campaign_id AND c.sample_id=w.sample_id
                WHERE w.campaign_id=$1 AND w.market_as_of_ts_ms<=$2 AND c.sample_id IS NULL
                ORDER BY w.market_as_of_ts_ms,w.symbol,w.timeframe,w.sample_id LIMIT 1 FOR UPDATE OF w""",
                campaign_id,
                now,
            )
            if row is None:
                return None
            claim = ResearchWindowClaimV1(
                campaign_id=campaign_id,
                sample_id=row["sample_id"],
                plan_receipt_sha256=plan.receipt_sha256,
                claim_id=canonical_sha256(
                    {"plan_receipt_sha256": plan.receipt_sha256, "sample_id": row["sample_id"]}
                ),
                claimed_at_ts_ms=now,
            )
            encoded, digest = canonical_payload(claim.model_dump(mode="json"))
            await connection.execute(
                "INSERT INTO sim_adaptive_window_claims"
                "(campaign_id,sample_id,claim_id,receipt_sha256,payload_json,payload_sha256) "
                "VALUES($1,$2,$3,$4,$5,$6)",
                campaign_id,
                claim.sample_id,
                claim.claim_id,
                claim.receipt_sha256,
                encoded,
                digest,
            )
            return claim

    async def freeze_bundle(self, claim: ResearchWindowClaimV1) -> ResearchCausalBundleV1:
        async with self._database.transaction() as connection:
            plan, schedule, _ = await self._claimed(connection, claim)
            old = await self._row(connection, claim.campaign_id, claim.sample_id, "all", "bundle", "one")
            if old is not None:
                return _decode_row(old, ResearchCausalBundleV1)
            window = self._window(schedule, claim.sample_id)
            sources = []
            for requirement in plan.required_sources:
                row = await self._row(
                    connection,
                    claim.campaign_id,
                    claim.sample_id,
                    "all",
                    "source",
                    f"{requirement.source_kind}:{requirement.source_name}",
                )
                if row is None:
                    raise MessageIdentityConflict("preregistered causal source is missing")
                source = _decode_row(row, ResearchSourceReceiptV1)
                cutoff = window.market_as_of_ts_ms
                if (
                    not cutoff - requirement.maximum_age_ms
                    <= source.source_as_of_ts_ms
                    <= source.observed_at_ts_ms
                    <= cutoff
                ):
                    raise MessageIdentityConflict(
                        "source is late, future or stale at the preregistered decision cutoff"
                    )
                sources.append(source)
            market = next(x for x in sources if x.source_kind == "MARKET_SNAPSHOT")
            if market.source_as_of_ts_ms >= window.market_as_of_ts_ms:
                raise MessageIdentityConflict(
                    "campaign decision cutoff must be after the captured market close"
                )
            if (
                window.market_snapshot_sha256 is not None
                and market.content_sha256 != window.market_snapshot_sha256
            ):
                raise MessageIdentityConflict("market differs from the frozen snapshot identity")
            bundle = ResearchCausalBundleV1(
                campaign_id=claim.campaign_id,
                sample_id=claim.sample_id,
                claim_id=claim.claim_id,
                market_snapshot_sha256=market.content_sha256,
                source_receipt_sha256s=tuple(sorted(x.receipt_sha256 for x in sources)),
                frozen_at_ts_ms=int(await connection.fetchval(_CLOCK)),
            )
            await self._store(connection, bundle, "bundle", "all", "one")
            return bundle

    async def resolve_causal_sources(
        self, *, campaign_id: str, sample_id: str, source_receipt_sha256s: tuple[str, ...]
    ):
        """Typed opt-in resolver for the existing coordinator; no caller accepted flag."""
        async with self._database.transaction() as connection:
            await self._lock(connection, campaign_id)
            _, schedule, _ = await self._context(connection, campaign_id)
            row = await self._row(connection, campaign_id, sample_id, "all", "bundle", "one")
            if row is None:
                raise MessageIdentityConflict("campaign has no independently frozen causal bundle")
            bundle = _decode_row(row, ResearchCausalBundleV1)
            if tuple(sorted(source_receipt_sha256s)) != bundle.source_receipt_sha256s:
                raise MessageIdentityConflict(
                    "coordinator source set differs from matched frozen causal bundle"
                )
            sources = tuple(
                [
                    await self._load_receipt(connection, x, ResearchSourceReceiptV1)
                    for x in bundle.source_receipt_sha256s
                ]
            )
            cutoff = self._window(schedule, sample_id).market_as_of_ts_ms
            if any(not x.source_as_of_ts_ms <= x.observed_at_ts_ms <= cutoff for x in sources):
                raise MessageIdentityConflict("causal source history failed replay")
            return sources

    async def record_evaluation(
        self, *, claim: ResearchWindowClaimV1, bundle: ResearchCausalBundleV1, intent: StrategyIntentV1 | None
    ) -> ResearchStrategyEvaluationReceiptV1:
        async with self._database.transaction() as connection:
            _, schedule, protocol = await self._claimed(connection, claim)
            self._same_bundle(await self._bundle(connection, claim), bundle)
            old = await self._row(connection, claim.campaign_id, claim.sample_id, "all", "evaluation", "one")
            if old is not None:
                saved = _decode_row(old, ResearchStrategyEvaluationReceiptV1)
                if saved.intent != intent:
                    raise MessageIdentityConflict("baseline evaluator cannot overwrite a prior outcome")
                return saved
            window = self._window(schedule, claim.sample_id)
            receipt = ResearchStrategyEvaluationReceiptV1(
                campaign_id=claim.campaign_id,
                sample_id=claim.sample_id,
                schedule_digest=schedule.schedule_digest,
                candidate_protocol_digest=protocol.protocol_digest,
                strategy_id=schedule.strategy_id,
                strategy_revision=schedule.strategy_revision,
                symbol=window.symbol,
                timeframe=window.timeframe,
                evidence_as_of_ts_ms=window.market_as_of_ts_ms,
                evaluated_at_ts_ms=int(await connection.fetchval(_CLOCK)),
                market_snapshot_sha256=bundle.market_snapshot_sha256,
                evaluator_sha256=schedule.evaluator_sha256,
                source_receipt_sha256s=bundle.source_receipt_sha256s,
                intent=intent,
            )
            await self._store(connection, receipt, "evaluation", "all", "one")
            return receipt

    async def load_source(self, receipt_sha256: str) -> ResearchSourceReceiptV1:
        async with self._database.pool.acquire() as connection:
            return await self._load_receipt(connection, receipt_sha256, ResearchSourceReceiptV1)

    async def load_evaluation(self, receipt_sha256: str) -> ResearchStrategyEvaluationReceiptV1:
        async with self._database.pool.acquire() as connection:
            return await self._load_receipt(connection, receipt_sha256, ResearchStrategyEvaluationReceiptV1)

    async def find_attempt(self, attempt_id: str):
        async with self._database.pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT * FROM sim_adaptive_campaign_receipts WHERE kind='start' AND slot_key=$1", attempt_id
            )
            if row is None:
                return None
            start = _decode_row(row, ResearchLLMAttemptStartV1)
            terminal_row = await self._row(
                connection, start.campaign_id, start.sample_id, start.arm_id, "terminal", attempt_id
            )
            terminal = _decode_row(terminal_row, ResearchLLMAttemptTerminalV1) if terminal_row else None
            return start, terminal

    async def start_attempt(self, attempt: ResearchLLMAttemptStartV1) -> bool:
        _verify_model(attempt, ResearchLLMAttemptStartV1)
        async with self._database.transaction() as connection:
            await self._lock(connection, attempt.campaign_id)
            plan, schedule, protocol = await self._context(connection, attempt.campaign_id)
            claim = await self._load_claim(connection, attempt.campaign_id, attempt.sample_id)
            bundle = await self._bundle(connection, claim)
            window = self._window(schedule, attempt.sample_id)
            arm = next(x for x in protocol.arms if x.arm_id == attempt.arm_id)
            now = int(await connection.fetchval(_CLOCK))
            if not window.market_as_of_ts_ms <= now < window.paired_at_ts_ms:
                raise MessageIdentityConflict("new attempt is outside its actual DB admission window")
            if abs(now - attempt.attempt_started_at_ts_ms) > plan.maximum_clock_skew_ms:
                raise MessageIdentityConflict("attempt local clock differs from actual DB observation time")
            for field in ("symbol", "timeframe", "market_as_of_ts_ms", "sample_deadline_ts_ms"):
                if getattr(attempt, field) != getattr(window, field):
                    raise MessageIdentityConflict("attempt differs from preregistered sample")
            if (
                attempt.schedule_digest,
                attempt.candidate_protocol_digest,
                attempt.arm_protocol_digest,
                attempt.provider,
                attempt.requested_model,
                attempt.prompt_sha256,
                attempt.market_snapshot_sha256,
            ) != (
                schedule.schedule_digest,
                protocol.protocol_digest,
                protocol.arm_digest(attempt.arm_id),
                arm.provider,
                arm.model,
                arm.prompt_sha256,
                bundle.market_snapshot_sha256,
            ):
                raise MessageIdentityConflict("attempt differs from frozen route/source/protocol")
            other = await connection.fetchrow(
                "SELECT * FROM sim_adaptive_campaign_receipts "
                "WHERE campaign_id=$1 AND sample_id=$2 AND arm_id=$3 AND kind='start'",
                attempt.campaign_id,
                attempt.sample_id,
                attempt.arm_id,
            )
            if other is not None:
                if _decode_row(other, ResearchLLMAttemptStartV1) != attempt:
                    raise MessageIdentityConflict("arm already has an immutable dispatch fence")
                return False
            return await self._store(connection, attempt, "start", attempt.arm_id, attempt.attempt_id)

    async def finish_attempt(self, terminal: ResearchLLMAttemptTerminalV1) -> bool:
        _verify_model(terminal, ResearchLLMAttemptTerminalV1)
        found = await self.find_attempt(terminal.attempt_id)
        if found is None:
            raise MessageIdentityConflict("terminal has no independently committed attempt")
        start, _ = found
        if (
            terminal.start_receipt_sha256 != start.receipt_sha256
            or terminal.observed_at_ts_ms < start.attempt_started_at_ts_ms
        ):
            raise MessageIdentityConflict("terminal differs from actual admitted attempt")
        for linked in (terminal.completion, terminal.failure):
            if linked is not None:
                for field in (
                    "campaign_id",
                    "arm_id",
                    "sample_id",
                    "symbol",
                    "timeframe",
                    "market_as_of_ts_ms",
                    "market_snapshot_sha256",
                    "sample_deadline_ts_ms",
                    "attempt_started_at_ts_ms",
                ):
                    if getattr(linked, field) != getattr(start, field):
                        raise MessageIdentityConflict("terminal provenance differs from durable START")
        async with self._database.transaction() as connection:
            await self._lock(connection, start.campaign_id)
            review = await self._row(
                connection, start.campaign_id, start.sample_id, start.arm_id, "review", start.attempt_id
            )
            if review is not None:
                raise MessageIdentityConflict("terminal cannot overwrite an independently completed review")
            existing = await self._row(
                connection, start.campaign_id, start.sample_id, start.arm_id, "terminal", start.attempt_id
            )
            if existing is not None:
                if _decode_row(existing, ResearchLLMAttemptTerminalV1) != terminal:
                    raise MessageIdentityConflict("terminal cannot rewrite an immutable completion")
                return False
            await self._response_clock(connection, start, terminal.observed_at_ts_ms)
            provenance = terminal.proposal.model_provenance if terminal.proposal else terminal.failure
            if provenance is not None and (
                provenance.provider != start.provider
                or provenance.requested_model != start.requested_model
                or provenance.prompt_sha256 != start.prompt_sha256
                or provenance.budget_reservation_id != start.budget_reservation_id
            ):
                raise MessageIdentityConflict("terminal route differs from independently admitted START")
            if (
                terminal.proposal is not None
                and terminal.proposal.model_provenance.resolved_model != start.requested_model
            ):
                raise MessageIdentityConflict("terminal backend differs from the frozen requested model")
            if terminal.proposal is not None:
                from kairos_core import EvidenceReferenceV1

                claim = await self._load_claim(connection, start.campaign_id, start.sample_id)
                bundle = await self._bundle(connection, claim)
                sources = [
                    await self._load_receipt(connection, x, ResearchSourceReceiptV1)
                    for x in bundle.source_receipt_sha256s
                ]
                allowed = {
                    canonical_sha256(
                        EvidenceReferenceV1(
                            kind=x.source_kind,
                            reference=x.reference,
                            content_sha256=x.content_sha256,
                            observed_at_ms=x.observed_at_ts_ms,
                        )
                    )
                    for x in sources
                }
                if any(canonical_sha256(x) not in allowed for x in terminal.proposal.evidence):
                    raise MessageIdentityConflict(
                        "proposal cited evidence absent from independently saved bundle"
                    )
            if start.arm_id != "llm-proposal-research" and terminal.terminal_status == "COMPLETED":
                raise MessageIdentityConflict("review completion cannot be recorded as a proposal")
            return await self._store(
                connection,
                terminal,
                "terminal",
                start.arm_id,
                terminal.attempt_id,
                campaign_id=start.campaign_id,
                sample_id=start.sample_id,
            )

    async def record_review(self, receipt: ResearchReviewReceiptV1) -> bool:
        _verify_model(receipt, ResearchReviewReceiptV1)
        found = await self.find_attempt(receipt.attempt_id)
        if found is None or found[0].arm_id != "strategy-review":
            raise MessageIdentityConflict("review completion lacks its exact durable review START")
        start, terminal = found
        if terminal is not None or receipt.start_receipt_sha256 != start.receipt_sha256:
            raise MessageIdentityConflict("review cannot replace an unresolved/failed terminal")
        async with self._database.transaction() as connection:
            await self._lock(connection, receipt.campaign_id)
            if (
                await self._row(
                    connection,
                    receipt.campaign_id,
                    receipt.sample_id,
                    "strategy-review",
                    "terminal",
                    receipt.attempt_id,
                )
                is not None
            ):
                raise MessageIdentityConflict("review cannot race a previously committed terminal")
            existing = await self._row(
                connection,
                receipt.campaign_id,
                receipt.sample_id,
                "strategy-review",
                "review",
                receipt.attempt_id,
            )
            if existing is not None:
                if _decode_row(existing, ResearchReviewReceiptV1) != receipt:
                    raise MessageIdentityConflict("review cannot rewrite an immutable completion")
                return False
            await self._response_clock(connection, start, receipt.observed_at_ts_ms)
            claim = await self._load_claim(connection, receipt.campaign_id, receipt.sample_id)
            bundle = await self._bundle(connection, claim)
            evaluation = await self._load_receipt(
                connection, receipt.evaluation_receipt_sha256, ResearchStrategyEvaluationReceiptV1
            )
            if (
                evaluation.intent is None
                or evaluation.source_receipt_sha256s != bundle.source_receipt_sha256s
                or evaluation.campaign_id != receipt.campaign_id
                or evaluation.sample_id != receipt.sample_id
                or start.campaign_id != receipt.campaign_id
                or start.sample_id != receipt.sample_id
            ):
                raise MessageIdentityConflict(
                    "review requires the matched independently saved strategy intent"
                )
            if (
                receipt.bundle_receipt_sha256,
                receipt.requested_model,
                receipt.resolved_model,
                receipt.prompt_sha256,
            ) != (
                bundle.receipt_sha256,
                start.requested_model,
                start.requested_model,
                start.prompt_sha256,
            ):
                raise MessageIdentityConflict("review differs from frozen matched provenance")
            if receipt.observed_at_ts_ms < start.attempt_started_at_ts_ms:
                raise MessageIdentityConflict("review observation precedes actual attempt START")
            from kairos_core import EvidenceReferenceV1

            sources = [
                await self._load_receipt(connection, x, ResearchSourceReceiptV1)
                for x in bundle.source_receipt_sha256s
            ]
            allowed = {
                canonical_sha256(
                    EvidenceReferenceV1(
                        kind=x.source_kind,
                        reference=x.reference,
                        content_sha256=x.content_sha256,
                        observed_at_ms=x.observed_at_ts_ms,
                    )
                )
                for x in sources
            }
            if set(receipt.output.evidence_ids) - allowed:
                raise MessageIdentityConflict(
                    "review cited evidence absent from the independently saved bundle"
                )
            return await self._store(connection, receipt, "review", "strategy-review", receipt.attempt_id)

    async def find_review(self, attempt_id: str) -> ResearchReviewReceiptV1 | None:
        async with self._database.pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT * FROM sim_adaptive_campaign_receipts WHERE kind='review' AND slot_key=$1", attempt_id
            )
            return _decode_row(row, ResearchReviewReceiptV1) if row else None

    async def decision_recorded_at(
        self, *, campaign_id: str, sample_id: str, arm_id: Arm, attempt_id: str
    ) -> int:
        """Actual PostgreSQL insertion clock, not a caller completion timestamp."""
        async with self._database.pool.acquire() as connection:
            rows = [
                row
                for kind in ("review", "terminal")
                if (row := await self._row(connection, campaign_id, sample_id, arm_id, kind, attempt_id))
                is not None
            ]
            if len(rows) > 1:
                raise MessageIdentityConflict("arm has conflicting independent completion histories")
            if not rows:
                raise MessageIdentityConflict("completion has no independently stored recording clock")
            return self._recorded_at(rows[0])

    async def record_cost(self, receipt: ResearchCostReceiptV1) -> bool:
        _verify_model(receipt, ResearchCostReceiptV1)
        async with self._database.transaction() as connection:
            await self._lock(connection, receipt.campaign_id)
            await self._load_claim(connection, receipt.campaign_id, receipt.sample_id)
            now = int(await connection.fetchval(_CLOCK))
            if receipt.observed_at_ts_ms != now:
                # Receipt creation and insertion occur in separate bounded calls;
                # permit honest elapsed recording, never future/backdated input.
                if not 0 <= now - receipt.observed_at_ts_ms <= 2_000:
                    raise MessageIdentityConflict("cost observation differs from actual recorder clock")
            if receipt.stage in ("RESERVED", "RESERVATION_DENIED"):
                requested = await self._row(
                    connection,
                    receipt.campaign_id,
                    receipt.sample_id,
                    receipt.arm_id,
                    "cost",
                    f"RESERVATION_REQUESTED:{receipt.attempt_id}",
                )
                if requested is None:
                    raise MessageIdentityConflict("reservation lacks its independently saved operation fence")
                request = _decode_row(requested, ResearchCostReceiptV1)
                expected_amount = request.amount_microusd if receipt.stage == "RESERVED" else 0
                if receipt.amount_microusd != expected_amount:
                    raise MessageIdentityConflict("reservation observation differs from recorded request")
                other_stage = "RESERVATION_DENIED" if receipt.stage == "RESERVED" else "RESERVED"
                if (
                    await self._row(
                        connection,
                        receipt.campaign_id,
                        receipt.sample_id,
                        receipt.arm_id,
                        "cost",
                        f"{other_stage}:{receipt.attempt_id}",
                    )
                    is not None
                ):
                    raise MessageIdentityConflict("reservation cannot be both admitted and denied")
            if receipt.stage in ("COMMIT_REQUESTED", "COMMITTED"):
                reserved = await self._row(
                    connection,
                    receipt.campaign_id,
                    receipt.sample_id,
                    receipt.arm_id,
                    "cost",
                    f"RESERVED:{receipt.attempt_id}",
                )
                if reserved is None:
                    raise MessageIdentityConflict("actual cost lacks an independently observed reservation")
                held = _decode_row(reserved, ResearchCostReceiptV1)
                if receipt.amount_microusd > held.amount_microusd:
                    raise MessageIdentityConflict("actual cost exceeds the frozen reserved bound")
                if receipt.stage == "COMMITTED":
                    requested = await self._row(
                        connection,
                        receipt.campaign_id,
                        receipt.sample_id,
                        receipt.arm_id,
                        "cost",
                        f"COMMIT_REQUESTED:{receipt.attempt_id}",
                    )
                    if (
                        requested is None
                        or _decode_row(requested, ResearchCostReceiptV1).amount_microusd
                        != receipt.amount_microusd
                    ):
                        raise MessageIdentityConflict("commit lacks its exact independent operation fence")
            return await self._store(
                connection, receipt, "cost", receipt.arm_id, f"{receipt.stage}:{receipt.attempt_id}"
            )

    async def record_verified_sample(self, sample: ResearchDecisionSampleV1) -> bool:
        """Rebuild from this new journal, never insert into the immutable legacy SIM25 path."""
        from kairos_core.research_pairing import ScheduledResearchSampleV1, build_research_decision_sample

        async with self._database.transaction() as connection:
            await self._lock(connection, sample.campaign_id)
            _, schedule, protocol = await self._context(connection, sample.campaign_id)
            window = self._window(schedule, sample.sample_id)
            evaluation = await self._load_receipt(
                connection, str(sample.strategy_evaluation_sha256), ResearchStrategyEvaluationReceiptV1
            )
            rows = await connection.fetch(
                "SELECT * FROM sim_adaptive_campaign_receipts "
                "WHERE campaign_id=$1 AND sample_id=$2 AND arm_id=$3 AND kind='start'",
                sample.campaign_id,
                sample.sample_id,
                sample.arm_id,
            )
            terminal = None
            if rows:
                start = _decode_row(rows[0], ResearchLLMAttemptStartV1)
                row = await self._row(
                    connection,
                    sample.campaign_id,
                    sample.sample_id,
                    sample.arm_id,
                    "terminal",
                    start.attempt_id,
                )
                if row is None:
                    raise MessageIdentityConflict("unfinished START cannot become a causal sample")
                if self._recorded_at(row) > window.paired_at_ts_ms:
                    raise MessageIdentityConflict("late-recorded completion cannot become a causal sample")
                terminal = _decode_row(row, ResearchLLMAttemptTerminalV1)
                if terminal.terminal_status == "UNRESOLVED":
                    raise MessageIdentityConflict("ambiguous START cannot become a causal sample")
            rebuilt = build_research_decision_sample(
                ScheduledResearchSampleV1(
                    campaign_id=sample.campaign_id,
                    arm_id=sample.arm_id,
                    sample_id=sample.sample_id,
                    symbol=window.symbol,
                    timeframe=window.timeframe,
                    market_as_of_ts_ms=window.market_as_of_ts_ms,
                    market_snapshot_sha256=evaluation.market_snapshot_sha256,
                    strategy_id=schedule.strategy_id,
                    strategy_revision=schedule.strategy_revision,
                    paired_at_ts_ms=window.paired_at_ts_ms,
                    sample_deadline_ts_ms=window.sample_deadline_ts_ms,
                ),
                strategy_evaluation=evaluation.as_evidence(sample.arm_id),
                strategy_intent=evaluation.intent,
                llm_proposal=terminal.proposal if terminal else None,
                llm_completion=terminal.completion if terminal else None,
                llm_failure=terminal.failure if terminal else None,
                llm_was_called=bool(rows),
            )
            expected = {
                **rebuilt.identity_payload(),
                "arm_protocol_digest": protocol.arm_digest(sample.arm_id),
            }
            if sample.identity_payload() != expected:
                raise MessageIdentityConflict("causal sample differs from independently stored replay facts")
            wrapped = _CampaignSampleV1(
                campaign_id=sample.campaign_id, sample_id=sample.sample_id, sample=sample
            )
            return await self._store(connection, wrapped, "sample", sample.arm_id, "one")

    async def record_outcome(self, outcome: ResearchArmOutcomeV1) -> bool:
        _verify_model(outcome, ResearchArmOutcomeV1)
        async with self._database.transaction() as connection:
            await self._lock(connection, outcome.campaign_id)
            claim = await self._load_claim(connection, outcome.campaign_id, outcome.sample_id)
            if claim.claim_id != outcome.claim_id:
                raise MessageIdentityConflict("outcome has no exact independently committed window claim")
            _, schedule, _ = await self._context(connection, outcome.campaign_id)
            window = self._window(schedule, outcome.sample_id)
            old = await self._row(
                connection, outcome.campaign_id, outcome.sample_id, outcome.arm_id, "outcome", "one"
            )
            if old is not None:
                if _decode_row(old, ResearchArmOutcomeV1) != outcome:
                    raise MessageIdentityConflict("scheduled outcome cannot be rewritten after observation")
                return False
            now = int(await connection.fetchval(_CLOCK))
            if not claim.claimed_at_ts_ms <= outcome.observed_at_ts_ms <= now:
                raise MessageIdentityConflict("scheduled outcome has no honest actual recorder time")
            bundle_row = await self._row(
                connection, outcome.campaign_id, outcome.sample_id, "all", "bundle", "one"
            )
            evaluation_row = await self._row(
                connection, outcome.campaign_id, outcome.sample_id, "all", "evaluation", "one"
            )
            bundle = _decode_row(bundle_row, ResearchCausalBundleV1) if bundle_row else None
            evaluation = (
                _decode_row(evaluation_row, ResearchStrategyEvaluationReceiptV1) if evaluation_row else None
            )
            if (outcome.bundle_receipt_sha256, outcome.evaluation_receipt_sha256) != (
                bundle.receipt_sha256 if bundle else None,
                evaluation.receipt_sha256 if evaluation else None,
            ):
                raise MessageIdentityConflict(
                    "all arms must retain the same independently saved causal input"
                )
            starts = await connection.fetch(
                "SELECT * FROM sim_adaptive_campaign_receipts "
                "WHERE campaign_id=$1 AND sample_id=$2 AND arm_id=$3 AND kind='start'",
                outcome.campaign_id,
                outcome.sample_id,
                outcome.arm_id,
            )
            if starts:
                start = _decode_row(starts[0], ResearchLLMAttemptStartV1)
                if outcome.attempt_id != start.attempt_id or outcome.status in (
                    "MISSED",
                    "SOURCE_MISSING",
                    "NO_INTENT",
                    "BUDGET_BLOCKED",
                ):
                    raise MessageIdentityConflict(
                        "outcome cannot suppress an independently stored admitted attempt"
                    )
            elif outcome.attempt_id is not None:
                raise MessageIdentityConflict("outcome invents an attempt not independently recorded")
            decision = None
            if starts:
                terminal_row = await self._row(
                    connection,
                    outcome.campaign_id,
                    outcome.sample_id,
                    outcome.arm_id,
                    "terminal",
                    start.attempt_id,
                )
                review_row = await self._row(
                    connection,
                    outcome.campaign_id,
                    outcome.sample_id,
                    outcome.arm_id,
                    "review",
                    start.attempt_id,
                )
                terminal = _decode_row(terminal_row, ResearchLLMAttemptTerminalV1) if terminal_row else None
                review = _decode_row(review_row, ResearchReviewReceiptV1) if review_row else None
                if terminal is not None and review is not None:
                    raise MessageIdentityConflict("arm has conflicting independent completion histories")
                decision = review or terminal
                expected_status = "UNKNOWN"
                if decision is not None and not (
                    terminal is not None and terminal.terminal_status == "UNRESOLVED"
                ):
                    if (
                        decision.observed_at_ts_ms > window.paired_at_ts_ms
                        or self._recorded_at(review_row or terminal_row) > window.paired_at_ts_ms
                    ):
                        expected_status = "LATE"
                    elif review is not None:
                        expected_status = review.output.action
                    else:
                        if terminal is None:
                            raise MessageIdentityConflict("independent completion history is unavailable")
                        expected_status = (
                            "CALL_FAILED" if terminal.terminal_status == "FAILED" else "PROPOSAL"
                        )
                if outcome.status != expected_status:
                    raise MessageIdentityConflict(
                        "arm outcome is not derived from its independent attempt history"
                    )
            elif evaluation is not None and evaluation.evaluated_at_ts_ms > window.paired_at_ts_ms:
                if outcome.status != "LATE":
                    raise MessageIdentityConflict("late evaluator cannot become a timely outcome")
            elif outcome.arm_id == "strategy-only" and evaluation is not None:
                expected_status = "BASELINE" if evaluation.intent is not None else "NO_INTENT"
                if outcome.status != expected_status:
                    raise MessageIdentityConflict("baseline outcome differs from saved evaluator result")
            elif outcome.arm_id == "strategy-review" and evaluation is not None and evaluation.intent is None:
                if outcome.status != "NO_INTENT":
                    raise MessageIdentityConflict("absent strategy intent requires a no-call review outcome")
            else:
                if outcome.status == "UNKNOWN":
                    requests = await connection.fetch(
                        "SELECT * FROM sim_adaptive_campaign_receipts "
                        "WHERE campaign_id=$1 AND sample_id=$2 AND arm_id=$3 AND kind='cost'",
                        outcome.campaign_id,
                        outcome.sample_id,
                        outcome.arm_id,
                    )
                    costs = [_decode_row(x, ResearchCostReceiptV1) for x in requests]
                    stages = {x.stage for x in costs}
                    if "RESERVATION_REQUESTED" not in stages or "RESERVATION_DENIED" in stages:
                        raise MessageIdentityConflict(
                            "no-call uncertainty lacks a durable budget-operation fence"
                        )
                elif outcome.status not in ("SOURCE_MISSING", "EVALUATOR_FAILED", "MISSED", "BUDGET_BLOCKED"):
                    raise MessageIdentityConflict(
                        "outcome invents a decision without an independent completion"
                    )
                if outcome.status == "SOURCE_MISSING" and bundle is not None:
                    raise MessageIdentityConflict("source-missing outcome suppresses a valid frozen bundle")
                if outcome.status == "MISSED" and now < window.paired_at_ts_ms:
                    raise MessageIdentityConflict("window cannot be missed before its frozen pairing cutoff")
                if outcome.status == "BUDGET_BLOCKED" and (
                    evaluation is None or now >= window.paired_at_ts_ms
                ):
                    raise MessageIdentityConflict(
                        "no-call budget denial is not a missed or unevaluated window"
                    )
            if outcome.decision_receipt_sha256 != (decision.receipt_sha256 if decision else None):
                raise MessageIdentityConflict("outcome must retain its actual independent completion link")
            for digest in (
                outcome.bundle_receipt_sha256,
                outcome.evaluation_receipt_sha256,
                outcome.decision_receipt_sha256,
                outcome.causal_sample_receipt_sha256,
            ):
                if digest is not None:
                    row = await connection.fetchrow(
                        "SELECT * FROM sim_adaptive_campaign_receipts WHERE receipt_sha256=$1", digest
                    )
                    if (
                        row is None
                        or row["campaign_id"] != outcome.campaign_id
                        or row["sample_id"] != outcome.sample_id
                    ):
                        raise MessageIdentityConflict(
                            "outcome link is not independently stored for the same sample"
                        )
            return await self._store(connection, outcome, "outcome", outcome.arm_id, "one")

    async def pending_claims(self, campaign_id: str, *, limit: int = 64) -> tuple[ResearchWindowClaimV1, ...]:
        if type(limit) is not int or not 1 <= limit <= 64:
            raise ValueError("pending claim page must be bounded from 1 through 64")
        async with self._database.pool.acquire() as connection:
            rows = await connection.fetch(
                """SELECT c.* FROM sim_adaptive_window_claims c WHERE c.campaign_id=$1
                AND (SELECT count(*) FROM sim_adaptive_campaign_receipts r WHERE r.campaign_id=c.campaign_id
                AND r.sample_id=c.sample_id AND r.kind='outcome')<3 ORDER BY c.sample_id LIMIT $2""",
                campaign_id,
                limit,
            )
            return tuple(_decode_row(row, ResearchWindowClaimV1) for row in rows)

    async def outcomes(self, campaign_id: str, sample_id: str) -> tuple[ResearchArmOutcomeV1, ...]:
        async with self._database.pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT * FROM sim_adaptive_campaign_receipts "
                "WHERE campaign_id=$1 AND sample_id=$2 AND kind='outcome' ORDER BY arm_id",
                campaign_id,
                sample_id,
            )
            return tuple(_decode_row(x, ResearchArmOutcomeV1) for x in rows)

    async def window_state(self, campaign_id: str, sample_id: str) -> dict[tuple[str, str, str], _Receipt]:
        """Bounded independent restart resolver; it never claims or dispatches work."""
        models: dict[str, type[_Receipt]] = {
            "source": ResearchSourceReceiptV1,
            "bundle": ResearchCausalBundleV1,
            "evaluation": ResearchStrategyEvaluationReceiptV1,
            "start": ResearchLLMAttemptStartV1,
            "terminal": ResearchLLMAttemptTerminalV1,
            "review": ResearchReviewReceiptV1,
            "cost": ResearchCostReceiptV1,
            "outcome": ResearchArmOutcomeV1,
            "sample": _CampaignSampleV1,
        }
        async with self._database.pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT * FROM sim_adaptive_campaign_receipts "
                "WHERE campaign_id=$1 AND sample_id=$2 ORDER BY kind,arm_id,slot_key LIMIT 65",
                campaign_id,
                sample_id,
            )
            if len(rows) > 64:
                raise MessageIdentityConflict(
                    "campaign window exceeds its bounded independent receipt roster"
                )
            return {
                (row["kind"], row["arm_id"], row["slot_key"]): _decode_row(row, models[row["kind"]])
                for row in rows
            }

    async def seal_denominator(self, campaign_id: str) -> ResearchDenominatorReceiptV1:
        """All scheduled outcomes, including missing and unknown; never a scientific PASS."""
        async with self._database.transaction() as connection:
            await self._lock(connection, campaign_id)
            plan, schedule, _ = await self._context(connection, campaign_id)
            row = await connection.fetchrow(
                "SELECT * FROM sim_adaptive_campaign_denominators WHERE campaign_id=$1", campaign_id
            )
            rows = await connection.fetch(
                "SELECT * FROM sim_adaptive_campaign_receipts "
                "WHERE campaign_id=$1 AND kind='outcome' ORDER BY sample_id,arm_id",
                campaign_id,
            )
            outcomes = tuple(_decode_row(x, ResearchArmOutcomeV1) for x in rows)
            expected = {(w.sample_id, arm) for w in schedule.windows for arm in RESEARCH_ARMS}
            if {(x.sample_id, x.arm_id) for x in outcomes} != expected or len(outcomes) != len(expected):
                raise MessageIdentityConflict(
                    "denominator requires every preregistered window and all three arms"
                )
            if row is not None:
                saved = _decode_row(row, ResearchDenominatorReceiptV1)
                if (
                    saved.plan_receipt_sha256,
                    saved.recording_mode,
                    saved.expected_outcomes,
                    saved.outcome_ids_sha256,
                    saved.status_counts,
                    saved.unknown_attempt_count,
                ) != (
                    plan.receipt_sha256,
                    plan.recording_mode,
                    len(expected),
                    canonical_sha256({"outcome_receipt_sha256s": sorted(x.receipt_sha256 for x in outcomes)}),
                    dict(Counter(x.status for x in outcomes)),
                    sum(x.status == "UNKNOWN" for x in outcomes),
                ):
                    raise MessageIdentityConflict(
                        "stored denominator differs from immutable scheduled outcomes"
                    )
                # Costs are deliberately a point-in-time snapshot: late actual
                # cost/completion facts do not rewrite the existing seal.
                return saved
            costs = tuple(
                _decode_row(x, ResearchCostReceiptV1)
                for x in await connection.fetch(
                    "SELECT * FROM sim_adaptive_campaign_receipts "
                    "WHERE campaign_id=$1 AND kind='cost' ORDER BY sample_id,arm_id,slot_key",
                    campaign_id,
                )
            )
            reserved = {x.attempt_id: x.amount_microusd for x in costs if x.stage == "RESERVED"}
            committed = {x.attempt_id: x.amount_microusd for x in costs if x.stage == "COMMITTED"}
            requested = {x.attempt_id: x.amount_microusd for x in costs if x.stage == "RESERVATION_REQUESTED"}
            denied = {x.attempt_id for x in costs if x.stage == "RESERVATION_DENIED"}
            commit_requested = {x.attempt_id for x in costs if x.stage == "COMMIT_REQUESTED"}
            uncertain_reserves = {
                key: value for key, value in requested.items() if key not in reserved and key not in denied
            }
            receipt = ResearchDenominatorReceiptV1(
                campaign_id=campaign_id,
                plan_receipt_sha256=plan.receipt_sha256,
                recording_mode=plan.recording_mode,
                expected_outcomes=len(expected),
                outcome_ids_sha256=canonical_sha256(
                    {"outcome_receipt_sha256s": sorted(x.receipt_sha256 for x in outcomes)}
                ),
                status_counts=dict(Counter(x.status for x in outcomes)),
                committed_cost_microusd=sum(committed.values()),
                outstanding_reservation_microusd=sum(
                    value for key, value in reserved.items() if key not in committed
                )
                + sum(uncertain_reserves.values()),
                unknown_attempt_count=sum(x.status == "UNKNOWN" for x in outcomes),
                unknown_budget_operation_count=len(uncertain_reserves)
                + len(commit_requested - committed.keys()),
            )
            encoded, digest = canonical_payload(receipt.model_dump(mode="json"))
            await connection.execute(
                "INSERT INTO sim_adaptive_campaign_denominators"
                "(campaign_id,receipt_sha256,payload_json,payload_sha256) "
                "VALUES($1,$2,$3,$4)",
                campaign_id,
                receipt.receipt_sha256,
                encoded,
                digest,
            )
            return receipt

    async def _identity(self, connection: Any, campaign_id: str):
        row = await connection.fetchrow(
            "SELECT * FROM sim_research_observation_schedules WHERE campaign_id=$1 FOR SHARE", campaign_id
        )
        protocol_row = await connection.fetchrow(
            "SELECT * FROM sim_research_adaptive_candidate_protocols WHERE campaign_id=$1 FOR SHARE",
            campaign_id,
        )
        if row is None or protocol_row is None:
            raise MessageIdentityConflict("campaign requires independent frozen schedule and protocol")
        schedule = _schedule_from_row(row)
        encoded, digest = canonical_payload(schedule.to_payload())
        await ResearchObservationScheduleRepository._verify_schedule(
            connection, row, schedule, encoded, digest
        )
        protocol = ResearchAdaptiveCandidateProtocolRepository._verify_row(protocol_row)
        if protocol.schedule_digest != schedule.schedule_digest:
            raise MessageIdentityConflict("campaign schedule/protocol identities disagree")
        return schedule, protocol

    async def _context(self, connection: Any, campaign_id: str):
        row = await connection.fetchrow(
            "SELECT * FROM sim_adaptive_campaign_plans WHERE campaign_id=$1", campaign_id
        )
        if row is None:
            raise MessageIdentityConflict("campaign has no independently committed plan")
        plan = _decode_row(row, ResearchCampaignPlanV1)
        schedule, protocol = await self._identity(connection, campaign_id)
        if (plan.schedule_digest, plan.candidate_protocol_digest) != (
            schedule.schedule_digest,
            protocol.protocol_digest,
        ):
            raise MessageIdentityConflict("stored campaign plan differs from frozen source identity")
        return plan, schedule, protocol

    async def _response_clock(self, connection, start: ResearchLLMAttemptStartV1, observed_at: int) -> None:
        plan, schedule, _ = await self._context(connection, start.campaign_id)
        window = self._window(schedule, start.sample_id)
        now = int(await connection.fetchval(_CLOCK))
        if (
            now < window.market_as_of_ts_ms
            or observed_at < start.attempt_started_at_ts_ms
            or abs(now - observed_at) > plan.maximum_clock_skew_ms
        ):
            raise MessageIdentityConflict("completion clock differs from actual campaign recorder time")

    @staticmethod
    def _recorded_at(row: Any) -> int:
        value = row["recorded_at_ts_ms"]
        if type(value) is not int or not 0 <= value <= 253_402_300_799_999:
            raise MessageIdentityConflict("completion lacks a bounded actual database recording clock")
        return value

    async def _claimed(self, connection: Any, claim: ResearchWindowClaimV1):
        _verify_model(claim, ResearchWindowClaimV1)
        await self._lock(connection, claim.campaign_id)
        context = await self._context(connection, claim.campaign_id)
        if await self._load_claim(connection, claim.campaign_id, claim.sample_id) != claim:
            raise MessageIdentityConflict("window claim does not match its independently stored facts")
        return context

    @staticmethod
    async def _lock(connection: Any, campaign_id: str) -> None:
        await connection.execute("SELECT pg_advisory_xact_lock($1,hashtext($2))", _LOCK, campaign_id)

    @staticmethod
    def _window(schedule: ResearchObservationScheduleV1, sample_id: str):
        window = next((x for x in schedule.windows if x.sample_id == sample_id), None)
        if window is None:
            raise MessageIdentityConflict("sample is absent from preregistered schedule")
        return window

    @staticmethod
    async def _row(connection: Any, campaign_id: str, sample_id: str, arm: str, kind: str, slot: str):
        return await connection.fetchrow(
            "SELECT *,floor(extract(epoch FROM recorded_at)*1000)::BIGINT AS recorded_at_ts_ms "
            "FROM sim_adaptive_campaign_receipts "
            "WHERE campaign_id=$1 AND sample_id=$2 AND arm_id=$3 AND kind=$4 AND slot_key=$5",
            campaign_id,
            sample_id,
            arm,
            kind,
            slot,
        )

    @staticmethod
    async def _load_claim(connection: Any, campaign_id: str, sample_id: str) -> ResearchWindowClaimV1:
        row = await connection.fetchrow(
            "SELECT * FROM sim_adaptive_window_claims WHERE campaign_id=$1 AND sample_id=$2",
            campaign_id,
            sample_id,
        )
        if row is None:
            raise MessageIdentityConflict("window has no durable dispatch claim")
        return _decode_row(row, ResearchWindowClaimV1)

    @classmethod
    async def _bundle(cls, connection: Any, claim: ResearchWindowClaimV1) -> ResearchCausalBundleV1:
        row = await cls._row(connection, claim.campaign_id, claim.sample_id, "all", "bundle", "one")
        if row is None:
            raise MessageIdentityConflict("window has no independently frozen causal bundle")
        return _decode_row(row, ResearchCausalBundleV1)

    @staticmethod
    def _same_bundle(saved: ResearchCausalBundleV1, given: ResearchCausalBundleV1) -> None:
        _verify_model(given, ResearchCausalBundleV1)
        if saved != given:
            raise MessageIdentityConflict("matched input differs from independently frozen bundle")

    @staticmethod
    async def _load_receipt(connection: Any, digest: str, model: type[_Receipt]):
        row = await connection.fetchrow(
            "SELECT * FROM sim_adaptive_campaign_receipts WHERE receipt_sha256=$1", digest
        )
        if row is None:
            raise MessageIdentityConflict("independently stored campaign receipt is missing")
        return _decode_row(row, model)

    @classmethod
    async def _store(
        cls, connection: Any, receipt: _Receipt, kind: str, arm: str, slot: str, **scope
    ) -> bool:
        _verify_model(receipt, type(receipt))
        campaign_id = scope.get("campaign_id", getattr(receipt, "campaign_id", None))
        sample_id = scope.get("sample_id", getattr(receipt, "sample_id", None))
        if not isinstance(campaign_id, str) or not isinstance(sample_id, str):
            raise MessageIdentityConflict("campaign receipt requires an explicit sample identity")
        encoded, digest = canonical_payload(receipt.model_dump(mode="json"))
        existing = await cls._row(connection, campaign_id, sample_id, arm, kind, slot)
        if existing is not None:
            if _decode_row(existing, type(receipt)) != receipt:
                raise MessageIdentityConflict(
                    "campaign receipt slot already contains different immutable facts"
                )
            return False
        await connection.execute(
            "INSERT INTO sim_adaptive_campaign_receipts"
            "(receipt_sha256,campaign_id,sample_id,arm_id,kind,slot_key,payload_json,payload_sha256) "
            "VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
            receipt.receipt_sha256,
            campaign_id,
            sample_id,
            arm,
            kind,
            slot,
            encoded,
            digest,
        )
        return True


class _CampaignSampleV1(_Receipt):
    contract_version: Literal["adaptive-causal-sample-link.v1"] = "adaptive-causal-sample-link.v1"
    campaign_id: _ID
    sample_id: _ID
    sample: ResearchDecisionSampleV1
