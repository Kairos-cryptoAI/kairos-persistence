"""Independent, append-only evidence and attempt history for opt-in SIM research.

No provider is invoked here. A persisted START is an admission fence, not proof
that a remote provider received a request. An unresolved START must never be
retried automatically or represented as an uncalled model observation.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from typing import Annotated, Any, Literal, Self, cast

from kairos_core import (
    LLMCallFailureV1,
    LLMProposalCompletionReceiptV1,
    LLMTradeProposalV1,
    ResearchCoverageSealV1,
    ResearchDecisionSampleV1,
    StrategyIntentV1,
    canonical_sha256,
)
from kairos_core.contracts.base import StrictValueModel, canonical_json_bytes
from kairos_core.research_pairing import (
    ScheduledResearchSampleV1,
    StrategyEvaluationEvidenceV1,
    build_research_decision_sample,
)
from pydantic import Field, StrictInt, StrictStr, model_validator

from .adaptive_candidate_protocols import ResearchAdaptiveCandidateProtocolRepository
from .database import Database, MigrationProfile
from .repository import MessageIdentityConflict
from .research_decision_samples import ResearchDecisionSampleRepository
from .research_observation_schedule import ResearchObservationScheduleRepository, _schedule_from_row
from .runtime import canonical_payload

_ID = Annotated[StrictStr, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")]
_SHA = Annotated[StrictStr, Field(pattern=r"^[0-9a-f]{64}$")]
_MS = Annotated[StrictInt, Field(ge=0, le=253_402_300_799_999)]
_SYMBOL = Annotated[StrictStr, Field(min_length=2, max_length=32, pattern=r"^[A-Z0-9][A-Z0-9._-]*$")]
_MAX_RECEIPT_BYTES = 262_144
_MAX_SOURCE_NODES = 10_000
_MAX_SOURCE_DEPTH = 32
_LOCK = 849621
_TABLE_KEYS = {
    "sim_research_evidence_campaigns": "campaign_id",
    "sim_research_source_receipts": "receipt_sha256",
    "sim_research_evaluation_receipts": "receipt_sha256",
    "sim_research_llm_attempt_starts": "attempt_id",
    "sim_research_llm_attempt_terminals": "attempt_id",
    "sim_research_source_qualified_seals": "campaign_id",
}
_SELECT_ROWS = {
    "sim_research_evidence_campaigns": "SELECT * FROM sim_research_evidence_campaigns WHERE campaign_id=$1",
    "sim_research_source_receipts": "SELECT * FROM sim_research_source_receipts WHERE receipt_sha256=$1",
    "sim_research_evaluation_receipts": (
        "SELECT * FROM sim_research_evaluation_receipts WHERE receipt_sha256=$1"
    ),
    "sim_research_llm_attempt_starts": "SELECT * FROM sim_research_llm_attempt_starts WHERE attempt_id=$1",
    "sim_research_llm_attempt_terminals": (
        "SELECT * FROM sim_research_llm_attempt_terminals WHERE attempt_id=$1"
    ),
    "sim_research_source_qualified_seals": (
        "SELECT * FROM sim_research_source_qualified_seals WHERE campaign_id=$1"
    ),
}
_INSERTS = {
    "sim_research_evidence_campaigns": (
        (
            "campaign_id",
            "schedule_digest",
            "candidate_protocol_digest",
            "authority",
            "payload_json",
            "payload_sha256",
        ),
        """INSERT INTO sim_research_evidence_campaigns
        (campaign_id,schedule_digest,candidate_protocol_digest,authority,payload_json,payload_sha256)
        VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING RETURNING campaign_id""",
    ),
    "sim_research_source_receipts": (
        (
            "receipt_sha256",
            "campaign_id",
            "sample_id",
            "source_kind",
            "source_name",
            "reference",
            "content_sha256",
            "payload_json",
            "payload_sha256",
        ),
        """INSERT INTO sim_research_source_receipts
        (receipt_sha256,campaign_id,sample_id,source_kind,source_name,reference,content_sha256,payload_json,payload_sha256)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING RETURNING receipt_sha256""",
    ),
    "sim_research_evaluation_receipts": (
        ("receipt_sha256", "campaign_id", "sample_id", "payload_json", "payload_sha256"),
        """INSERT INTO sim_research_evaluation_receipts
        (receipt_sha256,campaign_id,sample_id,payload_json,payload_sha256)
        VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING RETURNING receipt_sha256""",
    ),
    "sim_research_llm_attempt_starts": (
        (
            "attempt_id",
            "campaign_id",
            "arm_id",
            "sample_id",
            "receipt_sha256",
            "payload_json",
            "payload_sha256",
        ),
        """INSERT INTO sim_research_llm_attempt_starts
        (attempt_id,campaign_id,arm_id,sample_id,receipt_sha256,payload_json,payload_sha256)
        VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING RETURNING attempt_id""",
    ),
    "sim_research_llm_attempt_terminals": (
        (
            "attempt_id",
            "receipt_sha256",
            "start_receipt_sha256",
            "terminal_status",
            "observed_at_ts_ms",
            "payload_json",
            "payload_sha256",
        ),
        """INSERT INTO sim_research_llm_attempt_terminals
        (attempt_id,receipt_sha256,start_receipt_sha256,terminal_status,observed_at_ts_ms,payload_json,payload_sha256)
        VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING RETURNING attempt_id""",
    ),
    "sim_research_source_qualified_seals": (
        ("campaign_id", "receipt_sha256", "coverage_digest", "payload_json", "payload_sha256"),
        """INSERT INTO sim_research_source_qualified_seals
        (campaign_id,receipt_sha256,coverage_digest,payload_json,payload_sha256)
        VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING RETURNING campaign_id""",
    ),
}


class _Receipt(StrictValueModel):
    authority: Literal["SIM_RESEARCH_ONLY"] = "SIM_RESEARCH_ONLY"
    receipt_sha256: _SHA | None = None

    def identity_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_sha256"})

    @model_validator(mode="after")
    def canonical_identity(self) -> Self:
        payload = self.identity_payload()
        if len(canonical_json_bytes(payload)) > _MAX_RECEIPT_BYTES:
            raise ValueError("research receipt exceeds the bounded evidence envelope")
        expected = canonical_sha256(payload)
        if self.receipt_sha256 is not None and self.receipt_sha256 != expected:
            raise ValueError("research receipt hash differs from its independent content")
        object.__setattr__(self, "receipt_sha256", expected)
        return self


class ResearchSourceReceiptV1(_Receipt):
    """Saved source bytes plus trusted observation time, not a model assertion.

    Payloads are caller-sanitized JSON, never remote paths or fetch instructions.
    Late/old source observations may be retained, but causal resolution rejects
    sources unavailable at the sample clock. Freshness is a preregistered input
    transform policy; this storage layer invents no global news TTL.
    """

    contract_version: Literal["research-source-receipt.v1"] = "research-source-receipt.v1"
    campaign_id: _ID
    sample_id: _ID
    schedule_digest: _SHA
    candidate_protocol_digest: _SHA
    source_kind: Literal["MARKET_SNAPSHOT", "NEWS", "MACRO"]
    source_name: _ID
    reference: _ID
    source_as_of_ts_ms: _MS
    observed_at_ts_ms: _MS
    content_sha256: _SHA | None = None
    content: dict[str, Any]

    @model_validator(mode="before")
    @classmethod
    def fill_content_identity(cls, value):
        if isinstance(value, dict) and isinstance(value.get("content"), dict):
            value = dict(value)
            _bounded_json(value["content"])
            expected = canonical_sha256(value["content"])
            if value.get("content_sha256") is not None and value["content_sha256"] != expected:
                raise ValueError("source content hash does not match stored source bytes")
            value["content_sha256"] = expected
        return value

    @model_validator(mode="after")
    def content_identity(self) -> Self:
        if self.observed_at_ts_ms < self.source_as_of_ts_ms:
            raise ValueError("source cannot be observed before its asserted source time")
        expected = canonical_sha256(self.content)
        if self.content_sha256 is not None and self.content_sha256 != expected:
            raise ValueError("source content hash does not match stored source bytes")
        return self


class ResearchStrategyEvaluationReceiptV1(_Receipt):
    """One independently saved deterministic baseline evaluation, including zero intents."""

    contract_version: Literal["research-strategy-evaluation.v1"] = "research-strategy-evaluation.v1"
    campaign_id: _ID
    sample_id: _ID
    schedule_digest: _SHA
    candidate_protocol_digest: _SHA
    strategy_id: _ID
    strategy_revision: _ID
    symbol: _SYMBOL
    timeframe: _ID
    evidence_as_of_ts_ms: _MS
    evaluated_at_ts_ms: _MS
    market_snapshot_sha256: _SHA
    evaluator_sha256: _SHA
    source_receipt_sha256s: tuple[_SHA, ...] = Field(min_length=1, max_length=128)
    intent: StrategyIntentV1 | None = None

    @model_validator(mode="after")
    def evaluation_identity(self) -> Self:
        if self.evaluated_at_ts_ms < self.evidence_as_of_ts_ms:
            raise ValueError("evaluation cannot precede its market evidence")
        if self.source_receipt_sha256s != tuple(sorted(set(self.source_receipt_sha256s))):
            raise ValueError("evaluation source hashes must be distinct and sorted")
        if self.intent is not None:
            if self.intent.intent_id != canonical_sha256(self.intent.identity_payload()):
                raise ValueError("evaluation contains a noncanonical strategy intent")
            for field in ("strategy_id", "strategy_revision", "symbol", "timeframe"):
                if getattr(self.intent, field) != getattr(self, field):
                    raise ValueError("evaluation strategy intent differs from its evaluation scope")
            if self.intent.decision_ts_ms != self.evidence_as_of_ts_ms:
                raise ValueError("evaluation intent uses stale or future evidence")
            if self.intent.provenance.input_bar_sha256s[-1] != self.market_snapshot_sha256:
                raise ValueError("evaluation intent does not use its independently saved snapshot")
        return self

    def as_evidence(self, arm_id: str) -> StrategyEvaluationEvidenceV1:
        """Build the existing core attestation after the repository resolves this receipt."""

        if self.receipt_sha256 is None:
            raise MessageIdentityConflict("evaluation lacks its canonical independent identity")
        return StrategyEvaluationEvidenceV1(
            campaign_id=self.campaign_id,
            arm_id=arm_id,
            sample_id=self.sample_id,
            strategy_id=self.strategy_id,
            strategy_revision=self.strategy_revision,
            symbol=self.symbol,
            timeframe=self.timeframe,
            evidence_as_of_ts_ms=self.evidence_as_of_ts_ms,
            market_snapshot_sha256=self.market_snapshot_sha256,
            evaluation_sha256=self.receipt_sha256,
            intent_id=self.intent.intent_id if self.intent else None,
        )


class ResearchLLMAttemptStartV1(_Receipt):
    contract_version: Literal["research-llm-attempt-start.v1"] = "research-llm-attempt-start.v1"
    attempt_id: _ID
    campaign_id: _ID
    arm_id: Literal["strategy-review", "llm-proposal-research"]
    sample_id: _ID
    schedule_digest: _SHA
    candidate_protocol_digest: _SHA
    arm_protocol_digest: _SHA
    symbol: _SYMBOL
    timeframe: _ID
    market_as_of_ts_ms: _MS
    market_snapshot_sha256: _SHA
    sample_deadline_ts_ms: _MS
    provider: Literal["openai", "deepseek"]
    requested_model: _ID
    prompt_sha256: _SHA
    budget_reservation_id: _ID
    attempt_started_at_ts_ms: _MS

    @model_validator(mode="after")
    def start_identity(self) -> Self:
        if self.attempt_id != self.budget_reservation_id:
            raise ValueError("research attempt must use its exact deterministic budget reservation ID")
        if not self.market_as_of_ts_ms <= self.attempt_started_at_ts_ms < self.sample_deadline_ts_ms:
            raise ValueError("research attempt must be admitted within the frozen decision window")
        return self


class ResearchLLMAttemptTerminalV1(_Receipt):
    contract_version: Literal["research-llm-attempt-terminal.v1"] = "research-llm-attempt-terminal.v1"
    attempt_id: _ID
    start_receipt_sha256: _SHA
    terminal_status: Literal["COMPLETED", "FAILED", "UNRESOLVED"]
    observed_at_ts_ms: _MS
    proposal: LLMTradeProposalV1 | None = None
    completion: LLMProposalCompletionReceiptV1 | None = None
    failure: LLMCallFailureV1 | None = None

    @model_validator(mode="after")
    def terminal_identity(self) -> Self:
        if self.terminal_status == "COMPLETED":
            if self.proposal is None or self.completion is None or self.failure is not None:
                raise ValueError("completed research attempt requires its exact proposal and completion")
            if self.completion.proposal_id != self.proposal.proposal_id:
                raise ValueError("attempt completion differs from its independently stored proposal")
            if self.completion.model_provenance != self.proposal.model_provenance:
                raise ValueError("attempt completion provenance differs from its proposal")
            for field in (
                "campaign_id",
                "arm_id",
                "sample_id",
                "symbol",
                "timeframe",
                "market_as_of_ts_ms",
                "market_snapshot_sha256",
            ):
                if getattr(self.completion, field) != getattr(self.proposal, field):
                    raise ValueError("attempt completion scope differs from its proposal")
            if self.observed_at_ts_ms != self.completion.response_observed_at_ts_ms:
                raise ValueError("terminal observation differs from gateway completion time")
        elif self.terminal_status == "FAILED":
            if self.failure is None or self.proposal is not None or self.completion is not None:
                raise ValueError("failed research attempt requires only a caller-observed failure receipt")
            if self.observed_at_ts_ms != self.failure.failure_observed_at_ts_ms:
                raise ValueError("terminal observation differs from gateway failure time")
        elif any(value is not None for value in (self.proposal, self.completion, self.failure)):
            raise ValueError("unresolved attempt cannot fabricate a failure or model decision")
        for item in (self.completion, self.failure):
            if item is not None and item.attempt_id != self.attempt_id:
                raise ValueError("terminal receipt refers to a different admitted attempt")
        return self


class ResearchSourceQualifiedCoverageV1(_Receipt):
    """Engineering proof of independent receipts, never an economic sealed pass."""

    contract_version: Literal["research-source-qualified-coverage.v1"] = (
        "research-source-qualified-coverage.v1"
    )
    qualification: Literal["INDEPENDENT_SOURCE_REPLAY_ONLY"] = "INDEPENDENT_SOURCE_REPLAY_ONLY"
    economic_qualification: Literal[False] = False
    paper_qualification: Literal[False] = False
    live_orders_allowed: Literal[False] = False
    campaign_id: _ID
    coverage: ResearchCoverageSealV1
    source_ids_sha256: _SHA
    evaluation_ids_sha256: _SHA
    attempt_start_ids_sha256: _SHA
    attempt_terminal_ids_sha256: _SHA


class ResearchEvidenceRepository:
    """SIM-only source resolver, durable no-retry fence and verified coverage path."""

    def __init__(self, database: Database) -> None:
        if not isinstance(database, Database):
            raise TypeError("research evidence requires an explicit Database")
        if database.migration_profile is not MigrationProfile.SIMULATOR or database.read_only:
            raise ValueError("research evidence requires an isolated writable SIMULATOR database")
        self._database = database

    async def enroll_campaign(self, campaign_id: str) -> bool:
        """Opt into independent evidence before any observation; no historical adoption."""

        async with self._database.transaction() as connection:
            schedule, protocol, _ = await self._context(connection, campaign_id, None, enrolled=False)
            return await self._insert(
                connection,
                "sim_research_evidence_campaigns",
                "campaign_id",
                campaign_id,
                {
                    "campaign_id": campaign_id,
                    "schedule_digest": schedule.schedule_digest,
                    "candidate_protocol_digest": protocol.protocol_digest,
                    "authority": "SIM_RESEARCH_ONLY",
                },
                schedule_digest=schedule.schedule_digest,
                candidate_protocol_digest=protocol.protocol_digest,
                authority="SIM_RESEARCH_ONLY",
            )

    async def record_source(self, receipt: ResearchSourceReceiptV1) -> bool:
        _verify_model(receipt, ResearchSourceReceiptV1)
        async with self._database.transaction() as connection:
            schedule, protocol, window = await self._context(
                connection, receipt.campaign_id, receipt.sample_id
            )
            _bind(receipt, schedule, protocol)
            if receipt.source_kind == "MARKET_SNAPSHOT" and (
                receipt.source_as_of_ts_ms != window.market_as_of_ts_ms
                or (
                    window.market_snapshot_sha256 is not None
                    and receipt.content_sha256 != window.market_snapshot_sha256
                )
            ):
                raise MessageIdentityConflict(
                    "saved market source differs from the frozen observation window"
                )
            return await self._insert(
                connection,
                "sim_research_source_receipts",
                "receipt_sha256",
                receipt.receipt_sha256,
                receipt.model_dump(mode="json"),
                campaign_id=receipt.campaign_id,
                sample_id=receipt.sample_id,
                source_kind=receipt.source_kind,
                source_name=receipt.source_name,
                reference=receipt.reference,
                content_sha256=receipt.content_sha256,
            )

    async def record_evaluation(self, receipt: ResearchStrategyEvaluationReceiptV1) -> bool:
        _verify_model(receipt, ResearchStrategyEvaluationReceiptV1)
        async with self._database.transaction() as connection:
            await self._resolve_evaluation(connection, receipt)
            return await self._insert(
                connection,
                "sim_research_evaluation_receipts",
                "receipt_sha256",
                receipt.receipt_sha256,
                receipt.model_dump(mode="json"),
                campaign_id=receipt.campaign_id,
                sample_id=receipt.sample_id,
            )

    async def start_attempt(self, attempt: ResearchLLMAttemptStartV1) -> bool:
        """Commit START before dispatch. False is a replay fence, never permission to retry."""

        _verify_model(attempt, ResearchLLMAttemptStartV1)
        async with self._database.transaction() as connection:
            schedule, protocol, window = await self._context(
                connection, attempt.campaign_id, attempt.sample_id
            )
            _bind(attempt, schedule, protocol)
            for field in ("symbol", "timeframe", "market_as_of_ts_ms", "sample_deadline_ts_ms"):
                if getattr(attempt, field) != getattr(window, field):
                    raise MessageIdentityConflict("research attempt differs from its frozen decision window")
            if (
                window.market_snapshot_sha256 is not None
                and attempt.market_snapshot_sha256 != window.market_snapshot_sha256
            ):
                raise MessageIdentityConflict("research attempt uses a different frozen snapshot")
            arm = next(item for item in protocol.arms if item.arm_id == attempt.arm_id)
            if (
                attempt.arm_protocol_digest != protocol.arm_digest(attempt.arm_id)
                or attempt.provider != arm.provider
                or attempt.requested_model != arm.model
                or attempt.prompt_sha256 != arm.prompt_sha256
            ):
                raise MessageIdentityConflict("research attempt route or prompt differs from its frozen arm")
            await self._market_source(
                connection,
                attempt.campaign_id,
                attempt.sample_id,
                attempt.market_snapshot_sha256,
                attempt.market_as_of_ts_ms,
            )
            return await self._insert(
                connection,
                "sim_research_llm_attempt_starts",
                "attempt_id",
                attempt.attempt_id,
                attempt.model_dump(mode="json"),
                campaign_id=attempt.campaign_id,
                arm_id=attempt.arm_id,
                sample_id=attempt.sample_id,
                receipt_sha256=attempt.receipt_sha256,
            )

    async def finish_attempt(self, terminal: ResearchLLMAttemptTerminalV1) -> bool:
        """Retain actual observation time, including late failures and ambiguous dispatch."""

        _verify_model(terminal, ResearchLLMAttemptTerminalV1)
        async with self._database.transaction() as connection:
            start, _ = await self._load_attempt(connection, terminal.attempt_id)
            await connection.execute(
                "SELECT pg_advisory_xact_lock($1, hashtext($2))", _LOCK, start.campaign_id
            )
            if (
                terminal.start_receipt_sha256 != start.receipt_sha256
                or terminal.observed_at_ts_ms < start.attempt_started_at_ts_ms
            ):
                raise MessageIdentityConflict("attempt terminal differs from its admitted start")
            for item in (terminal.completion, terminal.failure):
                if item is None:
                    continue
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
                    if getattr(item, field) != getattr(start, field):
                        raise MessageIdentityConflict("terminal facts differ from the admitted attempt")
            provenance = terminal.proposal.model_provenance if terminal.proposal else terminal.failure
            if provenance is not None and (
                provenance.provider != start.provider
                or provenance.requested_model != start.requested_model
                or provenance.prompt_sha256 != start.prompt_sha256
                or provenance.budget_reservation_id != start.budget_reservation_id
            ):
                raise MessageIdentityConflict(
                    "terminal route or reservation differs from the admitted attempt"
                )
            if terminal.proposal is not None:
                await self._resolve_proposal_evidence(connection, terminal.proposal)
            return await self._insert(
                connection,
                "sim_research_llm_attempt_terminals",
                "attempt_id",
                terminal.attempt_id,
                terminal.model_dump(mode="json"),
                receipt_sha256=terminal.receipt_sha256,
                start_receipt_sha256=terminal.start_receipt_sha256,
                terminal_status=terminal.terminal_status,
                observed_at_ts_ms=terminal.observed_at_ts_ms,
            )

    async def load_attempt(
        self, attempt_id: str
    ) -> tuple[ResearchLLMAttemptStartV1, ResearchLLMAttemptTerminalV1 | None]:
        async with self._database.pool.acquire() as connection:
            return await self._load_attempt(connection, attempt_id)

    async def find_attempt(
        self, attempt_id: str
    ) -> tuple[ResearchLLMAttemptStartV1, ResearchLLMAttemptTerminalV1 | None] | None:
        """Return None only for true absence; malformed or changed facts fail closed."""

        async with self._database.pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT * FROM sim_research_llm_attempt_starts WHERE attempt_id=$1", attempt_id
            )
            if row is None:
                return None
            return await self._load_attempt(connection, attempt_id)

    async def load_source(self, receipt_sha256: str) -> ResearchSourceReceiptV1:
        async with self._database.pool.acquire() as connection:
            return await self._load(
                connection,
                "sim_research_source_receipts",
                "receipt_sha256",
                receipt_sha256,
                ResearchSourceReceiptV1,
            )

    async def load_evaluation(self, receipt_sha256: str) -> ResearchStrategyEvaluationReceiptV1:
        async with self._database.transaction() as connection:
            receipt = await self._load(
                connection,
                "sim_research_evaluation_receipts",
                "receipt_sha256",
                receipt_sha256,
                ResearchStrategyEvaluationReceiptV1,
            )
            await self._resolve_evaluation(connection, receipt)
            return receipt

    async def record_verified_sample(self, sample: ResearchDecisionSampleV1) -> bool:
        """Reject fabricated source references and suppressed or unfinished LLM attempts."""

        _verify_core(sample)
        async with self._database.transaction() as connection:
            await self._verify_sample(connection, sample)
        # Immutable evidence and enrollment guards are checked again by the DB
        # result insert trigger; a concurrent coverage seal rejects the insert.
        return await ResearchDecisionSampleRepository(self._database).record(sample)

    async def seal_verified_coverage(self, *, campaign_id: str) -> ResearchSourceQualifiedCoverageV1:
        from .research_decision_samples import _sample_from_row

        async with self._database.transaction() as connection:
            await self._context(connection, campaign_id, None)
            rows = await connection.fetch(
                "SELECT * FROM sim_research_decision_samples WHERE campaign_id=$1", campaign_id
            )
            for row in rows:
                sample = _sample_from_row(row)
                encoded, digest = canonical_payload(sample.to_payload())
                if not ResearchDecisionSampleRepository._row_matches(row, sample, encoded, digest):
                    raise MessageIdentityConflict("stored result failed independent integrity verification")
                await self._verify_sample(connection, sample)
            existing = await connection.fetchrow(
                "SELECT payload FROM sim_research_coverage_seals WHERE campaign_id=$1", campaign_id
            )
        if existing is None:
            coverage = await ResearchObservationScheduleRepository(self._database).seal_coverage(
                campaign_id=campaign_id
            )
        else:
            payload = (
                json.loads(existing["payload"])
                if isinstance(existing["payload"], str)
                else existing["payload"]
            )
            coverage = ResearchCoverageSealV1.model_validate(payload)
            if coverage.coverage_digest != canonical_sha256(coverage.identity_payload()):
                raise MessageIdentityConflict("existing coverage seal changed after construction")
        async with self._database.transaction() as connection:
            schedule, protocol, _ = await self._context(connection, campaign_id, None)
            if (
                coverage.campaign_id != campaign_id
                or coverage.schedule_digest != schedule.schedule_digest
                or coverage.candidate_protocol_digest != protocol.protocol_digest
            ):
                raise MessageIdentityConflict("existing seal differs from independently enrolled campaign")
            hashes = {}
            for label, query in (
                (
                    "source_ids_sha256",
                    "SELECT receipt_sha256 FROM sim_research_source_receipts "
                    "WHERE campaign_id=$1 ORDER BY receipt_sha256",
                ),
                (
                    "evaluation_ids_sha256",
                    "SELECT receipt_sha256 FROM sim_research_evaluation_receipts "
                    "WHERE campaign_id=$1 ORDER BY receipt_sha256",
                ),
                (
                    "attempt_start_ids_sha256",
                    "SELECT receipt_sha256 FROM sim_research_llm_attempt_starts "
                    "WHERE campaign_id=$1 ORDER BY receipt_sha256",
                ),
            ):
                evidence_rows = await connection.fetch(query, campaign_id)
                hashes[label] = canonical_sha256(
                    {"receipt_ids": [row["receipt_sha256"] for row in evidence_rows]}
                )
            terminal_rows = await connection.fetch(
                """SELECT t.receipt_sha256 FROM sim_research_llm_attempt_terminals t
                   JOIN sim_research_llm_attempt_starts s ON s.attempt_id=t.attempt_id
                   WHERE s.campaign_id=$1 ORDER BY t.receipt_sha256""",
                campaign_id,
            )
            hashes["attempt_terminal_ids_sha256"] = canonical_sha256(
                {"receipt_ids": [row["receipt_sha256"] for row in terminal_rows]}
            )
            qualified = ResearchSourceQualifiedCoverageV1(
                campaign_id=campaign_id,
                coverage=coverage,
                source_ids_sha256=hashes["source_ids_sha256"],
                evaluation_ids_sha256=hashes["evaluation_ids_sha256"],
                attempt_start_ids_sha256=hashes["attempt_start_ids_sha256"],
                attempt_terminal_ids_sha256=hashes["attempt_terminal_ids_sha256"],
            )
            await self._insert(
                connection,
                "sim_research_source_qualified_seals",
                "campaign_id",
                campaign_id,
                qualified.model_dump(mode="json"),
                receipt_sha256=qualified.receipt_sha256,
                coverage_digest=coverage.coverage_digest,
            )
            return qualified

    async def pending_observations(self, *, campaign_id: str) -> tuple[dict[str, str], ...]:
        """Bounded frozen scheduler roster. It does not start calls or infer missing outcomes."""

        async with self._database.transaction() as connection:
            await self._context(connection, campaign_id, None)
            rows = await connection.fetch(
                """SELECT w.sample_id, a.arm_id,
                   CASE WHEN r.sample_record_id IS NOT NULL THEN 'RECORDED'
                        WHEN t.attempt_id IS NOT NULL THEN t.terminal_status
                        WHEN s.attempt_id IS NOT NULL THEN 'STARTED_UNRESOLVED'
                        ELSE 'NOT_STARTED' END AS state
                   FROM sim_research_observation_windows w
                   CROSS JOIN (VALUES ('strategy-only'),('strategy-review'),
                                      ('llm-proposal-research')) a(arm_id)
                   LEFT JOIN sim_research_decision_samples r ON r.campaign_id=w.campaign_id
                        AND r.sample_id=w.sample_id AND r.arm_id=a.arm_id
                   LEFT JOIN sim_research_llm_attempt_starts s ON s.campaign_id=w.campaign_id
                        AND s.sample_id=w.sample_id AND s.arm_id=a.arm_id
                   LEFT JOIN sim_research_llm_attempt_terminals t ON t.attempt_id=s.attempt_id
                   WHERE w.campaign_id=$1
                   ORDER BY w.market_as_of_ts_ms,w.symbol,w.timeframe,w.sample_id,a.arm_id""",
                campaign_id,
            )
            return tuple(dict(row) for row in rows)

    async def _verify_sample(self, connection: Any, sample: ResearchDecisionSampleV1) -> None:
        schedule, protocol, _ = await self._context(connection, sample.campaign_id, sample.sample_id)
        if sample.arm_protocol_digest != protocol.arm_digest(sample.arm_id):
            raise MessageIdentityConflict("research sample differs from the enrolled frozen arm")
        evaluation = None
        if sample.strategy_evaluation_sha256 is not None:
            evaluation = await self._load(
                connection,
                "sim_research_evaluation_receipts",
                "receipt_sha256",
                sample.strategy_evaluation_sha256,
                ResearchStrategyEvaluationReceiptV1,
            )
            await self._resolve_evaluation(connection, evaluation)
            if (
                evaluation.campaign_id != sample.campaign_id
                or evaluation.sample_id != sample.sample_id
                or evaluation.market_snapshot_sha256 != sample.market_snapshot_sha256
            ):
                raise MessageIdentityConflict(
                    "sample evaluation differs from its independently stored source"
                )
            expected_outcome = evaluation.intent.side.value if evaluation.intent else "NO_INTENT"
            if sample.strategy_outcome != expected_outcome or sample.strategy_intent_id != (
                evaluation.intent.intent_id if evaluation.intent else None
            ):
                raise MessageIdentityConflict(
                    "sample suppresses or fabricates its independently stored strategy evaluation"
                )
        elif sample.strategy_outcome != "NOT_EVALUATED":
            raise MessageIdentityConflict(
                "evaluated research sample lacks an independently stored evaluation"
            )
        elif await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM sim_research_evaluation_receipts "
            "WHERE campaign_id=$1 AND sample_id=$2)",
            sample.campaign_id,
            sample.sample_id,
        ):
            raise MessageIdentityConflict("sample suppresses an independently stored strategy evaluation")
        await self._market_source(
            connection,
            sample.campaign_id,
            sample.sample_id,
            sample.market_snapshot_sha256,
            sample.market_as_of_ts_ms,
        )
        row = await connection.fetchrow(
            "SELECT attempt_id FROM sim_research_llm_attempt_starts "
            "WHERE campaign_id=$1 AND arm_id=$2 AND sample_id=$3",
            sample.campaign_id,
            sample.arm_id,
            sample.sample_id,
        )
        terminal = None
        if row is None:
            if sample.llm_outcome != "NOT_CALLED":
                raise MessageIdentityConflict("LLM observation lacks an independently stored attempt")
        else:
            _, terminal = await self._load_attempt(connection, row["attempt_id"])
        if row is not None and (terminal is None or terminal.terminal_status == "UNRESOLVED"):
            raise MessageIdentityConflict(
                "unfinished or ambiguous attempt cannot become NOT_CALLED or a model decision"
            )
        if terminal is not None and terminal.observed_at_ts_ms > sample.paired_at_ts_ms:
            raise MessageIdentityConflict(
                "late attempt terminal cannot be backdated into a causal observation"
            )
        if terminal is not None and terminal.terminal_status == "FAILED":
            if (
                sample.llm_outcome != "CALL_FAILED"
                or sample.llm_failure_receipt_id != terminal.failure.failure_receipt_id
            ):
                raise MessageIdentityConflict("sample suppresses or changes its independently stored failure")
        elif terminal is not None and (
            sample.llm_proposal_id != terminal.proposal.proposal_id
            or sample.llm_completion_receipt_id != terminal.completion.completion_receipt_id
            or sample.llm_outcome != terminal.proposal.action.value
        ):
            raise MessageIdentityConflict(
                "sample differs from its independently stored completed model decision"
            )
        if terminal is not None and terminal.proposal is not None:
            await self._resolve_proposal_evidence(connection, terminal.proposal)
        planned = ScheduledResearchSampleV1(
            campaign_id=sample.campaign_id,
            arm_id=sample.arm_id,
            sample_id=sample.sample_id,
            symbol=sample.symbol,
            timeframe=sample.timeframe,
            market_as_of_ts_ms=sample.market_as_of_ts_ms,
            market_snapshot_sha256=sample.market_snapshot_sha256,
            strategy_id=sample.strategy_id,
            strategy_revision=sample.strategy_revision,
            paired_at_ts_ms=sample.paired_at_ts_ms,
            sample_deadline_ts_ms=sample.sample_deadline_ts_ms,
        )
        rebuilt = build_research_decision_sample(
            planned,
            strategy_evaluation=evaluation.as_evidence(sample.arm_id) if evaluation else None,
            strategy_intent=evaluation.intent if evaluation else None,
            llm_proposal=terminal.proposal if terminal else None,
            llm_completion=terminal.completion if terminal else None,
            llm_failure=terminal.failure if terminal else None,
            llm_was_called=terminal is not None,
        )
        expected = {**rebuilt.identity_payload(), "arm_protocol_digest": protocol.arm_digest(sample.arm_id)}
        if sample.identity_payload() != expected:
            raise MessageIdentityConflict(
                "sample facts do not exactly replay from independently stored source receipts"
            )

    async def _resolve_proposal_evidence(self, connection: Any, proposal: LLMTradeProposalV1) -> None:
        for reference in proposal.evidence:
            row = await connection.fetchrow(
                "SELECT * FROM sim_research_source_receipts WHERE campaign_id=$1 AND sample_id=$2 "
                "AND source_kind=$3 AND reference=$4 AND content_sha256=$5",
                proposal.campaign_id,
                proposal.sample_id,
                reference.kind,
                reference.reference,
                reference.content_sha256,
            )
            if row is None:
                raise MessageIdentityConflict("model proposal cites absent independent source evidence")
            source = _decode_row(row, ResearchSourceReceiptV1)
            if (
                source.observed_at_ts_ms != reference.observed_at_ms
                or source.observed_at_ts_ms > proposal.market_as_of_ts_ms
            ):
                raise MessageIdentityConflict("model proposal cites unavailable independent source evidence")

    async def _resolve_evaluation(
        self, connection: Any, receipt: ResearchStrategyEvaluationReceiptV1
    ) -> None:
        schedule, protocol, window = await self._context(connection, receipt.campaign_id, receipt.sample_id)
        _bind(receipt, schedule, protocol)
        if (
            receipt.strategy_id != schedule.strategy_id
            or receipt.strategy_revision != schedule.strategy_revision
            or receipt.evaluator_sha256 != schedule.evaluator_sha256
            or receipt.symbol != window.symbol
            or receipt.timeframe != window.timeframe
            or receipt.evidence_as_of_ts_ms != window.market_as_of_ts_ms
            or receipt.evaluated_at_ts_ms > window.paired_at_ts_ms
        ):
            raise MessageIdentityConflict(
                "independent evaluation differs from its frozen causal window or evaluator"
            )
        market_found = False
        for digest in receipt.source_receipt_sha256s:
            source = await self._load(
                connection, "sim_research_source_receipts", "receipt_sha256", digest, ResearchSourceReceiptV1
            )
            if (
                source.campaign_id != receipt.campaign_id
                or source.sample_id != receipt.sample_id
                or source.observed_at_ts_ms > receipt.evidence_as_of_ts_ms
            ):
                raise MessageIdentityConflict(
                    "evaluation source was absent or unavailable at the frozen market clock"
                )
            if source.source_kind == "MARKET_SNAPSHOT":
                market_found = source.content_sha256 == receipt.market_snapshot_sha256
        if not market_found:
            raise MessageIdentityConflict("evaluation lacks its independently saved exact market snapshot")

    async def _market_source(
        self, connection: Any, campaign_id: str, sample_id: str, digest: str, clock: int
    ):
        row = await connection.fetchrow(
            "SELECT * FROM sim_research_source_receipts "
            "WHERE campaign_id=$1 AND sample_id=$2 AND source_kind='MARKET_SNAPSHOT'",
            campaign_id,
            sample_id,
        )
        if row is None:
            raise MessageIdentityConflict("research observation lacks an independently saved market snapshot")
        source = _decode_row(row, ResearchSourceReceiptV1)
        if (
            source.content_sha256 != digest
            or source.source_as_of_ts_ms != clock
            or source.observed_at_ts_ms > clock
        ):
            raise MessageIdentityConflict(
                "research market source is changed or unavailable at the causal clock"
            )
        return source

    async def _context(
        self, connection: Any, campaign_id: str, sample_id: str | None, *, enrolled: bool = True
    ):
        await connection.execute("SELECT pg_advisory_xact_lock($1, hashtext($2))", _LOCK, campaign_id)
        row = await connection.fetchrow(
            "SELECT * FROM sim_research_observation_schedules WHERE campaign_id=$1 FOR SHARE", campaign_id
        )
        if row is None or not row["independent_evidence_registration_allowed"]:
            raise MessageIdentityConflict(
                "independent evidence requires a new eligible preregistered SIM schedule"
            )
        schedule = _schedule_from_row(row)
        encoded, digest = canonical_payload(schedule.to_payload())
        await ResearchObservationScheduleRepository._verify_schedule(
            connection, row, schedule, encoded, digest
        )
        protocol_row = await connection.fetchrow(
            "SELECT * FROM sim_research_adaptive_candidate_protocols WHERE campaign_id=$1 FOR SHARE",
            campaign_id,
        )
        if protocol_row is None or protocol_row["freeze_txid"] == await connection.fetchval(
            "SELECT txid_current()"
        ):
            raise MessageIdentityConflict(
                "independent evidence requires a previously committed frozen protocol"
            )
        protocol = ResearchAdaptiveCandidateProtocolRepository._verify_row(protocol_row)
        if protocol.schedule_digest != schedule.schedule_digest:
            raise MessageIdentityConflict("independent evidence protocol differs from its schedule")
        if enrolled:
            enrollment = await connection.fetchrow(
                "SELECT * FROM sim_research_evidence_campaigns WHERE campaign_id=$1", campaign_id
            )
            expected = {
                "campaign_id": campaign_id,
                "schedule_digest": schedule.schedule_digest,
                "candidate_protocol_digest": protocol.protocol_digest,
                "authority": "SIM_RESEARCH_ONLY",
            }
            if enrollment is None or canonical_payload(expected) != (
                enrollment["payload_json"],
                enrollment["payload_sha256"],
            ):
                raise MessageIdentityConflict(
                    "campaign is not independently evidence-enrolled or enrollment changed"
                )
        window = (
            next((item for item in schedule.windows if item.sample_id == sample_id), None)
            if sample_id
            else None
        )
        if sample_id and window is None:
            raise MessageIdentityConflict("evidence sample is absent from the frozen schedule")
        return schedule, protocol, window

    async def _load_attempt(self, connection: Any, attempt_id: str):
        start = await self._load(
            connection, "sim_research_llm_attempt_starts", "attempt_id", attempt_id, ResearchLLMAttemptStartV1
        )
        row = await connection.fetchrow(
            "SELECT * FROM sim_research_llm_attempt_terminals WHERE attempt_id=$1", attempt_id
        )
        terminal = _decode_row(row, ResearchLLMAttemptTerminalV1) if row is not None else None
        if terminal is not None and terminal.start_receipt_sha256 != start.receipt_sha256:
            raise MessageIdentityConflict("stored attempt terminal refers to changed admitted facts")
        return start, terminal

    @staticmethod
    async def _load(connection: Any, table: str, key: str, value: str | None, model: type[_Receipt]):
        if _TABLE_KEYS.get(table) != key:
            raise ValueError("research receipt lookup is outside the fixed table identity allow-list")
        row = await connection.fetchrow(_SELECT_ROWS[table], value)
        if row is None:
            raise MessageIdentityConflict("independently stored research receipt is missing")
        return _decode_row(row, model)

    @staticmethod
    async def _insert(
        connection: Any, table: str, key: str, value: str | None, payload: Mapping, **columns
    ) -> bool:
        if _TABLE_KEYS.get(table) != key:
            raise ValueError("research receipt insert is outside the fixed table identity allow-list")
        encoded, digest = canonical_payload(dict(payload))
        fields = {key: value, **columns, "payload_json": encoded, "payload_sha256": digest}
        names, query = _INSERTS[table]
        if set(fields) != set(names):
            raise ValueError("research receipt insert columns differ from its closed-world table schema")
        inserted = await connection.fetchval(query, *(fields[name] for name in names))
        if inserted is not None:
            return True
        row = await connection.fetchrow(_SELECT_ROWS[table], value)
        if row is None or any(row[name] != expected for name, expected in fields.items()):
            raise MessageIdentityConflict(
                "research evidence identity already contains different immutable facts"
            )
        return False


def _bounded_json(value: Any) -> None:
    """Reject recursive, huge or non-JSON trees before canonical serialization."""
    pending = [(value, 0)]
    visited = 0
    text_bytes = 0
    while pending:
        node, depth = pending.pop()
        visited += 1
        if visited > _MAX_SOURCE_NODES or depth > _MAX_SOURCE_DEPTH:
            raise ValueError("source JSON exceeds its bounded tree envelope")
        if isinstance(node, dict):
            if any(type(key) is not str for key in node):
                raise ValueError("source JSON object keys must be strings")
            pending.extend((item, depth + 1) for item in node.values())
            text_bytes += sum(len(key.encode("utf-8")) for key in node)
        elif isinstance(node, list):
            pending.extend((item, depth + 1) for item in node)
        elif type(node) is str:
            text_bytes += len(node.encode("utf-8"))
        elif type(node) is float:
            if not math.isfinite(node):
                raise ValueError("source JSON numbers must be finite")
        elif node is not None and type(node) not in (bool, int):
            raise ValueError("source content must contain only exact JSON values")
        if text_bytes > _MAX_RECEIPT_BYTES:
            raise ValueError("source JSON exceeds its bounded text envelope")


def _decode_row(row: Any, model: type[_Receipt]) -> Any:
    encoded = row["payload_json"]
    if not isinstance(encoded, str) or len(encoded.encode("utf-8")) > _MAX_RECEIPT_BYTES:
        raise MessageIdentityConflict("stored research receipt exceeds its bounded JSON envelope")
    try:
        receipt = model.model_validate_json(encoded)
        _verify_model(receipt, model)
        expected_encoded, digest = canonical_payload(receipt.model_dump(mode="json"))
    except (TypeError, ValueError) as exc:
        raise MessageIdentityConflict(
            "stored independent research receipt failed typed integrity verification"
        ) from exc
    if (
        expected_encoded != encoded
        or digest != row["payload_sha256"]
        or receipt.receipt_sha256 != row["receipt_sha256"]
    ):
        raise MessageIdentityConflict(
            "stored independent research receipt failed byte integrity verification"
        )
    return receipt


def _verify_model(value: Any, model: type[_Receipt]) -> None:
    if type(value) is not model:
        raise TypeError("research evidence requires the exact versioned receipt type")
    value = cast(_Receipt, value)
    if value.receipt_sha256 != canonical_sha256(value.identity_payload()):
        raise MessageIdentityConflict("research receipt canonical identity changed after validation")
    if len(canonical_json_bytes(value.model_dump(mode="json"))) > _MAX_RECEIPT_BYTES:
        raise ValueError("research receipt exceeds its bounded JSON envelope")
    # Revalidation rejects mutated nested dicts and unchecked model_copy edits.
    model.model_validate_json(json.dumps(value.model_dump(mode="json"), allow_nan=False))


def _verify_core(sample: ResearchDecisionSampleV1) -> None:
    if type(sample) is not ResearchDecisionSampleV1 or sample.sample_record_id != canonical_sha256(
        sample.identity_payload()
    ):
        raise MessageIdentityConflict("research sample is not an exact canonical observation")


def _bind(receipt: Any, schedule: Any, protocol: Any) -> None:
    if (
        receipt.schedule_digest != schedule.schedule_digest
        or receipt.candidate_protocol_digest != protocol.protocol_digest
    ):
        raise MessageIdentityConflict("research evidence differs from its preregistered source identity")
