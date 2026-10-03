"""Opt-in causal campaign receipts, distinct from legacy Core V1 pairs.

The closed-bar anchor and the later context cutoff are deliberately different
clocks. These descriptors neither rewrite strategy provenance nor qualify alpha.
Window references are identifiers resolved by a separately injected read-only
resolver, never URLs, filesystem paths, or instructions to fetch remote data.
"""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal, Self

from kairos_core import ClosedBarEventV1, MarketSnapshot, StrategyIntentV1, canonical_sha256
from kairos_core.contracts.base import StrictValueModel
from pydantic import Field, StrictInt, StrictStr, model_validator

from .repository import MessageIdentityConflict
from .research_evidence import (
    ResearchStrategyEvaluationReceiptV1,
    _bounded_json,
    _decode_row,
    _Receipt,
)

_ID = Annotated[StrictStr, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")]
_SHA = Annotated[StrictStr, Field(pattern=r"^[0-9a-f]{64}$")]
_MS = Annotated[StrictInt, Field(ge=0, le=253_402_300_799_999)]
_COUNT = Annotated[StrictInt, Field(ge=1, le=50_000)]


class CampaignMarketContextV1(StrictValueModel):
    contract_version: Literal["campaign-market-context.v1"] = "campaign-market-context.v1"
    anchor_bar: ClosedBarEventV1
    market_snapshot: MarketSnapshot
    bar_window_reference: _ID
    bar_window_sha256: _SHA
    bar_count: _COUNT
    first_open_time_ms: _MS

    @model_validator(mode="before")
    @classmethod
    def explicit_snapshot_envelope(cls, value):
        if not isinstance(value, dict):
            return value
        snapshot = value.get("market_snapshot")
        if isinstance(snapshot, MarketSnapshot):
            if not {"source", "message_id", "produced_at"} <= snapshot.model_fields_set:
                raise ValueError("market context requires the original explicit snapshot envelope")
            snapshot = snapshot.model_dump(mode="json")
        if not isinstance(snapshot, dict) or not {"source", "message_id", "produced_at"} <= snapshot.keys():
            raise ValueError("market context requires the original explicit snapshot envelope")
        if set(snapshot) - MarketSnapshot.model_fields.keys():
            raise ValueError("market context cannot discard unknown snapshot fields")
        for name in ("order_book", "derivatives", "indicators"):
            nested = snapshot.get(name)
            model = MarketSnapshot.model_fields[name].annotation
            if not isinstance(nested, dict) or set(nested) - model.model_fields.keys():
                raise ValueError("market context cannot discard unknown nested snapshot fields")
        return {**value, "market_snapshot": snapshot}

    @model_validator(mode="after")
    def anchor_identity(self) -> Self:
        snapshot = self.market_snapshot
        if snapshot.produced_at.tzinfo is None or snapshot.produced_at.utcoffset() is None:
            raise ValueError("market context requires an aware snapshot clock")
        if snapshot.produced_at.microsecond % 1_000:
            raise ValueError("market context cannot truncate a sub-millisecond producer clock")
        if (
            snapshot.symbol != self.anchor_bar.symbol
            or snapshot.timeframe != "1m"
            or self.anchor_bar.timeframe != "1m"
        ):
            raise ValueError("market context snapshot and anchor must share the one-minute instrument")
        if int(snapshot.produced_at.timestamp() * 1000) < self.anchor_bar.close_time_ms:
            raise ValueError("market context cannot precede its closed bar")
        if self.first_open_time_ms + (self.bar_count - 1) * 60_000 != self.anchor_bar.open_time_ms:
            raise ValueError("market context window geometry differs from the anchor")
        _bounded_json(self.model_dump(mode="json"))
        return self


def campaign_bar_window_sha256(bars: tuple[ClosedBarEventV1, ...]) -> str:
    """The existing strategy's exact window identity, excluding message envelopes."""
    if not isinstance(bars, tuple) or not 1 <= len(bars) <= 50_000:
        raise ValueError("campaign bar window must be a bounded immutable tuple")
    for index, bar in enumerate(bars):
        if type(bar) is not ClosedBarEventV1 or bar.bar_sha256 != canonical_sha256(bar.identity_payload()):
            raise ValueError("campaign bar window contains a noncanonical closed bar")
        ClosedBarEventV1.model_validate_json(bar.model_dump_json())
        if index and (
            bar.symbol != bars[0].symbol
            or bar.timeframe != bars[0].timeframe
            or bar.open_time_ms != bars[index - 1].open_time_ms + 60_000
        ):
            raise ValueError("campaign bar window is not one contiguous instrument history")
    return canonical_sha256({"bars": [bar.identity_payload() for bar in bars]})


class CausalBaselineResultV1(StrictValueModel):
    context_source_receipt_sha256: _SHA
    bar_window_sha256: _SHA
    bar_count: _COUNT
    intent: StrategyIntentV1 | None = None


class ResearchCausalStrategyEvaluationReceiptV1(_Receipt):
    contract_version: Literal["research-causal-strategy-evaluation.v1"] = (
        "research-causal-strategy-evaluation.v1"
    )
    campaign_id: _ID
    sample_id: _ID
    schedule_digest: _SHA
    candidate_protocol_digest: _SHA
    strategy_id: _ID
    strategy_revision: _ID
    symbol: Annotated[StrictStr, Field(min_length=2, max_length=32, pattern=r"^[A-Z0-9][A-Z0-9._-]*$")]
    timeframe: Literal["1m"] = "1m"
    evidence_as_of_ts_ms: _MS
    evaluated_at_ts_ms: _MS
    market_snapshot_sha256: _SHA
    evaluator_sha256: _SHA
    source_receipt_sha256s: tuple[_SHA, ...] = Field(min_length=3, max_length=16)
    context_source_receipt_sha256: _SHA
    anchor_bar_sha256: _SHA
    anchor_bar_close_ts_ms: _MS
    bar_window_sha256: _SHA
    bar_count: _COUNT
    intent: StrategyIntentV1 | None = None

    @model_validator(mode="after")
    def actual_anchor(self) -> Self:
        if not self.anchor_bar_close_ts_ms < self.evidence_as_of_ts_ms <= self.evaluated_at_ts_ms:
            raise ValueError("causal evaluation requires a closed bar before context and actual evaluation")
        if self.source_receipt_sha256s != tuple(sorted(set(self.source_receipt_sha256s))):
            raise ValueError("causal evaluation source hashes must be distinct and sorted")
        if self.context_source_receipt_sha256 not in self.source_receipt_sha256s:
            raise ValueError("causal evaluation context is absent from its source roster")
        if self.intent is not None:
            for field in ("strategy_id", "strategy_revision", "symbol", "timeframe"):
                if getattr(self.intent, field) != getattr(self, field):
                    raise ValueError("causal evaluation intent differs from its frozen scope")
            if (
                self.intent.intent_id != canonical_sha256(self.intent.identity_payload())
                or self.intent.decision_ts_ms != self.anchor_bar_close_ts_ms
                or self.intent.provenance.input_bar_sha256s[-1] != self.anchor_bar_sha256
                or self.intent.provenance.input_window_sha256 != self.bar_window_sha256
                or len(self.intent.provenance.input_bar_sha256s) != self.bar_count
            ):
                raise ValueError("causal evaluation cannot rewrite the original strategy clock or window")
        return self


class ResearchCausalPairReceiptV1(_Receipt):
    """Independently resolved advisory comparison, never a Core V1 sample or seal."""

    contract_version: Literal["research-causal-pair.v1"] = "research-causal-pair.v1"
    campaign_id: _ID
    sample_id: _ID
    arm_id: Literal["llm-proposal-research"] = "llm-proposal-research"
    schedule_digest: _SHA
    candidate_protocol_digest: _SHA
    arm_protocol_digest: _SHA
    bundle_receipt_sha256: _SHA
    evaluation_receipt_sha256: _SHA
    source_receipt_sha256s: tuple[_SHA, ...] = Field(min_length=3, max_length=16)
    market_snapshot_sha256: _SHA
    anchor_bar_sha256: _SHA
    bar_window_sha256: _SHA
    bar_count: _COUNT
    market_as_of_ts_ms: _MS
    paired_at_ts_ms: _MS
    attempt_id: _ID
    start_receipt_sha256: _SHA
    terminal_receipt_sha256: _SHA
    completion_receipt_id: _SHA
    proposal_id: _SHA
    strategy_outcome: Literal["LONG", "SHORT", "NO_INTENT"]
    strategy_intent_id: _SHA | None
    llm_outcome: Literal["LONG_BIAS", "SHORT_BIAS", "NO_PROPOSAL", "DEFER", "VOLATILITY_ALERT"]

    @model_validator(mode="after")
    def comparison_identity(self) -> Self:
        if self.source_receipt_sha256s != tuple(sorted(set(self.source_receipt_sha256s))):
            raise ValueError("causal pair source hashes must be distinct and sorted")
        if self.paired_at_ts_ms < self.market_as_of_ts_ms:
            raise ValueError("causal pair cannot precede its context")
        if (self.strategy_outcome == "NO_INTENT") != (self.strategy_intent_id is None):
            raise ValueError("causal pair baseline action differs from its original intent identity")
        return self


CampaignEvaluationReceipt = ResearchStrategyEvaluationReceiptV1 | ResearchCausalStrategyEvaluationReceiptV1


def campaign_receipt_contract(row: Any) -> str:
    """Bounded discriminator only; the selected decoder still verifies all bytes."""
    encoded = row["payload_json"]
    if type(encoded) is not str or len(encoded.encode("utf-8")) > 262_144:
        raise MessageIdentityConflict("stored campaign receipt exceeds its bounded JSON envelope")
    try:
        value = json.loads(encoded)
        _bounded_json(value)
        if not isinstance(value, dict) or type(value.get("contract_version")) is not str:
            raise ValueError("missing contract discriminator")
        return value["contract_version"]
    except (TypeError, ValueError, RecursionError):
        raise MessageIdentityConflict(
            "stored campaign receipt has an invalid contract discriminator"
        ) from None


def decode_campaign_evaluation_row(row: Any) -> CampaignEvaluationReceipt:
    """Closed version dispatch; it cannot coerce the new clock family into Core V1."""
    if row["kind"] != "evaluation":
        raise MessageIdentityConflict("campaign evaluation contract is stored in a different receipt kind")
    contract = campaign_receipt_contract(row)
    if contract == "research-strategy-evaluation.v1":
        return _decode_row(row, ResearchStrategyEvaluationReceiptV1)
    if contract == "research-causal-strategy-evaluation.v1":
        return _decode_row(row, ResearchCausalStrategyEvaluationReceiptV1)
    raise MessageIdentityConflict("unknown campaign evaluation contract")
