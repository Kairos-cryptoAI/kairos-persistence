"""Explicit producer capture for opt-in SIM research, never an automatic consumer.

Publication/event time is not availability time. Only capture_source supplies
the independent PostgreSQL observation clock. Derived Text/Macro messages do
not attest their absent raw upstream bytes or model completion provenance.
Current producers with sub-millisecond clocks require an explicitly versioned
future millisecond envelope; this bridge never truncates or backdates them.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, TypeVar

from kairos_core import ClosedBarEventV1, MarketSnapshot, SentimentSignal, StrategicAllocation
from kairos_core.contracts.base import KairosMessage, canonical_json_bytes, canonical_sha256

from .causal_campaign import CampaignMarketContextV1
from .research_campaign import ResearchCampaignPlanV1, ResearchCampaignRepository
from .research_evidence import ResearchSourceReceiptV1, _bounded_json, _verify_model

_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_MAX_CONTENT_BYTES = 240_000  # Leave room inside the existing 262,144-byte receipt.
_MAX_BARS = 50_000
_ENVELOPE = ("message_id", "source", "schema_version", "produced_at")
_Message = TypeVar("_Message", bound=KairosMessage)

CAMPAIGN_INPUT_BRIDGE_SHA256 = canonical_sha256(
    {
        "contract_version": "campaign-input-bridge.v1",
        "implementation_sha256": hashlib.sha256(
            Path(__file__).read_bytes().replace(b"\r\n", b"\n")
        ).hexdigest(),
        "envelope": "explicit-known-json-fields-schema-1.0-aware-time",
        "sources": ["macro-strategist", "quant-scouts", "text-scouts"],
        "clock": "repository-postgresql-observation-only",
        "window": "injected-readonly-resolver-full-gapfree-identity-hash",
        "authority": "SIM_RESEARCH_ONLY-no-subscription-or-dispatch",
        "maximum_content_bytes": _MAX_CONTENT_BYTES,
        "maximum_bar_count": _MAX_BARS,
    }
)


class CampaignBarWindowResolver(Protocol):
    """Caller-owned read-only lookup; the bridge adds no URL/path capability."""

    async def load(self, reference: str) -> tuple[ClosedBarEventV1, ...]: ...


def _identifier(value: object) -> str:
    if type(value) is not str or not _ID.fullmatch(value):
        raise ValueError("campaign input identifier must be an exact normalized ID")
    return value


def _time_ms(value: object) -> int:
    if type(value) is not str:
        raise ValueError("producer timestamp must be explicit JSON text")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("producer timestamp must be timezone-aware ISO text") from exc
    if parsed.utcoffset() is None:
        raise ValueError("producer timestamp must be timezone-aware ISO text")
    if parsed.microsecond % 1_000:
        raise ValueError("producer timestamp must preserve exact millisecond precision, never be truncated")
    delta = parsed.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    result = (delta.days * 86_400 + delta.seconds) * 1_000 + delta.microseconds // 1_000
    if not 0 <= result <= 253_402_300_799_999:
        raise ValueError("producer timestamp is outside the research clock envelope")
    return result


def _content(value: dict[str, Any]) -> dict[str, Any]:
    _bounded_json(value)
    encoded = canonical_json_bytes(value)
    if len(encoded) > _MAX_CONTENT_BYTES:
        raise ValueError("producer content exceeds the compact research input bound")
    # Freeze caller-owned nested JSON before any await or repository operation.
    return json.loads(encoded)


def _wire(
    payload: Mapping[str, Any] | _Message, model: type[_Message], *, source: str
) -> tuple[dict[str, Any], _Message, int]:
    if type(payload) is model:
        if not {"message_id", "source", "produced_at"} <= payload.model_fields_set:
            raise ValueError("typed producer identity and clock cannot be manufactured from legacy defaults")
        raw = payload.model_dump(mode="json")
    elif isinstance(payload, Mapping):
        raw = dict(payload)
    else:
        raise TypeError("producer capture requires the exact message type or its JSON payload")
    raw = _content(raw)
    if any(field not in raw for field in _ENVELOPE):
        raise ValueError("producer envelope cannot be manufactured from legacy defaults")
    _identifier(raw["message_id"])
    if raw["source"] != source or raw["schema_version"] != "1.0":
        raise ValueError("producer source/schema differs from the exact input contract")
    event_time = _time_ms(raw["produced_at"])
    if set(raw) != set(model.model_fields):
        raise ValueError("producer wire fields must be exact, without ignored fields or invented defaults")
    if model is MarketSnapshot:
        for field in ("order_book", "derivatives", "indicators"):
            nested = raw.get(field)
            nested_model = model.model_fields[field].annotation
            if (
                nested_model is None
                or type(nested) is not dict
                or set(nested) != set(nested_model.model_fields)
            ):
                raise ValueError("unknown nested market wire fields are not research evidence")
    parsed = model.model_validate_json(canonical_json_bytes(raw), strict=True)
    if model is StrategicAllocation:
        for strategy, weight in raw["strategy_weights"].items():
            _identifier(strategy)
            if not 0.0 <= weight <= 1.0:
                raise ValueError(
                    "macro strategy weights must retain the producer's bounded allocation schema"
                )
    return raw, parsed, event_time


def _decode(
    content: Mapping[str, Any],
    *,
    version: str,
    model: type[_Message],
    source: str,
    provenance: Mapping[str, str],
) -> _Message:
    if not isinstance(content, Mapping):
        raise TypeError("stored producer wrapper must be explicit JSON object content")
    value = _content(dict(content))
    if (
        set(value) != {"contract_version", "message", "provenance"}
        or value["contract_version"] != version
        or value["provenance"] != provenance
    ):
        raise ValueError("stored producer wrapper differs from its exact version/provenance contract")
    return _wire(value["message"], model, source=source)[1]


def decode_campaign_news(content: Mapping[str, Any]) -> SentimentSignal:
    """Pure strict decoder; never supply an observation clock or query a database."""
    return _decode(
        content,
        version="campaign-news-input.v1",
        model=SentimentSignal,
        source="text-scouts",
        provenance={"raw_article_bytes": "UNAVAILABLE", "model_completion": "UNAVAILABLE"},
    )


def decode_campaign_macro(content: Mapping[str, Any]) -> StrategicAllocation:
    """Pure strict decoder with explicit absent upstream/model provenance."""
    return _decode(
        content,
        version="campaign-macro-input.v1",
        model=StrategicAllocation,
        source="macro-strategist",
        provenance={"upstream_context": "UNAVAILABLE", "model_completion": "UNAVAILABLE"},
    )


def _bars(value: Sequence[ClosedBarEventV1]) -> tuple[ClosedBarEventV1, ...]:
    if not isinstance(value, (tuple, list)) or not 1 <= len(value) <= _MAX_BARS:
        raise ValueError("bar window must be an explicit bounded nonempty sequence")
    checked = []
    for bar in value:
        if type(bar) is not ClosedBarEventV1:
            raise TypeError("bar window requires exact strict closed-bar messages")
        checked.append(ClosedBarEventV1.model_validate_json(bar.model_dump_json(), strict=True))
    for prior, current in zip(checked, checked[1:], strict=False):
        if (
            current.symbol != prior.symbol
            or current.timeframe != prior.timeframe
            or current.open_time_ms != prior.open_time_ms + 60_000
        ):
            raise ValueError("bar window must preserve one gap-free symbol/timeframe chain")
    return tuple(checked)


def _window_hash(bars: tuple[ClosedBarEventV1, ...]) -> str:
    return canonical_sha256({"bars": [bar.identity_payload() for bar in bars]})


class CampaignInputCaptureBridge:
    """Explicit methods only: no feeds, consumers, provider factory or campaign start."""

    def __init__(
        self,
        repository: ResearchCampaignRepository,
        *,
        bar_window_resolver: CampaignBarWindowResolver | None = None,
    ) -> None:
        if not isinstance(repository, ResearchCampaignRepository):
            raise TypeError("producer bridge requires the typed campaign repository")
        self.repository = repository
        self.bar_window_resolver = bar_window_resolver
        self._scheduler()

    def _scheduler(self) -> str:
        value = self.repository.causal_scheduler_sha256
        if type(value) is not str or not _SHA.fullmatch(value):
            raise ValueError("producer bridge requires explicit causal scheduler opt-in")
        return value

    async def _capture(
        self, *, campaign_id, sample_id, source_name, source_kind, reference, event_time, content
    ) -> ResearchSourceReceiptV1:
        campaign_id, sample_id, source_name = map(_identifier, (campaign_id, sample_id, source_name))
        reference = _identifier(reference)
        content = _content(content)
        plan, schedule, protocol = await self.repository.load_campaign(campaign_id)
        _verify_model(plan, ResearchCampaignPlanV1)
        if (
            plan.scheduler_sha256 != self._scheduler()
            or plan.campaign_id != campaign_id
            or schedule.campaign_id != campaign_id
            or protocol.campaign_id != campaign_id
            or plan.schedule_digest != schedule.schedule_digest
            or plan.candidate_protocol_digest != protocol.protocol_digest
        ):
            raise ValueError("producer capture differs from the preregistered causal campaign identity")
        window = next((item for item in schedule.windows if item.sample_id == sample_id), None)
        if window is None or (source_kind, source_name) not in {
            (item.source_kind, item.source_name) for item in plan.required_sources
        }:
            raise ValueError("producer capture is outside the preregistered sample/source roster")
        if source_kind == "MARKET_SNAPSHOT":
            context = CampaignMarketContextV1.model_validate(content)
            if context.anchor_bar.symbol != window.symbol or context.anchor_bar.timeframe != window.timeframe:
                raise ValueError("market context differs from the preregistered symbol/timeframe")
        receipt = await self.repository.capture_source(
            campaign_id=campaign_id,
            sample_id=sample_id,
            source_kind=source_kind,
            source_name=source_name,
            reference=reference,
            source_as_of_ts_ms=event_time,
            content=content,
        )
        _verify_model(receipt, ResearchSourceReceiptV1)
        if (
            receipt.campaign_id != campaign_id
            or receipt.sample_id != sample_id
            or receipt.source_kind != source_kind
            or receipt.source_name != source_name
            or receipt.reference != reference
            or receipt.source_as_of_ts_ms != event_time
            or receipt.schedule_digest != schedule.schedule_digest
            or receipt.candidate_protocol_digest != protocol.protocol_digest
            or receipt.content != content
        ):
            raise ValueError("producer capture returned different independently saved source facts")
        return receipt

    async def capture_news(
        self, *, campaign_id: str, sample_id: str, source_name: str, payload: Mapping | SentimentSignal
    ) -> ResearchSourceReceiptV1:
        raw, _, event_time = _wire(payload, SentimentSignal, source="text-scouts")
        return await self._capture(
            campaign_id=campaign_id,
            sample_id=sample_id,
            source_name=source_name,
            source_kind="NEWS",
            reference=raw["message_id"],
            event_time=event_time,
            content={
                "contract_version": "campaign-news-input.v1",
                "message": raw,
                "provenance": {"raw_article_bytes": "UNAVAILABLE", "model_completion": "UNAVAILABLE"},
            },
        )

    async def capture_macro(
        self, *, campaign_id: str, sample_id: str, source_name: str, payload: Mapping | StrategicAllocation
    ) -> ResearchSourceReceiptV1:
        raw, _, event_time = _wire(payload, StrategicAllocation, source="macro-strategist")
        return await self._capture(
            campaign_id=campaign_id,
            sample_id=sample_id,
            source_name=source_name,
            source_kind="MACRO",
            reference=raw["message_id"],
            event_time=event_time,
            content={
                "contract_version": "campaign-macro-input.v1",
                "message": raw,
                "provenance": {"upstream_context": "UNAVAILABLE", "model_completion": "UNAVAILABLE"},
            },
        )

    async def capture_market(
        self,
        *,
        campaign_id: str,
        sample_id: str,
        source_name: str,
        payload: Mapping | MarketSnapshot,
        bars: Sequence[ClosedBarEventV1],
        bar_window_reference: str,
    ) -> ResearchSourceReceiptV1:
        _, snapshot, snapshot_time = _wire(payload, MarketSnapshot, source="quant-scouts")
        reference = _identifier(bar_window_reference)
        supplied = _bars(bars)
        if self.bar_window_resolver is None:
            raise ValueError("market capture requires an explicitly injected read-only bar resolver")
        resolved = _bars(await self.bar_window_resolver.load(reference))
        digest = _window_hash(supplied)
        if len(supplied) != len(resolved) or digest != _window_hash(resolved):
            raise ValueError("resolved bar window differs from the passed immutable full history")
        context = CampaignMarketContextV1(
            anchor_bar=supplied[-1],
            market_snapshot=snapshot,
            bar_window_reference=reference,
            bar_window_sha256=digest,
            bar_count=len(supplied),
            first_open_time_ms=supplied[0].open_time_ms,
        )
        return await self._capture(
            campaign_id=campaign_id,
            sample_id=sample_id,
            source_name=source_name,
            source_kind="MARKET_SNAPSHOT",
            reference=snapshot.message_id,
            event_time=max(supplied[-1].close_time_ms, snapshot_time),
            content=context.model_dump(mode="json"),
        )


__all__ = [
    "CAMPAIGN_INPUT_BRIDGE_SHA256",
    "CampaignBarWindowResolver",
    "CampaignInputCaptureBridge",
    "decode_campaign_macro",
    "decode_campaign_news",
]
