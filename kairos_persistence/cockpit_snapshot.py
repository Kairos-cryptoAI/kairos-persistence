"""Read-only Cockpit snapshot projection from the durable runtime journal.

This module intentionally has no execution, strategy, risk, or venue mutation
dependencies.  Missing or malformed durable evidence is an error, not a
synthetic market state.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from kairos_core.contracts import (
    ClosedBarEventV1,
    MarketSnapshot,
    RiskTradeDecisionV1,
    TradeExecutionEventV1,
    VenueQualityV1,
)
from kairos_core.enums import ReviewDecision, TradeExecutionEventType
from kairos_core.topics import Topics

from .database import Database, MigrationProfile

SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT")
MAX_BARS_PER_SYMBOL = 500
MAX_DECISIONS = 200
MAX_EXECUTION_EVENTS = 2_000

_VENUE_SYMBOLS = {
    "BTCUSD:DEV": "BTCUSDT",
    "BTCUSD:DEMO": "BTCUSDT",
    "ETHUSD:DEV": "ETHUSDT",
    "ETHUSD:DEMO": "ETHUSDT",
    "SOLUSD:DEV": "SOLUSDT",
    "SOLUSD:DEMO": "SOLUSDT",
    "BNBUSD:DEV": "BNBUSDT",
    "BNBUSD:DEMO": "BNBUSDT",
    "XRPUSD:DEV": "XRPUSDT",
    "XRPUSD:DEMO": "XRPUSDT",
    "BTCUSD:PROD": "BTCUSDT",
    "ETHUSD:PROD": "ETHUSDT",
    "SOLUSD:PROD": "SOLUSDT",
    "BNBUSD:PROD": "BNBUSDT",
    "XRPUSD:PROD": "XRPUSDT",
}
_COCKPIT_VENUE_SYMBOLS = tuple(symbol for symbol in _VENUE_SYMBOLS if symbol.endswith(":DEV"))

_READ_ONLY_ACCESS_QUERY = """
SELECT
    current_setting('transaction_read_only') = 'on' AS transaction_read_only,
    NOT role.rolsuper
        AND NOT role.rolcreatedb
        AND NOT role.rolcreaterole
        AND NOT role.rolreplication
        AND NOT role.rolbypassrls AS restricted_role,
    NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_database AS database_info
         WHERE database_info.datname = current_database()
           AND database_info.datdba = role.oid
    ) AS no_database_ownership,
    NOT has_database_privilege(current_user, current_database(), 'CREATE') AS no_database_create,
    NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_class AS relation
          JOIN pg_catalog.pg_namespace AS schema_info ON schema_info.oid = relation.relnamespace
         WHERE schema_info.nspname = 'public'
           AND relation.relowner = role.oid
    ) AS no_public_relation_ownership,
    NOT has_schema_privilege(current_user, 'public', 'CREATE') AS no_schema_create,
    has_table_privilege(current_user, 'public.event_audit', 'SELECT') AS can_read_audit,
    NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_tables AS table_info
         WHERE table_info.schemaname = 'public'
           AND (
               has_table_privilege(
                   current_user,
                   pg_catalog.format('%I.%I', table_info.schemaname, table_info.tablename),
                   'INSERT'
               )
               OR has_table_privilege(
                   current_user,
                   pg_catalog.format('%I.%I', table_info.schemaname, table_info.tablename),
                   'UPDATE'
               )
               OR has_table_privilege(
                   current_user,
                   pg_catalog.format('%I.%I', table_info.schemaname, table_info.tablename),
                   'DELETE'
               )
               OR has_table_privilege(
                   current_user,
                   pg_catalog.format('%I.%I', table_info.schemaname, table_info.tablename),
                   'TRUNCATE'
               )
           )
    ) AS no_table_mutation,
    NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_auth_members AS membership
         WHERE membership.member = role.oid
    ) AS no_role_memberships
  FROM pg_catalog.pg_roles AS role
 WHERE role.rolname = current_user
"""

_CLOSED_BARS_QUERY = """
SELECT symbol_list.symbol, event.payload
  FROM unnest($2::text[]) AS symbol_list(symbol)
 CROSS JOIN LATERAL (
       SELECT payload
         FROM event_audit
        WHERE topic = $1
          AND payload->>'symbol' = symbol_list.symbol
        ORDER BY (payload->>'open_time_ms')::bigint DESC, produced_at DESC, message_id DESC
        LIMIT $3
  ) AS event
 ORDER BY symbol_list.symbol, (event.payload->>'open_time_ms')::bigint ASC
"""

_MARKET_SNAPSHOTS_QUERY = """
SELECT symbol_list.symbol, event.payload, event.produced_at, event.message_id
  FROM unnest($2::text[]) AS symbol_list(symbol)
 CROSS JOIN LATERAL (
       SELECT payload, produced_at, message_id
         FROM event_audit
        WHERE topic = $1
          AND payload->>'symbol' = symbol_list.symbol
        ORDER BY produced_at DESC, message_id DESC
        LIMIT $3
  ) AS event
 ORDER BY symbol_list.symbol, event.produced_at ASC, event.message_id ASC
"""

_VENUE_QUALITY_QUERY = """
SELECT DISTINCT ON (payload->>'symbol') payload
  FROM event_audit
 WHERE topic = $1
   AND payload->>'symbol' = ANY($2::text[])
 ORDER BY payload->>'symbol', produced_at DESC, message_id DESC
"""

_RISK_DECISIONS_QUERY = """
SELECT payload
  FROM event_audit
 WHERE topic = $1
 ORDER BY produced_at DESC, message_id DESC
 LIMIT $2
"""

_EXECUTION_EVENTS_QUERY = """
SELECT payload
  FROM event_audit
 WHERE topic = $1
   AND payload->>'trade_id' = ANY($2::text[])
 ORDER BY produced_at DESC, message_id DESC
 LIMIT $3
"""


class CockpitSnapshotError(RuntimeError):
    """A durable event cannot be represented safely by Cockpit snapshot v1."""


def _payload(value: object) -> dict[str, Any]:
    try:
        decoded = json.loads(value) if isinstance(value, str) else value
    except (TypeError, ValueError) as exc:
        raise CockpitSnapshotError("durable event payload is not valid JSON") from exc
    if not isinstance(decoded, dict):
        raise CockpitSnapshotError("durable event payload must be a JSON object")
    return decoded


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise CockpitSnapshotError("durable event timestamp must include a timezone")
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _timestamp_ms_iso(value: int) -> str:
    try:
        return _iso(datetime.fromtimestamp(value / 1_000, tz=UTC))
    except (OverflowError, OSError, ValueError) as exc:
        raise CockpitSnapshotError("durable event timestamp is outside the supported range") from exc


def _bars(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in SYMBOLS}
    for row in rows:
        symbol = str(row["symbol"])
        if symbol not in result:
            raise CockpitSnapshotError("closed-bar event is outside the Cockpit symbol universe")
        event = ClosedBarEventV1.model_validate(_payload(row["payload"]))
        result[symbol].append(
            {
                "closed_at": _timestamp_ms_iso(event.close_time_ms),
                "open": float(event.open),
                "high": float(event.high),
                "low": float(event.low),
                "close": float(event.close),
            }
        )
    for symbol, bars in result.items():
        if len(bars) > MAX_BARS_PER_SYMBOL:
            raise CockpitSnapshotError("closed-bar query exceeded its explicit per-symbol bound")
        bars.sort(key=lambda bar: bar["closed_at"])
        timestamps = [bar["closed_at"] for bar in bars]
        if len(timestamps) != len(set(timestamps)):
            raise CockpitSnapshotError(f"closed-bar timestamps are not unique for {symbol}")
    return result


def _indicators(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    fields = (
        ("rsi_14", "RSI 14", "rsi_14"),
        ("macd", "MACD", "macd"),
        ("macd_signal", "MACD signal", "macd_signal"),
        ("macd_hist", "MACD histogram", "macd_hist"),
        ("atr_pct", "ATR %", "atr_pct"),
    )
    grouped: dict[str, list[tuple[datetime, MarketSnapshot]]] = defaultdict(list)
    for row in rows:
        symbol = str(row["symbol"])
        if symbol not in SYMBOLS:
            raise CockpitSnapshotError("market snapshot is outside the Cockpit symbol universe")
        market = MarketSnapshot.model_validate(_payload(row["payload"]))
        produced_at = row["produced_at"]
        if not isinstance(produced_at, datetime) or produced_at.tzinfo is None:
            raise CockpitSnapshotError("market snapshot row has an invalid produced_at timestamp")
        grouped[symbol].append((produced_at.astimezone(UTC), market))

    result: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in SYMBOLS}
    for symbol, snapshots in grouped.items():
        ordered = sorted(snapshots, key=lambda item: item[0])
        times = [item[0] for item in ordered]
        if len(times) != len(set(times)):
            raise CockpitSnapshotError(f"market snapshots contain duplicate timestamps for {symbol}")
        for identifier, label, field in fields:
            points = []
            for produced_at, market in ordered:
                value = getattr(market.indicators, field)
                if value is not None:
                    points.append({"closed_at": _iso(produced_at), "value": float(value)})
            if points:
                result[symbol].append({"id": identifier, "label": label, "values": points})
    return result


def _venue_quality(payloads: list[object], *, now: datetime) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {
        symbol: {
            "state": "UNAVAILABLE",
            "as_of": _iso(now),
            "spread_bps": None,
            "book_age_ms": None,
            "slippage_bps": None,
        }
        for symbol in SYMBOLS
    }
    for raw in payloads:
        event = VenueQualityV1.model_validate(_payload(raw))
        symbol = _VENUE_SYMBOLS.get(event.symbol)
        if symbol is None:
            raise CockpitSnapshotError("venue-quality event is outside the Cockpit symbol universe")
        observed_at = datetime.fromtimestamp(event.observed_at_ms / 1_000, tz=UTC)
        expired = now.timestamp() * 1_000 > event.expires_at_ms
        state = "STALE" if expired else "HEALTHY" if event.entry_allowed else "DEGRADED"
        result[symbol] = {
            "state": state,
            "as_of": _iso(observed_at),
            "spread_bps": float(event.spread_bps),
            "book_age_ms": int(event.book_age_ms),
            "slippage_bps": float(max(event.buy_slippage_bps, event.sell_slippage_bps)),
        }
    return result


def _execution_state(events: list[TradeExecutionEventV1], *, approved: bool) -> str:
    if not approved:
        return "NOT_REQUESTED"
    if not events:
        return "PENDING"
    latest = events[0]
    if latest.event_type is TradeExecutionEventType.ENTRY_PARTIAL_FILL:
        return "PARTIAL"
    if latest.event_type in {
        TradeExecutionEventType.ENTRY_FILLED,
        TradeExecutionEventType.EXIT_FILLED,
    }:
        return "FILLED"
    if latest.event_type is TradeExecutionEventType.ENTRY_CANCELLED:
        return "CANCELLED"
    if latest.event_type is TradeExecutionEventType.FAILED and latest.filled_quantity == 0:
        return "REJECTED"
    if latest.lifecycle_state.value in {"RECEIVED", "ENTRY_PENDING"}:
        return "PENDING"
    if latest.filled_quantity > 0:
        return "PARTIAL"
    return "UNKNOWN"


def _decisions(
    decisions: list[RiskTradeDecisionV1],
    execution_payloads: list[object],
    *,
    now: datetime,
) -> tuple[list[dict[str, Any]], dict[str, list[TradeExecutionEventV1]]]:
    execution_by_trade: dict[str, list[TradeExecutionEventV1]] = defaultdict(list)
    for raw in execution_payloads:
        event = TradeExecutionEventV1.model_validate(_payload(raw))
        execution_by_trade[event.trade_id].append(event)

    result = []
    for decision in decisions:
        if decision.intent.symbol not in SYMBOLS:
            raise CockpitSnapshotError("risk-decision event is outside the Cockpit symbol universe")
        if len(decision.intent.strategy_id) > 96:
            raise CockpitSnapshotError("risk-decision strategy identifier exceeds the Cockpit contract limit")
        reason_code = decision.rejection_reasons[0] if decision.rejection_reasons else None
        if reason_code is not None and len(reason_code) > 96:
            raise CockpitSnapshotError("risk-decision reason exceeds the Cockpit contract limit")
        review = decision.review
        if review.reviewer == "LLM":
            llm_review = review.decision.value
        else:
            llm_review = "NOT_REQUESTED"
        is_expired = now.timestamp() * 1_000 > decision.intent.entry_expires_ts_ms
        candidate_state = "EXPIRED" if is_expired else "CREATED" if decision.approved else "REJECTED"
        risk_state = (
            "APPROVED"
            if decision.approved
            else "DEFER"
            if review.decision is ReviewDecision.DEFER
            else "VETO"
        )
        events = execution_by_trade.get(str(decision.trade_id), [])
        events.sort(key=lambda item: (item.event_seq, item.produced_at), reverse=True)
        result.append(
            {
                "id": str(decision.decision_id),
                "created_at": _iso(decision.produced_at),
                "symbol": decision.intent.symbol,
                "strategy_id": decision.intent.strategy_id,
                "intent_side": decision.intent.side.value,
                "candidate_state": candidate_state,
                "llm_review": llm_review,
                "risk_decision": risk_state,
                "execution_state": _execution_state(events, approved=decision.approved),
                "reason_code": reason_code,
            }
        )
    return result, execution_by_trade


def _lifecycle(events: list[TradeExecutionEventV1]) -> list[dict[str, Any]]:
    kind_by_event = {
        TradeExecutionEventType.DECISION_RECEIVED: "RISK",
        TradeExecutionEventType.EFFECT_PREPARED: "ORDER",
        TradeExecutionEventType.VENUE_ACK: "ORDER",
        TradeExecutionEventType.ENTRY_PARTIAL_FILL: "FILL",
        TradeExecutionEventType.ENTRY_FILLED: "FILL",
        TradeExecutionEventType.ENTRY_CANCELLED: "ORDER",
        TradeExecutionEventType.STOP_CREATED: "PROTECTION",
        TradeExecutionEventType.STOP_RECONCILED: "PROTECTION",
        TradeExecutionEventType.TARGET_CREATED: "PROTECTION",
        TradeExecutionEventType.TARGET_RECONCILED: "PROTECTION",
        TradeExecutionEventType.EXIT_TRIGGERED: "TIMEOUT",
        TradeExecutionEventType.EXIT_FILLED: "FILL",
        TradeExecutionEventType.RECONCILIATION: "RECOVERY",
        TradeExecutionEventType.RECOVERY_BLOCKED: "RECOVERY",
        TradeExecutionEventType.EMERGENCY_CLOSE: "RECOVERY",
        TradeExecutionEventType.FAILED: "RECOVERY",
    }
    result = []
    for event in events[:200]:
        symbol = _VENUE_SYMBOLS.get(event.venue_symbol)
        if symbol is None:
            raise CockpitSnapshotError("execution event venue symbol is not in the configured universe")
        kind = kind_by_event[event.event_type]
        if (
            event.event_type is TradeExecutionEventType.EXIT_TRIGGERED
            and event.exit_reason is not None
            and event.exit_reason.value != "TIMEOUT"
        ):
            kind = "PROTECTION"
        result.append(
            {
                "id": str(event.event_id or event.message_id),
                "occurred_at": _timestamp_ms_iso(event.occurred_at_ms),
                "symbol": symbol,
                "event_type": kind,
                "state": event.lifecycle_state.value,
            }
        )
    return result


class CockpitSnapshotRepository:
    """Read the current runtime journal using an explicitly read-only Database."""

    def __init__(self, database: Database) -> None:
        if not isinstance(database, Database):
            raise TypeError("Cockpit requires an explicit persistence Database")
        if not database.read_only:
            raise ValueError("Cockpit snapshot access requires a read-only database connection")
        if database.migration_profile is not MigrationProfile.RUNTIME:
            raise ValueError("Cockpit snapshot access cannot target the simulator database profile")
        self._database = database

    async def verify_read_only_access(self) -> None:
        async with self._database.pool.acquire() as connection:
            access = await connection.fetchrow(_READ_ONLY_ACCESS_QUERY)
        required = (
            "transaction_read_only",
            "restricted_role",
            "no_database_ownership",
            "no_database_create",
            "no_public_relation_ownership",
            "no_schema_create",
            "can_read_audit",
            "no_table_mutation",
            "no_role_memberships",
        )
        if access is None or any(not bool(access[name]) for name in required):
            raise CockpitSnapshotError("Cockpit database role is not restricted to read-only access")

    async def load_snapshot(self, *, now: datetime | None = None) -> dict[str, Any]:
        generated_at = (now or datetime.now(UTC)).astimezone(UTC)
        async with self._database.pool.acquire() as connection:
            async with connection.transaction(readonly=True):
                bars_rows = await connection.fetch(
                    _CLOSED_BARS_QUERY, Topics.CLOSED_BAR, list(SYMBOLS), MAX_BARS_PER_SYMBOL
                )
                market_rows = await connection.fetch(
                    _MARKET_SNAPSHOTS_QUERY, Topics.MARKET_SNAPSHOT, list(SYMBOLS), MAX_BARS_PER_SYMBOL
                )
                venue_rows = await connection.fetch(
                    _VENUE_QUALITY_QUERY,
                    Topics.VENUE_QUALITY,
                    list(_COCKPIT_VENUE_SYMBOLS),
                )
                risk_rows = await connection.fetch(
                    _RISK_DECISIONS_QUERY, Topics.RISK_TRADE_DECISION, MAX_DECISIONS
                )
                risk_payloads = [row["payload"] for row in risk_rows]
                risk_models = [RiskTradeDecisionV1.model_validate(_payload(value)) for value in risk_payloads]
                trade_ids = [str(model.trade_id) for model in risk_models]
                execution_rows = (
                    await connection.fetch(
                        _EXECUTION_EVENTS_QUERY,
                        Topics.TRADE_EXECUTION_EVENT,
                        trade_ids,
                        MAX_EXECUTION_EVENTS,
                    )
                    if trade_ids
                    else []
                )

        decisions, execution_by_trade = _decisions(
            risk_models,
            [row["payload"] for row in execution_rows],
            now=generated_at,
        )
        execution_events = [event for events in execution_by_trade.values() for event in events]
        execution_events.sort(
            key=lambda item: (item.occurred_at_ms, item.event_seq, str(item.event_id or item.message_id)),
            reverse=True,
        )

        markets_bars = _bars([dict(row) for row in bars_rows])
        markets_indicators = _indicators([dict(row) for row in market_rows])
        markets_venue = _venue_quality([row["payload"] for row in venue_rows], now=generated_at)
        markets = [
            {
                "symbol": symbol,
                "bar_interval": "1m",
                "bars": markets_bars[symbol],
                "indicators": markets_indicators[symbol],
                "venue_quality": markets_venue[symbol],
            }
            for symbol in SYMBOLS
        ]

        # No durable TCA fact contract is currently persisted.  Empty is honest;
        # estimated fees/slippage are not relabelled as actual execution quality.
        return {
            "schema_version": "kairos.cockpit.snapshot.v1",
            "generated_at": _iso(generated_at),
            "system": {
                "runtime_state": "DEGRADED",
                "trading_mode": "DRY_RUN",
                "strategy_policy": "REJECT_ALL",
                "readiness": {
                    "technical_paper_ready": False,
                    "paper_qualified": False,
                    "alpha_ready": False,
                    "live_ready": False,
                },
            },
            "markets": markets,
            "decisions": decisions,
            "lifecycle": _lifecycle(execution_events),
            "tca": [],
        }
