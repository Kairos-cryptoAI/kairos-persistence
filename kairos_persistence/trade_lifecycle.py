"""Durable PAPER/LIVE trade lifecycle and restart recovery primitives.

The repository deliberately exposes explicit state transitions instead of an
ORM.  Every transition is serialized with a PostgreSQL advisory lock and is
appended to a per-trade SHA-256 chain before the new state is returned.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import asyncpg
from kairos_core.contracts import RiskTradeDecisionV1
from kairos_core.enums import (
    EvedexProfile,
    OrderSide,
    Side,
    TradingMode,
)
from kairos_core.enums import (
    OrderRole as OrderRole,
)
from kairos_core.enums import (
    TradeLifecycleState as TradeState,
)

from .repository import MessageIdentityConflict
from .runtime import canonical_payload

TERMINAL_TRADE_STATES = frozenset({TradeState.FLAT, TradeState.CANCELLED})

_ALLOWED_TRANSITIONS: dict[TradeState, frozenset[TradeState]] = {
    TradeState.RECEIVED: frozenset(
        {TradeState.ENTRY_PENDING, TradeState.CANCELLED, TradeState.FAILED_BLOCKED}
    ),
    TradeState.ENTRY_PENDING: frozenset(
        {
            TradeState.ENTRY_PENDING,
            TradeState.PROTECTING,
            TradeState.CANCELLED,
            TradeState.FAILED_BLOCKED,
        }
    ),
    TradeState.PROTECTING: frozenset(
        {
            TradeState.PROTECTING,
            TradeState.ACTIVE,
            TradeState.EXITING_EMERGENCY,
            TradeState.FAILED_BLOCKED,
        }
    ),
    TradeState.ACTIVE: frozenset(
        {
            TradeState.ACTIVE,
            TradeState.EXITING_STOP,
            TradeState.EXITING_TARGET,
            TradeState.EXITING_TIMEOUT,
            TradeState.EXITING_EMERGENCY,
            TradeState.FAILED_BLOCKED,
        }
    ),
    TradeState.EXITING_STOP: frozenset({TradeState.FLAT, TradeState.FAILED_BLOCKED}),
    TradeState.EXITING_TARGET: frozenset({TradeState.FLAT, TradeState.FAILED_BLOCKED}),
    TradeState.EXITING_TIMEOUT: frozenset({TradeState.FLAT, TradeState.FAILED_BLOCKED}),
    TradeState.EXITING_EMERGENCY: frozenset({TradeState.FLAT, TradeState.FAILED_BLOCKED}),
    TradeState.FAILED_BLOCKED: frozenset({TradeState.EXITING_EMERGENCY, TradeState.FLAT}),
}


def validate_trade_transition(current: TradeState, target: TradeState) -> None:
    """Reject lifecycle shortcuts that could leave exposure unprotected."""
    if target not in _ALLOWED_TRANSITIONS.get(current, frozenset()):
        raise ValueError(f"invalid trade transition {current.value}->{target.value}")


@dataclass(frozen=True, slots=True)
class NewTrade:
    trade_id: str
    strategy_intent_id: str
    risk_decision_id: str
    risk_decision_payload: dict[str, Any]
    strategy_id: str
    strategy_revision: str
    trading_mode: str
    environment: str
    profile: str
    exchange: str
    account_id: str
    symbol: str
    venue_symbol: str
    side: str
    quantity: float
    leverage: float
    stop_price: float
    target_price: float
    entry_eligible_at: datetime
    entry_expires_at: datetime
    max_holding_ms: int
    entry_client_order_id: str

    def validate(self) -> None:
        required = {
            "trade_id": self.trade_id,
            "strategy_intent_id": self.strategy_intent_id,
            "risk_decision_id": self.risk_decision_id,
            "strategy_id": self.strategy_id,
            "strategy_revision": self.strategy_revision,
            "environment": self.environment,
            "exchange": self.exchange,
            "account_id": self.account_id,
            "symbol": self.symbol,
            "venue_symbol": self.venue_symbol,
            "entry_client_order_id": self.entry_client_order_id,
        }
        if any(not value.strip() for value in required.values()):
            raise ValueError("trade lineage fields must not be empty")
        if self.trading_mode not in {TradingMode.PAPER.value, TradingMode.LIVE.value}:
            raise ValueError("durable trades are only valid for PAPER or LIVE")
        if self.profile not in {profile.value for profile in EvedexProfile}:
            raise ValueError("unknown EVEDEX profile")
        if self.side not in {OrderSide.BUY.value, OrderSide.SELL.value}:
            raise ValueError("trade side must be BUY or SELL")
        for name, value in {
            "quantity": self.quantity,
            "leverage": self.leverage,
            "stop_price": self.stop_price,
            "target_price": self.target_price,
        }.items():
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if not 1 <= self.leverage <= 125:
            raise ValueError("leverage must be between 1 and 125")
        if self.side == "BUY" and self.stop_price >= self.target_price:
            raise ValueError("BUY stop must be below target")
        if self.side == "SELL" and self.stop_price <= self.target_price:
            raise ValueError("SELL stop must be above target")
        if self.entry_eligible_at.tzinfo is None or self.entry_expires_at.tzinfo is None:
            raise ValueError("entry timestamps must be timezone-aware")
        if self.entry_expires_at <= self.entry_eligible_at:
            raise ValueError("entry expiry must follow eligibility")
        if self.max_holding_ms <= 0:
            raise ValueError("max_holding_ms must be positive")
        decision = RiskTradeDecisionV1.model_validate(self.risk_decision_payload)
        self._validate_decision_lineage(decision)

    def _validate_decision_lineage(self, decision: RiskTradeDecisionV1) -> None:
        if not decision.approved:
            raise ValueError("a durable trade requires an approved risk decision")
        expected_side = OrderSide.BUY if decision.intent.side is Side.LONG else OrderSide.SELL
        comparisons = {
            "trade_id": (self.trade_id, decision.trade_id),
            "strategy_intent_id": (self.strategy_intent_id, decision.intent.intent_id),
            "risk_decision_id": (self.risk_decision_id, decision.decision_id),
            "strategy_id": (self.strategy_id, decision.intent.strategy_id),
            "strategy_revision": (self.strategy_revision, decision.intent.strategy_revision),
            "trading_mode": (self.trading_mode, decision.trading_mode.value),
            "profile": (self.profile, decision.evedex_profile.value),
            "account_id": (self.account_id, decision.account_id),
            "symbol": (self.symbol, decision.intent.symbol),
            "venue_symbol": (self.venue_symbol, decision.venue_symbol),
            "side": (self.side, expected_side.value),
            "quantity": (self.quantity, decision.quantity),
            "leverage": (self.leverage, decision.leverage),
            "stop_price": (self.stop_price, decision.exit_plan.stop_price),
            "target_price": (self.target_price, decision.exit_plan.target_price),
            "max_holding_ms": (self.max_holding_ms, decision.exit_plan.max_holding_ms),
            "entry_eligible_at": (
                self._unix_ms(self.entry_eligible_at),
                decision.intent.entry_eligible_ts_ms,
            ),
            "entry_expires_at": (
                self._unix_ms(self.entry_expires_at),
                decision.intent.entry_expires_ts_ms,
            ),
        }
        mismatches = [name for name, (actual, expected) in comparisons.items() if actual != expected]
        if mismatches:
            raise ValueError(
                "risk decision payload does not match durable trade lineage: " + ", ".join(mismatches)
            )
        if self.trading_mode == TradingMode.PAPER.value and (
            self.profile != EvedexProfile.DEV.value
            or self.exchange != "evedex"
            or not self.venue_symbol.endswith(":DEV")
        ):
            raise ValueError("PAPER persistence requires the exact EVEDEX DEV venue")
        if self.trading_mode == TradingMode.LIVE.value and self.profile != EvedexProfile.PROD.value:
            raise ValueError("LIVE persistence requires the EVEDEX PROD profile")

    @staticmethod
    def _unix_ms(value: datetime) -> int:
        if value.tzinfo is None:
            raise ValueError("entry timestamps must be timezone-aware")
        normalized = value.astimezone(UTC)
        if normalized.microsecond % 1_000:
            raise ValueError("entry timestamps must have millisecond precision")
        delta = normalized - datetime(1970, 1, 1, tzinfo=UTC)
        return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


@dataclass(frozen=True, slots=True)
class TradeRecord:
    trade_id: str
    strategy_intent_id: str
    risk_decision_id: str
    risk_decision_payload: dict[str, Any]
    strategy_id: str
    strategy_revision: str
    trading_mode: str
    environment: str
    profile: str
    exchange: str
    account_id: str
    symbol: str
    venue_symbol: str
    side: str
    quantity: float
    leverage: float
    stop_price: float
    target_price: float
    entry_eligible_at: datetime
    entry_expires_at: datetime
    max_holding_ms: int
    state: TradeState
    entry_client_order_id: str
    entry_exchange_order_id: str | None
    stop_client_order_id: str | None
    stop_exchange_order_id: str | None
    target_client_order_id: str | None
    target_exchange_order_id: str | None
    close_client_order_id: str | None
    close_exchange_order_id: str | None
    filled_quantity: float
    first_fill_at: datetime | None
    timeout_at: datetime | None
    last_reconciled_at: datetime | None
    reconciliation_detail: str | None
    state_version: int
    journal_head_sha256: str


@dataclass(frozen=True, slots=True)
class RecoveryState:
    environment: str
    account_id: str
    exchange: str
    entries_blocked: bool
    recovery_epoch: int
    started_at: datetime
    completed_at: datetime | None
    detail: str


@dataclass(frozen=True, slots=True)
class EquityState:
    environment: str
    account_id: str
    exchange: str
    trading_day: date
    day_start_equity_usd: float
    peak_equity_usd: float
    last_equity_usd: float
    first_captured_at: datetime
    last_captured_at: datetime
    reconciliation_seq: int


class TradeLifecycleRepository:
    """Serialize trade exits and make restart recovery an explicit gate."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def create(self, trade: NewTrade) -> TradeRecord:
        trade.validate()
        immutable = self._new_trade_payload(trade)
        event = {"trade": immutable}
        event_sha = self._event_sha(trade.trade_id, None, TradeState.RECEIVED, "CREATED", event, None)
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await self._xact_lock(connection, trade.trade_id)
                existing = await connection.fetchrow(
                    "SELECT * FROM execution_trades WHERE trade_id=$1 FOR UPDATE", trade.trade_id
                )
                if existing is not None:
                    record = self._record(existing)
                    if self._immutable_record_payload(record) != immutable:
                        raise MessageIdentityConflict(
                            f"trade_id {trade.trade_id!r} was reused with different immutable content"
                        )
                    return record
                row = await connection.fetchrow(
                    """INSERT INTO execution_trades
                       (trade_id, strategy_intent_id, risk_decision_id,
                        risk_decision_sha256, risk_decision_payload, strategy_id, strategy_revision,
                        trading_mode, environment, profile, exchange, account_id, symbol, venue_symbol, side,
                        quantity, leverage, stop_price, target_price, entry_eligible_at, entry_expires_at,
                        max_holding_ms, state, entry_client_order_id, journal_head_sha256)
                       VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
                               $18,$19,$20,$21,$22,'RECEIVED',$23,$24)
                       RETURNING *""",
                    trade.trade_id,
                    trade.strategy_intent_id,
                    trade.risk_decision_id,
                    canonical_payload(trade.risk_decision_payload)[1],
                    self._json(trade.risk_decision_payload),
                    trade.strategy_id,
                    trade.strategy_revision,
                    trade.trading_mode,
                    trade.environment,
                    trade.profile,
                    trade.exchange,
                    trade.account_id,
                    trade.symbol,
                    trade.venue_symbol,
                    trade.side,
                    trade.quantity,
                    trade.leverage,
                    trade.stop_price,
                    trade.target_price,
                    trade.entry_eligible_at.astimezone(UTC),
                    trade.entry_expires_at.astimezone(UTC),
                    trade.max_holding_ms,
                    trade.entry_client_order_id,
                    event_sha,
                )
                await self._append_event(
                    connection,
                    trade.trade_id,
                    None,
                    TradeState.RECEIVED,
                    "CREATED",
                    event,
                    None,
                    event_sha,
                )
        if row is None:
            raise RuntimeError("trade lifecycle create returned no row")
        return self._record(row)

    async def get(self, trade_id: str) -> TradeRecord | None:
        row = await self.pool.fetchrow("SELECT * FROM execution_trades WHERE trade_id=$1", trade_id)
        return None if row is None else self._record(row)

    async def transition(
        self,
        trade_id: str,
        target: TradeState,
        *,
        event_type: str,
        event_payload: dict[str, Any] | None = None,
        filled_quantity: float | None = None,
        first_fill_at: datetime | None = None,
        entry_exchange_order_id: str | None = None,
        stop_client_order_id: str | None = None,
        stop_exchange_order_id: str | None = None,
        target_client_order_id: str | None = None,
        target_exchange_order_id: str | None = None,
        close_client_order_id: str | None = None,
        close_exchange_order_id: str | None = None,
    ) -> TradeRecord:
        if not trade_id.strip() or not event_type.strip():
            raise ValueError("trade_id and event_type must not be empty")
        payload = event_payload or {}
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await self._xact_lock(connection, trade_id)
                row = await connection.fetchrow(
                    "SELECT * FROM execution_trades WHERE trade_id=$1 FOR UPDATE", trade_id
                )
                if row is None:
                    raise KeyError(f"unknown trade {trade_id!r}")
                current = self._record(row)
                validate_trade_transition(current.state, target)
                next_filled = current.filled_quantity if filled_quantity is None else filled_quantity
                if not math.isfinite(next_filled) or not 0 <= next_filled <= current.quantity:
                    raise ValueError("filled_quantity is outside the trade quantity")
                if next_filled < current.filled_quantity:
                    raise ValueError("filled_quantity must be monotonic")
                if first_fill_at is not None and first_fill_at.tzinfo is None:
                    raise ValueError("first_fill_at must be timezone-aware")
                supplied_first_fill = None if first_fill_at is None else first_fill_at.astimezone(UTC)
                current_first_fill = (
                    None if current.first_fill_at is None else current.first_fill_at.astimezone(UTC)
                )
                if (
                    supplied_first_fill is not None
                    and current_first_fill is not None
                    and supplied_first_fill != current_first_fill
                ):
                    raise MessageIdentityConflict("first_fill_at is immutable once recorded")
                effective_first_fill = current_first_fill or supplied_first_fill
                if next_filled > 0 and effective_first_fill is None:
                    raise ValueError("the first non-zero fill must record first_fill_at")
                if next_filled == 0 and effective_first_fill is not None:
                    raise ValueError("first_fill_at requires a non-zero fill")
                if target is TradeState.CANCELLED and next_filled > 0:
                    raise ValueError("a partially filled trade cannot be cancelled")
                if (
                    target
                    in {
                        TradeState.PROTECTING,
                        TradeState.ACTIVE,
                        TradeState.EXITING_STOP,
                        TradeState.EXITING_TARGET,
                        TradeState.EXITING_TIMEOUT,
                        TradeState.EXITING_EMERGENCY,
                    }
                    and next_filled <= 0
                ):
                    raise ValueError(f"{target.value} requires a non-zero fill")
                try:
                    timeout_at = (
                        None
                        if effective_first_fill is None
                        else effective_first_fill + timedelta(milliseconds=current.max_holding_ms)
                    )
                except OverflowError as exc:
                    raise ValueError("max_holding_ms exceeds the supported timestamp range") from exc
                effective_entry_exchange_id = self._merge_identifier(
                    "entry_exchange_order_id",
                    current.entry_exchange_order_id,
                    entry_exchange_order_id,
                )
                effective_stop_client_id = self._merge_identifier(
                    "stop_client_order_id", current.stop_client_order_id, stop_client_order_id
                )
                effective_stop_exchange_id = self._merge_identifier(
                    "stop_exchange_order_id",
                    current.stop_exchange_order_id,
                    stop_exchange_order_id,
                )
                effective_target_client_id = self._merge_identifier(
                    "target_client_order_id", current.target_client_order_id, target_client_order_id
                )
                effective_target_exchange_id = self._merge_identifier(
                    "target_exchange_order_id",
                    current.target_exchange_order_id,
                    target_exchange_order_id,
                )
                effective_close_client_id = self._merge_identifier(
                    "close_client_order_id", current.close_client_order_id, close_client_order_id
                )
                effective_close_exchange_id = self._merge_identifier(
                    "close_exchange_order_id",
                    current.close_exchange_order_id,
                    close_exchange_order_id,
                )
                transition_payload = {
                    "event": payload,
                    "filled_quantity_hex": float(next_filled).hex(),
                    "first_fill_at": (
                        None
                        if effective_first_fill is None
                        else effective_first_fill.astimezone(UTC).isoformat()
                    ),
                    "entry_exchange_order_id": effective_entry_exchange_id,
                    "stop_client_order_id": effective_stop_client_id,
                    "stop_exchange_order_id": effective_stop_exchange_id,
                    "target_client_order_id": effective_target_client_id,
                    "target_exchange_order_id": effective_target_exchange_id,
                    "close_client_order_id": effective_close_client_id,
                    "close_exchange_order_id": effective_close_exchange_id,
                }
                event_sha = self._event_sha(
                    trade_id,
                    current.state,
                    target,
                    event_type,
                    transition_payload,
                    current.journal_head_sha256,
                )
                updated = await connection.fetchrow(
                    """UPDATE execution_trades SET
                         state=$2, filled_quantity=$3, first_fill_at=$4, timeout_at=$5,
                         entry_exchange_order_id=COALESCE($6,entry_exchange_order_id),
                         stop_client_order_id=COALESCE($7,stop_client_order_id),
                         stop_exchange_order_id=COALESCE($8,stop_exchange_order_id),
                         target_client_order_id=COALESCE($9,target_client_order_id),
                         target_exchange_order_id=COALESCE($10,target_exchange_order_id),
                         close_client_order_id=COALESCE($11,close_client_order_id),
                         close_exchange_order_id=COALESCE($12,close_exchange_order_id),
                         state_version=state_version+1, journal_head_sha256=$13, updated_at=now()
                       WHERE trade_id=$1 AND state_version=$14 RETURNING *""",
                    trade_id,
                    target.value,
                    next_filled,
                    None if effective_first_fill is None else effective_first_fill.astimezone(UTC),
                    timeout_at,
                    effective_entry_exchange_id,
                    effective_stop_client_id,
                    effective_stop_exchange_id,
                    effective_target_client_id,
                    effective_target_exchange_id,
                    effective_close_client_id,
                    effective_close_exchange_id,
                    event_sha,
                    current.state_version,
                )
                if updated is None:
                    raise RuntimeError("trade transition lost its serialized state version")
                await self._append_event(
                    connection,
                    trade_id,
                    current.state,
                    target,
                    event_type,
                    transition_payload,
                    current.journal_head_sha256,
                    event_sha,
                )
        return self._record(updated)

    async def mark_reconciled(self, trade_id: str, detail: str) -> TradeRecord:
        self._validate_text("trade_id", trade_id)
        self._validate_text("reconciliation detail", detail)
        row = await self.pool.fetchrow(
            """UPDATE execution_trades SET last_reconciled_at=now(), reconciliation_detail=$2,
                 updated_at=now() WHERE trade_id=$1 RETURNING *""",
            trade_id,
            detail[:4000],
        )
        if row is None:
            raise KeyError(f"unknown trade {trade_id!r}")
        return self._record(row)

    async def recovery_required(self, *, environment: str, account_id: str) -> list[TradeRecord]:
        self._validate_scope(environment, account_id)
        rows = await self.pool.fetch(
            """SELECT * FROM execution_trades
               WHERE environment=$1 AND account_id=$2
                 AND state NOT IN ('FLAT','CANCELLED')
               ORDER BY created_at, trade_id""",
            environment,
            account_id,
        )
        return [self._record(row) for row in rows]

    @asynccontextmanager
    async def trade_lock(self, trade_id: str) -> AsyncIterator[None]:
        """Cross-process lock used by STOP/TP/timeout race reconciliation."""
        if not trade_id.strip():
            raise ValueError("trade_id must not be empty")
        async with self.pool.acquire() as connection:
            lock_identity = f"kairos.trade-race:{trade_id}"
            await connection.execute("SELECT pg_advisory_lock(hashtextextended($1, 0))", lock_identity)
            try:
                yield
            finally:
                await connection.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", lock_identity)

    async def begin_recovery(self, *, environment: str, account_id: str, exchange: str) -> RecoveryState:
        self._validate_scope(environment, account_id, exchange)
        row = await self.pool.fetchrow(
            """INSERT INTO execution_recovery_state
               (environment,account_id,exchange,entries_blocked,recovery_epoch,started_at,completed_at,detail)
               VALUES ($1,$2,$3,TRUE,1,now(),NULL,'authoritative startup reconciliation required')
               ON CONFLICT (environment,account_id,exchange) DO UPDATE SET
                 entries_blocked=TRUE,
                 recovery_epoch=execution_recovery_state.recovery_epoch+1,
                 started_at=now(), completed_at=NULL,
                 detail='authoritative startup reconciliation required'
               RETURNING *""",
            environment,
            account_id,
            exchange,
        )
        if row is None:
            raise RuntimeError("recovery barrier initialization returned no row")
        return self._recovery_record(row)

    async def complete_recovery(
        self,
        *,
        environment: str,
        account_id: str,
        exchange: str,
        expected_epoch: int,
        detail: str,
    ) -> RecoveryState:
        self._validate_scope(environment, account_id, exchange)
        self._validate_text("recovery detail", detail)
        if expected_epoch <= 0:
            raise ValueError("expected_epoch must be positive")
        row = await self.pool.fetchrow(
            """UPDATE execution_recovery_state SET entries_blocked=FALSE, completed_at=now(), detail=$4
               WHERE environment=$1 AND account_id=$2 AND exchange=$3 AND recovery_epoch=$5
               RETURNING *""",
            environment,
            account_id,
            exchange,
            detail[:4000],
            expected_epoch,
        )
        if row is None:
            raise RuntimeError("recovery epoch is stale or recovery did not begin")
        return self._recovery_record(row)

    async def recovery_state(
        self, *, environment: str, account_id: str, exchange: str
    ) -> RecoveryState | None:
        self._validate_scope(environment, account_id, exchange)
        row = await self.pool.fetchrow(
            """SELECT * FROM execution_recovery_state
               WHERE environment=$1 AND account_id=$2 AND exchange=$3""",
            environment,
            account_id,
            exchange,
        )
        return None if row is None else self._recovery_record(row)

    async def entries_allowed(self, *, environment: str, account_id: str, exchange: str) -> bool:
        state = await self.recovery_state(environment=environment, account_id=account_id, exchange=exchange)
        return state is not None and not state.entries_blocked

    async def next_event_sequence(self, trade_id: str) -> int:
        """Reserve one monotonic public execution-event sequence for a trade."""
        self._validate_text("trade_id", trade_id)
        value = await self.pool.fetchval(
            """UPDATE execution_trades
               SET next_execution_event_seq=next_execution_event_seq+1, updated_at=now()
               WHERE trade_id=$1 RETURNING next_execution_event_seq-1""",
            trade_id,
        )
        if value is None:
            raise KeyError(f"unknown trade {trade_id!r}")
        return int(value)

    async def record_equity(
        self,
        *,
        environment: str,
        account_id: str,
        exchange: str,
        captured_at: datetime,
        equity_usd: float,
    ) -> EquityState:
        self._validate_scope(environment, account_id, exchange)
        if captured_at.tzinfo is None:
            raise ValueError("captured_at must be timezone-aware")
        if not math.isfinite(equity_usd) or equity_usd <= 0:
            raise ValueError("equity_usd must be finite and positive")
        trading_day = captured_at.astimezone(UTC).date()
        captured_utc = captured_at.astimezone(UTC)
        row = await self.pool.fetchrow(
            """INSERT INTO account_equity_state
               (environment,account_id,exchange,trading_day,
                day_start_equity_usd,peak_equity_usd,last_equity_usd,
                first_captured_at,last_captured_at)
               VALUES ($1,$2,$3,$4,$5,$5,$5,$6,$6)
               ON CONFLICT (environment,account_id,exchange,trading_day) DO UPDATE SET
                 day_start_equity_usd=CASE
                   WHEN EXCLUDED.first_captured_at < account_equity_state.first_captured_at
                   THEN EXCLUDED.day_start_equity_usd
                   ELSE account_equity_state.day_start_equity_usd END,
                 peak_equity_usd=GREATEST(account_equity_state.peak_equity_usd,$5),
                 last_equity_usd=CASE
                   WHEN EXCLUDED.last_captured_at >= account_equity_state.last_captured_at
                   THEN EXCLUDED.last_equity_usd
                   ELSE account_equity_state.last_equity_usd END,
                 first_captured_at=LEAST(
                   account_equity_state.first_captured_at,EXCLUDED.first_captured_at),
                 last_captured_at=GREATEST(
                   account_equity_state.last_captured_at,EXCLUDED.last_captured_at),
                 reconciliation_seq=account_equity_state.reconciliation_seq+1,
                 updated_at=now()
               RETURNING *""",
            environment,
            account_id,
            exchange,
            trading_day,
            equity_usd,
            captured_utc,
        )
        if row is None:
            raise RuntimeError("equity state update returned no row")
        return self._equity_record(row)

    async def verify_chain(self, trade_id: str) -> bool:
        rows = await self.pool.fetch(
            """SELECT from_state,to_state,event_type,event_payload,previous_event_sha256,event_sha256
               FROM execution_trade_events WHERE trade_id=$1 ORDER BY sequence""",
            trade_id,
        )
        previous: str | None = None
        for row in rows:
            from_state = None if row["from_state"] is None else TradeState(row["from_state"])
            expected = self._event_sha(
                trade_id,
                from_state,
                TradeState(row["to_state"]),
                row["event_type"],
                self._object(row["event_payload"]),
                previous,
            )
            if row["previous_event_sha256"] != previous or row["event_sha256"] != expected:
                return False
            previous = expected
        trade = await self.get(trade_id)
        return trade is not None and bool(rows) and trade.journal_head_sha256 == previous

    @staticmethod
    async def _xact_lock(connection: asyncpg.Connection, trade_id: str) -> None:
        await connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            f"kairos.trade-state:{trade_id}",
        )

    @staticmethod
    def _merge_identifier(name: str, current: str | None, supplied: str | None) -> str | None:
        if supplied is not None and not supplied.strip():
            raise ValueError(f"{name} must be absent or non-empty")
        if current is not None and supplied is not None and current != supplied:
            raise MessageIdentityConflict(f"{name} is immutable once recorded")
        return current or supplied

    @staticmethod
    def _validate_text(name: str, value: str) -> None:
        if not value.strip():
            raise ValueError(f"{name} must not be empty")

    @classmethod
    def _validate_scope(cls, *values: str) -> None:
        for name, value in zip(("environment", "account_id", "exchange"), values, strict=False):
            cls._validate_text(name, value)

    @classmethod
    async def _append_event(
        cls,
        connection: asyncpg.Connection,
        trade_id: str,
        from_state: TradeState | None,
        to_state: TradeState,
        event_type: str,
        payload: dict[str, Any],
        previous_sha: str | None,
        event_sha: str,
    ) -> None:
        await connection.execute(
            """INSERT INTO execution_trade_events
               (trade_id,from_state,to_state,event_type,event_payload,
                previous_event_sha256,event_sha256)
               VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7)""",
            trade_id,
            None if from_state is None else from_state.value,
            to_state.value,
            event_type,
            cls._json(payload),
            previous_sha,
            event_sha,
        )

    @staticmethod
    def _event_sha(
        trade_id: str,
        from_state: TradeState | None,
        to_state: TradeState,
        event_type: str,
        payload: dict[str, Any],
        previous_sha: str | None,
    ) -> str:
        encoded, _ = canonical_payload(
            {
                "domain": "kairos.trade-lifecycle-event.v1",
                "trade_id": trade_id,
                "from_state": None if from_state is None else from_state.value,
                "to_state": to_state.value,
                "event_type": event_type,
                "payload": payload,
                "previous_event_sha256": previous_sha,
            }
        )
        return hashlib.sha256(encoded.encode()).hexdigest()

    @staticmethod
    def _new_trade_payload(trade: NewTrade) -> dict[str, Any]:
        return {
            "trade_id": trade.trade_id,
            "strategy_intent_id": trade.strategy_intent_id,
            "risk_decision_id": trade.risk_decision_id,
            "risk_decision_payload": trade.risk_decision_payload,
            "strategy_id": trade.strategy_id,
            "strategy_revision": trade.strategy_revision,
            "trading_mode": trade.trading_mode,
            "environment": trade.environment,
            "profile": trade.profile,
            "exchange": trade.exchange,
            "account_id": trade.account_id,
            "symbol": trade.symbol,
            "venue_symbol": trade.venue_symbol,
            "side": trade.side,
            "quantity_hex": float(trade.quantity).hex(),
            "leverage_hex": float(trade.leverage).hex(),
            "stop_price_hex": float(trade.stop_price).hex(),
            "target_price_hex": float(trade.target_price).hex(),
            "entry_eligible_at": trade.entry_eligible_at.astimezone(UTC).isoformat(),
            "entry_expires_at": trade.entry_expires_at.astimezone(UTC).isoformat(),
            "max_holding_ms": trade.max_holding_ms,
            "entry_client_order_id": trade.entry_client_order_id,
        }

    @staticmethod
    def _immutable_record_payload(record: TradeRecord) -> dict[str, Any]:
        return TradeLifecycleRepository._new_trade_payload(
            NewTrade(
                trade_id=record.trade_id,
                strategy_intent_id=record.strategy_intent_id,
                risk_decision_id=record.risk_decision_id,
                risk_decision_payload=record.risk_decision_payload,
                strategy_id=record.strategy_id,
                strategy_revision=record.strategy_revision,
                trading_mode=record.trading_mode,
                environment=record.environment,
                profile=record.profile,
                exchange=record.exchange,
                account_id=record.account_id,
                symbol=record.symbol,
                venue_symbol=record.venue_symbol,
                side=record.side,
                quantity=record.quantity,
                leverage=record.leverage,
                stop_price=record.stop_price,
                target_price=record.target_price,
                entry_eligible_at=record.entry_eligible_at,
                entry_expires_at=record.entry_expires_at,
                max_holding_ms=record.max_holding_ms,
                entry_client_order_id=record.entry_client_order_id,
            )
        )

    @staticmethod
    def _record(row: asyncpg.Record) -> TradeRecord:
        risk_decision_payload = TradeLifecycleRepository._object(row["risk_decision_payload"])
        if canonical_payload(risk_decision_payload)[1] != row["risk_decision_sha256"]:
            raise MessageIdentityConflict("stored risk decision payload fingerprint does not match")
        return TradeRecord(
            trade_id=row["trade_id"],
            strategy_intent_id=row["strategy_intent_id"],
            risk_decision_id=row["risk_decision_id"],
            risk_decision_payload=risk_decision_payload,
            strategy_id=row["strategy_id"],
            strategy_revision=row["strategy_revision"],
            trading_mode=row["trading_mode"],
            environment=row["environment"],
            profile=row["profile"],
            exchange=row["exchange"],
            account_id=row["account_id"],
            symbol=row["symbol"],
            venue_symbol=row["venue_symbol"],
            side=row["side"],
            quantity=float(row["quantity"]),
            leverage=float(row["leverage"]),
            stop_price=float(row["stop_price"]),
            target_price=float(row["target_price"]),
            entry_eligible_at=row["entry_eligible_at"],
            entry_expires_at=row["entry_expires_at"],
            max_holding_ms=row["max_holding_ms"],
            state=TradeState(row["state"]),
            entry_client_order_id=row["entry_client_order_id"],
            entry_exchange_order_id=row["entry_exchange_order_id"],
            stop_client_order_id=row["stop_client_order_id"],
            stop_exchange_order_id=row["stop_exchange_order_id"],
            target_client_order_id=row["target_client_order_id"],
            target_exchange_order_id=row["target_exchange_order_id"],
            close_client_order_id=row["close_client_order_id"],
            close_exchange_order_id=row["close_exchange_order_id"],
            filled_quantity=float(row["filled_quantity"]),
            first_fill_at=row["first_fill_at"],
            timeout_at=row["timeout_at"],
            last_reconciled_at=row["last_reconciled_at"],
            reconciliation_detail=row["reconciliation_detail"],
            state_version=row["state_version"],
            journal_head_sha256=row["journal_head_sha256"],
        )

    @staticmethod
    def _recovery_record(row: asyncpg.Record) -> RecoveryState:
        return RecoveryState(
            environment=row["environment"],
            account_id=row["account_id"],
            exchange=row["exchange"],
            entries_blocked=row["entries_blocked"],
            recovery_epoch=row["recovery_epoch"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            detail=row["detail"],
        )

    @staticmethod
    def _equity_record(row: asyncpg.Record) -> EquityState:
        return EquityState(
            environment=row["environment"],
            account_id=row["account_id"],
            exchange=row["exchange"],
            trading_day=row["trading_day"],
            day_start_equity_usd=float(row["day_start_equity_usd"]),
            peak_equity_usd=float(row["peak_equity_usd"]),
            last_equity_usd=float(row["last_equity_usd"]),
            first_captured_at=row["first_captured_at"],
            last_captured_at=row["last_captured_at"],
            reconciliation_seq=row["reconciliation_seq"],
        )

    @staticmethod
    def _json(value: dict[str, Any]) -> str:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)

    @staticmethod
    def _object(value: str | dict[str, Any]) -> dict[str, Any]:
        parsed = json.loads(value) if isinstance(value, str) else value
        if not isinstance(parsed, dict):
            raise ValueError("trade event payload is not an object")
        return parsed
