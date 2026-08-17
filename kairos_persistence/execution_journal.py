"""Tamper-evident state journal for non-transactional exchange mutations."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Any

import asyncpg

from .repository import MessageIdentityConflict
from .runtime import canonical_payload


class EffectType(StrEnum):
    PLACE_ORDER = "PLACE_ORDER"
    CLOSE_POSITION = "CLOSE_POSITION"
    PROTECTIVE_STOP = "PROTECTIVE_STOP"
    CANCEL_ORDER = "CANCEL_ORDER"
    SET_LEVERAGE = "SET_LEVERAGE"


class EffectStatus(StrEnum):
    PREPARED = "PREPARED"
    CONFIRMED = "CONFIRMED"
    RECONCILED = "RECONCILED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class ExecutionEffect:
    effect_key: str
    effect_type: EffectType
    exchange: str
    symbol: str
    client_order_id: str | None
    request_sha256: str
    request_payload: dict[str, Any]
    status: EffectStatus
    exchange_effect_id: str | None
    response_payload: dict[str, Any] | None
    error: str | None
    journal_head_sha256: str


@dataclass(frozen=True, slots=True)
class EffectPreparation:
    effect: ExecutionEffect
    created: bool


class ExecutionJournalRepository:
    """Serialize effect transitions and preserve a per-effect SHA-256 chain."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def prepare(
        self,
        *,
        effect_key: str,
        effect_type: EffectType,
        exchange: str,
        symbol: str,
        client_order_id: str | None,
        request_payload: dict[str, Any],
        recovery_delay: timedelta = timedelta(minutes=2),
    ) -> EffectPreparation:
        self._validate_identity(effect_key, exchange, symbol, client_order_id)
        if recovery_delay < timedelta(0):
            raise ValueError("recovery_delay must not be negative")
        _request_json, request_sha256 = canonical_payload(request_payload)
        event_payload = {
            "effect_type": effect_type.value,
            "exchange": exchange,
            "symbol": symbol,
            "client_order_id": client_order_id,
            "request_sha256": request_sha256,
        }
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # Row locks cannot serialize the first insertion because no row
                # exists yet. A stable PostgreSQL hash lock closes that race.
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    effect_key,
                )
                existing = await connection.fetchrow(
                    "SELECT * FROM execution_effects WHERE effect_key=$1 FOR UPDATE",
                    effect_key,
                )
                if existing is not None:
                    effect = self._record(existing)
                    if (
                        effect.effect_type is not effect_type
                        or effect.exchange != exchange
                        or effect.symbol != symbol
                        or effect.client_order_id != client_order_id
                        or effect.request_sha256 != request_sha256
                    ):
                        raise MessageIdentityConflict(
                            f"execution effect {effect_key!r} was reused with different immutable content"
                        )
                    return EffectPreparation(effect=effect, created=False)
                event_sha256 = self._event_sha(effect_key, EffectStatus.PREPARED, event_payload, None)
                row = await connection.fetchrow(
                    """INSERT INTO execution_effects
                       (effect_key, effect_type, exchange, symbol, client_order_id,
                        request_sha256, request_payload, status, journal_head_sha256,
                        recovery_after)
                       VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,'PREPARED',$8,now()+$9::interval)
                       RETURNING *""",
                    effect_key,
                    effect_type.value,
                    exchange,
                    symbol,
                    client_order_id,
                    request_sha256,
                    self._json(request_payload),
                    event_sha256,
                    recovery_delay,
                )
                await self._append_event(
                    connection,
                    effect_key,
                    EffectStatus.PREPARED,
                    event_payload,
                    previous_sha256=None,
                    event_sha256=event_sha256,
                )
        if row is None:  # defensive: INSERT ... RETURNING must return one row
            raise RuntimeError("execution journal prepare returned no row")
        return EffectPreparation(effect=self._record(row), created=True)

    async def confirm(
        self,
        effect_key: str,
        *,
        exchange_effect_id: str,
        response_payload: dict[str, Any],
    ) -> ExecutionEffect:
        if not exchange_effect_id.strip():
            raise ValueError("exchange_effect_id must not be empty")
        return await self._transition(
            effect_key,
            EffectStatus.CONFIRMED,
            exchange_effect_id=exchange_effect_id,
            response_payload=response_payload,
        )

    async def reconcile(
        self,
        effect_key: str,
        *,
        exchange_effect_id: str | None = None,
        response_payload: dict[str, Any] | None = None,
    ) -> ExecutionEffect:
        return await self._transition(
            effect_key,
            EffectStatus.RECONCILED,
            exchange_effect_id=exchange_effect_id,
            response_payload=response_payload,
        )

    async def fail(self, effect_key: str, error: str) -> ExecutionEffect:
        if not error.strip():
            raise ValueError("journal failure detail must not be empty")
        return await self._transition(effect_key, EffectStatus.FAILED, error=error[:4000])

    async def get(self, effect_key: str) -> ExecutionEffect | None:
        row = await self.pool.fetchrow("SELECT * FROM execution_effects WHERE effect_key=$1", effect_key)
        return None if row is None else self._record(row)

    async def recovery_required(self, *, exchange: str | None = None) -> list[ExecutionEffect]:
        if exchange is None:
            rows = await self.pool.fetch(
                """SELECT * FROM execution_effects
                   WHERE status IN ('PREPARED','FAILED') AND recovery_after <= now()
                   ORDER BY prepared_at, effect_key"""
            )
        else:
            rows = await self.pool.fetch(
                """SELECT * FROM execution_effects
                   WHERE status IN ('PREPARED','FAILED') AND exchange=$1
                     AND recovery_after <= now()
                   ORDER BY prepared_at, effect_key""",
                exchange,
            )
        return [self._record(row) for row in rows]

    @asynccontextmanager
    async def recovery_lock(self, effect_key: str) -> AsyncIterator[None]:
        """Hold a cross-process session lock while reconciling one external effect."""
        if not effect_key.strip():
            raise ValueError("effect_key must not be empty")
        async with self.pool.acquire() as connection:
            await connection.execute(
                "SELECT pg_advisory_lock(hashtextextended($1, 0))",
                effect_key,
            )
            try:
                yield
            finally:
                await connection.execute(
                    "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                    effect_key,
                )

    async def verify_chain(self, effect_key: str) -> bool:
        rows = await self.pool.fetch(
            """SELECT phase, event_payload, previous_event_sha256, event_sha256
               FROM execution_effect_events WHERE effect_key=$1 ORDER BY sequence""",
            effect_key,
        )
        previous: str | None = None
        for row in rows:
            payload = self._object(row["event_payload"])
            if row["previous_event_sha256"] != previous:
                return False
            expected = self._event_sha(effect_key, EffectStatus(row["phase"]), payload, previous)
            if row["event_sha256"] != expected:
                return False
            previous = expected
        effect = await self.get(effect_key)
        return effect is not None and bool(rows) and effect.journal_head_sha256 == previous

    async def _transition(
        self,
        effect_key: str,
        new_status: EffectStatus,
        *,
        exchange_effect_id: str | None = None,
        response_payload: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> ExecutionEffect:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    "SELECT * FROM execution_effects WHERE effect_key=$1 FOR UPDATE",
                    effect_key,
                )
                if row is None:
                    raise KeyError(f"execution effect {effect_key!r} was not prepared")
                current = self._record(row)
                self._validate_transition(current.status, new_status)
                effective_exchange_id = exchange_effect_id or current.exchange_effect_id
                effective_response = (
                    response_payload if response_payload is not None else current.response_payload
                )
                event_payload = {
                    "exchange_effect_id": effective_exchange_id,
                    "response_payload": effective_response,
                    "error": error,
                }
                if current.status is new_status:
                    if (
                        current.exchange_effect_id != effective_exchange_id
                        or current.response_payload != effective_response
                        or current.error != error
                    ):
                        raise MessageIdentityConflict(
                            f"execution effect {effect_key!r} transition payload changed"
                        )
                    return current
                event_sha256 = self._event_sha(
                    effect_key,
                    new_status,
                    event_payload,
                    current.journal_head_sha256,
                )
                updated = await connection.fetchrow(
                    """UPDATE execution_effects SET
                         status=$2, exchange_effect_id=$3, response_payload=$4::jsonb,
                         error=$5, journal_head_sha256=$6, updated_at=now(),
                         confirmed_at=CASE WHEN $2='CONFIRMED' THEN now() ELSE confirmed_at END,
                         reconciled_at=CASE WHEN $2='RECONCILED' THEN now() ELSE reconciled_at END
                       WHERE effect_key=$1 RETURNING *""",
                    effect_key,
                    new_status.value,
                    effective_exchange_id,
                    None if effective_response is None else self._json(effective_response),
                    error,
                    event_sha256,
                )
                await self._append_event(
                    connection,
                    effect_key,
                    new_status,
                    event_payload,
                    previous_sha256=current.journal_head_sha256,
                    event_sha256=event_sha256,
                )
        if updated is None:  # defensive: the row is locked in this transaction
            raise RuntimeError("execution journal transition returned no row")
        return self._record(updated)

    @staticmethod
    def _validate_transition(current: EffectStatus, new: EffectStatus) -> None:
        allowed = {
            EffectStatus.PREPARED: {
                EffectStatus.PREPARED,
                EffectStatus.CONFIRMED,
                EffectStatus.RECONCILED,
                EffectStatus.FAILED,
            },
            EffectStatus.CONFIRMED: {EffectStatus.CONFIRMED, EffectStatus.RECONCILED},
            EffectStatus.RECONCILED: {EffectStatus.RECONCILED},
            EffectStatus.FAILED: {EffectStatus.FAILED, EffectStatus.RECONCILED},
        }
        if new not in allowed[current]:
            raise RuntimeError(f"invalid execution effect transition {current.value}->{new.value}")

    @staticmethod
    def _validate_identity(
        effect_key: str,
        exchange: str,
        symbol: str,
        client_order_id: str | None,
    ) -> None:
        if not effect_key.strip() or not exchange.strip() or not symbol.strip():
            raise ValueError("effect key, exchange and symbol must not be empty")
        if client_order_id is not None and not client_order_id.strip():
            raise ValueError("client_order_id must be absent or non-empty")

    @staticmethod
    def _event_sha(
        effect_key: str,
        phase: EffectStatus,
        payload: dict[str, Any],
        previous_sha256: str | None,
    ) -> str:
        encoded, _digest = canonical_payload(
            {
                "domain": "kairos.execution-effect-event.v1",
                "effect_key": effect_key,
                "phase": phase.value,
                "payload": payload,
                "previous_event_sha256": previous_sha256,
            }
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @staticmethod
    async def _append_event(
        connection: asyncpg.Connection,
        effect_key: str,
        phase: EffectStatus,
        payload: dict[str, Any],
        *,
        previous_sha256: str | None,
        event_sha256: str,
    ) -> None:
        await connection.execute(
            """INSERT INTO execution_effect_events
               (effect_key, phase, event_payload, previous_event_sha256, event_sha256)
               VALUES ($1,$2,$3::jsonb,$4,$5)""",
            effect_key,
            phase.value,
            ExecutionJournalRepository._json(payload),
            previous_sha256,
            event_sha256,
        )

    @classmethod
    def _record(cls, row: asyncpg.Record) -> ExecutionEffect:
        return ExecutionEffect(
            effect_key=row["effect_key"],
            effect_type=EffectType(row["effect_type"]),
            exchange=row["exchange"],
            symbol=row["symbol"],
            client_order_id=row["client_order_id"],
            request_sha256=row["request_sha256"],
            request_payload=cls._object(row["request_payload"]),
            status=EffectStatus(row["status"]),
            exchange_effect_id=row["exchange_effect_id"],
            response_payload=(
                None if row["response_payload"] is None else cls._object(row["response_payload"])
            ),
            error=row["error"],
            journal_head_sha256=row["journal_head_sha256"],
        )

    @staticmethod
    def _json(value: dict[str, Any]) -> str:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )

    @staticmethod
    def _object(value: str | dict[str, Any]) -> dict[str, Any]:
        parsed = json.loads(value) if isinstance(value, str) else value
        if not isinstance(parsed, dict):
            raise ValueError("execution journal payload is not an object")
        return parsed
