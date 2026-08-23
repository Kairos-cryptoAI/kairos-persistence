"""Durable, remote-account scoped EVEDEX mutation admission."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import asyncpg

DEFAULT_MUTATION_CAPACITY = 30
DEFAULT_MUTATION_WINDOW_MS = 60_000
DEFAULT_COMPENSATION_RESERVE = 4


def _identifier(value: str, name: str) -> str:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a normalized non-empty string")
    return value


@dataclass(frozen=True, slots=True)
class ExecutionMutationReservation:
    environment: str
    account_id: str
    exchange: str
    effect_id: str
    operation: str
    compensation: bool
    occurred_at: datetime | None
    granted: bool
    replay: bool
    remaining: int


class ExecutionMutationBudgetRepository:
    """Serialize and bound all mutations for one authoritative venue account."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def reserve(
        self,
        *,
        environment: str,
        account_id: str,
        exchange: str,
        effect_id: str,
        operation: str,
        compensation: bool,
        capacity: int = DEFAULT_MUTATION_CAPACITY,
        window_ms: int = DEFAULT_MUTATION_WINDOW_MS,
        compensation_reserve: int = DEFAULT_COMPENSATION_RESERVE,
    ) -> ExecutionMutationReservation:
        """Reserve one durable slot, or replay/deny without consuming another."""

        environment = _identifier(environment, "environment")
        account_id = _identifier(account_id, "account_id")
        exchange = _identifier(exchange, "exchange")
        effect_id = _identifier(effect_id, "effect_id")
        operation = _identifier(operation, "operation")
        if len(effect_id) > 256 or len(operation) > 64:
            raise ValueError("effect_id/operation exceed their maximum length")
        if not isinstance(compensation, bool):
            raise ValueError("compensation must be boolean")
        if (
            isinstance(capacity, bool)
            or not isinstance(capacity, int)
            or capacity <= 0
            or capacity > DEFAULT_MUTATION_CAPACITY
        ):
            raise ValueError("capacity must be an integer in [1, 30]")
        if (
            isinstance(window_ms, bool)
            or not isinstance(window_ms, int)
            or window_ms < DEFAULT_MUTATION_WINDOW_MS
        ):
            raise ValueError("window_ms must be an integer of at least 60000")
        if (
            isinstance(compensation_reserve, bool)
            or not isinstance(compensation_reserve, int)
            or compensation_reserve < DEFAULT_COMPENSATION_RESERVE
            or compensation_reserve >= capacity
        ):
            raise ValueError("compensation_reserve must be at least 4 and below capacity")

        scope = f"kairos.execution-mutation-budget:{environment}:{account_id}:{exchange}"
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    scope,
                )
                await connection.execute(
                    """INSERT INTO execution_mutation_budget_scopes
                           (environment,account_id,exchange,capacity,window_ms,compensation_reserve)
                       VALUES ($1,$2,$3,$4,$5,$6)
                       ON CONFLICT (environment,account_id,exchange) DO NOTHING""",
                    environment,
                    account_id,
                    exchange,
                    capacity,
                    window_ms,
                    compensation_reserve,
                )
                configured = await connection.fetchrow(
                    """SELECT capacity,window_ms,compensation_reserve
                         FROM execution_mutation_budget_scopes
                        WHERE environment=$1 AND account_id=$2 AND exchange=$3""",
                    environment,
                    account_id,
                    exchange,
                )
                if configured is None:
                    raise RuntimeError("mutation budget scope disappeared while locked")
                if (
                    configured["capacity"] != capacity
                    or configured["window_ms"] != window_ms
                    or configured["compensation_reserve"] != compensation_reserve
                ):
                    raise ValueError("mutation budget configuration conflicts with durable scope")

                existing = await connection.fetchrow(
                    """SELECT operation,compensation,occurred_at
                         FROM execution_mutation_reservations
                        WHERE environment=$1 AND account_id=$2 AND exchange=$3 AND effect_id=$4""",
                    environment,
                    account_id,
                    exchange,
                    effect_id,
                )
                if existing is not None:
                    if existing["operation"] != operation or existing["compensation"] != compensation:
                        raise ValueError("effect_id was reused with different mutation semantics")
                    used = await self._used(connection, environment, account_id, exchange, window_ms)
                    return ExecutionMutationReservation(
                        environment=environment,
                        account_id=account_id,
                        exchange=exchange,
                        effect_id=effect_id,
                        operation=operation,
                        compensation=compensation,
                        occurred_at=existing["occurred_at"],
                        granted=True,
                        replay=True,
                        remaining=max(capacity - used, 0),
                    )

                used = await self._used(connection, environment, account_id, exchange, window_ms)
                remaining_before = max(capacity - used, 0)
                minimum_before = 1 if compensation else compensation_reserve + 1
                if remaining_before < minimum_before:
                    return ExecutionMutationReservation(
                        environment=environment,
                        account_id=account_id,
                        exchange=exchange,
                        effect_id=effect_id,
                        operation=operation,
                        compensation=compensation,
                        occurred_at=None,
                        granted=False,
                        replay=False,
                        remaining=remaining_before,
                    )
                row = await connection.fetchrow(
                    """INSERT INTO execution_mutation_reservations
                           (environment,account_id,exchange,effect_id,operation,compensation)
                       VALUES ($1,$2,$3,$4,$5,$6)
                       RETURNING occurred_at""",
                    environment,
                    account_id,
                    exchange,
                    effect_id,
                    operation,
                    compensation,
                )
                if row is None:
                    raise RuntimeError("mutation reservation insert returned no row")
                return ExecutionMutationReservation(
                    environment=environment,
                    account_id=account_id,
                    exchange=exchange,
                    effect_id=effect_id,
                    operation=operation,
                    compensation=compensation,
                    occurred_at=row["occurred_at"],
                    granted=True,
                    replay=False,
                    remaining=remaining_before - 1,
                )

    @staticmethod
    async def _used(
        connection: asyncpg.Connection,
        environment: str,
        account_id: str,
        exchange: str,
        window_ms: int,
    ) -> int:
        return int(
            await connection.fetchval(
                """SELECT count(*) FROM execution_mutation_reservations
                    WHERE environment=$1 AND account_id=$2 AND exchange=$3
                      AND occurred_at > clock_timestamp()
                          - ($4::double precision * interval '1 millisecond')""",
                environment,
                account_id,
                exchange,
                window_ms,
            )
        )
