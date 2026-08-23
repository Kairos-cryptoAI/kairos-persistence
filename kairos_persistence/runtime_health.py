"""Durable, non-secret execution authentication and rate-limit health."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


def _identifier(value: str, name: str) -> str:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a normalized non-empty string")
    return value


@dataclass(frozen=True, slots=True)
class ExecutionRuntimeHealth:
    environment: str
    account_id: str
    exchange: str
    observed_at: datetime
    auth_age_ms: int
    auth_expires_in_ms: int | None
    local_mutation_reserve: int
    local_mutation_capacity: int
    local_mutation_compensation_reserve: int
    local_mutation_window_ms: int
    venue_rate_limit_observable: bool
    venue_rate_limit_reserve: int | None

    def validate(self) -> None:
        _identifier(self.environment, "environment")
        _identifier(self.account_id, "account_id")
        _identifier(self.exchange, "exchange")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        for name, value in {
            "auth_age_ms": self.auth_age_ms,
            "local_mutation_reserve": self.local_mutation_reserve,
            "local_mutation_compensation_reserve": self.local_mutation_compensation_reserve,
        }.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        for name, value in {
            "local_mutation_capacity": self.local_mutation_capacity,
            "local_mutation_window_ms": self.local_mutation_window_ms,
        }.items():
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.local_mutation_reserve > self.local_mutation_capacity:
            raise ValueError("local mutation reserve cannot exceed capacity")
        if self.local_mutation_compensation_reserve > self.local_mutation_capacity:
            raise ValueError("local compensation reserve cannot exceed capacity")
        optional_values: tuple[tuple[str, int | None], ...] = (
            ("auth_expires_in_ms", self.auth_expires_in_ms),
            ("venue_rate_limit_reserve", self.venue_rate_limit_reserve),
        )
        for optional_name, optional_value in optional_values:
            if optional_value is not None and (
                isinstance(optional_value, bool) or not isinstance(optional_value, int) or optional_value < 0
            ):
                raise ValueError(f"{optional_name} must be null or a non-negative integer")
        if not isinstance(self.venue_rate_limit_observable, bool):
            raise ValueError("venue_rate_limit_observable must be boolean")


class ExecutionRuntimeHealthRepository:
    """Monotonic latest-value store keyed by execution environment/account."""

    def __init__(self, pool) -> None:
        self.pool = pool

    async def record(self, health: ExecutionRuntimeHealth) -> bool:
        health.validate()
        result = await self.pool.execute(
            """INSERT INTO execution_runtime_health (
                   environment, account_id, exchange, observed_at,
                   auth_age_ms, auth_expires_in_ms,
                   local_mutation_reserve, local_mutation_capacity,
                   local_mutation_compensation_reserve,
                   local_mutation_window_ms, venue_rate_limit_observable,
                   venue_rate_limit_reserve
               ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
               ON CONFLICT (environment, account_id, exchange) DO UPDATE SET
                   observed_at=EXCLUDED.observed_at,
                   auth_age_ms=EXCLUDED.auth_age_ms,
                   auth_expires_in_ms=EXCLUDED.auth_expires_in_ms,
                   local_mutation_reserve=EXCLUDED.local_mutation_reserve,
                   local_mutation_capacity=EXCLUDED.local_mutation_capacity,
                   local_mutation_compensation_reserve=
                       EXCLUDED.local_mutation_compensation_reserve,
                   local_mutation_window_ms=EXCLUDED.local_mutation_window_ms,
                   venue_rate_limit_observable=EXCLUDED.venue_rate_limit_observable,
                   venue_rate_limit_reserve=EXCLUDED.venue_rate_limit_reserve,
                   updated_at=now()
               WHERE EXCLUDED.observed_at >= execution_runtime_health.observed_at""",
            health.environment,
            health.account_id,
            health.exchange,
            health.observed_at,
            health.auth_age_ms,
            health.auth_expires_in_ms,
            health.local_mutation_reserve,
            health.local_mutation_capacity,
            health.local_mutation_compensation_reserve,
            health.local_mutation_window_ms,
            health.venue_rate_limit_observable,
            health.venue_rate_limit_reserve,
        )
        return result.endswith(" 1")

    async def latest(
        self, *, environment: str, account_id: str, exchange: str
    ) -> ExecutionRuntimeHealth | None:
        row = await self.pool.fetchrow(
            """SELECT environment, account_id, exchange, observed_at,
                      auth_age_ms, auth_expires_in_ms,
                      local_mutation_reserve, local_mutation_capacity,
                      local_mutation_compensation_reserve,
                      local_mutation_window_ms, venue_rate_limit_observable,
                      venue_rate_limit_reserve
                 FROM execution_runtime_health
                WHERE environment=$1 AND account_id=$2 AND exchange=$3""",
            _identifier(environment, "environment"),
            _identifier(account_id, "account_id"),
            _identifier(exchange, "exchange"),
        )
        if row is None:
            return None
        return ExecutionRuntimeHealth(**dict(row))
