"""Durable paid-source cursors and fail-closed monthly spend reservations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum

import asyncpg

from .repository import MessageIdentityConflict

_MAX_CURSOR = 2**64 - 1
_MAX_BIGINT = 2**63 - 1


class SourceBudgetExceeded(RuntimeError):
    """A reservation would cross the configured monthly provider budget."""


class UsageStatus(StrEnum):
    RESERVED = "RESERVED"
    COMMITTED = "COMMITTED"
    RELEASED = "RELEASED"


@dataclass(frozen=True, slots=True)
class SourceCursor:
    service: str
    source: str
    cursor_key: str
    cursor_value: str
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class UsageReservation:
    service: str
    source: str
    reservation_id: str
    billing_month: date
    reserved_units: int
    unit_cost_microusd: int
    status: UsageStatus
    actual_units: int | None

    @property
    def reserved_cost_microusd(self) -> int:
        return self.reserved_units * self.unit_cost_microusd

    @property
    def actual_cost_microusd(self) -> int | None:
        if self.actual_units is None:
            return None
        return self.actual_units * self.unit_cost_microusd


@dataclass(frozen=True, slots=True)
class MonthlySourceUsage:
    service: str
    source: str
    billing_month: date
    committed_units: int
    committed_cost_microusd: int
    reserved_units: int
    reserved_cost_microusd: int

    @property
    def budgeted_cost_microusd(self) -> int:
        return self.committed_cost_microusd + self.reserved_cost_microusd


class SourceStateRepository:
    """Persist monotonic source cursors and reserve paid API capacity atomically."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def get_cursor(self, service: str, source: str, cursor_key: str) -> SourceCursor | None:
        identity = self._identity(service, source, cursor_key)
        row = await self.pool.fetchrow(
            """SELECT service, source, cursor_key, cursor_value, updated_at
               FROM source_cursors WHERE service=$1 AND source=$2 AND cursor_key=$3""",
            *identity,
        )
        return None if row is None else self._cursor(row)

    async def advance_cursor(
        self,
        service: str,
        source: str,
        cursor_key: str,
        cursor_value: str,
    ) -> bool:
        identity = self._identity(service, source, cursor_key)
        normalized, numeric = self._cursor_value(cursor_value)
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    "\x1f".join(identity),
                )
                row = await connection.fetchrow(
                    """SELECT cursor_value, cursor_numeric FROM source_cursors
                       WHERE service=$1 AND source=$2 AND cursor_key=$3 FOR UPDATE""",
                    *identity,
                )
                if row is not None:
                    current = int(row["cursor_numeric"])
                    if numeric < current:
                        raise ValueError(
                            f"source cursor regression for {source}/{cursor_key}: {numeric} < {current}"
                        )
                    if numeric == current:
                        if row["cursor_value"] != normalized:
                            raise MessageIdentityConflict(
                                "equal source cursor has a different representation"
                            )
                        return False
                    await connection.execute(
                        """UPDATE source_cursors
                           SET cursor_value=$4, cursor_numeric=$5, updated_at=now()
                           WHERE service=$1 AND source=$2 AND cursor_key=$3""",
                        *identity,
                        normalized,
                        Decimal(numeric),
                    )
                    return True
                await connection.execute(
                    """INSERT INTO source_cursors
                       (service, source, cursor_key, cursor_value, cursor_numeric)
                       VALUES ($1,$2,$3,$4,$5)""",
                    *identity,
                    normalized,
                    Decimal(numeric),
                )
                return True

    async def reserve_usage(
        self,
        *,
        service: str,
        source: str,
        reservation_id: str,
        reserved_units: int,
        unit_cost_microusd: int,
        monthly_budget_microusd: int,
        requested_at: datetime | None = None,
    ) -> UsageReservation:
        service, source, reservation_id = self._identity(service, source, reservation_id)
        self._positive_int(reserved_units, "reserved_units")
        self._nonnegative_int(unit_cost_microusd, "unit_cost_microusd")
        self._nonnegative_int(monthly_budget_microusd, "monthly_budget_microusd")
        reserved_cost = self._cost(reserved_units, unit_cost_microusd)
        requested = self._aware(requested_at or datetime.now(UTC))
        month = date(requested.year, requested.month, 1)
        lock_key = "\x1f".join((service, source, month.isoformat()))
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    lock_key,
                )
                existing = await connection.fetchrow(
                    """SELECT * FROM source_usage_reservations
                       WHERE service=$1 AND source=$2 AND reservation_id=$3 FOR UPDATE""",
                    service,
                    source,
                    reservation_id,
                )
                if existing is not None:
                    reservation = self._reservation(existing)
                    if (
                        reservation.billing_month != month
                        or reservation.reserved_units != reserved_units
                        or reservation.unit_cost_microusd != unit_cost_microusd
                    ):
                        raise MessageIdentityConflict(
                            f"source usage reservation {reservation_id!r} was reused with different content"
                        )
                    return reservation
                budgeted = int(
                    await connection.fetchval(
                        """SELECT COALESCE(sum(
                             CASE status
                               WHEN 'COMMITTED' THEN actual_cost_microusd
                               WHEN 'RESERVED' THEN reserved_cost_microusd
                               ELSE 0
                             END), 0)
                           FROM source_usage_reservations
                           WHERE service=$1 AND source=$2 AND billing_month=$3""",
                        service,
                        source,
                        month,
                    )
                )
                if budgeted + reserved_cost > monthly_budget_microusd:
                    raise SourceBudgetExceeded(
                        f"monthly {source} budget would be exceeded by the requested reservation"
                    )
                row = await connection.fetchrow(
                    """INSERT INTO source_usage_reservations
                       (service, source, reservation_id, billing_month, reserved_units,
                        unit_cost_microusd, reserved_cost_microusd, status, reserved_at)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,'RESERVED',$8) RETURNING *""",
                    service,
                    source,
                    reservation_id,
                    month,
                    reserved_units,
                    unit_cost_microusd,
                    reserved_cost,
                    requested,
                )
        if row is None:
            raise RuntimeError("source usage reservation returned no row")
        return self._reservation(row)

    async def commit_usage(
        self, service: str, source: str, reservation_id: str, actual_units: int
    ) -> UsageReservation:
        identity = self._identity(service, source, reservation_id)
        self._nonnegative_int(actual_units, "actual_units")
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """SELECT * FROM source_usage_reservations
                       WHERE service=$1 AND source=$2 AND reservation_id=$3 FOR UPDATE""",
                    *identity,
                )
                if row is None:
                    raise KeyError(f"source usage reservation {reservation_id!r} does not exist")
                reservation = self._reservation(row)
                if actual_units > reservation.reserved_units:
                    raise ValueError("actual_units exceeds the durable reservation")
                if reservation.status is UsageStatus.RELEASED:
                    raise RuntimeError("released source usage cannot be committed")
                if reservation.status is UsageStatus.COMMITTED:
                    if reservation.actual_units != actual_units:
                        raise MessageIdentityConflict("committed source usage was reused with a new amount")
                    return reservation
                updated = await connection.fetchrow(
                    """UPDATE source_usage_reservations
                       SET status='COMMITTED', actual_units=$4::integer,
                           actual_cost_microusd=$4::integer * unit_cost_microusd,
                           finalized_at=now()
                       WHERE service=$1 AND source=$2 AND reservation_id=$3 RETURNING *""",
                    *identity,
                    actual_units,
                )
        if updated is None:
            raise RuntimeError("source usage commit returned no row")
        return self._reservation(updated)

    async def release_usage(self, service: str, source: str, reservation_id: str) -> UsageReservation:
        identity = self._identity(service, source, reservation_id)
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """SELECT * FROM source_usage_reservations
                       WHERE service=$1 AND source=$2 AND reservation_id=$3 FOR UPDATE""",
                    *identity,
                )
                if row is None:
                    raise KeyError(f"source usage reservation {reservation_id!r} does not exist")
                reservation = self._reservation(row)
                if reservation.status is UsageStatus.COMMITTED:
                    raise RuntimeError("committed source usage cannot be released")
                if reservation.status is UsageStatus.RELEASED:
                    return reservation
                updated = await connection.fetchrow(
                    """UPDATE source_usage_reservations
                       SET status='RELEASED', finalized_at=now()
                       WHERE service=$1 AND source=$2 AND reservation_id=$3 RETURNING *""",
                    *identity,
                )
        if updated is None:
            raise RuntimeError("source usage release returned no row")
        return self._reservation(updated)

    async def monthly_usage(
        self,
        service: str,
        source: str,
        at: datetime | None = None,
    ) -> MonthlySourceUsage:
        service, source, _unused = self._identity(service, source, "monthly")
        instant = self._aware(at or datetime.now(UTC))
        month = date(instant.year, instant.month, 1)
        row = await self.pool.fetchrow(
            """SELECT
                 COALESCE(sum(actual_units) FILTER (WHERE status='COMMITTED'), 0)::bigint
                   AS committed_units,
                 COALESCE(sum(actual_cost_microusd) FILTER (WHERE status='COMMITTED'), 0)::bigint
                   AS committed_cost_microusd,
                 COALESCE(sum(reserved_units) FILTER (WHERE status='RESERVED'), 0)::bigint
                   AS reserved_units,
                 COALESCE(sum(reserved_cost_microusd) FILTER (WHERE status='RESERVED'), 0)::bigint
                   AS reserved_cost_microusd
               FROM source_usage_reservations
               WHERE service=$1 AND source=$2 AND billing_month=$3""",
            service,
            source,
            month,
        )
        if row is None:
            raise RuntimeError("monthly source usage query returned no row")
        return MonthlySourceUsage(
            service=service,
            source=source,
            billing_month=month,
            committed_units=int(row["committed_units"]),
            committed_cost_microusd=int(row["committed_cost_microusd"]),
            reserved_units=int(row["reserved_units"]),
            reserved_cost_microusd=int(row["reserved_cost_microusd"]),
        )

    @staticmethod
    def _identity(service: str, source: str, key: str) -> tuple[str, str, str]:
        values = tuple(value.strip() for value in (service, source, key))
        if not all(values):
            raise ValueError("service, source and key must not be empty")
        if any(len(value) > 255 for value in values):
            raise ValueError("source state identity values must not exceed 255 characters")
        return values  # type: ignore[return-value]

    @staticmethod
    def _cursor_value(value: str) -> tuple[str, int]:
        if not isinstance(value, str) or not value or not value.isascii() or not value.isdigit():
            raise ValueError("source cursor must be an unsigned decimal string")
        if value != "0" and value.startswith("0"):
            raise ValueError("source cursor must use canonical decimal form")
        numeric = int(value)
        if numeric > _MAX_CURSOR:
            raise ValueError("source cursor exceeds unsigned 64-bit range")
        return value, numeric

    @staticmethod
    def _positive_int(value: int, label: str) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{label} must be a positive integer")

    @staticmethod
    def _nonnegative_int(value: int, label: str) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{label} must be a non-negative integer")

    @staticmethod
    def _cost(units: int, unit_cost_microusd: int) -> int:
        cost = units * unit_cost_microusd
        if cost > _MAX_BIGINT:
            raise ValueError("source usage cost exceeds PostgreSQL BIGINT range")
        return cost

    @staticmethod
    def _aware(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("source usage timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @staticmethod
    def _cursor(row: asyncpg.Record) -> SourceCursor:
        return SourceCursor(
            service=row["service"],
            source=row["source"],
            cursor_key=row["cursor_key"],
            cursor_value=row["cursor_value"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _reservation(row: asyncpg.Record) -> UsageReservation:
        return UsageReservation(
            service=row["service"],
            source=row["source"],
            reservation_id=row["reservation_id"],
            billing_month=row["billing_month"],
            reserved_units=int(row["reserved_units"]),
            unit_cost_microusd=int(row["unit_cost_microusd"]),
            status=UsageStatus(row["status"]),
            actual_units=None if row["actual_units"] is None else int(row["actual_units"]),
        )
