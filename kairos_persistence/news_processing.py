"""Conservative durable news fences, separate from spend accounting.

A committed claim is permanent, including an unknown model/publish outcome.
No provider or bus operation runs inside these short database transactions.
This is not exactly-once external delivery and never refunds a budget reserve.
"""

from __future__ import annotations

import re

import asyncpg

_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_MAX_BATCH = 1000


def _digest(value: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ValueError("news processing identity must be a canonical SHA-256")
    return value


def _keys(values: tuple[str, ...]) -> tuple[str, ...]:
    if not isinstance(values, tuple) or len(values) > _MAX_BATCH:
        raise ValueError("news processing requires a bounded tuple of event keys")
    validated = tuple(_digest(value) for value in values)
    if len(set(validated)) != len(validated):
        raise ValueError("news processing event keys must be unique")
    return tuple(sorted(validated))


class NewsProcessingRepository:
    """Atomic reserve-before-wire fence in an explicitly admitted database.

    The scope binds the reviewed service/deployment/feed-account configuration;
    changing it is not an automatic way to retry old claims. Schema provisioning
    and runtime-role grants are separate operator steps, never performed here.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def claim_batch(
        self, scope_key: str, event_keys: tuple[str, ...], attempt_id: str
    ) -> tuple[str, ...]:
        scope_key, attempt_id = _digest(scope_key), _digest(attempt_id)
        keys = _keys(event_keys)
        if not keys:
            return ()
        async with self.pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """INSERT INTO news_processing(scope_key,event_key,attempt_id,state)
                   SELECT $1,event_key,$3,'CLAIMED'
                   FROM unnest($2::text[]) AS requested(event_key) ORDER BY event_key
                   ON CONFLICT (scope_key,event_key) DO NOTHING RETURNING event_key""",
                scope_key,
                list(keys),
                attempt_id,
            )
            claimed = tuple(sorted(row["event_key"] for row in rows))
            if len(set(claimed)) != len(claimed) or not set(claimed).issubset(keys):
                raise RuntimeError("news claim result does not match the requested batch")
        # Commit must be confirmed before the caller may contact a provider.
        return claimed

    async def complete_batch(self, scope_key: str, event_keys: tuple[str, ...], attempt_id: str) -> None:
        scope_key, attempt_id = _digest(scope_key), _digest(attempt_id)
        keys = _keys(event_keys)
        if not keys:
            return
        async with self.pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """SELECT event_key,attempt_id,state FROM news_processing
                   WHERE scope_key=$1 AND event_key=ANY($2::text[])
                   ORDER BY event_key FOR UPDATE""",
                scope_key,
                list(keys),
            )
            if tuple(row["event_key"] for row in rows) != keys or any(
                row["attempt_id"] != attempt_id or row["state"] not in {"CLAIMED", "COMPLETED"}
                for row in rows
            ):
                raise ValueError("news completion requires every exact owned claim")
            pending = tuple(row["event_key"] for row in rows if row["state"] == "CLAIMED")
            if not pending:
                return
            updated = await connection.fetch(
                """UPDATE news_processing SET state='COMPLETED',completed_at=clock_timestamp()
                   WHERE scope_key=$1 AND event_key=ANY($2::text[])
                     AND attempt_id=$3 AND state='CLAIMED' RETURNING event_key""",
                scope_key,
                list(pending),
                attempt_id,
            )
            if tuple(sorted(row["event_key"] for row in updated)) != pending:
                raise RuntimeError("news completion was not confirmed for the complete owned batch")
