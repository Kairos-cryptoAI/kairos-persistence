"""No-migration, no-dispatch durable writer for explicitly offline maintenance."""

from __future__ import annotations

from typing import Any

from kairos_core.bus.base import Publishable
from kairos_core.contracts.base import KairosMessage

from .config import PersistenceSettings
from .database import Database
from .database_target import connect_verified_database
from .repository import AuditRepository
from .runtime import canonical_payload

SCHEMA_MIGRATION_LOCK_KEY = 4_907_627_681_104_115_019


class OfflineWriterError(RuntimeError):
    """A fail-closed operational precondition for an offline maintenance writer."""


def _payload(message: Publishable) -> dict[str, Any]:
    if isinstance(message, KairosMessage):
        return message.to_payload()
    if isinstance(message, dict):
        return message
    raise TypeError(f"cannot append object of type {type(message)!r}")


class OfflineDurableWriter:
    """Atomically append audit/outbox facts without migrations or transport activity.

    This is intentionally not a :class:`DurableMessageBus`: it owns no Redis
    transport, starts no dispatcher and never calls ``Database.migrate``. Its
    one held PostgreSQL connection owns the same advisory producer lease used
    by live Quant so a maintenance run and a live producer cannot coexist.
    """

    def __init__(
        self,
        *,
        service_name: str,
        expected_database_name: str,
        expected_schema_versions: tuple[str, ...],
        settings: PersistenceSettings | None = None,
        database: Database | None = None,
    ) -> None:
        if not isinstance(service_name, str) or not service_name.strip():
            raise ValueError("service_name must not be empty")
        if (
            not expected_schema_versions
            or tuple(sorted(expected_schema_versions)) != expected_schema_versions
        ):
            raise ValueError("expected schema versions must be a non-empty sorted tuple")
        self.service_name = service_name.strip()
        self.expected_database_name = expected_database_name
        self.expected_schema_versions = expected_schema_versions
        self.settings = settings or PersistenceSettings()
        self.database = database or Database(self.settings)
        self.repository: AuditRepository | None = None
        self._connection: Any | None = None
        self._lease_acquired = False
        self._schema_guard_acquired = False
        self._started = False
        self._closing = False

    async def start(self) -> None:
        """Verify the exact preserved schema and acquire the exclusive producer lease."""
        if self._started:
            return
        if self._closing:
            raise OfflineWriterError("offline writer is closing")
        try:
            await connect_verified_database(self.database, self.expected_database_name)
            self._connection = await self.database.pool.acquire()
            self._schema_guard_acquired = bool(
                await self._connection.fetchval("SELECT pg_try_advisory_lock($1)", SCHEMA_MIGRATION_LOCK_KEY)
            )
            if not self._schema_guard_acquired:
                raise OfflineWriterError("schema migration is already running")
            versions = tuple(
                str(row["version"])
                for row in await self.database.pool.fetch(
                    "SELECT version FROM schema_migrations ORDER BY version"
                )
            )
            if versions != self.expected_schema_versions:
                raise OfflineWriterError("offline writer schema does not match its exact verified profile")
            self.repository = AuditRepository(self.database.pool)
            key = f"closed-bar-producer:{self.service_name}"
            self._lease_acquired = bool(
                await self._connection.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", key)
            )
            if not self._lease_acquired:
                raise OfflineWriterError("closed-bar producer/recovery already running")
            self._started = True
        except BaseException:
            await self._release()
            raise

    async def append(self, topic: str, message: Publishable) -> bool:
        """Atomically store one immutable audit fact and matching outbox effect.

        The boolean tells the caller whether this invocation inserted a new
        audit fact. An exact duplicate is safe and returns ``False``; a
        mismatched stable ID raises and rolls back the entire transaction.
        """
        if not self._started or self._connection is None or self.repository is None:
            raise OfflineWriterError("offline writer is not started")
        payload = _payload(message)
        message_id = payload.get("message_id")
        if not isinstance(message_id, str) or not message_id.strip():
            raise ValueError("durable messages require a non-empty message_id")
        encoded, payload_sha256 = canonical_payload(payload)
        async with self._connection.transaction():
            inserted = await self.repository.append_payload_strict(
                topic,
                payload,
                encoded,
                payload_sha256,
                connection=self._connection,
            )
            await self.repository.enqueue_outbox(
                self._connection,
                message_id=message_id,
                topic=topic,
                payload=encoded,
                payload_sha256=payload_sha256,
                producer=self.service_name,
            )
        return inserted

    async def _release(self) -> None:
        connection, self._connection = self._connection, None
        errors: list[BaseException] = []
        if connection is not None:
            if self._lease_acquired:
                try:
                    await connection.execute(
                        "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                        f"closed-bar-producer:{self.service_name}",
                    )
                except BaseException as exc:
                    errors.append(exc)
            self._lease_acquired = False
            if self._schema_guard_acquired:
                try:
                    await connection.execute("SELECT pg_advisory_unlock($1)", SCHEMA_MIGRATION_LOCK_KEY)
                except BaseException as exc:
                    errors.append(exc)
            self._schema_guard_acquired = False
            try:
                await self.database.pool.release(connection)
            except BaseException as exc:
                errors.append(exc)
        try:
            await self.database.close()
        except BaseException as exc:
            errors.append(exc)
        self.repository = None
        self._started = False
        if errors:
            raise errors[0]

    async def close(self) -> None:
        """Release the advisory lease and database pool; no transport exists to flush."""
        if self._closing:
            return
        self._closing = True
        await self._release()
