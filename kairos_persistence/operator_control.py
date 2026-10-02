"""Durable account-wide PAPER entry latch, not trading or scientific authority.

Only an authenticated, separately provisioned non-superuser PostgreSQL operator
role may change the latch. A label is descriptive, never authorization. Missing
or expired state denies entries. Protective/recovery mutations do not use this
repository. CONTROLLED_RUNTIME adoption is a separate reviewed migration gate.
"""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal

import asyncpg
from kairos_core.contracts import RiskTradeDecisionV1
from pydantic import Field, model_validator

from .canary_session import CanaryScope, CanarySessionRepository, OperationalModel, digest, millis
from .repository import MessageIdentityConflict
from .runtime import canonical_payload

SHA = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
OPERATOR_ROLE = "kairos_operator"
ZERO_SHA = "0" * 64
MAX_PAYLOAD_BYTES = 262_144


class OperatorControlRefused(PermissionError):
    """Non-secret-bearing, known entry-authority refusal."""


class OperatorControlUnavailable(OperatorControlRefused):
    """Durable operator authority could not be verified; never infer permission."""


class OperatorCommandV1(OperationalModel):
    command_id: SHA
    scope: CanaryScope
    action: Literal["ARM", "DISARM", "KILL"]
    expected_version: Annotated[int, Field(ge=0, strict=True)]
    operator_label: Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]{1,64}$")]
    session_id: SHA | None = None
    expires_at_ms: Annotated[int, Field(gt=0, strict=True)] | None = None

    @model_validator(mode="after")
    def arm_has_fixed_session_lease(self) -> OperatorCommandV1:
        if (self.action == "ARM") != (self.session_id is not None and self.expires_at_ms is not None):
            raise ValueError("ARM requires a fixed bounded session and deadline")
        if self.action != "ARM" and (self.session_id is not None or self.expires_at_ms is not None):
            raise ValueError("DISARM/KILL cannot carry an entry lease")
        return self


class OperatorSnapshotV1(OperationalModel):
    scope: CanaryScope
    version: Annotated[int, Field(gt=0, strict=True)]
    state: Literal["ARMED", "DISARMED", "KILLED"]
    session_id: SHA | None
    expires_at_ms: Annotated[int, Field(gt=0, strict=True)] | None
    audit_head_sha256: SHA


class OperatorAdmissionV1(OperationalModel):
    scope_sha256: SHA
    control_version: Annotated[int, Field(gt=0, strict=True)]
    session_id: SHA
    decision_id: SHA
    decision_sha256: SHA
    trade_id: SHA
    expires_at_ms: Annotated[int, Field(gt=0, strict=True)]


def scope_key(scope: CanaryScope) -> str:
    """Local aliases cannot bypass one remote-account kill latch."""
    checked = CanaryScope.model_validate(scope.model_dump(mode="json"))
    return digest(
        {
            "domain": "kairos.operator-account.v1",
            "profile": "DEV",
            "exchange": "evedex",
            "remote_account_id": checked.remote_account_id,
        }
    )


def _payload(value: Any, expected_sha: str) -> dict[str, Any]:
    if isinstance(value, str):
        if len(value.encode("utf-8")) > MAX_PAYLOAD_BYTES:
            raise MessageIdentityConflict("operator evidence exceeds its bounded size")
        value = json.loads(value)
    if not isinstance(value, dict):
        raise MessageIdentityConflict("operator evidence must be an object")
    pending: list[tuple[Any, int]] = [(value, 0)]
    nodes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > 10_000 or depth > 32:
            raise MessageIdentityConflict("operator evidence exceeds bounded structure")
        if isinstance(item, dict):
            if len(item) > 10_000:
                raise MessageIdentityConflict("operator evidence object is too large")
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            if len(item) > 10_000:
                raise MessageIdentityConflict("operator evidence array is too large")
            pending.extend((child, depth + 1) for child in item)
        elif isinstance(item, str) and len(item.encode("utf-8")) > MAX_PAYLOAD_BYTES:
            raise MessageIdentityConflict("operator evidence text is too large")
    encoded, actual = canonical_payload(value)
    if len(encoded.encode("utf-8")) > MAX_PAYLOAD_BYTES or actual != expected_sha:
        raise MessageIdentityConflict("operator evidence fingerprint differs")
    return value


def _decision(decision: RiskTradeDecisionV1, scope: CanaryScope) -> tuple[str, str]:
    checked = RiskTradeDecisionV1.model_validate(decision.model_dump(mode="json"))
    if (
        not checked.approved
        or checked.trading_mode.value != "PAPER"
        or checked.evedex_profile.value != "DEV"
        or checked.account_id != scope.account_id
    ):
        raise OperatorControlRefused("operator admission requires exact risk-approved DEV account")
    if not checked.decision_id or not checked.trade_id:
        raise OperatorControlRefused("operator admission requires canonical decision lineage")
    return checked.decision_id, digest(checked.model_dump(mode="json"))


class OperatorControlRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    @staticmethod
    def _lock_key(scope: CanaryScope) -> str:
        return f"kairos.operator-dispatch:{scope_key(scope)}"

    @staticmethod
    async def _assert_runtime_privileges(connection: asyncpg.Connection) -> None:
        """Refuse owner/operator/DDL authority; deployment never provisions it here."""
        role = await connection.fetchrow(
            """SELECT current_user AS runtime_actor,r.rolsuper,r.rolbypassrls,
               r.rolcreaterole,r.rolcreatedb,r.rolreplication,
               pg_has_role(current_user,'kairos_operator','MEMBER') AS operator_member,
               has_schema_privilege(current_user,'public','CREATE') AS schema_create,
               pg_has_role(current_user,d.datdba,'MEMBER') AS owns_database,
               EXISTS(SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n
                 ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname IN
                   ('operator_control_latch_writer','operator_control_append_only')
                 AND pg_has_role(current_user,p.proowner,'MEMBER')) AS owns_control_functions,
               (SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n
                ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='r'
                AND c.relname IN ('operator_controls','operator_control_commands',
                  'operator_control_admissions','operator_control_dispatch_claims')) AS table_count,
               EXISTS(SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n
                 ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p')
                 AND pg_has_role(current_user,c.relowner,'MEMBER')) AS owns_runtime_tables,
               EXISTS(SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n
                 ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname IN
                   ('operator_controls','operator_control_commands','operator_control_admissions',
                    'operator_control_dispatch_claims') AND
                   (has_table_privilege(current_user,c.oid,'UPDATE,DELETE,TRUNCATE,TRIGGER')
                    OR (c.relname IN ('operator_controls','operator_control_commands')
                        AND has_table_privilege(current_user,c.oid,'INSERT')))) AS unsafe_control_write
               FROM pg_catalog.pg_roles r,pg_catalog.pg_database d
               WHERE r.rolname=current_user AND d.datname=current_database()"""
        )
        if (
            role is None
            or role["rolsuper"]
            or role["rolbypassrls"]
            or role["rolcreaterole"]
            or role["rolcreatedb"]
            or role["rolreplication"]
            or role["operator_member"]
            or role["schema_create"]
            or role["owns_database"]
            or role["owns_control_functions"]
            or role["owns_runtime_tables"]
            or role["unsafe_control_write"]
            or role["table_count"] != 4
        ):
            raise OperatorControlRefused(
                "runtime operator control requires independently separated database roles"
            )

    async def verify_runtime_access(self) -> None:
        """Verify-only startup prerequisite before publishers/consumers start."""
        try:
            async with self.pool.acquire() as connection, connection.transaction(readonly=True):
                await self._assert_runtime_privileges(connection)
        except OperatorControlRefused:
            raise
        except Exception:
            raise OperatorControlUnavailable("runtime operator privileges cannot be verified") from None

    @staticmethod
    async def _snapshot(
        connection: asyncpg.Connection, scope: CanaryScope, *, active: bool
    ) -> OperatorSnapshotV1:
        if active:
            await OperatorControlRepository._assert_runtime_privileges(connection)
        row = await connection.fetchrow(
            "SELECT * FROM operator_controls WHERE scope_key=$1", scope_key(scope)
        )
        if row is None:
            raise OperatorControlRefused("operator control is missing")
        stored = _payload(row["scope_payload"], row["scope_sha256"])
        if stored != scope.model_dump(mode="json"):
            raise OperatorControlRefused("operator control scope/config differs")
        head = await connection.fetchrow(
            "SELECT * FROM operator_control_commands WHERE scope_key=$1 AND version=$2",
            scope_key(scope),
            row["version"],
        )
        if head is None or head["actor"] != OPERATOR_ROLE or head["event_sha256"] != row["audit_head_sha256"]:
            raise MessageIdentityConflict("operator audit head is missing or inconsistent")
        command = OperatorCommandV1.model_validate(_payload(head["command_payload"], head["command_sha256"]))
        event = _payload(head["event_payload"], head["event_sha256"])
        expected_state = (
            "ARMED" if command.action == "ARM" else "DISARMED" if command.action == "DISARM" else "KILLED"
        )
        if (
            command.scope != scope
            or command.expected_version + 1 != row["version"]
            or row["state"] != expected_state
            or row["session_id"] != command.session_id
            or row["expires_at_ms"] != command.expires_at_ms
            or event.get("actor") != OPERATOR_ROLE
            or event.get("command_sha256") != head["command_sha256"]
            or event.get("version") != row["version"]
        ):
            raise MessageIdentityConflict("operator latch differs from its authenticated command")
        snapshot = OperatorSnapshotV1(
            scope=scope,
            version=row["version"],
            state=row["state"],
            session_id=row["session_id"],
            expires_at_ms=row["expires_at_ms"],
            audit_head_sha256=row["audit_head_sha256"],
        )
        if active:
            now_ms = millis(await connection.fetchval("SELECT clock_timestamp()"))
            if (
                snapshot.state != "ARMED"
                or snapshot.expires_at_ms is None
                or now_ms >= snapshot.expires_at_ms
            ):
                raise OperatorControlRefused("operator control is disarmed, killed or expired")
            await OperatorControlRepository._session(
                connection, scope, snapshot.session_id, snapshot.expires_at_ms, now_ms
            )
        return snapshot

    @staticmethod
    async def _session(
        connection: asyncpg.Connection, scope: CanaryScope, session_id: str | None, expiry: int, now_ms: int
    ) -> None:
        row = await connection.fetchrow("SELECT * FROM paper_canary_sessions WHERE session_id=$1", session_id)
        if (
            row is None
            or row["state"] not in {"ARMED", "RUNNING"}
            or row["scope_sha256"] != digest(scope.model_dump(mode="json"))
            or _payload(row["scope"], row["scope_sha256"]) != scope.model_dump(mode="json")
            or not now_ms < expiry <= millis(row["entry_deadline_at"])
        ):
            raise OperatorControlRefused("operator lease requires the exact active bounded canary session")

    async def apply_command(self, command: OperatorCommandV1) -> OperatorSnapshotV1:
        command = OperatorCommandV1.model_validate(command.model_dump(mode="json"))
        encoded, command_hash = canonical_payload(command.model_dump(mode="json"))
        async with self.pool.acquire() as connection, connection.transaction():
            role = await connection.fetchrow(
                "SELECT current_user AS actor,rolsuper,rolbypassrls,rolcanlogin "
                "FROM pg_catalog.pg_roles WHERE rolname=current_user"
            )
            if (
                role is None
                or role["actor"] != OPERATOR_ROLE
                or role["rolsuper"]
                or role["rolbypassrls"]
                or not role["rolcanlogin"]
            ):
                raise OperatorControlRefused(
                    "manual command requires the separately reviewed operator database role"
                )
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", self._lock_key(command.scope)
            )
            old_command = await connection.fetchrow(
                "SELECT * FROM operator_control_commands WHERE command_id=$1", command.command_id
            )
            if old_command is not None:
                if _payload(
                    old_command["command_payload"], old_command["command_sha256"]
                ) != command.model_dump(mode="json"):
                    raise MessageIdentityConflict(
                        "operator command identity was reused with different content"
                    )
                # A duplicate is an observation, not replay/re-arm of an old command.
                return await self._snapshot(connection, command.scope, active=False)
            row = await connection.fetchrow(
                "SELECT * FROM operator_controls WHERE scope_key=$1 FOR UPDATE", scope_key(command.scope)
            )
            version = 0 if row is None else row["version"]
            if version != command.expected_version:
                raise OperatorControlRefused("operator command fencing version is stale")
            if row is not None:
                await self._snapshot(connection, command.scope, active=False)
            now_ms = millis(await connection.fetchval("SELECT clock_timestamp()"))
            if command.action == "ARM":
                if command.expires_at_ms is None:
                    raise OperatorControlRefused("operator ARM requires a fixed entry lease")
                await self._session(
                    connection, command.scope, command.session_id, command.expires_at_ms, now_ms
                )
            state = (
                "ARMED" if command.action == "ARM" else "DISARMED" if command.action == "DISARM" else "KILLED"
            )
            event = {
                "domain": "kairos.operator-command-event.v1",
                "actor": role["actor"],
                "command_sha256": command_hash,
                "version": version + 1,
                "previous_sha256": ZERO_SHA if row is None else row["audit_head_sha256"],
                "observed_at_ms": now_ms,
            }
            event_encoded, event_hash = canonical_payload(event)
            await connection.execute(
                """INSERT INTO operator_control_commands
                   (command_id,scope_key,version,command_payload,command_sha256,event_payload,event_sha256,actor)
                   VALUES($1,$2,$3,$4::jsonb,$5,$6::jsonb,$7,$8)""",
                command.command_id,
                scope_key(command.scope),
                version + 1,
                encoded,
                command_hash,
                event_encoded,
                event_hash,
                role["actor"],
            )
            await connection.execute(
                """INSERT INTO operator_controls
                   (scope_key,scope_payload,scope_sha256,version,state,session_id,expires_at_ms,audit_head_sha256)
                   VALUES($1,$2::jsonb,$3,$4,$5,$6,$7,$8) ON CONFLICT(scope_key) DO UPDATE SET
                   version=EXCLUDED.version,state=EXCLUDED.state,session_id=EXCLUDED.session_id,
                   expires_at_ms=EXCLUDED.expires_at_ms,audit_head_sha256=EXCLUDED.audit_head_sha256""",
                scope_key(command.scope),
                canonical_payload(command.scope.model_dump(mode="json"))[0],
                digest(command.scope.model_dump(mode="json")),
                version + 1,
                state,
                command.session_id,
                command.expires_at_ms,
                event_hash,
            )
            return await self._snapshot(connection, command.scope, active=False)

    async def snapshot(self, expected_scope: CanaryScope) -> OperatorSnapshotV1:
        try:
            async with self.pool.acquire() as connection, connection.transaction(readonly=True):
                return await self._snapshot(connection, expected_scope, active=True)
        except OperatorControlRefused:
            raise
        except Exception:
            raise OperatorControlUnavailable("operator control cannot be independently verified") from None

    async def bind_decision(
        self, *, decision: RiskTradeDecisionV1, expected_scope: CanaryScope, expected_version: int
    ) -> OperatorAdmissionV1:
        decision_id, decision_hash = _decision(decision, expected_scope)
        try:
            async with self.pool.acquire() as connection, connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", self._lock_key(expected_scope)
                )
                current = await self._snapshot(connection, expected_scope, active=True)
                if current.version != expected_version:
                    raise OperatorControlRefused("operator authority changed before risk publication")
                if current.session_id is None or current.expires_at_ms is None:
                    raise OperatorControlRefused("operator admission requires a fixed session lease")
                binding = OperatorAdmissionV1(
                    scope_sha256=digest(expected_scope.model_dump(mode="json")),
                    control_version=current.version,
                    session_id=current.session_id,
                    decision_id=decision_id,
                    decision_sha256=decision_hash,
                    trade_id=str(decision.trade_id),
                    expires_at_ms=min(
                        current.expires_at_ms,
                        decision.intent.entry_expires_ts_ms,
                        decision.venue_quality.expires_at_ms,
                    ),
                )
                encoded, binding_hash = canonical_payload(binding.model_dump(mode="json"))
                old = await connection.fetchrow(
                    "SELECT * FROM operator_control_admissions WHERE decision_id=$1", decision_id
                )
                if old is not None:
                    if _payload(old["payload"], old["payload_sha256"]) != binding.model_dump(mode="json"):
                        raise OperatorControlRefused(
                            "existing risk decision belongs to a different operator fence"
                        )
                    return binding
                await connection.execute(
                    """INSERT INTO operator_control_admissions
                       (decision_id,scope_key,version,decision_sha256,payload,payload_sha256)
                       VALUES($1,$2,$3,$4,$5::jsonb,$6)""",
                    decision_id,
                    scope_key(expected_scope),
                    current.version,
                    decision_hash,
                    encoded,
                    binding_hash,
                )
                return binding
        except OperatorControlRefused:
            raise
        except Exception:
            raise OperatorControlUnavailable("operator risk admission could not be persisted") from None

    @staticmethod
    async def _check(
        connection: asyncpg.Connection, decision: RiskTradeDecisionV1, scope: CanaryScope, effect_id: str
    ) -> OperatorAdmissionV1:
        decision_id, decision_hash = _decision(decision, scope)
        expected_effect = digest_effect(str(decision.trade_id))
        if effect_id != expected_effect:
            raise OperatorControlRefused("operator entry effect lineage differs")
        current = await OperatorControlRepository._snapshot(connection, scope, active=True)
        row = await connection.fetchrow(
            "SELECT * FROM operator_control_admissions WHERE decision_id=$1", decision_id
        )
        if row is None:
            raise OperatorControlRefused("entry has no durable operator risk admission")
        binding = OperatorAdmissionV1.model_validate(_payload(row["payload"], row["payload_sha256"]))
        now_ms = millis(await connection.fetchval("SELECT clock_timestamp()"))
        if (
            row["scope_key"] != scope_key(scope)
            or row["version"] != binding.control_version
            or row["decision_sha256"] != binding.decision_sha256
            or binding.control_version != current.version
            or binding.session_id != current.session_id
            or binding.scope_sha256 != digest(scope.model_dump(mode="json"))
            or binding.decision_id != decision_id
            or binding.decision_sha256 != decision_hash
            or binding.trade_id != decision.trade_id
            or now_ms >= binding.expires_at_ms
        ):
            raise OperatorControlRefused("operator entry admission is stale or mismatched")
        return binding

    async def check_entry(
        self, *, decision: RiskTradeDecisionV1, expected_scope: CanaryScope, effect_id: str
    ) -> OperatorAdmissionV1:
        try:
            async with self.pool.acquire() as connection, connection.transaction(readonly=True):
                return await self._check(connection, decision, expected_scope, effect_id)
        except OperatorControlRefused:
            raise
        except Exception:
            raise OperatorControlUnavailable("operator entry admission cannot be verified") from None

    @asynccontextmanager
    async def final_dispatch_guard(
        self,
        *,
        decision: RiskTradeDecisionV1,
        expected_scope: CanaryScope,
        effect_id: str,
        max_hold_s: float = 30.0,
    ) -> AsyncIterator[OperatorAdmissionV1]:
        """Sanitized setup with durable no-resend claim before caller I/O."""
        caller_entered = False
        try:
            async with self._final_dispatch_guard(
                decision=decision, expected_scope=expected_scope, effect_id=effect_id, max_hold_s=max_hold_s
            ) as binding:
                caller_entered = True
                yield binding
        except OperatorControlRefused:
            raise
        except Exception:
            if caller_entered:
                # Preserve a caller timeout/venue error category, not a DB
                # setup error disguised as a provider failure or retry signal.
                raise
            raise OperatorControlUnavailable("operator dispatch authority cannot be verified") from None

    @asynccontextmanager
    async def _final_dispatch_guard(
        self,
        *,
        decision: RiskTradeDecisionV1,
        expected_scope: CanaryScope,
        effect_id: str,
        max_hold_s: float = 30.0,
    ) -> AsyncIterator[OperatorAdmissionV1]:
        """account -> trade -> operator -> canary. KILL takes operator only.

        Claim commits before yield. Caller must still take the canary final
        dispatch guard. A crash can sacrifice an attempt, never authorize retry.
        No SQL transaction spans caller/venue code.
        """
        if isinstance(max_hold_s, bool) or not math.isfinite(max_hold_s) or not 0 < max_hold_s <= 30:
            raise ValueError("operator dispatch hold must be bounded by 30 seconds")
        async with self.pool.acquire() as connection:
            key = self._lock_key(expected_scope)
            try:
                async with asyncio.timeout(5):
                    while not await connection.fetchval(
                        "SELECT pg_try_advisory_lock(hashtextextended($1,0))", key
                    ):
                        await asyncio.sleep(0.025)
                async with connection.transaction():
                    binding = await self._check(connection, decision, expected_scope, effect_id)
                    await CanarySessionRepository._assert_prepared_effect(
                        connection, decision, expected_scope, effect_id
                    )
                    if await connection.fetchval(
                        "SELECT 1 FROM operator_control_dispatch_claims WHERE effect_id=$1 OR decision_id=$2",
                        effect_id,
                        binding.decision_id,
                    ):
                        raise OperatorControlRefused(
                            "operator dispatch already claimed; reconcile without resending"
                        )
                    claim = {
                        "domain": "kairos.operator-dispatch.v1",
                        "effect_id": effect_id,
                        "admission": binding.model_dump(mode="json"),
                    }
                    encoded, claim_hash = canonical_payload(claim)
                    await connection.execute(
                        """INSERT INTO operator_control_dispatch_claims
                           (effect_id,decision_id,scope_key,version,payload,payload_sha256)
                           VALUES($1,$2,$3,$4,$5::jsonb,$6)""",
                        effect_id,
                        binding.decision_id,
                        scope_key(expected_scope),
                        binding.control_version,
                        encoded,
                        claim_hash,
                    )
                # Recheck database time after commit, immediately before caller guard/send.
                if millis(await connection.fetchval("SELECT clock_timestamp()")) >= binding.expires_at_ms:
                    raise OperatorControlRefused("operator entry expired during claim; no resend")
                async with asyncio.timeout(max_hold_s):
                    yield binding
            finally:
                try:
                    await asyncio.shield(
                        asyncio.wait_for(
                            connection.execute("SELECT pg_advisory_unlock(hashtextextended($1,0))", key),
                            timeout=2,
                        )
                    )
                except asyncio.CancelledError:
                    # Never return a possibly locked connection to the shared pool.
                    connection.terminate()
                    raise
                except Exception:
                    connection.terminate()
                    raise OperatorControlUnavailable("operator dispatch lock could not be released") from None
                except BaseException:
                    connection.terminate()
                    raise


def digest_effect(trade_id: str) -> str:
    import hashlib

    return hashlib.sha256(f"paper.v1:{trade_id}:ENTRY:place".encode()).hexdigest()
