"""Offline transaction model, never PostgreSQL/venue qualification evidence."""

import asyncio
import copy
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from kairos_core.contracts import RiskTradeDecisionV1
from test_trade_lifecycle import T0, _decision

from kairos_persistence.canary_session import CanaryScope, digest
from kairos_persistence.operator_control import (
    OperatorCommandV1,
    OperatorControlRefused,
    OperatorControlRepository,
    OperatorControlUnavailable,
    _payload,
    digest_effect,
    scope_key,
)
from kairos_persistence.repository import MessageIdentityConflict


def scope():
    return CanaryScope(
        environment="paper-dev",
        account_id="kairos-paper-dev-unit",
        remote_account_id="synthetic-unit",
        config_sha256="a" * 64,
        recorder_code_sha256="b" * 64,
    )


def decision():
    return RiskTradeDecisionV1.model_validate(
        _decision().model_dump(exclude={"decision_id", "trade_id", "message_id"})
        | {"account_id": scope().account_id}
    )


def command(action="ARM", *, version=0, nonce="1", **updates):
    values = dict(
        command_id=nonce * 64,
        scope=scope(),
        action=action,
        expected_version=version,
        operator_label="descriptive-only",
        session_id="a" * 64 if action == "ARM" else None,
        expires_at_ms=T0 + 120_000 if action == "ARM" else None,
    )
    return OperatorCommandV1(**(values | updates))


class Connection:
    def __init__(self):
        self.actor = "kairos_operator"
        self.rolsuper = False
        self.runtime_flags = {}
        self.now = T0 + 60_500
        self.controls = {}
        self.commands = {}
        self.admissions = {}
        self.claims = {}
        self.events = []
        self.in_transaction = False
        self.terminated = False
        self.session = {
            "state": "RUNNING",
            "scope": scope().model_dump(mode="json"),
            "scope_sha256": digest(scope().model_dump(mode="json")),
            "entry_deadline_at": datetime.fromtimestamp((T0 + 7_200_000) / 1000, UTC),
        }

    @asynccontextmanager
    async def transaction(self, **kwargs):
        saved = copy.deepcopy((self.controls, self.commands, self.admissions, self.claims))
        self.in_transaction = True
        try:
            yield
        except BaseException:
            self.controls, self.commands, self.admissions, self.claims = saved
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")
        finally:
            self.in_transaction = False

    async def fetchrow(self, sql, *args):
        if "current_user AS runtime_actor" in sql:
            return (
                dict(
                    runtime_actor="kairos_runtime",
                    rolsuper=False,
                    rolbypassrls=False,
                    rolcreaterole=False,
                    rolcreatedb=False,
                    rolreplication=False,
                    operator_member=False,
                    schema_create=False,
                    owns_database=False,
                    owns_control_functions=False,
                    owns_runtime_tables=False,
                    unsafe_control_write=False,
                    table_count=4,
                )
                | self.runtime_flags
            )
        if "current_user AS actor" in sql:
            return {
                "actor": self.actor,
                "rolsuper": self.rolsuper,
                "rolbypassrls": False,
                "rolcanlogin": True,
            }
        if "FROM paper_canary_sessions" in sql:
            return self.session if args[0] == "a" * 64 else None
        if "FROM operator_controls" in sql:
            return self.controls.get(args[0])
        if "FROM operator_control_commands" in sql:
            if "command_id=$1" in sql:
                return self.commands.get(args[0])
            return next(
                (row for row in self.commands.values() if (row["scope_key"], row["version"]) == args), None
            )
        if "FROM operator_control_admissions" in sql:
            return self.admissions.get(args[0])
        raise AssertionError(sql)

    async def fetchval(self, sql, *args):
        if "clock_timestamp" in sql:
            return datetime.fromtimestamp(self.now / 1000, UTC)
        if "pg_try_advisory_lock" in sql:
            self.events.append("lock")
            return True
        if "FROM operator_control_dispatch_claims" in sql:
            return 1 if args[0] in self.claims else None
        raise AssertionError(sql)

    async def execute(self, sql, *args):
        if "pg_advisory_unlock" in sql:
            self.events.append("unlock")
        elif "pg_advisory_xact_lock" in sql:
            pass
        elif "INSERT INTO operator_control_commands" in sql:
            self.commands[args[0]] = dict(
                zip(
                    (
                        "command_id",
                        "scope_key",
                        "version",
                        "command_payload",
                        "command_sha256",
                        "event_payload",
                        "event_sha256",
                        "actor",
                    ),
                    args,
                    strict=True,
                )
            )
        elif "INSERT INTO operator_controls" in sql:
            self.controls[args[0]] = dict(
                zip(
                    (
                        "scope_key",
                        "scope_payload",
                        "scope_sha256",
                        "version",
                        "state",
                        "session_id",
                        "expires_at_ms",
                        "audit_head_sha256",
                    ),
                    args,
                    strict=True,
                )
            )
        elif "INSERT INTO operator_control_admissions" in sql:
            self.admissions[args[0]] = dict(
                zip(
                    ("decision_id", "scope_key", "version", "decision_sha256", "payload", "payload_sha256"),
                    args,
                    strict=True,
                )
            )
        elif "INSERT INTO operator_control_dispatch_claims" in sql:
            self.claims[args[0]] = dict(
                zip(
                    ("effect_id", "decision_id", "scope_key", "version", "payload", "payload_sha256"),
                    args,
                    strict=True,
                )
            )
            self.events.append("claim")
        else:
            raise AssertionError(sql)
        return "OK"

    def terminate(self):
        self.terminated = True


class Pool:
    def __init__(self, connection):
        self.connection = connection

    @asynccontextmanager
    async def acquire(self):
        yield self.connection


@pytest.fixture
def state():
    connection = Connection()
    return connection, OperatorControlRepository(Pool(connection))


@pytest.mark.asyncio
async def test_missing_control_never_defaults_to_arm(state):
    _, repository = state
    with pytest.raises(OperatorControlRefused, match="missing"):
        await repository.snapshot(scope())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "flag",
    [
        "rolsuper",
        "rolbypassrls",
        "rolcreaterole",
        "rolcreatedb",
        "rolreplication",
        "operator_member",
        "schema_create",
        "owns_database",
        "owns_control_functions",
        "owns_runtime_tables",
        "unsafe_control_write",
    ],
)
async def test_runtime_privilege_separation_is_mandatory_for_admission(state, flag):
    connection, repository = state
    await repository.apply_command(command())
    connection.runtime_flags[flag] = True
    with pytest.raises(OperatorControlRefused, match="separated database roles"):
        await repository.snapshot(scope())
    assert connection.admissions == connection.claims == {}


@pytest.mark.asyncio
async def test_runtime_privilege_probe_is_verify_only_and_sanitizes_backend_failure(state):
    connection, repository = state
    await repository.verify_runtime_access()
    connection.fetchrow = AsyncMock(side_effect=RuntimeError("private-dsn-placeholder"))
    with pytest.raises(OperatorControlUnavailable, match="privileges cannot be verified") as error:
        await repository.verify_runtime_access()
    assert "private-dsn-placeholder" not in str(error.value)
    assert error.value.__suppress_context__


@pytest.mark.asyncio
@pytest.mark.parametrize("actor,superuser", [("kairos_runtime", False), ("kairos_operator", True)])
async def test_actual_database_role_not_operator_label_is_command_authority(state, actor, superuser):
    connection, repository = state
    connection.actor, connection.rolsuper = actor, superuser
    with pytest.raises(OperatorControlRefused, match="database role"):
        await repository.apply_command(command(operator_label="kairos_operator"))
    assert connection.controls == connection.commands == {}


@pytest.mark.asyncio
async def test_manual_fixed_lease_cas_duplicates_and_restart_kill(state):
    connection, repository = state
    assert (await repository.apply_command(command())).version == 1
    assert (await repository.apply_command(command())).version == 1
    assert len(connection.commands) == 1
    with pytest.raises(MessageIdentityConflict):
        await repository.apply_command(command(operator_label="other"))
    with pytest.raises(OperatorControlRefused, match="stale"):
        await repository.apply_command(command("KILL", version=0, nonce="2"))
    killed = await repository.apply_command(command("KILL", version=1, nonce="2"))
    assert killed.state == "KILLED" and killed.version == 2
    restarted = OperatorControlRepository(Pool(connection))
    with pytest.raises(OperatorControlRefused, match="killed"):
        await restarted.snapshot(scope())


@pytest.mark.asyncio
@pytest.mark.parametrize("expiry", [T0 + 60_500, T0 + 8_000_000])
async def test_arm_lease_never_extends_session_or_uses_expired_clock(state, expiry):
    connection, repository = state
    with pytest.raises(OperatorControlRefused, match="session"):
        await repository.apply_command(command(expires_at_ms=expiry))
    assert connection.commands == {}


@pytest.mark.asyncio
async def test_scope_alias_cannot_bypass_remote_account_latch(state):
    _, repository = state
    await repository.apply_command(command())
    alias = scope().model_copy(update={"account_id": "kairos-paper-dev-alias"})
    assert scope_key(scope()) == scope_key(alias)
    with pytest.raises(OperatorControlRefused, match="scope"):
        await repository.snapshot(alias)


@pytest.mark.asyncio
async def test_old_decision_fence_is_not_rebound_after_kill_and_manual_rearm(state):
    _, repository = state
    await repository.apply_command(command())
    item = decision()
    binding = await repository.bind_decision(decision=item, expected_scope=scope(), expected_version=1)
    assert binding.control_version == 1
    await repository.apply_command(command("KILL", version=1, nonce="2"))
    await repository.apply_command(command(version=2, nonce="3"))
    with pytest.raises(OperatorControlRefused, match="different operator fence"):
        await repository.bind_decision(decision=item, expected_scope=scope(), expected_version=3)
    with pytest.raises(OperatorControlRefused, match="stale"):
        await repository.check_entry(
            decision=item, expected_scope=scope(), effect_id=digest_effect(item.trade_id)
        )


@pytest.mark.asyncio
async def test_direct_bus_approved_decision_without_admission_is_refused(state):
    _, repository = state
    await repository.apply_command(command())
    item = decision()
    with pytest.raises(OperatorControlRefused, match="no durable"):
        await repository.check_entry(
            decision=item, expected_scope=scope(), effect_id=digest_effect(item.trade_id)
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_mode", ["timeout", "cancel", "crash"])
async def test_dispatch_claim_commits_before_caller_and_restart_never_resends(
    state, monkeypatch, failure_mode
):
    from kairos_persistence.canary_session import CanarySessionRepository

    connection, repository = state
    await repository.apply_command(command())
    item = decision()
    await repository.bind_decision(decision=item, expected_scope=scope(), expected_version=1)
    monkeypatch.setattr(CanarySessionRepository, "_assert_prepared_effect", AsyncMock())
    entered = asyncio.Event()
    effect = digest_effect(item.trade_id)

    async def caller():
        async with repository.final_dispatch_guard(
            decision=item,
            expected_scope=scope(),
            effect_id=effect,
            max_hold_s=0.01 if failure_mode == "timeout" else 30,
        ):
            assert effect in connection.claims and not connection.in_transaction
            entered.set()
            if failure_mode == "crash":
                raise RuntimeError("synthetic crash")
            await asyncio.Future()

    task = asyncio.create_task(caller())
    await asyncio.wait_for(entered.wait(), timeout=1)
    if failure_mode == "cancel":
        task.cancel()
    error = (
        TimeoutError
        if failure_mode == "timeout"
        else asyncio.CancelledError
        if failure_mode == "cancel"
        else RuntimeError
    )
    with pytest.raises(error):
        await task
    assert effect in connection.claims and connection.events[-1] == "unlock"
    with pytest.raises(OperatorControlRefused, match="already claimed"):
        async with OperatorControlRepository(Pool(connection)).final_dispatch_guard(
            decision=item, expected_scope=scope(), effect_id=effect
        ):
            pytest.fail("restarted dispatch ran caller")


@pytest.mark.asyncio
async def test_db_failure_is_sanitized_and_never_permission():
    repository = OperatorControlRepository(
        SimpleNamespace(acquire=lambda: (_ for _ in ()).throw(RuntimeError("synthetic-private-value")))
    )
    with pytest.raises(OperatorControlUnavailable) as error:
        await repository.snapshot(scope())
    assert "synthetic-private-value" not in str(error.value)


def test_store_decode_rejects_hash_extra_types_and_bounded_depth():
    with pytest.raises(MessageIdentityConflict):
        _payload({"state": "ARMED"}, "0" * 64)
    with pytest.raises(MessageIdentityConflict):
        _payload("[]", digest({}))
    nested = {}
    for _ in range(34):
        nested = {"nested": nested}
    with pytest.raises(MessageIdentityConflict, match="structure"):
        _payload(nested, digest(nested))
    assert _payload(json.dumps({"ok": True}), digest({"ok": True})) == {"ok": True}
