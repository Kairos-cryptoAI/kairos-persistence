"""Real PG concurrency/permissions, FAKE callbacks, exact disposable DB ONLY.

Synthetic/backdated venue fixtures here never certify PAPER/alpha/LIVE. The
fixture creates LOGIN roles only inside the independently launched disposable
server and drops its own role grants after tests. No existing runtime DB/role is
accepted. Credentials are generated in memory and are never logged.
"""

import asyncio
import os
import secrets
from urllib.parse import urlsplit

import asyncpg
import pytest
import pytest_asyncio
from test_canary_session_integration import entry_fixture, prepare_entry_effect

from kairos_persistence import Database, MigrationProfile, PersistenceSettings
from kairos_persistence.operator_control import (
    OperatorCommandV1,
    OperatorControlRefused,
    OperatorControlRepository,
    digest_effect,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]
TEST_DATABASE = "kairos_operator_test_20261002"
RUNTIME_ROLE = "kairos_operator_runtime_fixture"
OPERATOR_ROLE = "kairos_operator"


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def fixture_database():
    url = os.getenv("KAIROS_OPERATOR_TEST_DATABASE_URL")
    if not url or os.getenv("KAIROS_OPERATOR_TEST_DATABASE_NAME") != TEST_DATABASE:
        pytest.skip("explicit new isolated operator database opt-in is required")
    parsed = urlsplit(url)
    if parsed.path != f"/{TEST_DATABASE}" or parsed.hostname != "127.0.0.1" or parsed.port != 55433:
        raise ValueError("refusing any non-disposable operator target")
    database = Database(
        PersistenceSettings(database_url=url, command_timeout_s=5),
        migration_profile=MigrationProfile.CONTROLLED_RUNTIME,
    )
    await database.connect()
    runtime = operator = None
    created = []
    try:
        assert await database.pool.fetchval("SELECT current_database()") == TEST_DATABASE
        await database.migrate()
        assert await database.pool.fetchval("SELECT count(*) FROM operator_controls") == 0
        passwords = {name: secrets.token_hex(24) for name in (OPERATOR_ROLE, RUNTIME_ROLE)}
        for name, password in passwords.items():
            if await database.pool.fetchval("SELECT 1 FROM pg_roles WHERE rolname=$1", name):
                raise RuntimeError("operator fixture refuses an existing role")
            await database.pool.execute(
                f"CREATE ROLE {name} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '{password}'"
            )
            created.append(name)
            await database.pool.execute(f"GRANT USAGE ON SCHEMA public TO {name}")
            await database.pool.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {name}")
        await database.pool.execute("GRANT INSERT,UPDATE ON operator_controls TO kairos_operator")
        await database.pool.execute("GRANT INSERT ON operator_control_commands TO kairos_operator")
        await database.pool.execute(
            "GRANT INSERT ON operator_control_admissions,operator_control_dispatch_claims "
            "TO kairos_operator_runtime_fixture"
        )
        common = dict(
            host="127.0.0.1",
            port=55433,
            database=TEST_DATABASE,
            min_size=1,
            max_size=5,
            command_timeout=5,
            server_settings={"application_name": "operator-disposable-proof"},
        )
        operator = await asyncpg.create_pool(user=OPERATOR_ROLE, password=passwords[OPERATOR_ROLE], **common)
        runtime = await asyncpg.create_pool(user=RUNTIME_ROLE, password=passwords[RUNTIME_ROLE], **common)
        yield database, operator, runtime
    finally:
        if runtime is not None:
            await runtime.close()
        if operator is not None:
            await operator.close()
        for name in reversed(created):
            await database.pool.execute(f"REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {name}")
            await database.pool.execute(f"REVOKE USAGE ON SCHEMA public FROM {name}")
            await database.pool.execute(f"DROP ROLE {name}")
        await database.close()


async def entry(fixture):
    database, operator, runtime = fixture
    await database.pool.execute(
        "UPDATE paper_canary_sessions SET state='ABORTED',stop_reason='SYNTHETIC_TEST_CLEANUP' "
        "WHERE state IN ('ARMED','RUNNING','DRAINING') AND remote_account_id LIKE 'synthetic-%'"
    )
    sessions, scope, session, decision, _ = await entry_fixture(database)
    expiry = min(int(session["entry_deadline_at"].timestamp() * 1000), decision.intent.entry_expires_ts_ms)
    operator_repo, runtime_repo = OperatorControlRepository(operator), OperatorControlRepository(runtime)
    cmd = OperatorCommandV1(
        command_id=secrets.token_hex(32),
        scope=scope,
        action="ARM",
        expected_version=0,
        operator_label="synthetic-owned-fixture",
        session_id=session["session_id"],
        expires_at_ms=expiry,
    )
    await operator_repo.apply_command(cmd)
    await runtime_repo.bind_decision(decision=decision, expected_scope=scope, expected_version=1)
    return database, operator_repo, runtime_repo, sessions, scope, session, decision


@pytest.mark.asyncio(loop_scope="module")
async def test_real_runtime_role_cannot_insert_update_or_forge_operator_latch(fixture_database):
    database, operator, runtime, _, scope, _, decision = await entry(fixture_database)
    _, _, runtime_pool = fixture_database
    assert await runtime_pool.fetchval("SELECT current_user") == RUNTIME_ROLE
    with pytest.raises(OperatorControlRefused, match="database role"):
        await runtime.apply_command(
            OperatorCommandV1(
                command_id=secrets.token_hex(32),
                scope=scope,
                action="KILL",
                expected_version=1,
                operator_label=OPERATOR_ROLE,
            )
        )
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await runtime_pool.execute(
            "UPDATE operator_controls SET state='KILLED',session_id=NULL,expires_at_ms=NULL"
        )
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await runtime_pool.execute("INSERT INTO operator_controls SELECT * FROM operator_controls")
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await runtime_pool.execute("ALTER TABLE operator_controls DISABLE TRIGGER ALL")
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await runtime_pool.execute("SET ROLE kairos_operator")
    await runtime.verify_runtime_access()
    with pytest.raises(OperatorControlRefused, match="separated database roles"):
        await operator.verify_runtime_access()
    # Even an accidental UPDATE grant cannot defeat the current_user SQL guard.
    await database.pool.execute(f"GRANT UPDATE ON operator_controls TO {RUNTIME_ROLE}")
    try:
        with pytest.raises(OperatorControlRefused, match="separated database roles"):
            await runtime.verify_runtime_access()
        with pytest.raises(asyncpg.RaiseError, match="operator role"):
            await runtime_pool.execute(
                "UPDATE operator_controls SET state='KILLED',session_id=NULL,expires_at_ms=NULL"
            )
    finally:
        await database.pool.execute(f"REVOKE UPDATE ON operator_controls FROM {RUNTIME_ROLE}")
    assert (await runtime.snapshot(scope)).version == 1
    await runtime.check_entry(
        decision=decision, expected_scope=scope, effect_id=digest_effect(decision.trade_id)
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_real_concurrent_commands_cas_alias_scope_and_append_only(fixture_database):
    database, operator, runtime, _, scope, _, _ = await entry(fixture_database)
    commands = [
        OperatorCommandV1(
            command_id=secrets.token_hex(32),
            scope=scope,
            action=action,
            expected_version=1,
            operator_label="synthetic-race",
        )
        for action in ("KILL", "DISARM")
    ]
    outcomes = await asyncio.gather(
        *(operator.apply_command(cmd) for cmd in commands), return_exceptions=True
    )
    assert sum(isinstance(item, OperatorControlRefused) for item in outcomes) == 1
    assert sum(not isinstance(item, BaseException) for item in outcomes) == 1
    with pytest.raises(OperatorControlRefused):
        await runtime.snapshot(scope)
    with pytest.raises(OperatorControlRefused):
        await runtime.snapshot(scope.model_copy(update={"account_id": "kairos-paper-dev-alias"}))
    with pytest.raises(asyncpg.RaiseError, match="append-only"):
        await database.pool.execute("UPDATE operator_control_commands SET actor=actor")


@pytest.mark.asyncio(loop_scope="module")
async def test_real_committed_claim_serializes_kill_has_no_open_transaction_and_never_resends(
    fixture_database,
):
    database, operator, runtime, sessions, scope, _, decision = await entry(fixture_database)
    effect = digest_effect(decision.trade_id)
    args = dict(decision=decision, expected_scope=scope, effect_id=effect)
    await sessions.bind_entry(**args)
    await prepare_entry_effect(database, scope, decision, effect)
    kill_task = None
    calls = 0
    with pytest.raises(RuntimeError, match="synthetic process loss"):
        async with runtime.final_dispatch_guard(**args), sessions.final_dispatch(**args):
            # Separate connection observes committed claim while caller is in
            # the simulated external call; no database tx spans that callback.
            assert (
                await database.pool.fetchval(
                    "SELECT count(*) FROM operator_control_dispatch_claims WHERE effect_id=$1", effect
                )
                == 1
            )
            assert (
                await database.pool.fetchval(
                    "SELECT count(*) FROM pg_stat_activity WHERE usename=$1 AND state='idle in transaction'",
                    RUNTIME_ROLE,
                )
                == 0
            )
            kill = OperatorCommandV1(
                command_id=secrets.token_hex(32),
                scope=scope,
                action="KILL",
                expected_version=1,
                operator_label="synthetic-kill",
            )
            kill_task = asyncio.create_task(operator.apply_command(kill))
            await asyncio.sleep(0.05)
            assert not kill_task.done()
            calls += 1  # fake callback, NEVER an exchange/provider call
            raise RuntimeError("synthetic process loss")
    assert kill_task is not None
    assert (await asyncio.wait_for(kill_task, timeout=5)).state == "KILLED"
    restarted = OperatorControlRepository(fixture_database[2])
    with pytest.raises(OperatorControlRefused):
        async with restarted.final_dispatch_guard(**args):
            calls += 1
    assert calls == 1
    assert (
        await database.pool.fetchval(
            "SELECT count(*) FROM operator_control_dispatch_claims WHERE effect_id=$1", effect
        )
        == 1
    )
