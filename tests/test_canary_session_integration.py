"""Real PostgreSQL transactions, SYNTHETIC evidence, exact disposable DB only.

Backdated rows are inserted only by this guarded fixture. Production APIs cannot
backdate evidence. These tests cannot be used as DEV qualification receipts.
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import pytest
import pytest_asyncio
from kairos_core.contracts import RiskTradeDecisionV1, VenueQualityV1
from test_canary_arm import _canary
from test_canary_session import evidence, observation, plan, rechain

from kairos_persistence.canary_arm import PaperCanaryArmRepository
from kairos_persistence.canary_session import (
    BoundedCanaryPlan,
    CanaryAdmissionError,
    CanaryScope,
    CanarySessionRepository,
    digest,
    millis,
)
from kairos_persistence.config import PersistenceSettings
from kairos_persistence.database import Database
from kairos_persistence.execution_journal import EffectType, ExecutionJournalRepository
from kairos_persistence.repository import AuditRepository, MessageIdentityConflict
from kairos_persistence.runtime import canonical_payload

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]
TEST_DATABASE = "kairos_canary_test_20260912"


@pytest_asyncio.fixture
async def database():
    url = os.getenv("KAIROS_PERSISTENCE_DATABASE_URL")
    if not url or os.getenv("KAIROS_CANARY_TEST_DATABASE_NAME") != TEST_DATABASE:
        pytest.skip("explicit isolated canary DB opt-in is required")
    if unquote(urlsplit(url).path.lstrip("/")) != TEST_DATABASE:
        raise ValueError("refusing non-disposable canary database URL")
    db = Database(PersistenceSettings(database_url=url))
    await db.connect()
    try:
        assert await db.pool.fetchval("SELECT current_database()") == TEST_DATABASE
        # Preserve synthetic rows/claims, but retire test-only admission leases
        # before each test. This is deliberately NOT a production release API.
        # The exact disposable DB opt-in and verified synthetic account markers
        # are required before this write, including upgrades from014/015 tests.
        exists = await db.pool.fetchval("SELECT to_regclass('paper_canary_sessions')")
        if exists:
            unexpected = await db.pool.fetchval(
                "SELECT 1 FROM paper_canary_sessions WHERE remote_account_id NOT LIKE 'synthetic-%' LIMIT 1"
            )
            if unexpected:
                raise ValueError("disposable DB contains a non-synthetic session; refusing test cleanup")
            await db.pool.execute(
                """UPDATE paper_canary_sessions SET state='ABORTED',stop_reason='SYNTHETIC_TEST_CLEANUP'
                   WHERE state IN ('ARMED','RUNNING','DRAINING')"""
            )
        await db.migrate()
        yield db
    finally:
        await db.close()


async def seed_evidence(db: Database, *, age_ms: int = 0) -> tuple[CanaryScope, str]:
    # Unique synthetic account scope preserves evidence from all prior runs.
    suffix = uuid4().hex
    scope = CanaryScope(
        environment="paper-dev",
        account_id=f"kairos-paper-dev-{suffix}",
        remote_account_id=f"synthetic-{suffix}",
        config_sha256="a" * 64,
        recorder_code_sha256="b" * 64,
    )
    now = await db.pool.fetchval("SELECT clock_timestamp()")
    run, samples, _ = evidence(now - timedelta(milliseconds=age_ms))
    run["run_id"] = digest({"synthetic": suffix})
    run["scope"] = scope.model_dump(mode="json")
    run["scope_sha256"] = digest(run["scope"])
    run["database_instance_id"] = await db.pool.fetchval(
        "SELECT instance_id FROM paper_canary_database_identity"
    )
    samples.append({"received_at": now, "payload": observation(now).model_dump(mode="json")})
    rechain(run, samples)
    async with db.pool.acquire() as connection, connection.transaction():
        await connection.execute(
            """INSERT INTO paper_readonly_runs
               (run_id,scope,scope_sha256,database_instance_id,started_at,sample_period_ms,sample_count,head_sha256)
               VALUES($1,$2::jsonb,$3,$4,$5,$6,$7,$8)""",
            run["run_id"],
            canonical_payload(run["scope"])[0],
            run["scope_sha256"],
            run["database_instance_id"],
            run["started_at"],
            run["sample_period_ms"],
            run["sample_count"],
            run["head_sha256"],
        )
        await connection.executemany(
            """INSERT INTO paper_readonly_samples
               (run_id,seq,received_at,payload,previous_sha256,sample_sha256)
               VALUES($1,$2,$3,$4::jsonb,$5,$6)""",
            [
                (
                    run["run_id"],
                    row["seq"],
                    row["received_at"],
                    canonical_payload(row["payload"])[0],
                    row["previous_sha256"],
                    row["sample_sha256"],
                )
                for row in samples
            ],
        )
    return scope, run["run_id"]


def refusal(review, account_id: str) -> RiskTradeDecisionV1:
    now = review.reviewed_at_ms
    venue_symbol = dict(review.intent.metadata)["venue_symbol"]
    venue = VenueQualityV1(
        source="synthetic-integration",
        profile="DEV",
        symbol=venue_symbol,
        observed_at_ms=now,
        expires_at_ms=now + 5_000,
        reference_timestamp_ms=now,
        book_timestamp_ms=now,
        reference_mid_price=100,
        best_bid=99.99,
        best_ask=100.01,
        venue_mid_price=100,
        basis_bps=0,
        spread_bps=2,
        assessed_notional_usd=1_000,
        depth_usd=5_000,
        buy_slippage_bps=1,
        sell_slippage_bps=1,
        taker_fee_bps=5,
        reference_age_ms=0,
        book_age_ms=0,
        latency_ms=10,
        timestamp_skew_ms=0,
        entry_allowed=True,
    )
    return RiskTradeDecisionV1(
        source="kairos-risk-manager",
        intent=review.intent,
        review=review,
        venue_quality=venue,
        approved=False,
        rejection_reasons=("synthetic_integration_refusal",),
        decided_at_ms=now,
        trading_mode="PAPER",
        evedex_profile="DEV",
        account_id=account_id,
        venue_symbol=venue_symbol,
        quantity=0,
        leverage=1,
        notional_usd=0,
        loss_budget_usd=0,
        worst_case_loss_usd=0,
        worst_entry_price=100.01,
        estimated_fees_usd=0,
        estimated_slippage_usd=0,
        exit_plan=review.intent.exit_plan,
    )


async def reject_and_refresh(db, repo, session_id, review, account_id):
    await AuditRepository(db.pool).append_event("kairos.risk.trade_decision.v1", refusal(review, account_id))
    return await repo.refresh(session_id)


async def test_real_atomic_arm_attempt_outbox_caps_replay_and_restart(database) -> None:
    db = database
    sessions = CanarySessionRepository(db.pool)
    scope, run_id = await seed_evidence(db)
    receipt_id = await sessions.certify_readonly(run_id, expected_scope=scope)
    assert await sessions.certify_readonly(run_id, expected_scope=scope) == receipt_id
    slots = tuple(
        slot.model_copy(update={"stop_distance_bps": 50.0 + index})
        for index, slot in enumerate(plan(count=10).slots)
    )
    session_plan = BoundedCanaryPlan(slots=slots)
    args = dict(receipt_id=receipt_id, scope=scope, plan=session_plan, operator_nonce="synthetic-test")
    session = await sessions.arm_session(**args)
    session_id = session["session_id"]
    assert await sessions.arm_session(**args) == session
    # A different local+remote account cannot mint another simultaneous session.
    other_scope, other_run = await seed_evidence(db)
    other_receipt = await sessions.certify_readonly(other_run, expected_scope=other_scope)
    with pytest.raises(CanaryAdmissionError, match="another DEV session"):
        await sessions.arm_session(
            receipt_id=other_receipt, scope=other_scope, plan=plan(), operator_nonce="other"
        )
    arms = PaperCanaryArmRepository(db.pool)
    all_arm_ids = []
    for index, slot in enumerate(slots):
        clock = millis(await db.pool.fetchval("SELECT clock_timestamp()"))
        review, allocation = _canary(
            clock, symbol=slot.symbol, operational_slot=slot, account_id=scope.account_id
        )
        bind = dict(
            account_id=scope.account_id,
            review=review,
            allocation=allocation,
            session_id=session_id,
            slot_id=slot.slot_id,
        )
        # Eleven racing deliveries of one effect reserve exactly one attempt/outbox row.
        replayed = await asyncio.gather(*(arms.arm(**bind) for _ in range(11 if index == 0 else 2)))
        assert all(item == replayed[0] for item in replayed)
        arm = replayed[0]
        all_arm_ids.append(arm.arm_id)
        assert (
            arm.session_id == session_id
            and arm.attempt_id
            and arm.session_expires_at == session["entry_deadline_at"]
        )
        assert (
            await db.pool.fetchval(
                "SELECT count(*) FROM message_outbox WHERE message_id=$1", review.review_id
            )
            == 1
        )
        consumed = await arms.consume(account_id=scope.account_id, review=review)
        assert consumed and consumed.status == "CONSUMED"
        assert await arms.consume(account_id=scope.account_id, review=review) == consumed
        if index == 0:
            other = slots[1]
            next_review, next_allocation = _canary(
                clock, symbol=other.symbol, operational_slot=other, account_id=scope.account_id
            )
            with pytest.raises(CanaryAdmissionError, match="not authoritatively terminal"):
                await arms.arm(
                    account_id=scope.account_id,
                    review=next_review,
                    allocation=next_allocation,
                    session_id=session_id,
                    slot_id=other.slot_id,
                )
            assert (
                await db.pool.fetchval(
                    "SELECT 1 FROM paper_canary_arms WHERE review_id=$1", next_review.review_id
                )
                is None
            )
            assert (
                await db.pool.fetchval(
                    "SELECT 1 FROM message_outbox WHERE message_id=$1", next_review.review_id
                )
                is None
            )
            changed, changed_allocation = _canary(
                clock + 60_000, symbol=slot.symbol, operational_slot=slot, account_id=scope.account_id
            )
            with pytest.raises(MessageIdentityConflict, match="different bytes"):
                await arms.arm(**(bind | {"review": changed, "allocation": changed_allocation}))
        status = await reject_and_refresh(db, sessions, session_id, review, scope.account_id)
        assert status["attempts_reserved"] == index + 1  # Refusals do not refund.
        assert all(item["state"] == "TERMINAL" for item in status["attempts"])
    assert len(set(all_arm_ids)) == 10
    assert status["state"] == "INCOMPLETE"  # Ten refusals never qualify PAPER.
    await db.close()
    await db.connect()
    restored = await CanarySessionRepository(db.pool).status(session_id)
    assert (
        restored["attempts_reserved"] == 10 and restored["entry_deadline_at"] == session["entry_deadline_at"]
    )
    assert restored["state"] == "INCOMPLETE"
    assert (await CanarySessionRepository(db.pool).arm_session(**args))["attempts_reserved"] == 10
    with pytest.raises(CanaryAdmissionError, match="one permitted session"):
        await CanarySessionRepository(db.pool).arm_session(**(args | {"operator_nonce": "another"}))


async def test_stop_expiry_old_arms_and_no_implicit_rearming(database) -> None:
    db = database
    sessions = CanarySessionRepository(db.pool)
    scope, run_id = await seed_evidence(db)
    receipt_id = await sessions.certify_readonly(run_id, expected_scope=scope)
    session_plan = plan()
    session = await sessions.arm_session(
        receipt_id=receipt_id, scope=scope, plan=session_plan, operator_nonce="stop"
    )
    sid = session["session_id"]
    clock = millis(await db.pool.fetchval("SELECT clock_timestamp()"))
    slot = session_plan.slots[0]
    review, allocation = _canary(
        clock, symbol=slot.symbol, operational_slot=slot, account_id=scope.account_id
    )
    arms = PaperCanaryArmRepository(db.pool)
    armed = await arms.arm(
        account_id=scope.account_id,
        review=review,
        allocation=allocation,
        session_id=sid,
        slot_id=slot.slot_id,
    )
    await sessions.stop(sid)
    assert await arms.consume(account_id=scope.account_id, review=review) is None
    second_slot = session_plan.slots[1]
    second, second_allocation = _canary(
        clock, symbol=second_slot.symbol, operational_slot=second_slot, account_id=scope.account_id
    )
    with pytest.raises(ValueError):
        await arms.arm(
            account_id=scope.account_id,
            review=second,
            allocation=second_allocation,
            session_id=sid,
            slot_id=second_slot.slot_id,
        )
    # Simulate elapsed expiry only in this guarded synthetic DB.
    await db.pool.execute(
        "UPDATE paper_canary_arms SET expires_at=clock_timestamp()-interval '1 second' WHERE arm_id=$1",
        armed.arm_id,
    )
    final = await sessions.refresh(sid)
    assert final["state"] == "ABORTED" and final["attempts_reserved"] == 1
    assert final["attempts"][0]["terminal_reason"] == "ENTRY_EXPIRED"
    assert (await sessions.stop(sid))["state"] == "ABORTED"

    scope2, run2 = await seed_evidence(db)
    rid2 = await sessions.certify_readonly(run2, expected_scope=scope2)
    args = dict(receipt_id=rid2, scope=scope2, plan=plan(), operator_nonce="expiry")
    s2 = await sessions.arm_session(**args)
    await db.pool.execute(
        """UPDATE paper_canary_sessions SET armed_at=armed_at-interval '3 hours',
           entry_deadline_at=entry_deadline_at-interval '3 hours' WHERE session_id=$1""",
        s2["session_id"],
    )
    s2_status = await sessions.refresh(s2["session_id"])
    assert s2_status["state"] == "INCOMPLETE" and s2_status["stop_reason"] == "SESSION_DEADLINE"
    assert (await sessions.arm_session(**args))["entry_deadline_at"] == s2_status["entry_deadline_at"]
    assert (await sessions.arm_session(**args))["attempts_reserved"] == 0


async def test_receipt_requires_persisted_unmodified_scope_chain_and_actual_time(database) -> None:
    db = database
    sessions = CanarySessionRepository(db.pool)
    scope, run_id = await seed_evidence(db)
    wrong_scope = scope.model_copy(update={"config_sha256": "f" * 64})
    with pytest.raises(CanaryAdmissionError, match="scope"):
        await sessions.certify_readonly(run_id, expected_scope=wrong_scope)
    rid = await sessions.certify_readonly(run_id, expected_scope=scope)
    with pytest.raises(CanaryAdmissionError, match="stale or belongs"):
        await sessions.arm_session(receipt_id=rid, scope=wrong_scope, plan=plan(), operator_nonce="wrong")
    with pytest.raises(CanaryAdmissionError, match="persisted verified"):
        await sessions.arm_session(receipt_id="f" * 64, scope=scope, plan=plan(), operator_nonce="false")
    with pytest.raises(CanaryAdmissionError, match="stale or belongs"):
        await sessions.arm_session(
            receipt_id=rid, scope=scope, plan=plan(), operator_nonce="stale", receipt_max_age_ms=1
        )
    # Corrupt one persisted row after certification. Arming re-verifies, not trusts the receipt flag.
    await db.pool.execute(
        "UPDATE paper_readonly_samples SET sample_sha256=$2 WHERE run_id=$1 AND seq=20", run_id, "f" * 64
    )
    with pytest.raises(MessageIdentityConflict, match="hash chain"):
        await sessions.arm_session(receipt_id=rid, scope=scope, plan=plan(), operator_nonce="tampered")

    live_run = await sessions.begin_readonly(scope)
    now = await db.pool.fetchval("SELECT clock_timestamp()")
    with pytest.raises(CanaryAdmissionError, match="backdated"):
        await sessions.append_observation(live_run, observation(now - timedelta(days=1)))
    assert await sessions.append_observation(live_run, observation(now)) == 1
    assert await sessions.append_observation(live_run, observation(now)) == 1
    with pytest.raises(CanaryAdmissionError, match="oversample"):
        await sessions.append_observation(live_run, observation(now + timedelta(milliseconds=1)))
    with pytest.raises(CanaryAdmissionError, match="24-hour"):
        await sessions.certify_readonly(live_run, expected_scope=scope)


async def entry_fixture(db):
    sessions = CanarySessionRepository(db.pool)
    scope, run_id = await seed_evidence(db)
    receipt_id = await sessions.certify_readonly(run_id, expected_scope=scope)
    session_plan = plan()
    session = await sessions.arm_session(
        receipt_id=receipt_id, scope=scope, plan=session_plan, operator_nonce="synthetic-dispatch"
    )
    # Honour the real DB clock and the immutable minute/30s entry window. At
    # most40s of test-only waiting; never backdate an entry or call a venue.
    now = millis(await db.pool.fetchval("SELECT clock_timestamp()"))
    if now % 60_000 > 20_000:
        await asyncio.sleep((60_000 - now % 60_000) / 1_000 + 0.05)
    now = millis(await db.pool.fetchval("SELECT clock_timestamp()"))
    slot = session_plan.slots[0]
    review, allocation = _canary(now - 60_000, operational_slot=slot, account_id=scope.account_id)
    arms = PaperCanaryArmRepository(db.pool)
    await arms.arm(
        account_id=scope.account_id,
        review=review,
        allocation=allocation,
        session_id=session["session_id"],
        slot_id=slot.slot_id,
    )
    consumed = await arms.consume(account_id=scope.account_id, review=review)
    assert consumed is not None
    decision_data = refusal(review, scope.account_id).model_dump(mode="json")
    for key in ("decision_id", "trade_id", "message_id", "produced_at"):
        decision_data.pop(key, None)
    venue_data = decision_data["venue_quality"]
    for key in ("measurement_id", "message_id", "produced_at"):
        venue_data.pop(key, None)
    venue_data.update(
        observed_at_ms=consumed.decided_at_ms,
        reference_timestamp_ms=consumed.decided_at_ms,
        book_timestamp_ms=consumed.decided_at_ms,
        expires_at_ms=consumed.decided_at_ms + 5_000,
    )
    quantity = 0.05
    fees = quantity * (100.01 + review.intent.exit_plan.stop_price) * 5 / 10_000
    slippage = quantity * 100 / 10_000
    loss = quantity * abs(100.01 - review.intent.exit_plan.stop_price) + fees + slippage
    decision_data.update(
        approved=True,
        rejection_reasons=[],
        quantity=quantity,
        notional_usd=quantity * 100.01,
        loss_budget_usd=25,
        worst_case_loss_usd=loss,
        estimated_fees_usd=fees,
        estimated_slippage_usd=slippage,
        decided_at_ms=consumed.decided_at_ms,
    )
    decision = RiskTradeDecisionV1.model_validate(decision_data)
    effect_id = f"synthetic-entry:{decision.trade_id}"
    return sessions, scope, session, decision, effect_id


async def prepare_entry_effect(db, scope, decision, effect_id):
    client_id = f"synthetic-{decision.trade_id[:24]}"
    await ExecutionJournalRepository(db.pool).prepare(
        effect_key=effect_id,
        effect_type=EffectType.PLACE_ORDER,
        exchange="evedex",
        symbol=decision.intent.symbol,
        client_order_id=client_id,
        request_payload={
            "trade_id": decision.trade_id,
            "intent_id": decision.intent.intent_id,
            "client_order_id": client_id,
            "venue_symbol": decision.venue_symbol,
            "side": "BUY",
            "quantity_hex": float(decision.quantity).hex(),
            "limit_price_hex": float(decision.worst_entry_price).hex(),
            "leverage_hex": float(decision.leverage).hex(),
        },
        environment=f"{scope.environment}:EVEDEX:DEV:PAPER",
        account_id=scope.account_id,
        trade_id=decision.trade_id,
        order_role="ENTRY",
    )


@pytest.mark.parametrize("crash_after_callback", [False, True])
async def test_dispatch_claim_commits_before_callback_survives_loss_and_serializes_stop(
    database, crash_after_callback
) -> None:
    db = database
    sessions, scope, session, decision, effect_id = await entry_fixture(db)
    args = dict(decision=decision, expected_scope=scope, effect_id=effect_id)
    binding = await sessions.bind_entry(**args)
    assert not binding.dispatch_claimed
    assert await sessions.bind_entry(**args) == binding
    with pytest.raises(MessageIdentityConflict, match="another decision/trade/effect"):
        await sessions.bind_entry(**(args | {"effect_id": "another-effect"}))
    with pytest.raises(CanaryAdmissionError, match="scope/account/config"):
        await sessions.bind_entry(
            **(args | {"expected_scope": scope.model_copy(update={"config_sha256": "f" * 64})})
        )
    with pytest.raises(CanaryAdmissionError, match="PREPARED"):
        async with sessions.final_dispatch(**args):
            raise AssertionError("unprepared entry received dispatch authority")
    assert (
        await db.pool.fetchval(
            "SELECT count(*) FROM paper_canary_dispatch_claims WHERE effect_id=$1", effect_id
        )
        == 0
    )
    await prepare_entry_effect(db, scope, decision, effect_id)
    callback_count = 0
    stop_task = None
    with pytest.raises(RuntimeError, match="synthetic process loss"):
        async with sessions.final_dispatch(**args) as lease:
            assert lease.dispatch_claimed
            # Separate pool connection sees the committed claim BEFORE caller I/O.
            assert (
                await db.pool.fetchval(
                    "SELECT count(*) FROM paper_canary_dispatch_claims WHERE effect_id=$1", effect_id
                )
                == 1
            )
            stop_task = asyncio.create_task(sessions.stop(session["session_id"]))
            await asyncio.sleep(0.05)
            assert not stop_task.done()  # Stop cannot race between admission and dispatch.
            if crash_after_callback:
                callback_count += 1  # Synthetic callback, NEVER a venue call.
            raise RuntimeError("synthetic process loss")
    assert stop_task is not None
    stopped = await asyncio.wait_for(stop_task, timeout=5)
    assert stopped["state"] == "DRAINING"
    with pytest.raises(CanaryAdmissionError):
        async with sessions.final_dispatch(**args):
            callback_count += 1
    assert callback_count == int(crash_after_callback)
    await db.close()
    await db.connect()
    restored = CanarySessionRepository(db.pool)
    recovery = await restored.recovery_binding(**args)
    assert recovery.dispatch_claimed and recovery.effect_id == effect_id
    assert (await restored.status(session["session_id"]))["attempts_reserved"] == 1
    with pytest.raises(CanaryAdmissionError):
        await restored.bind_entry(**args)


async def test_stop_before_dispatch_cannot_obtain_a_claim(database) -> None:
    sessions, scope, session, decision, effect_id = await entry_fixture(database)
    args = dict(decision=decision, expected_scope=scope, effect_id=effect_id)
    await sessions.bind_entry(**args)
    await prepare_entry_effect(database, scope, decision, effect_id)
    await sessions.stop(session["session_id"])
    with pytest.raises(CanaryAdmissionError, match="stopped"):
        async with sessions.final_dispatch(**args):
            raise AssertionError("stopped session acquired a dispatch lease")
    assert (
        await database.pool.fetchval(
            "SELECT count(*) FROM paper_canary_dispatch_claims WHERE effect_id=$1", effect_id
        )
        == 0
    )
    assert not (await sessions.recovery_binding(**args)).dispatch_claimed
