"""No API calls or live evidence: all observation rows here are synthetic fixtures."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from kairos_persistence.canary_arm import PaperCanaryArmRepository
from kairos_persistence.canary_session import (
    MAX_RECEIPT_AGE_MS,
    READONLY_DURATION_MS,
    SYMBOLS,
    ZERO_SHA,
    BoundedCanaryPlan,
    CanaryAdmissionError,
    CanaryScope,
    CanarySessionRepository,
    CanarySlot,
    ReadonlyObservation,
    SymbolObservation,
    digest,
    millis,
    sample_identity,
    verify_readonly_evidence,
)
from kairos_persistence.repository import MessageIdentityConflict


def scope(**updates) -> CanaryScope:
    return CanaryScope(
        environment="paper-dev",
        account_id="kairos-paper-dev-test",
        remote_account_id="synthetic-dev-account",
        config_sha256="a" * 64,
        recorder_code_sha256="b" * 64,
        **updates,
    )


def plan(*, count: int = 5, duration_ms: int = 7_200_000) -> BoundedCanaryPlan:
    scenarios = ("STOP", "TARGET", "TIMEOUT", "RESTART", "ENTRY_CANCEL")
    return BoundedCanaryPlan(
        slots=tuple(
            CanarySlot(
                slot_id=f"slot-{index + 1}",
                symbol=SYMBOLS[index % 5],
                side="LONG",
                scenario=scenarios[index % 5],
            )
            for index in range(count)
        ),
        max_attempts=count,
        duration_ms=duration_ms,
    )


def observation(now: datetime) -> ReadonlyObservation:
    return ReadonlyObservation(
        observed_at_ms=millis(now),
        unresolved_bar_gaps=0,
        reconciliation_drift=False,
        entry_mutations=0,
        symbols=tuple(
            SymbolObservation(
                symbol=symbol,
                available=True,
                basis_bps=1.0,
                spread_bps=2.0,
                slippage_bps=3.0,
                book_age_ms=100,
                timestamp_skew_ms=100,
                book_nonempty=True,
            )
            for symbol in SYMBOLS
        ),
    )


def evidence(ended_at: datetime | None = None) -> tuple[dict, list[dict], datetime]:
    ended = ended_at or datetime(2026, 9, 12, tzinfo=UTC)
    started = ended - timedelta(milliseconds=READONLY_DURATION_MS)
    data = scope().model_dump(mode="json")
    run = {
        "run_id": "c" * 64,
        "scope": data,
        "scope_sha256": digest(data),
        "database_instance_id": "00000000-0000-0000-0000-000000000001",
        "started_at": started,
        "sample_period_ms": 60_000,
        "sample_count": 1440,
        "head_sha256": ZERO_SHA,
    }
    samples = []
    for index in range(1440):
        received = started + timedelta(minutes=index)
        data = observation(received).model_dump(mode="json")
        sha = sample_identity(run["run_id"], index + 1, received, run["head_sha256"], data)
        samples.append(
            {
                "seq": index + 1,
                "received_at": received,
                "payload": data,
                "previous_sha256": run["head_sha256"],
                "sample_sha256": sha,
            }
        )
        run["head_sha256"] = sha
    return run, samples, ended


def rechain(run: dict, samples: list[dict]) -> None:
    head = ZERO_SHA
    for seq, item in enumerate(samples, start=1):
        item["seq"] = seq
        item["previous_sha256"] = head
        item["sample_sha256"] = sample_identity(
            run["run_id"], seq, item["received_at"], head, item["payload"]
        )
        head = item["sample_sha256"]
    run["sample_count"], run["head_sha256"] = len(samples), head


@pytest.fixture(scope="module")
def frozen_evidence():
    return evidence()


def test_receipt_proof_derived_from_real_duration_and_full_hash_chain(frozen_evidence) -> None:
    run, rows, ended = frozen_evidence
    proof = verify_readonly_evidence(run, rows, ended)
    assert proof["duration_ms"] == READONLY_DURATION_MS
    assert proof["sample_count"] == 1440
    assert all(values["availability"] == 1 for values in proof["metrics"].values())
    assert "accepted" not in proof
    assert proof == verify_readonly_evidence(run, rows, ended)


@pytest.mark.parametrize("mutation", ["count", "head", "previous", "payload", "sequence", "scope"])
def test_evidence_hash_mutations_fail_closed(frozen_evidence, mutation) -> None:
    run, rows, ended = deepcopy(frozen_evidence)
    if mutation == "count":
        run["sample_count"] += 1
    elif mutation == "head":
        run["head_sha256"] = "f" * 64
    elif mutation == "scope":
        run["scope"]["config_sha256"] = "f" * 64
    else:
        key = {"previous": "previous_sha256", "sequence": "seq"}.get(mutation)
        if key:
            rows[2][key] = "f" * 64 if key != "seq" else 999
        else:
            rows[2]["payload"]["symbols"][0]["basis_bps"] = 12
    with pytest.raises(MessageIdentityConflict):
        verify_readonly_evidence(run, rows, ended)


@pytest.mark.parametrize(
    "mutation",
    ["gap", "drift", "mutation", "age", "skew", "backdate", "p95", "missing", "empty_last", "reorder"],
)
def test_complete_chain_is_not_enough_for_unsafe_or_missing_measurements(frozen_evidence, mutation) -> None:
    run, rows, ended = deepcopy(frozen_evidence)
    if mutation == "missing":
        rows = rows[:700] + rows[730:]
    elif mutation == "p95":
        for row in rows:
            row["payload"]["symbols"][0]["basis_bps"] = 26
    elif mutation == "empty_last":
        rows[-1]["payload"]["symbols"][0].update(available=False, book_nonempty=False)
    elif mutation == "reorder":
        rows[1]["received_at"] = rows[0]["received_at"]
    else:
        item = rows[1]["payload"]
        if mutation == "gap":
            item["unresolved_bar_gaps"] = 1
        elif mutation == "drift":
            item["reconciliation_drift"] = True
        elif mutation == "mutation":
            item["entry_mutations"] = 1
        elif mutation == "backdate":
            item["observed_at_ms"] -= 2_001
        else:
            item["symbols"][0]["book_age_ms" if mutation == "age" else "timestamp_skew_ms"] = 5_001
    rechain(run, rows)
    with pytest.raises(CanaryAdmissionError):
        verify_readonly_evidence(run, rows, ended)


def test_twenty_three_hours_cannot_be_certified(frozen_evidence) -> None:
    run, rows, ended = frozen_evidence
    with pytest.raises(CanaryAdmissionError, match="24-hour"):
        verify_readonly_evidence(run, rows, ended - timedelta(hours=1))


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_observations_and_plan_are_rejected(value) -> None:
    with pytest.raises(ValidationError):
        SymbolObservation(symbol="BTCUSDT", available=False, basis_bps=value)
    with pytest.raises(ValidationError):
        CanarySlot(slot_id="slot-1", symbol="BTCUSDT", side="LONG", scenario="STOP", stop_distance_bps=value)


def test_scope_and_plan_cannot_shrink_symbols_or_expand_authority() -> None:
    for changed in (
        {"chain_id": 42161},
        {"project": "kairos"},
        {"account_id": "primary"},
        {"accepted": True},
        {"exchange_url": "https://trading-api.evedex.io"},
    ):
        with pytest.raises(ValidationError):
            CanaryScope.model_validate(scope().model_dump(mode="json") | changed)
    for changed in ({"max_attempts": 11}, {"duration_ms": 7_200_001}, {"slots": []}):
        with pytest.raises(ValidationError):
            BoundedCanaryPlan.model_validate(plan().model_dump(mode="json") | changed)


@pytest.mark.asyncio
async def test_old_arm_calls_fail_before_any_database_io() -> None:
    class NoDatabase:
        def __getattr__(self, name):
            raise AssertionError(f"unexpected database IO: {name}")

    with pytest.raises(CanaryAdmissionError, match="bounded"):
        await PaperCanaryArmRepository(NoDatabase()).arm(account_id="x", review=None, allocation=None)
    for invalid in (0, -1, True, MAX_RECEIPT_AGE_MS + 1):
        with pytest.raises(CanaryAdmissionError, match="freshness"):
            await CanarySessionRepository(NoDatabase()).arm_session(
                receipt_id="x",
                scope=scope(),
                plan=plan(),
                operator_nonce="manual",
                receipt_max_age_ms=invalid,
            )
