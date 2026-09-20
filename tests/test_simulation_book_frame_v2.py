"""Pure integrity tests for simulator V2 raw book-frame evidence."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from kairos_core import (
    RecordedBookLevelV1,
    RecordedTopNBookFrameV1,
    RecordedTopNBookFrameV2,
)

from kairos_persistence import MessageIdentityConflict, SimulationRepository, canonical_payload

_T0 = 1_800_000_000_000
_RAW_PAYLOAD = '{"lastUpdateId":100,"bids":[["99.9","2"]],"asks":[["100.1","2"]]}'


def _frame_v1() -> RecordedTopNBookFrameV1:
    return RecordedTopNBookFrameV1(
        source="sim-book-v2-test",
        tape_id="tape-v2-test",
        stream_epoch="epoch-1",
        symbol="BTCUSDT",
        tape_sequence=1,
        exchange_update_id=100,
        exchange_at_ms=_T0,
        received_at_ms=_T0 + 10,
        persisted_at_ms=_T0 + 20,
        raw_payload_sha256=hashlib.sha256(b"v1-no-retained-text").hexdigest(),
        continuity="ADMITTED",
        bids=(RecordedBookLevelV1(price=99.9, quantity=2.0),),
        asks=(RecordedBookLevelV1(price=100.1, quantity=2.0),),
    )


def _frame_v2() -> RecordedTopNBookFrameV2:
    return RecordedTopNBookFrameV2(
        source="sim-book-v2-test",
        tape_id="tape-v2-test",
        stream_epoch="epoch-1",
        symbol="BTCUSDT",
        tape_sequence=1,
        exchange_update_id=100,
        exchange_at_ms=_T0,
        received_at_ms=_T0 + 10,
        persisted_at_ms=_T0 + 20,
        raw_payload=_RAW_PAYLOAD,
        raw_payload_sha256=hashlib.sha256(_RAW_PAYLOAD.encode("utf-8")).hexdigest(),
        continuity="ADMITTED",
        source_reason="SNAPSHOT_RECEIVED",
        bids=(RecordedBookLevelV1(price=99.9, quantity=2.0),),
        asks=(RecordedBookLevelV1(price=100.1, quantity=2.0),),
    )


def _row(frame: RecordedTopNBookFrameV1 | RecordedTopNBookFrameV2) -> dict[str, object]:
    encoded, payload_sha256 = canonical_payload(frame.to_payload())
    source_reason, raw_payload_text = SimulationRepository._book_frame_raw_evidence(frame)
    if frame.frame_sha256 is None:
        raise AssertionError("test requires canonical frame identity")
    return {
        "payload": json.loads(encoded),
        "payload_sha256": payload_sha256,
        "frame_sha256": frame.frame_sha256,
        "raw_payload_sha256": frame.raw_payload_sha256,
        "frame_contract_version": frame.contract_version,
        "source_reason": source_reason,
        "raw_payload_text": raw_payload_text,
    }


def test_stored_v2_book_frame_binds_text_columns_to_the_canonical_payload() -> None:
    frame = _frame_v2()

    assert SimulationRepository._stored_book_frame(_row(frame)) == frame


def test_stored_v1_book_frame_remains_readable_without_retained_raw_text() -> None:
    frame = _frame_v1()

    assert SimulationRepository._stored_book_frame(_row(frame)) == frame


@pytest.mark.parametrize("column", ("raw_payload_text", "source_reason", "frame_contract_version"))
def test_stored_v2_book_frame_rejects_a_mutated_duplicate_evidence_column(column: str) -> None:
    row = _row(_frame_v2())
    row[column] = "MUTATED"

    with pytest.raises(MessageIdentityConflict, match="durable payload"):
        SimulationRepository._stored_book_frame(row)


def test_simulator_v2_migration_is_not_in_the_runtime_topology() -> None:
    migration_path = Path(__file__).parents[1] / "kairos_persistence" / "migrations"
    migration = (migration_path / "019_simulator_book_frame_v2.sql").read_text(encoding="utf-8")

    assert "ALTER TABLE sim_book_frames" in migration
    assert "raw_payload_text" in migration
    assert "sim-book-frame.v2" in migration
