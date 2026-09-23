"""Durable, isolated journal for the offline market-data simulator.

The simulator has its own tables and public contracts.  Nothing in this module
reuses PAPER/LIVE trade state, execution effects, an exchange account, or a
venue adapter.  The only shared primitives are the generic canonical audit and
outbox transaction helpers.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, TypeVar, cast

import asyncpg
from kairos_core.contracts import (
    ClosedBarEventV1,
    RecordedTopNBookFrameV1,
    RecordedTopNBookFrameV2,
    SimulationAdmissionV2,
    SimulationBookChainHeadV1,
    SimulationChainHeadV1,
    SimulationCommandReceiptV1,
    SimulationCommandV1,
    SimulationResultV1,
    SimulationRiskDecisionV1,
    SimulationSessionReceiptV1,
    SimulationSessionV1,
    SimulationTapeSealV1,
    SimulationTradeEventV2,
    SimulationTradeJournalHeadV1,
    SimulationTradeV1,
)
from pydantic import BaseModel

from .repository import AuditRepository, MessageIdentityConflict
from .runtime import canonical_payload

_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT")
_SIMULATOR_OUTBOX_PRODUCER = "kairos-market-simulator"
_Model = TypeVar("_Model", bound=BaseModel)
_SimulationSymbol = Literal["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
_SimulationTradeState = Literal["PENDING", "ACTIVE", "FLAT", "UNRESOLVED", "NO_FILL", "BLOCKED"]
_SimulationCommandStatus = Literal["PREPARED", "COMPLETED", "FAILED"]
_RecordedBookFrame = RecordedTopNBookFrameV1 | RecordedTopNBookFrameV2


@dataclass(frozen=True, slots=True)
class SimulationCommandPreparation:
    """The immutable command persisted before its pure model calculation."""

    command: SimulationCommandV1
    created: bool
    status: Literal["PREPARED", "COMPLETED", "FAILED"]


@dataclass(frozen=True, slots=True)
class SimulationCommandCompletion:
    """Terminal receipt and whether this call created the durable fact."""

    receipt: SimulationCommandReceiptV1
    created: bool


@dataclass(frozen=True, slots=True)
class SimulationTradeJournal:
    """Verified materialized SIM lifecycle state and its immutable event chain.

    This is a read model only.  The controller uses it to resume a prepared
    command without assuming that an in-memory lifecycle state survived a
    process restart.  It intentionally carries no venue or account state.
    """

    trade: SimulationTradeV1
    state: _SimulationTradeState
    next_event_seq: int
    journal_head_sha256: str | None
    events: tuple[SimulationTradeEventV2, ...]


@dataclass(frozen=True, slots=True)
class SimulationBookSymbolCursor:
    """The durable tail for one symbol in an open simulator book tape.

    The recorder uses this only to resume its own deterministic continuity
    checks.  It is not a market-data frame and carries no trading authority.
    """

    symbol: _SimulationSymbol
    stream_epoch: str
    exchange_update_id: int
    exchange_at_ms: int
    received_at_ms: int
    persisted_at_ms: int


@dataclass(frozen=True, slots=True)
class SimulationBookRecordingCursor:
    """Verified append position for a still-open, isolated simulator tape.

    ``None`` from :meth:`load_open_book_recording_cursor` means no tape exists
    yet.  A sealed or blocked tape is rejected rather than reopened.
    """

    tape_id: str
    next_tape_sequence: int
    previous_frame_sha256: str | None
    symbol_cursors: tuple[SimulationBookSymbolCursor, ...]


class SimulationRepository:
    """Append-only input tape plus crash-safe SIM lifecycle journal.

    Lock order is always ``tape -> session -> trade``.  Raw tape writers take
    only the tape lock; session work takes the session lock and then its trade
    lock.  This deliberately favors deterministic, reviewable simulation over
    concurrent throughput.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool
        self.audit = AuditRepository(pool)

    async def record_closed_bar(self, tape_id: str, bar: ClosedBarEventV1) -> bool:
        """Append one canonical, contiguous Binance closed bar to an open tape."""

        self._validate_tape_id(tape_id)
        if bar.symbol not in _SYMBOLS or bar.venue != "BINANCE_UM" or bar.bar_sha256 is None:
            raise ValueError("simulator accepts only canonical five-symbol Binance UM closed bars")
        if bar.message_id != bar.bar_sha256:
            raise ValueError("simulator closed bar message_id must equal its immutable bar hash")
        encoded, payload_sha256 = canonical_payload(bar.to_payload())
        async with self.pool.acquire() as connection, connection.transaction():
            await self._tape_lock(connection, tape_id)
            await self._ensure_open_tape(connection, tape_id)
            existing = await connection.fetchrow(
                """SELECT bar_sha256, payload_sha256, payload
                   FROM sim_closed_bars
                   WHERE tape_id=$1 AND symbol=$2 AND open_time_ms=$3 FOR UPDATE""",
                tape_id,
                bar.symbol,
                bar.open_time_ms,
            )
            if existing is not None:
                self._assert_payload_identity(
                    existing,
                    expected_payload_sha256=payload_sha256,
                    expected_identifier=bar.bar_sha256,
                    identifier_column="bar_sha256",
                    entity="simulation closed bar",
                )
                return False
            previous = await connection.fetchrow(
                """SELECT close_time_ms, bar_sha256, chain_sha256
                   FROM sim_closed_bars
                   WHERE tape_id=$1 AND symbol=$2
                   ORDER BY open_time_ms DESC LIMIT 1 FOR UPDATE""",
                tape_id,
                bar.symbol,
            )
            previous_bar_sha256: str | None = None
            previous_chain_sha256: str | None = None
            if previous is not None:
                if bar.open_time_ms != int(previous["close_time_ms"]) + 1:
                    raise ValueError("simulation tape closed bars must be a contiguous prefix")
                previous_bar_sha256 = str(previous["bar_sha256"])
                previous_chain_sha256 = str(previous["chain_sha256"])
            chain_sha256 = self._chain_sha256(
                domain="kairos.sim.closed-bar-chain.v1",
                tape_id=tape_id,
                symbol=bar.symbol,
                previous_chain_sha256=previous_chain_sha256,
                leaf_sha256=bar.bar_sha256,
            )
            await connection.execute(
                """INSERT INTO sim_closed_bars
                   (tape_id, symbol, open_time_ms, close_time_ms, bar_sha256,
                    previous_bar_sha256, chain_sha256, payload_sha256, payload)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)""",
                tape_id,
                bar.symbol,
                bar.open_time_ms,
                bar.close_time_ms,
                bar.bar_sha256,
                previous_bar_sha256,
                chain_sha256,
                payload_sha256,
                encoded,
            )
        return True

    async def record_book_frame(self, frame: _RecordedBookFrame) -> bool:
        """Append a globally ordered frame and retain every source barrier."""

        if frame.frame_sha256 is None or frame.message_id != frame.frame_sha256:
            raise ValueError("simulation book frame requires its canonical message and frame identity")
        encoded, payload_sha256 = canonical_payload(frame.to_payload())
        async with self.pool.acquire() as connection, connection.transaction():
            await self._tape_lock(connection, frame.tape_id)
            await self._ensure_open_tape(connection, frame.tape_id)
            existing = await connection.fetchrow(
                """SELECT *
                   FROM sim_book_frames WHERE tape_id=$1 AND tape_sequence=$2 FOR UPDATE""",
                frame.tape_id,
                frame.tape_sequence,
            )
            if existing is not None:
                self._assert_payload_identity(
                    existing,
                    expected_payload_sha256=payload_sha256,
                    expected_identifier=frame.frame_sha256,
                    identifier_column="frame_sha256",
                    entity="simulation book frame",
                )
                self._assert_book_frame_storage(existing, frame)
                return False
            same_hash = await connection.fetchrow(
                """SELECT *
                   FROM sim_book_frames WHERE tape_id=$1 AND frame_sha256=$2 FOR UPDATE""",
                frame.tape_id,
                frame.frame_sha256,
            )
            if same_hash is not None:
                self._assert_payload_identity(
                    same_hash,
                    expected_payload_sha256=payload_sha256,
                    expected_identifier=frame.frame_sha256,
                    identifier_column="frame_sha256",
                    entity="simulation book frame",
                )
                self._assert_book_frame_storage(same_hash, frame)
                raise MessageIdentityConflict(
                    "simulation book frame was replayed at a different tape sequence"
                )
            previous = await connection.fetchrow(
                """SELECT tape_sequence, frame_sha256, chain_sha256
                   FROM sim_book_frames WHERE tape_id=$1
                   ORDER BY tape_sequence DESC LIMIT 1 FOR UPDATE""",
                frame.tape_id,
            )
            previous_chain_sha256: str | None = None
            if previous is None:
                if frame.tape_sequence != 1 or frame.previous_frame_sha256 is not None:
                    raise ValueError("a simulation book tape must start at global sequence one")
            else:
                if frame.tape_sequence != int(previous["tape_sequence"]) + 1:
                    raise ValueError("simulation book frames must have contiguous global tape sequences")
                if frame.previous_frame_sha256 != str(previous["frame_sha256"]):
                    raise MessageIdentityConflict(
                        "simulation book predecessor does not match the durable tape"
                    )
                previous_chain_sha256 = str(previous["chain_sha256"])
            same_symbol = await connection.fetchrow(
                """SELECT stream_epoch, exchange_update_id, exchange_at_ms, received_at_ms, persisted_at_ms
                   FROM sim_book_frames WHERE tape_id=$1 AND symbol=$2
                   ORDER BY tape_sequence DESC LIMIT 1 FOR UPDATE""",
                frame.tape_id,
                frame.symbol,
            )
            if same_symbol is not None:
                if frame.stream_epoch == str(same_symbol["stream_epoch"]):
                    if (
                        frame.exchange_update_id <= int(same_symbol["exchange_update_id"])
                        or frame.exchange_at_ms < int(same_symbol["exchange_at_ms"])
                        or frame.received_at_ms < int(same_symbol["received_at_ms"])
                        or frame.persisted_at_ms < int(same_symbol["persisted_at_ms"])
                    ):
                        raise ValueError("simulation frame clocks or update IDs regressed for its symbol")
                elif frame.continuity != "RECONNECT":
                    raise ValueError(
                        "a simulation stream epoch change must be recorded as a reconnect barrier"
                    )
            chain_sha256 = self._chain_sha256(
                domain="kairos.sim.book-frame-chain.v1",
                tape_id=frame.tape_id,
                symbol=None,
                previous_chain_sha256=previous_chain_sha256,
                leaf_sha256=frame.frame_sha256,
            )
            source_reason, raw_payload_text = self._book_frame_raw_evidence(frame)
            await connection.execute(
                """INSERT INTO sim_book_frames
                   (tape_id, tape_sequence, symbol, stream_epoch, exchange_update_id,
                    exchange_at_ms, received_at_ms, persisted_at_ms, continuity,
                    frame_sha256, previous_frame_sha256, chain_sha256, raw_payload_sha256,
                    frame_contract_version, source_reason, raw_payload_text, payload_sha256, payload)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::jsonb)""",
                frame.tape_id,
                frame.tape_sequence,
                frame.symbol,
                frame.stream_epoch,
                frame.exchange_update_id,
                frame.exchange_at_ms,
                frame.received_at_ms,
                frame.persisted_at_ms,
                frame.continuity,
                frame.frame_sha256,
                frame.previous_frame_sha256,
                chain_sha256,
                frame.raw_payload_sha256,
                frame.contract_version,
                source_reason,
                raw_payload_text,
                payload_sha256,
                encoded,
            )
            if frame.continuity == "ADMITTED":
                await connection.execute(
                    """UPDATE sim_tapes
                       SET book_frame_count=book_frame_count+1, book_chain_head_sha256=$2
                       WHERE tape_id=$1""",
                    frame.tape_id,
                    chain_sha256,
                )
            else:
                await connection.execute(
                    """UPDATE sim_tapes SET state='BLOCKED', blocked_at=now(),
                       blocked_reason=$2, book_frame_count=book_frame_count+1,
                       book_chain_head_sha256=$3 WHERE tape_id=$1""",
                    frame.tape_id,
                    f"SOURCE_{frame.continuity}",
                    chain_sha256,
                )
        return True

    async def load_open_book_recording_cursor(self, tape_id: str) -> SimulationBookRecordingCursor | None:
        """Load a verified append cursor without reopening a completed tape.

        The tape advisory lock makes the returned tail a coherent snapshot for
        a single serial recorder.  The caller must still construct the next
        frame and let :meth:`record_book_frame` enforce continuity at commit
        time; a cursor is never an authority to bypass durable checks.
        """

        self._validate_tape_id(tape_id)
        async with self.pool.acquire() as connection, connection.transaction():
            await self._tape_lock(connection, tape_id)
            tape = await connection.fetchrow(
                "SELECT state FROM sim_tapes WHERE tape_id=$1 FOR UPDATE", tape_id
            )
            if tape is None:
                return None
            if str(tape["state"]) != "OPEN":
                raise ValueError("simulation book recording cursor requires an open tape")
            tail = await connection.fetchrow(
                """SELECT * FROM sim_book_frames WHERE tape_id=$1
                   ORDER BY tape_sequence DESC LIMIT 1 FOR UPDATE""",
                tape_id,
            )
            symbol_rows = await connection.fetch(
                """SELECT DISTINCT ON (symbol) * FROM sim_book_frames
                   WHERE tape_id=$1
                   ORDER BY symbol, tape_sequence DESC""",
                tape_id,
            )
            symbol_cursors: list[SimulationBookSymbolCursor] = []
            for row in symbol_rows:
                frame = self._stored_book_frame(row)
                symbol_cursors.append(
                    SimulationBookSymbolCursor(
                        symbol=cast(_SimulationSymbol, frame.symbol),
                        stream_epoch=frame.stream_epoch,
                        exchange_update_id=frame.exchange_update_id,
                        exchange_at_ms=frame.exchange_at_ms,
                        received_at_ms=frame.received_at_ms,
                        persisted_at_ms=frame.persisted_at_ms,
                    )
                )
            if tail is None:
                return SimulationBookRecordingCursor(
                    tape_id=tape_id,
                    next_tape_sequence=1,
                    previous_frame_sha256=None,
                    symbol_cursors=(),
                )
            tail_frame = self._stored_book_frame(tail)
            if tail_frame.frame_sha256 is None:
                raise MessageIdentityConflict("simulation book tail has no immutable frame identity")
            return SimulationBookRecordingCursor(
                tape_id=tape_id,
                next_tape_sequence=tail_frame.tape_sequence + 1,
                previous_frame_sha256=tail_frame.frame_sha256,
                symbol_cursors=tuple(sorted(symbol_cursors, key=lambda cursor: cursor.symbol)),
            )

    async def seal_tape(
        self,
        tape_id: str,
        *,
        sealed_at_ms: int,
        source: str = "market-simulator",
    ) -> SimulationTapeSealV1:
        """Seal an intact five-symbol tape into one immutable public manifest."""

        self._validate_tape_id(tape_id)
        self._validate_timestamp("sealed_at_ms", sealed_at_ms)
        self._validate_text("source", source)
        async with self.pool.acquire() as connection, connection.transaction():
            await self._tape_lock(connection, tape_id)
            tape = await connection.fetchrow("SELECT * FROM sim_tapes WHERE tape_id=$1 FOR UPDATE", tape_id)
            if tape is None:
                raise KeyError(f"unknown simulation tape {tape_id!r}")
            if tape["state"] == "BLOCKED":
                raise ValueError("a simulation tape with a gap or reconnect barrier cannot seal")
            if tape["state"] == "SEALED":
                return self._stored_model(
                    tape, SimulationTapeSealV1, "simulation tape seal", column="seal_payload"
                )
            bar_rows = await connection.fetch(
                """SELECT symbol, count(*) AS entry_count,
                          (array_agg(chain_sha256 ORDER BY open_time_ms DESC))[1] AS chain_head_sha256
                   FROM sim_closed_bars WHERE tape_id=$1 GROUP BY symbol ORDER BY symbol""",
                tape_id,
            )
            if {str(row["symbol"]) for row in bar_rows} != set(_SYMBOLS):
                raise ValueError("a simulation tape requires a closed-bar chain for all five symbols")
            frame_symbols = await connection.fetch(
                "SELECT DISTINCT symbol FROM sim_book_frames WHERE tape_id=$1 AND continuity='ADMITTED'",
                tape_id,
            )
            if {str(row["symbol"]) for row in frame_symbols} != set(_SYMBOLS):
                raise ValueError("a simulation tape requires admitted books for all five symbols")
            book_tail = await connection.fetchrow(
                """SELECT count(*) AS entry_count,
                          (array_agg(chain_sha256 ORDER BY tape_sequence DESC))[1] AS chain_head_sha256
                   FROM sim_book_frames WHERE tape_id=$1""",
                tape_id,
            )
            if book_tail is None or int(book_tail["entry_count"]) <= 0:
                raise ValueError("a simulation tape requires recorded top-N book frames")
            seal = SimulationTapeSealV1(
                source=source,
                tape_id=tape_id,
                sealed_at_ms=sealed_at_ms,
                bar_chains=tuple(
                    SimulationChainHeadV1(
                        symbol=cast(_SimulationSymbol, str(row["symbol"])),
                        entry_count=int(row["entry_count"]),
                        head_sha256=str(row["chain_head_sha256"]),
                    )
                    for row in bar_rows
                ),
                book_chain=SimulationBookChainHeadV1(
                    entry_count=int(book_tail["entry_count"]),
                    head_sha256=str(book_tail["chain_head_sha256"]),
                ),
            )
            encoded, payload_sha256 = canonical_payload(seal.to_payload())
            await connection.execute(
                """UPDATE sim_tapes
                   SET state='SEALED', sealed_at=now(), tape_sha256=$2,
                       seal_payload_sha256=$3, seal_payload=$4::jsonb
                   WHERE tape_id=$1 AND state='OPEN'""",
                tape_id,
                seal.tape_sha256,
                payload_sha256,
                encoded,
            )
            await self._append_artifact(connection, "kairos.simulation.tape_seal.v1", seal)
        return seal

    async def create_session(self, session: SimulationSessionV1) -> bool:
        """Persist a session only when its immutable tape seal exists exactly."""

        if session.session_id is None or session.assumptions.assumptions_sha256 is None:
            raise ValueError("simulation session requires canonical identities")
        encoded, payload_sha256 = canonical_payload(session.to_payload())
        async with self.pool.acquire() as connection, connection.transaction():
            await self._tape_lock(connection, session.tape_id)
            tape = await connection.fetchrow(
                """SELECT state, tape_sha256 FROM sim_tapes WHERE tape_id=$1 FOR UPDATE""", session.tape_id
            )
            if tape is None or tape["state"] != "SEALED" or str(tape["tape_sha256"]) != session.tape_sha256:
                raise ValueError("simulation session requires the exact sealed simulation tape")
            existing = await connection.fetchrow(
                "SELECT payload_sha256, payload FROM sim_sessions WHERE session_id=$1 FOR UPDATE",
                session.session_id,
            )
            if existing is not None:
                self._assert_payload_identity(existing, payload_sha256, entity="simulation session")
                return False
            await connection.execute(
                """INSERT INTO sim_sessions
                   (session_id, tape_id, tape_sha256, assumptions_sha256, started_at_ms, ends_at_ms,
                    execution_environment, paper_qualification_eligible, trial15_eligible, alpha_claim,
                    payload_sha256, payload)
                   VALUES ($1,$2,$3,$4,$5,$6,'SIMULATED',FALSE,FALSE,FALSE,$7,$8::jsonb)""",
                session.session_id,
                session.tape_id,
                session.tape_sha256,
                session.assumptions.assumptions_sha256,
                session.started_at_ms,
                session.ends_at_ms,
                payload_sha256,
                encoded,
            )
            await self._append_artifact(connection, "kairos.simulation.session.v1", session)
        return True

    async def record_risk_decision(self, decision: SimulationRiskDecisionV1) -> bool:
        """Persist a SIM-only review overlay decision, including rejected facts."""

        if (
            decision.decision_id is None
            or decision.session_id is None
            or decision.intent_id is None
            or decision.review_id is None
        ):
            raise ValueError("simulation risk decision requires canonical identities")
        encoded, payload_sha256 = canonical_payload(decision.to_payload())
        async with self.pool.acquire() as connection, connection.transaction():
            await self._session_lock(connection, decision.session_id)
            session = await connection.fetchrow(
                "SELECT payload_sha256, payload FROM sim_sessions WHERE session_id=$1 FOR UPDATE",
                decision.session_id,
            )
            if session is None:
                raise KeyError("simulation risk decision references an unknown session")
            self._assert_exact_model(session, decision.session, "simulation risk decision session")
            input_hashes = tuple(decision.intent.provenance.input_bar_sha256s)
            stored_inputs = await connection.fetch(
                """SELECT bar_sha256 FROM sim_closed_bars
                   WHERE tape_id=$1 AND bar_sha256 = ANY($2::text[]) FOR UPDATE""",
                decision.session.tape_id,
                list(input_hashes),
            )
            if {str(row["bar_sha256"]) for row in stored_inputs} != set(input_hashes):
                raise ValueError(
                    "simulation risk decision intent provenance is not fully recorded in its sealed tape"
                )
            if decision.selected_book_frame is not None:
                frame = await connection.fetchrow(
                    """SELECT * FROM sim_book_frames
                       WHERE tape_id=$1 AND frame_sha256=$2 FOR UPDATE""",
                    decision.session.tape_id,
                    decision.selected_book_frame.frame_sha256,
                )
                if frame is None:
                    raise ValueError("simulation decision selected book frame is not durably recorded")
                self._assert_exact_book_frame(
                    frame, decision.selected_book_frame, "simulation risk decision book frame"
                )
            existing = await connection.fetchrow(
                "SELECT payload_sha256, payload FROM sim_risk_decisions WHERE decision_id=$1 FOR UPDATE",
                decision.decision_id,
            )
            if existing is not None:
                self._assert_payload_identity(existing, payload_sha256, entity="simulation risk decision")
                return False
            await connection.execute(
                """INSERT INTO sim_risk_decisions
                   (decision_id, session_id, intent_id, review_id, selected_book_frame_sha256, symbol, side,
                    approved, rejection_reasons, quantity, price_cap, decided_at_ms, execution_environment,
                    paper_qualification_eligible, trial15_eligible, alpha_claim, payload_sha256, payload)
                   VALUES (
                       $1,$2,$3,$4,$5,$6,$7,$8,$9::text[],$10,$11,$12,
                       'SIMULATED',FALSE,FALSE,FALSE,$13,$14::jsonb
                   )""",
                decision.decision_id,
                decision.session_id,
                decision.intent_id,
                decision.review_id,
                decision.selected_book_frame_sha256,
                decision.intent.symbol,
                decision.intent.side.value,
                decision.approved,
                list(decision.rejection_reasons),
                decision.quantity,
                decision.price_cap,
                decision.decided_at_ms,
                payload_sha256,
                encoded,
            )
            await self._append_artifact(connection, "kairos.simulation.risk_decision.v1", decision)
        return True

    async def create_admission(self, admission: SimulationAdmissionV2) -> bool:
        """Persist only an admission derived from a recorded approved SIM decision."""

        if (
            admission.admission_id is None
            or admission.session_id is None
            or admission.intent_sha256 is None
            or admission.decision_id is None
            or admission.review_id is None
            or admission.selected_book_frame_sha256 is None
            or admission.intent is None
            or admission.quantity is None
            or admission.price_cap is None
        ):
            raise ValueError("simulation admission requires complete canonical decision lineage")
        encoded, payload_sha256 = canonical_payload(admission.to_payload())
        async with self.pool.acquire() as connection, connection.transaction():
            await self._session_lock(connection, admission.session_id)
            decision = await connection.fetchrow(
                "SELECT payload_sha256, payload FROM sim_risk_decisions WHERE decision_id=$1 FOR UPDATE",
                admission.decision_id,
            )
            if decision is None:
                raise KeyError("simulation admission references an unknown simulation risk decision")
            self._assert_exact_model(decision, admission.decision, "simulation admission decision")
            existing = await connection.fetchrow(
                "SELECT payload_sha256, payload FROM sim_admissions WHERE admission_id=$1 FOR UPDATE",
                admission.admission_id,
            )
            if existing is not None:
                self._assert_payload_identity(existing, payload_sha256, entity="simulation admission")
                return False
            await connection.execute(
                """INSERT INTO sim_admissions
                   (admission_id, decision_id, session_id, intent_id, review_id, selected_book_frame_sha256,
                    symbol, side, quantity, price_cap, admitted_at_ms, execution_environment,
                    paper_qualification_eligible, trial15_eligible, alpha_claim, payload_sha256, payload)
                   VALUES (
                       $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
                       'SIMULATED',FALSE,FALSE,FALSE,$12,$13::jsonb
                   )""",
                admission.admission_id,
                admission.decision_id,
                admission.session_id,
                admission.intent_sha256,
                admission.review_id,
                admission.selected_book_frame_sha256,
                admission.intent.symbol,
                admission.intent.side.value,
                admission.quantity,
                admission.price_cap,
                admission.admitted_at_ms,
                payload_sha256,
                encoded,
            )
            await self._append_artifact(connection, "kairos.simulation.admission.v2", admission)
        return True

    async def create_trade(self, trade: SimulationTradeV1) -> bool:
        """Create one isolated SIM lifecycle after its decision-bound admission exists."""

        admission = trade.admission
        if not isinstance(admission, SimulationAdmissionV2):
            raise ValueError("durable simulator trades require a decision-bound simulation admission.v2")
        if (
            trade.trade_id is None
            or trade.session_id is None
            or trade.admission_id is None
            or trade.intent_id is None
            or trade.symbol is None
            or trade.side is None
        ):
            raise ValueError("simulation trade requires complete canonical lineage")
        encoded, payload_sha256 = canonical_payload(trade.to_payload())
        async with self.pool.acquire() as connection, connection.transaction():
            await self._session_lock(connection, trade.session_id)
            await self._trade_lock(connection, trade.trade_id)
            stored_admission = await connection.fetchrow(
                """SELECT payload_sha256, payload FROM sim_admissions
                   WHERE session_id=$1 AND admission_id=$2 FOR UPDATE""",
                trade.session_id,
                trade.admission_id,
            )
            if stored_admission is None:
                raise KeyError("simulation trade references an unknown admission")
            self._assert_exact_model(stored_admission, admission, "simulation trade admission")
            existing = await connection.fetchrow(
                "SELECT payload_sha256, payload FROM sim_trades WHERE trade_id=$1 FOR UPDATE", trade.trade_id
            )
            if existing is not None:
                self._assert_payload_identity(existing, payload_sha256, entity="simulation trade")
                return False
            await connection.execute(
                """INSERT INTO sim_trades
                   (trade_id, session_id, admission_id, intent_id, symbol, side, state,
                    execution_environment, paper_qualification_eligible, trial15_eligible, alpha_claim,
                    payload_sha256, payload, created_at_ms)
                   VALUES ($1,$2,$3,$4,$5,$6,'PENDING','SIMULATED',FALSE,FALSE,FALSE,$7,$8::jsonb,$9)""",
                trade.trade_id,
                trade.session_id,
                trade.admission_id,
                trade.intent_id,
                trade.symbol,
                trade.side.value,
                payload_sha256,
                encoded,
                trade.created_at_ms,
            )
            await self._append_artifact(connection, "kairos.simulation.trade.v1", trade)
        return True

    async def append_trade_event(self, event: SimulationTradeEventV2) -> bool:
        """Append one ordered lifecycle fact; exact delivery is idempotent."""

        async with self.pool.acquire() as connection, connection.transaction():
            await self._session_lock(connection, event.session_id)
            await self._trade_lock(connection, event.trade_id)
            created = await self._append_trade_event_locked(connection, event)
        return created

    async def prepare_command(self, command: SimulationCommandV1) -> SimulationCommandPreparation:
        """Durably prepare a typed model command before invoking the pure kernel."""

        if (
            command.command_id is None
            or command.session_id is None
            or command.admission_id is None
            or command.intent_id is None
            or command.trade_id is None
            or command.symbol is None
            or command.side is None
        ):
            raise ValueError("simulation command requires complete canonical trade lineage")
        encoded, payload_sha256 = canonical_payload(command.to_payload())
        async with self.pool.acquire() as connection, connection.transaction():
            await self._session_lock(connection, command.session_id)
            await self._trade_lock(connection, command.trade_id)
            await self._assert_exact_trade(connection, command.trade, command.session_id, command.trade_id)
            existing = await connection.fetchrow(
                "SELECT * FROM sim_commands WHERE command_id=$1 FOR UPDATE", command.command_id
            )
            if existing is not None:
                self._assert_payload_identity(existing, payload_sha256, entity="simulation command")
                stored = self._stored_model(existing, SimulationCommandV1, "simulation command")
                status = str(existing["status"])
                if status not in {"PREPARED", "COMPLETED", "FAILED"}:
                    raise MessageIdentityConflict("simulation command has an unknown durable status")
                return SimulationCommandPreparation(
                    command=stored,
                    created=False,
                    status=cast(_SimulationCommandStatus, status),
                )
            await connection.execute(
                """INSERT INTO sim_commands
                    (command_id, session_id, admission_id, intent_id, trade_id, symbol, side, order_side,
                     command_kind, payload_sha256, status, payload)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'PREPARED',$11::jsonb)""",
                command.command_id,
                command.session_id,
                command.admission_id,
                command.intent_id,
                command.trade_id,
                command.symbol,
                command.side.value,
                command.order_side,
                command.command_kind,
                payload_sha256,
                encoded,
            )
            await self._append_artifact(connection, "kairos.simulation.command.v1", command)
        return SimulationCommandPreparation(command=command, created=True, status="PREPARED")

    async def complete_command(
        self,
        command: SimulationCommandV1,
        receipt: SimulationCommandReceiptV1,
        *,
        model_state_schema_version: str,
        model_state_payload: Mapping[str, Any],
        events: Sequence[SimulationTradeEventV2] = (),
    ) -> SimulationCommandCompletion:
        """Atomically save a terminal model receipt, state snapshot, and events.

        ``model_state_payload`` is a canonicalized private snapshot supplied by
        the already-tested pure kernel.  It is deliberately not a public
        execution contract; only ``receipt`` and ``events`` are public SIM
        facts.  A replay never recalculates a receipt or consumes depth twice.
        """

        if (
            command.command_id is None
            or command.session_id is None
            or command.admission_id is None
            or command.intent_id is None
            or command.trade_id is None
            or command.symbol is None
        ):
            raise ValueError("simulation command requires complete canonical trade lineage")
        if receipt.command_id != command.command_id or receipt.command.command_id != command.command_id:
            raise MessageIdentityConflict("simulation receipt does not belong to the prepared command")
        if self._model_payload(receipt.command) != self._model_payload(command):
            raise MessageIdentityConflict(
                "simulation receipt command differs from the prepared command payload"
            )
        if receipt.receipt_id is None:
            raise ValueError("simulation receipt requires its canonical identity")
        self._validate_text("model_state_schema_version", model_state_schema_version)
        state_object = dict(model_state_payload)
        if (
            state_object.get("session_id") != command.session_id
            or state_object.get("symbol") != command.symbol
        ):
            raise ValueError(
                "private simulator liquidity snapshot does not match the command session and symbol"
            )
        encoded_state, state_sha256 = canonical_payload(state_object)
        encoded_command, command_payload_sha256 = canonical_payload(command.to_payload())
        encoded_receipt, receipt_payload_sha256 = canonical_payload(receipt.to_payload())
        async with self.pool.acquire() as connection, connection.transaction():
            await self._session_lock(connection, command.session_id)
            await self._trade_lock(connection, command.trade_id)
            row = await connection.fetchrow(
                "SELECT * FROM sim_commands WHERE command_id=$1 FOR UPDATE", command.command_id
            )
            if row is None:
                raise KeyError("simulation command was not prepared")
            self._assert_payload_identity(row, command_payload_sha256, entity="simulation command")
            existing_receipt = await connection.fetchrow(
                "SELECT * FROM sim_command_receipts WHERE command_id=$1 FOR UPDATE", command.command_id
            )
            if existing_receipt is not None:
                self._assert_payload_identity(
                    existing_receipt,
                    receipt_payload_sha256,
                    entity="simulation command receipt",
                )
                if row["status"] != "COMPLETED":
                    raise MessageIdentityConflict(
                        "a simulator command receipt exists without a completed command"
                    )
                return SimulationCommandCompletion(
                    receipt=self._stored_model(
                        existing_receipt,
                        SimulationCommandReceiptV1,
                        "simulation command receipt",
                    ),
                    created=False,
                )
            if row["status"] != "PREPARED":
                raise MessageIdentityConflict("simulation command is not available for terminal completion")
            await self._assert_exact_trade(connection, command.trade, command.session_id, command.trade_id)
            await connection.execute(
                """INSERT INTO sim_command_receipts
                    (receipt_id, command_id, session_id, admission_id, intent_id, trade_id, status,
                     payload_sha256, payload)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)""",
                receipt.receipt_id,
                command.command_id,
                receipt.session_id,
                receipt.admission_id,
                receipt.intent_id,
                receipt.trade_id,
                receipt.status,
                receipt_payload_sha256,
                encoded_receipt,
            )
            await connection.execute(
                """INSERT INTO sim_liquidity_states
                   (session_id, symbol, state_schema_version, state_sha256, payload)
                   VALUES ($1,$2,$3,$4,$5::jsonb)
                   ON CONFLICT (session_id, symbol) DO UPDATE SET
                       state_schema_version=EXCLUDED.state_schema_version,
                       state_sha256=EXCLUDED.state_sha256, payload=EXCLUDED.payload, updated_at=now()""",
                command.session_id,
                command.symbol,
                model_state_schema_version,
                state_sha256,
                encoded_state,
            )
            for event in events:
                if event.command_id != command.command_id or event.receipt_id != receipt.receipt_id:
                    raise MessageIdentityConflict(
                        "command completion event must reference this command and receipt"
                    )
                await self._append_trade_event_locked(connection, event)
            updated = await connection.execute(
                """UPDATE sim_commands SET status='COMPLETED', completed_at=now()
                   WHERE command_id=$1 AND status='PREPARED'""",
                command.command_id,
            )
            if not updated.endswith("1"):
                raise RuntimeError("simulation command completion lost its prepared state")
            await self._append_artifact(connection, "kairos.simulation.command_receipt.v1", receipt)
        return SimulationCommandCompletion(receipt=receipt, created=True)

    async def record_result(self, result: SimulationResultV1) -> bool:
        """Attach exactly one terminal SIM result to its own terminal event."""

        if result.result_id is None:
            raise ValueError("simulation result requires its canonical identity")
        encoded, payload_sha256 = canonical_payload(result.to_payload())
        async with self.pool.acquire() as connection, connection.transaction():
            await self._session_lock(connection, result.session_id)
            await self._trade_lock(connection, result.trade_id)
            trade = await connection.fetchrow(
                "SELECT * FROM sim_trades WHERE trade_id=$1 FOR UPDATE", result.trade_id
            )
            if trade is None:
                raise KeyError("simulation result references an unknown trade")
            self._assert_trade_lineage(trade, result.session_id, result.admission_id, result.intent_id)
            if str(trade["state"]) != result.final_state:
                raise ValueError("simulation result final state does not match the durable trade state")
            terminal = await connection.fetchrow(
                """SELECT event_id, to_state FROM sim_trade_events
                   WHERE trade_id=$1 AND event_id=$2 FOR UPDATE""",
                result.trade_id,
                result.terminal_event_id,
            )
            if terminal is None or str(terminal["to_state"]) != result.final_state:
                raise ValueError("simulation result must reference its trade's terminal lifecycle event")
            existing = await connection.fetchrow(
                "SELECT payload_sha256, payload FROM sim_results WHERE result_id=$1 FOR UPDATE",
                result.result_id,
            )
            if existing is not None:
                self._assert_payload_identity(existing, payload_sha256, entity="simulation result")
                return False
            prior_for_trade = await connection.fetchrow(
                "SELECT result_id FROM sim_results WHERE trade_id=$1 FOR UPDATE", result.trade_id
            )
            if prior_for_trade is not None:
                raise MessageIdentityConflict("a simulation trade already has a different terminal result")
            await connection.execute(
                """INSERT INTO sim_results
                   (result_id, session_id, admission_id, intent_id, trade_id, terminal_event_id, final_state,
                    execution_environment, venue_execution_observed, paper_qualification_eligible,
                    trial15_eligible, alpha_claim, payload_sha256, payload, completed_at_ms)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,'SIMULATED',FALSE,FALSE,FALSE,FALSE,$8,$9::jsonb,$10)""",
                result.result_id,
                result.session_id,
                result.admission_id,
                result.intent_id,
                result.trade_id,
                result.terminal_event_id,
                result.final_state,
                payload_sha256,
                encoded,
                result.completed_at_ms,
            )
            await self._append_artifact(connection, "kairos.simulation.result.v1", result)
        return True

    async def record_session_receipt(self, receipt: SimulationSessionReceiptV1) -> bool:
        """Seal a session only after every supplied journal head verifies exactly."""

        if (
            receipt.receipt_id is None
            or receipt.session_id is None
            or receipt.tape_id is None
            or receipt.tape_sha256 is None
        ):
            raise ValueError("simulation session receipt requires canonical session lineage")
        encoded, payload_sha256 = canonical_payload(receipt.to_payload())
        async with self.pool.acquire() as connection, connection.transaction():
            await self._session_lock(connection, receipt.session_id)
            session = await connection.fetchrow(
                "SELECT * FROM sim_sessions WHERE session_id=$1 FOR UPDATE", receipt.session_id
            )
            if session is None:
                raise KeyError("simulation session receipt references an unknown session")
            self._assert_exact_model(session, receipt.session, "simulation session receipt session")
            if (
                str(session["tape_id"]) != receipt.tape_id
                or str(session["tape_sha256"]) != receipt.tape_sha256
            ):
                raise MessageIdentityConflict(
                    "simulation session receipt tape lineage does not match its session"
                )
            rows = await connection.fetch(
                """SELECT trade_id, state, event_count, journal_head_sha256
                   FROM sim_trades WHERE session_id=$1 ORDER BY trade_id FOR UPDATE""",
                receipt.session_id,
            )
            expected_heads = tuple(
                SimulationTradeJournalHeadV1(
                    trade_id=str(row["trade_id"]),
                    state=cast(_SimulationTradeState, str(row["state"])),
                    event_count=int(row["event_count"]),
                    journal_head_sha256=(
                        None if row["journal_head_sha256"] is None else str(row["journal_head_sha256"])
                    ),
                )
                for row in rows
            )
            if tuple(receipt.trade_journals) != expected_heads:
                raise MessageIdentityConflict(
                    "simulation session receipt journal heads do not match durable state"
                )
            if receipt.receipt_state == "COMPLETED" and any(
                row["state"] not in {"FLAT", "UNRESOLVED", "NO_FILL", "BLOCKED"} for row in rows
            ):
                raise ValueError("a completed simulation session still has a non-terminal trade")
            existing = await connection.fetchrow(
                "SELECT payload_sha256, payload FROM sim_session_receipts WHERE receipt_id=$1 FOR UPDATE",
                receipt.receipt_id,
            )
            if existing is not None:
                self._assert_payload_identity(existing, payload_sha256, entity="simulation session receipt")
                return False
            prior = await connection.fetchrow(
                "SELECT receipt_id FROM sim_session_receipts WHERE session_id=$1 FOR UPDATE",
                receipt.session_id,
            )
            if prior is not None:
                raise MessageIdentityConflict("simulation session already has a different terminal receipt")
            await connection.execute(
                """INSERT INTO sim_session_receipts
                   (receipt_id, session_id, tape_id, tape_sha256, receipt_state, payload_sha256, payload,
                    completed_at_ms)
                   VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)""",
                receipt.receipt_id,
                receipt.session_id,
                receipt.tape_id,
                receipt.tape_sha256,
                receipt.receipt_state,
                payload_sha256,
                encoded,
                receipt.completed_at_ms,
            )
            state = "COMPLETED" if receipt.receipt_state == "COMPLETED" else "BLOCKED"
            await connection.execute(
                "UPDATE sim_sessions SET state=$2 WHERE session_id=$1", receipt.session_id, state
            )
            await self._append_artifact(connection, "kairos.simulation.session_receipt.v1", receipt)
        return True

    async def load_liquidity_state(self, session_id: str, symbol: str) -> tuple[str, dict[str, Any]] | None:
        """Return the immutable private kernel snapshot for an isolated SIM session."""

        self._validate_session_symbol(session_id, symbol)
        row = await self.pool.fetchrow(
            """SELECT state_schema_version, state_sha256, payload
               FROM sim_liquidity_states WHERE session_id=$1 AND symbol=$2""",
            session_id,
            symbol,
        )
        if row is None:
            return None
        payload = self._object(row["payload"])
        if canonical_payload(payload)[1] != str(row["state_sha256"]):
            raise MessageIdentityConflict("simulation private liquidity state payload hash does not match")
        return str(row["state_schema_version"]), payload

    async def load_latest_book_frame(
        self,
        tape_id: str,
        symbol: str,
        *,
        as_of_ms: int,
    ) -> _RecordedBookFrame | None:
        """Return the newest admitted, sealed-tape book no later than ``as_of_ms``.

        The frame's captured ``persisted_at_ms`` is the only clock used for
        selection.  This prevents the controller from reaching forward into a
        recorded tape based on later knowledge, and refuses an unsealed tape
        rather than silently modelling mutable market data.
        """

        self._validate_tape_id(tape_id)
        if symbol not in _SYMBOLS:
            raise ValueError("simulation symbol is outside the fixed five-symbol universe")
        self._validate_timestamp("as_of_ms", as_of_ms)
        tape = await self.pool.fetchrow("SELECT state FROM sim_tapes WHERE tape_id=$1", tape_id)
        if tape is None:
            raise KeyError(f"unknown simulation tape {tape_id!r}")
        if str(tape["state"]) != "SEALED":
            raise ValueError("simulation book selection requires an immutable sealed tape")
        row = await self.pool.fetchrow(
            """SELECT * FROM sim_book_frames
               WHERE tape_id=$1 AND symbol=$2 AND continuity='ADMITTED'
                 AND persisted_at_ms <= $3
               ORDER BY persisted_at_ms DESC, tape_sequence DESC
               LIMIT 1""",
            tape_id,
            symbol,
            as_of_ms,
        )
        if row is None:
            return None
        return self._stored_book_frame(row)

    async def load_closed_bar_page(
        self,
        tape_id: str,
        symbol: str,
        *,
        after_open_time_ms: int | None = None,
        limit: int = 1_000,
    ) -> tuple[ClosedBarEventV1, ...]:
        """Read a bounded, ordered page from one sealed SIM tape.

        Replay callers must first run :meth:`verify_tape` for the immutable
        tape. This method then checks every returned row's canonical payload
        and bar identity, and requires a supplied cursor to be an exact stored
        bar boundary so pagination cannot silently jump over a gap.
        """

        self._validate_tape_id(tape_id)
        if symbol not in _SYMBOLS:
            raise ValueError("simulation bar selection is outside the fixed five-symbol universe")
        if after_open_time_ms is not None:
            self._validate_timestamp("after_open_time_ms", after_open_time_ms)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 10_000:
            raise ValueError("simulation bar page limit must be an integer from 1 through 10000")

        async with (
            self.pool.acquire() as connection,
            connection.transaction(isolation="repeatable_read", readonly=True),
        ):
            tape = await connection.fetchrow(
                "SELECT state, tape_sha256 FROM sim_tapes WHERE tape_id=$1", tape_id
            )
            if tape is None:
                raise KeyError(f"unknown simulation tape {tape_id!r}")
            if tape["state"] != "SEALED" or tape["tape_sha256"] is None:
                raise ValueError("simulation bar replay requires an immutable sealed tape")

            previous_close_time_ms: int | None = None
            if after_open_time_ms is not None:
                cursor = await connection.fetchrow(
                    """SELECT symbol, open_time_ms, close_time_ms, bar_sha256, payload_sha256, payload
                       FROM sim_closed_bars
                       WHERE tape_id=$1 AND symbol=$2 AND open_time_ms=$3""",
                    tape_id,
                    symbol,
                    after_open_time_ms,
                )
                if cursor is None:
                    raise ValueError("simulation bar page cursor is not a stored bar boundary")
                previous = self._stored_model(cursor, ClosedBarEventV1, "simulation closed bar cursor")
                if (
                    previous.symbol != symbol
                    or previous.symbol != str(cursor["symbol"])
                    or previous.open_time_ms != int(cursor["open_time_ms"])
                    or previous.close_time_ms != int(cursor["close_time_ms"])
                    or previous.bar_sha256 != cursor["bar_sha256"]
                ):
                    raise MessageIdentityConflict(
                        "simulation bar page cursor identity does not match its row"
                    )
                previous_close_time_ms = previous.close_time_ms

            rows = await connection.fetch(
                """SELECT symbol, open_time_ms, close_time_ms, bar_sha256, payload_sha256, payload
                   FROM sim_closed_bars
                   WHERE tape_id=$1 AND symbol=$2
                     AND ($3::bigint IS NULL OR open_time_ms > $3)
                   ORDER BY open_time_ms
                   LIMIT $4""",
                tape_id,
                symbol,
                after_open_time_ms,
                limit,
            )

        bars = tuple(self._stored_model(row, ClosedBarEventV1, "simulation closed bar") for row in rows)
        for bar, row in zip(bars, rows, strict=True):
            if (
                bar.symbol != symbol
                or bar.symbol != str(row["symbol"])
                or bar.open_time_ms != int(row["open_time_ms"])
                or bar.close_time_ms != int(row["close_time_ms"])
                or bar.bar_sha256 != row["bar_sha256"]
            ):
                raise MessageIdentityConflict("simulation closed bar identity does not match its tape row")
            if previous_close_time_ms is not None and bar.open_time_ms != previous_close_time_ms + 1:
                raise MessageIdentityConflict("simulation closed bar page is not a contiguous tape prefix")
            previous_close_time_ms = bar.close_time_ms
        return bars

    async def load_command_receipt(self, command_id: str) -> SimulationCommandReceiptV1 | None:
        """Load one terminal receipt exactly as it was persisted, if any."""

        self._validate_sha256("command_id", command_id)
        row = await self.pool.fetchrow(
            "SELECT payload_sha256, payload FROM sim_command_receipts WHERE command_id=$1",
            command_id,
        )
        if row is None:
            return None
        receipt = self._stored_model(row, SimulationCommandReceiptV1, "simulation command receipt")
        if receipt.command_id != command_id:
            raise MessageIdentityConflict("simulation command receipt does not match its durable command")
        return receipt

    async def list_prepared_commands(self, session_id: str) -> tuple[SimulationCommandV1, ...]:
        """Return only still-prepared commands for one isolated session.

        A caller may safely resume these commands.  Completed commands are
        deliberately omitted so recovery cannot recalculate a durable model
        receipt or consume simulated depth twice.
        """

        self._validate_sha256("session_id", session_id)
        rows = await self.pool.fetch(
            """SELECT payload_sha256, payload FROM sim_commands
               WHERE session_id=$1 AND status='PREPARED'
               ORDER BY prepared_at, command_id""",
            session_id,
        )
        commands = tuple(self._stored_model(row, SimulationCommandV1, "simulation command") for row in rows)
        if any(command.session_id != session_id for command in commands):
            raise MessageIdentityConflict("prepared simulation command differs from requested session")
        return commands

    async def load_trade_journal(self, trade_id: str) -> SimulationTradeJournal | None:
        """Load and verify the durable state materialization for one SIM trade."""

        self._validate_sha256("trade_id", trade_id)
        async with self.pool.acquire() as connection, connection.transaction():
            await self._trade_lock(connection, trade_id)
            trade_row = await connection.fetchrow(
                "SELECT * FROM sim_trades WHERE trade_id=$1 FOR SHARE", trade_id
            )
            if trade_row is None:
                return None
            event_rows = await connection.fetch(
                """SELECT * FROM sim_trade_events WHERE trade_id=$1
                   ORDER BY event_seq FOR SHARE""",
                trade_id,
            )
            return self._materialize_trade_journal(trade_row, event_rows)

    async def list_terminal_trades_without_result(self, session_id: str) -> tuple[SimulationTradeV1, ...]:
        """Return terminal lifecycles that need idempotent result recovery."""

        self._validate_sha256("session_id", session_id)
        rows = await self.pool.fetch(
            """SELECT trade.payload_sha256, trade.payload
               FROM sim_trades AS trade
               LEFT JOIN sim_results AS result ON result.trade_id=trade.trade_id
               WHERE trade.session_id=$1
                 AND trade.state = ANY($2::text[])
                 AND result.trade_id IS NULL
               ORDER BY trade.trade_id""",
            session_id,
            ["FLAT", "UNRESOLVED", "NO_FILL", "BLOCKED"],
        )
        trades = tuple(self._stored_model(row, SimulationTradeV1, "simulation trade") for row in rows)
        if any(trade.session_id != session_id for trade in trades):
            raise MessageIdentityConflict("terminal simulation trade differs from requested session")
        return trades

    async def verify_tape(self, tape_id: str) -> bool:
        """Verify every stored public input and its per-bar/global-book hash chains."""

        self._validate_tape_id(tape_id)
        async with self.pool.acquire() as connection, connection.transaction():
            await self._tape_lock(connection, tape_id)
            tape = await connection.fetchrow("SELECT * FROM sim_tapes WHERE tape_id=$1 FOR UPDATE", tape_id)
            if tape is None:
                return False
            bars = await connection.fetch(
                """SELECT * FROM sim_closed_bars WHERE tape_id=$1
                   ORDER BY symbol, open_time_ms FOR UPDATE""",
                tape_id,
            )
            bar_heads: dict[str, tuple[int, str]] = {}
            previous_by_symbol: dict[str, tuple[int, str, str]] = {}
            for row in bars:
                bar = self._stored_model(row, ClosedBarEventV1, "simulation closed bar")
                bar_sha256 = bar.bar_sha256
                if (
                    bar_sha256 is None
                    or bar_sha256 != row["bar_sha256"]
                    or bar.symbol != str(row["symbol"])
                    or bar.open_time_ms != int(row["open_time_ms"])
                    or bar.close_time_ms != int(row["close_time_ms"])
                ):
                    return False
                symbol = str(row["symbol"])
                previous = previous_by_symbol.get(symbol)
                if previous is None:
                    expected_previous_bar = None
                    expected_previous_chain = None
                else:
                    previous_close_time, expected_previous_bar, expected_previous_chain = previous
                    if bar.open_time_ms != previous_close_time + 1:
                        return False
                if row["previous_bar_sha256"] != expected_previous_bar:
                    return False
                expected_chain = self._chain_sha256(
                    domain="kairos.sim.closed-bar-chain.v1",
                    tape_id=tape_id,
                    symbol=symbol,
                    previous_chain_sha256=expected_previous_chain,
                    leaf_sha256=bar_sha256,
                )
                if row["chain_sha256"] != expected_chain:
                    return False
                previous_by_symbol[symbol] = (bar.close_time_ms, bar_sha256, expected_chain)
                count, _head = bar_heads.get(symbol, (0, ""))
                bar_heads[symbol] = (count + 1, expected_chain)
            frames = await connection.fetch(
                "SELECT * FROM sim_book_frames WHERE tape_id=$1 ORDER BY tape_sequence FOR UPDATE", tape_id
            )
            previous_sequence = 0
            previous_frame_sha256: str | None = None
            previous_chain_sha256: str | None = None
            symbol_clocks: dict[str, tuple[str, int, int, int, int]] = {}
            for row in frames:
                frame = self._stored_book_frame(row)
                frame_sha256 = frame.frame_sha256
                if frame_sha256 is None or frame_sha256 != row["frame_sha256"]:
                    return False
                if frame.tape_sequence != previous_sequence + 1:
                    return False
                if frame.previous_frame_sha256 != previous_frame_sha256:
                    return False
                expected_chain = self._chain_sha256(
                    domain="kairos.sim.book-frame-chain.v1",
                    tape_id=tape_id,
                    symbol=None,
                    previous_chain_sha256=previous_chain_sha256,
                    leaf_sha256=frame_sha256,
                )
                if row["chain_sha256"] != expected_chain:
                    return False
                prior_symbol = symbol_clocks.get(frame.symbol)
                if prior_symbol is not None:
                    epoch, update_id, exchange_at, received_at, persisted_at = prior_symbol
                    if frame.stream_epoch == epoch and (
                        frame.exchange_update_id <= update_id
                        or frame.exchange_at_ms < exchange_at
                        or frame.received_at_ms < received_at
                        or frame.persisted_at_ms < persisted_at
                    ):
                        return False
                    if frame.stream_epoch != epoch and frame.continuity != "RECONNECT":
                        return False
                symbol_clocks[frame.symbol] = (
                    frame.stream_epoch,
                    frame.exchange_update_id,
                    frame.exchange_at_ms,
                    frame.received_at_ms,
                    frame.persisted_at_ms,
                )
                previous_sequence = frame.tape_sequence
                previous_frame_sha256 = frame_sha256
                previous_chain_sha256 = expected_chain
            if (
                int(tape["book_frame_count"]) != len(frames)
                or tape["book_chain_head_sha256"] != previous_chain_sha256
            ):
                return False
            if tape["state"] == "SEALED":
                seal = self._stored_model(
                    tape, SimulationTapeSealV1, "simulation tape seal", column="seal_payload"
                )
                if seal.tape_sha256 != tape["tape_sha256"]:
                    return False
                expected_bars = tuple(
                    SimulationChainHeadV1(
                        symbol=cast(_SimulationSymbol, symbol),
                        entry_count=count,
                        head_sha256=head,
                    )
                    for symbol, (count, head) in sorted(bar_heads.items())
                )
                expected_book = SimulationBookChainHeadV1(
                    entry_count=len(frames),
                    head_sha256=previous_chain_sha256,
                )
                if seal.bar_chains != expected_bars or seal.book_chain != expected_book:
                    return False
            return True

    async def verify_trade_chain(self, trade_id: str) -> bool:
        """Verify an append-only V2 lifecycle chain and its materialized trade state."""

        self._validate_sha256("trade_id", trade_id)
        async with self.pool.acquire() as connection, connection.transaction():
            await self._trade_lock(connection, trade_id)
            trade = await connection.fetchrow(
                "SELECT * FROM sim_trades WHERE trade_id=$1 FOR UPDATE", trade_id
            )
            if trade is None:
                return False
            events = await connection.fetch(
                "SELECT * FROM sim_trade_events WHERE trade_id=$1 ORDER BY event_seq FOR UPDATE", trade_id
            )
            expected_sequence = 1
            previous_event_sha256: str | None = None
            state = "PENDING"
            for row in events:
                event = self._stored_model(row, SimulationTradeEventV2, "simulation trade event")
                if (
                    event.event_seq != expected_sequence
                    or event.previous_event_sha256 != previous_event_sha256
                ):
                    return False
                if event.event_id != row["event_id"] or event.event_id != row["event_sha256"]:
                    return False
                if event.trade_id != trade_id or event.session_id != trade["session_id"]:
                    return False
                if expected_sequence == 1:
                    if event.from_state is not None or event.to_state != state:
                        return False
                elif event.from_state != state:
                    return False
                state = event.to_state
                previous_event_sha256 = event.event_id
                expected_sequence += 1
            return (
                str(trade["state"]) == state
                and int(trade["next_event_seq"]) == expected_sequence
                and trade["journal_head_sha256"] == previous_event_sha256
                and int(trade["event_count"]) == len(events)
            )

    @classmethod
    def _materialize_trade_journal(
        cls,
        trade_row: asyncpg.Record,
        event_rows: Sequence[asyncpg.Record],
    ) -> SimulationTradeJournal:
        """Validate a read-model snapshot before it is used for recovery.

        ``load_trade_journal`` uses the same invariants as the explicit chain
        verifier, but returns the verified public events for a controller that
        needs the next immutable transition.  No lifecycle fact is inferred
        from table columns alone.
        """

        trade = cls._stored_model(trade_row, SimulationTradeV1, "simulation trade")
        if (
            trade.trade_id != str(trade_row["trade_id"])
            or trade.session_id != str(trade_row["session_id"])
            or trade.admission_id != str(trade_row["admission_id"])
            or trade.intent_id != str(trade_row["intent_id"])
            or trade.symbol != str(trade_row["symbol"])
            or trade.side is None
            or trade.side.value != str(trade_row["side"])
        ):
            raise MessageIdentityConflict("simulation trade table lineage differs from its public payload")
        expected_sequence = 1
        previous_event_sha256: str | None = None
        state: _SimulationTradeState = "PENDING"
        events: list[SimulationTradeEventV2] = []
        for row in event_rows:
            event = cls._stored_model(row, SimulationTradeEventV2, "simulation trade event")
            if (
                event.trade_id != trade.trade_id
                or event.session_id != trade.session_id
                or event.admission_id != trade.admission_id
                or event.intent_id != trade.intent_id
                or event.symbol != trade.symbol
                or event.side != trade.side.value
                or event.event_id is None
                or event.event_id != str(row["event_id"])
                or event.event_id != str(row["event_sha256"])
                or event.event_seq != expected_sequence
                or event.previous_event_sha256 != previous_event_sha256
            ):
                raise MessageIdentityConflict("simulation trade event chain differs from durable lifecycle")
            expected_from_state = None if expected_sequence == 1 else state
            if event.from_state != expected_from_state:
                raise MessageIdentityConflict("simulation event prior state differs from durable lifecycle")
            state = cast(_SimulationTradeState, event.to_state)
            previous_event_sha256 = event.event_id
            expected_sequence += 1
            events.append(event)
        materialized_state = str(trade_row["state"])
        if materialized_state not in {"PENDING", "ACTIVE", "FLAT", "UNRESOLVED", "NO_FILL", "BLOCKED"}:
            raise MessageIdentityConflict("simulation trade has an unknown materialized lifecycle state")
        if (
            materialized_state != state
            or int(trade_row["next_event_seq"]) != expected_sequence
            or trade_row["journal_head_sha256"] != previous_event_sha256
            or int(trade_row["event_count"]) != len(events)
        ):
            raise MessageIdentityConflict("simulation trade materialization does not match its event chain")
        return SimulationTradeJournal(
            trade=trade,
            state=cast(_SimulationTradeState, materialized_state),
            next_event_seq=expected_sequence,
            journal_head_sha256=previous_event_sha256,
            events=tuple(events),
        )

    async def _append_trade_event_locked(
        self,
        connection: asyncpg.Connection,
        event: SimulationTradeEventV2,
    ) -> bool:
        if event.event_id is None:
            raise ValueError("simulation trade event requires its canonical identity")
        encoded, payload_sha256 = canonical_payload(event.to_payload())
        existing = await connection.fetchrow(
            "SELECT * FROM sim_trade_events WHERE event_id=$1 FOR UPDATE", event.event_id
        )
        if existing is not None:
            if str(existing["trade_id"]) != event.trade_id:
                raise MessageIdentityConflict("simulation event ID was reused by a different trade")
            self._assert_payload_identity(existing, payload_sha256, entity="simulation trade event")
            return False
        trade = await connection.fetchrow(
            "SELECT * FROM sim_trades WHERE trade_id=$1 FOR UPDATE", event.trade_id
        )
        if trade is None:
            raise KeyError("simulation event references an unknown trade")
        self._assert_trade_lineage(trade, event.session_id, event.admission_id, event.intent_id)
        if event.symbol != trade["symbol"] or event.side != trade["side"]:
            raise MessageIdentityConflict("simulation event symbol or side differs from its durable trade")
        if event.event_seq != int(trade["next_event_seq"]):
            raise MessageIdentityConflict(
                "simulation event sequence is not the next durable lifecycle sequence"
            )
        if event.previous_event_sha256 != trade["journal_head_sha256"]:
            raise MessageIdentityConflict(
                "simulation event predecessor does not match the durable journal head"
            )
        if event.event_seq == 1:
            if event.from_state is not None or event.to_state != trade["state"]:
                raise MessageIdentityConflict(
                    "simulation admission event does not match the initial trade state"
                )
        elif event.from_state != trade["state"]:
            raise MessageIdentityConflict(
                "simulation event prior state does not match durable lifecycle state"
            )
        if event.command_id is not None or event.receipt_id is not None:
            if event.command_id is None or event.receipt_id is None:
                raise MessageIdentityConflict(
                    "simulation model events require both command and receipt lineage"
                )
            receipt = await connection.fetchrow(
                """SELECT command_id, trade_id, receipt_id FROM sim_command_receipts
                   WHERE receipt_id=$1 FOR UPDATE""",
                event.receipt_id,
            )
            if (
                receipt is None
                or str(receipt["command_id"]) != event.command_id
                or str(receipt["trade_id"]) != event.trade_id
            ):
                raise MessageIdentityConflict("simulation event command receipt lineage is not durable")
        updated = await connection.execute(
            """UPDATE sim_trades SET state=$2, state_version=state_version+1,
                   next_event_seq=next_event_seq+1, journal_head_sha256=$3,
                   event_count=event_count+1, updated_at=now()
               WHERE trade_id=$1 AND state_version=$4""",
            event.trade_id,
            event.to_state,
            event.event_id,
            trade["state_version"],
        )
        if not updated.endswith("1"):
            raise RuntimeError("simulation lifecycle transition lost its serialized state version")
        await connection.execute(
            """INSERT INTO sim_trade_events
               (event_id, trade_id, event_seq, previous_event_sha256, event_sha256, from_state, to_state,
                event_type, occurred_at_ms, command_id, receipt_id, payload_sha256, payload)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb)""",
            event.event_id,
            event.trade_id,
            event.event_seq,
            event.previous_event_sha256,
            event.event_id,
            event.from_state,
            event.to_state,
            event.event_type,
            event.occurred_at_ms,
            event.command_id,
            event.receipt_id,
            payload_sha256,
            encoded,
        )
        await self._append_artifact(connection, "kairos.simulation.trade_event.v2", event)
        return True

    async def _ensure_open_tape(self, connection: asyncpg.Connection, tape_id: str) -> None:
        await connection.execute(
            """INSERT INTO sim_tapes(tape_id, market_data_venue)
               VALUES ($1, 'BINANCE_UM') ON CONFLICT (tape_id) DO NOTHING""",
            tape_id,
        )
        row = await connection.fetchrow("SELECT state FROM sim_tapes WHERE tape_id=$1 FOR UPDATE", tape_id)
        if row is None or row["state"] != "OPEN":
            raise ValueError("simulation tape is not open for another immutable input")

    async def _assert_exact_trade(
        self,
        connection: asyncpg.Connection,
        trade: SimulationTradeV1,
        session_id: str,
        trade_id: str,
    ) -> None:
        row = await connection.fetchrow("SELECT * FROM sim_trades WHERE trade_id=$1 FOR UPDATE", trade_id)
        if row is None:
            raise KeyError("simulation command references an unknown trade")
        if str(row["session_id"]) != session_id:
            raise MessageIdentityConflict("simulation trade session differs from command lineage")
        self._assert_exact_model(row, trade, "simulation command trade")

    async def _append_artifact(
        self,
        connection: asyncpg.Connection,
        topic: str,
        message: BaseModel,
    ) -> None:
        payload = self._model_payload(message)
        message_id = payload.get("message_id")
        if not isinstance(message_id, str) or not message_id:
            raise ValueError("simulation public artifact requires a stable message_id")
        encoded, payload_sha256 = canonical_payload(payload)
        await self.audit.append_payload_strict(
            topic,
            payload,
            encoded,
            payload_sha256,
            connection=connection,
        )
        await self.audit.enqueue_outbox(
            connection,
            message_id=message_id,
            topic=topic,
            payload=encoded,
            payload_sha256=payload_sha256,
            producer=_SIMULATOR_OUTBOX_PRODUCER,
        )

    @staticmethod
    def _book_frame_raw_evidence(frame: _RecordedBookFrame) -> tuple[str | None, str | None]:
        """Return V2-only immutable recorder evidence for one insert."""

        if isinstance(frame, RecordedTopNBookFrameV2):
            return frame.source_reason, frame.raw_payload
        return None, None

    @classmethod
    def _stored_book_frame(cls, row: asyncpg.Record) -> _RecordedBookFrame:
        """Rehydrate a V1/V2 frame and bind its duplicate raw-evidence columns.

        Payload JSON alone is not enough for V2: the separate ``TEXT`` column
        is what retains the recorder's exact source bytes through ordinary
        JSONB canonicalization.  Every reader verifies both representations,
        so direct column mutation becomes a fail-closed integrity error.
        """

        payload = cls._object(row["payload"])
        if canonical_payload(payload)[1] != row["payload_sha256"]:
            raise MessageIdentityConflict(
                "simulation book frame stored payload hash does not match its durable JSON"
            )
        version = payload.get("contract_version")
        model_type: type[RecordedTopNBookFrameV1] | type[RecordedTopNBookFrameV2]
        if version == "sim-book-frame.v1":
            model_type = RecordedTopNBookFrameV1
        elif version == "sim-book-frame.v2":
            model_type = RecordedTopNBookFrameV2
        else:
            raise MessageIdentityConflict("simulation book frame has an unknown contract version")
        try:
            frame = model_type.model_validate(payload)
        except Exception as exc:  # public validation errors are intentionally not a storage API
            raise MessageIdentityConflict(
                "simulation book frame stored payload no longer satisfies its public contract"
            ) from exc
        if frame.frame_sha256 is None or str(row["frame_sha256"]) != frame.frame_sha256:
            raise MessageIdentityConflict("simulation book frame identifier does not match durable payload")
        if str(row["raw_payload_sha256"]) != frame.raw_payload_sha256:
            raise MessageIdentityConflict("simulation book raw payload hash does not match durable payload")
        if str(row["frame_contract_version"]) != frame.contract_version:
            raise MessageIdentityConflict("simulation book contract version does not match durable payload")
        source_reason, raw_payload_text = cls._book_frame_raw_evidence(frame)
        if row["source_reason"] != source_reason or row["raw_payload_text"] != raw_payload_text:
            raise MessageIdentityConflict("simulation book raw evidence columns do not match durable payload")
        return frame

    @classmethod
    def _assert_book_frame_storage(cls, row: asyncpg.Record, expected: _RecordedBookFrame) -> None:
        """Require a replayed frame to match both its public and raw evidence."""

        actual = cls._stored_book_frame(row)
        if actual != expected:
            raise MessageIdentityConflict(
                "simulation book frame stable ID was reused with different evidence"
            )

    @classmethod
    def _assert_exact_book_frame(cls, row: asyncpg.Record, expected: _RecordedBookFrame, entity: str) -> None:
        actual = cls._stored_book_frame(row)
        if actual != expected:
            raise MessageIdentityConflict(f"{entity} stable ID was reused with a different immutable payload")

    @classmethod
    def _stored_model(
        cls,
        row: asyncpg.Record,
        model_type: type[_Model],
        entity: str,
        *,
        column: str = "payload",
    ) -> _Model:
        payload = cls._object(row[column])
        fingerprint_column = "payload_sha256" if column == "payload" else f"{column}_sha256"
        stored_fingerprint = row[fingerprint_column]
        if canonical_payload(payload)[1] != stored_fingerprint:
            raise MessageIdentityConflict(f"{entity} stored payload hash does not match its durable JSON")
        try:
            return model_type.model_validate(payload)
        except Exception as exc:  # validation error type is intentionally an implementation detail here
            raise MessageIdentityConflict(
                f"{entity} stored payload no longer satisfies its public contract"
            ) from exc

    @classmethod
    def _assert_exact_model(cls, row: asyncpg.Record, model: BaseModel, entity: str) -> None:
        encoded, expected_payload_sha256 = canonical_payload(cls._model_payload(model))
        del encoded
        cls._assert_payload_identity(row, expected_payload_sha256, entity=entity)

    @classmethod
    def _assert_payload_identity(
        cls,
        row: asyncpg.Record,
        expected_payload_sha256: str,
        *,
        entity: str,
        expected_identifier: str | None = None,
        identifier_column: str | None = None,
    ) -> None:
        payload = cls._object(row["payload"])
        stored_payload_sha256 = row["payload_sha256"]
        actual_payload_sha256 = canonical_payload(payload)[1]
        if stored_payload_sha256 != actual_payload_sha256:
            raise MessageIdentityConflict(f"{entity} stored payload hash does not match its durable JSON")
        if actual_payload_sha256 != expected_payload_sha256:
            raise MessageIdentityConflict(f"{entity} stable ID was reused with a different immutable payload")
        if expected_identifier is not None:
            if identifier_column is None or str(row[identifier_column]) != expected_identifier:
                raise MessageIdentityConflict(f"{entity} stable identifier was reused with a different value")

    @staticmethod
    def _model_payload(model: BaseModel) -> dict[str, Any]:
        payload = model.model_dump(mode="json")
        if not isinstance(payload, dict):
            raise TypeError("simulation public model did not produce an object payload")
        return payload

    @staticmethod
    def _object(value: Any) -> dict[str, Any]:
        if isinstance(value, str):
            parsed = json.loads(value)
        elif isinstance(value, Mapping):
            parsed = dict(value)
        else:
            raise MessageIdentityConflict("durable simulator payload is not a JSON object")
        if not isinstance(parsed, dict):
            raise MessageIdentityConflict("durable simulator payload is not a JSON object")
        return parsed

    @staticmethod
    def _chain_sha256(
        *,
        domain: str,
        tape_id: str,
        symbol: str | None,
        previous_chain_sha256: str | None,
        leaf_sha256: str,
    ) -> str:
        return canonical_payload(
            {
                "domain": domain,
                "leaf_sha256": leaf_sha256,
                "previous_chain_sha256": previous_chain_sha256,
                "symbol": symbol,
                "tape_id": tape_id,
            }
        )[1]

    @staticmethod
    def _assert_trade_lineage(
        trade: asyncpg.Record,
        session_id: str,
        admission_id: str,
        intent_id: str,
    ) -> None:
        expected = {
            "session_id": session_id,
            "admission_id": admission_id,
            "intent_id": intent_id,
        }
        mismatches = [name for name, value in expected.items() if str(trade[name]) != value]
        if mismatches:
            raise MessageIdentityConflict(
                "simulation artifact differs from durable trade lineage: " + ", ".join(sorted(mismatches))
            )

    @staticmethod
    async def _tape_lock(connection: asyncpg.Connection, tape_id: str) -> None:
        await connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            f"kairos.sim.tape:{tape_id}",
        )

    @staticmethod
    async def _session_lock(connection: asyncpg.Connection, session_id: str) -> None:
        await connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            f"kairos.sim.session:{session_id}",
        )

    @staticmethod
    async def _trade_lock(connection: asyncpg.Connection, trade_id: str) -> None:
        await connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            f"kairos.sim.trade:{trade_id}",
        )

    @staticmethod
    def _validate_text(name: str, value: str) -> None:
        if not value or value != value.strip() or len(value) > 128:
            raise ValueError(f"{name} must be a normalized non-empty string no longer than 128 characters")

    @classmethod
    def _validate_tape_id(cls, tape_id: str) -> None:
        cls._validate_text("tape_id", tape_id)

    @classmethod
    def _validate_session_symbol(cls, session_id: str, symbol: str) -> None:
        cls._validate_sha256("session_id", session_id)
        if symbol not in _SYMBOLS:
            raise ValueError("simulation symbol is outside the fixed five-symbol universe")

    @staticmethod
    def _validate_sha256(name: str, value: str) -> None:
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError(f"{name} must be a lowercase SHA-256 hex string")

    @staticmethod
    def _validate_timestamp(name: str, value: int) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be a non-negative integer Unix timestamp in milliseconds")


# Kept as a source-compatible spelling for the unpublished simulator draft.
SimulationJournalRepository = SimulationRepository
