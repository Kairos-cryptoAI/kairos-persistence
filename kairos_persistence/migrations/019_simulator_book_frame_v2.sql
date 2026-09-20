-- Durable raw-source evidence for the isolated kairos-sim book recorder.
--
-- This migration belongs exclusively to MigrationProfile.SIMULATOR.  It does
-- not alter a runtime/PAPER table or grant any readiness authority.  V1 rows
-- remain readable without a synthetic raw payload; V2 rows must retain the
-- exact UTF-8 text that the recorder hashed before durable insertion.

ALTER TABLE sim_book_frames
    ADD COLUMN frame_contract_version TEXT NOT NULL DEFAULT 'sim-book-frame.v1',
    ADD COLUMN source_reason TEXT,
    ADD COLUMN raw_payload_text TEXT;

ALTER TABLE sim_book_frames
    ADD CONSTRAINT sim_book_frames_contract_version
    CHECK (frame_contract_version IN ('sim-book-frame.v1', 'sim-book-frame.v2')),
    ADD CONSTRAINT sim_book_frames_raw_evidence
    CHECK (
        (
            frame_contract_version = 'sim-book-frame.v1'
            AND source_reason IS NULL
            AND raw_payload_text IS NULL
        )
        OR
        (
            frame_contract_version = 'sim-book-frame.v2'
            AND source_reason IS NOT NULL
            AND source_reason ~ '^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*$'
            AND raw_payload_text IS NOT NULL
            AND octet_length(raw_payload_text) > 0
        )
    );
