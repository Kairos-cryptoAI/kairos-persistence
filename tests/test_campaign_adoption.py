from __future__ import annotations

import hashlib
import json

import pytest

from kairos_persistence.campaign_adoption import RECEIPT_SCHEMA, load_receipt, main
from kairos_persistence.source_state import QUALIFICATION_CAMPAIGN_ID


def _receipt(tmp_path, **changes):
    payload = {
        "schema_version": RECEIPT_SCHEMA,
        "campaign_id": QUALIFICATION_CAMPAIGN_ID,
        "source": "openai",
        "budget_microusd": 12_000_000,
        "off_ledger_cost_microusd": 250_000,
        "reconciled_through": "2026-09-12T00:00:00Z",
        "evidence_note": "Synthetic receipt for offline tests; includes unresolved probe estimates.",
        **changes,
    }
    path = tmp_path / "adoption.json"
    raw = json.dumps(payload).encode()
    path.write_bytes(raw)
    return path, hashlib.sha256(raw).hexdigest()


def test_adoption_receipt_has_no_default_zero_and_binds_exact_bytes(tmp_path):
    path, digest = _receipt(tmp_path)
    assert load_receipt(path, digest)["off_ledger_cost_microusd"] == 250_000
    with pytest.raises(ValueError, match="SHA-256"):
        load_receipt(path, "0" * 64)
    path, digest = _receipt(tmp_path, off_ledger_cost_microusd=0)
    with pytest.raises(ValueError, match="zero"):
        load_receipt(path, digest)
    assert load_receipt(path, digest, confirm_zero=True)["off_ledger_cost_microusd"] == 0


@pytest.mark.parametrize(
    "changes",
    [
        {"off_ledger_cost_microusd": -1},
        {"off_ledger_cost_microusd": True},
        {"budget_microusd": 12_000_001},
        {"source": "other"},
        {"extra": "forbidden"},
        {"reconciled_through": "2026-09-12"},
        {"evidence_note": ""},
    ],
)
def test_invalid_receipts_fail_before_database(tmp_path, changes):
    path, digest = _receipt(tmp_path, **changes)
    with pytest.raises(ValueError):
        load_receipt(path, digest)


def test_adoption_cli_defaults_to_validation_and_guards_database(tmp_path, monkeypatch, capsys):
    path, digest = _receipt(tmp_path)
    monkeypatch.setenv("KAIROS_PERSISTENCE_DATABASE_URL", "postgresql://not-used@localhost:5432/test-only")
    args = [
        "--receipt-file",
        str(path),
        "--expected-receipt-sha256",
        digest,
        "--expected-database-name",
        "test-only",
    ]
    assert main(args) == 0
    assert "VALIDATED_NOT_APPLIED" in capsys.readouterr().out
    with pytest.raises(ValueError, match="stop every"):
        main(args + ["--apply"])
    with pytest.raises(ValueError, match="database"):
        main(args[:-1] + ["wrong-database"])


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://test@localhost:5432/test-only?dbname=kairos",
        "postgresql://test@localhost:5432/%74est-only",
        "postgresql://test@localhost:5432/test-only#ignored",
        "postgresql://test@localhost:5432/test-only/../kairos",
    ],
)
def test_adoption_cli_rejects_ambiguous_url_before_applying(tmp_path, monkeypatch, url):
    path, digest = _receipt(tmp_path)
    monkeypatch.setenv("KAIROS_PERSISTENCE_DATABASE_URL", url)
    with pytest.raises(ValueError, match="database"):
        main(
            [
                "--receipt-file",
                str(path),
                "--expected-receipt-sha256",
                digest,
                "--expected-database-name",
                "test-only",
                "--apply",
                "--confirm-paid-producers-stopped",
            ]
        )
