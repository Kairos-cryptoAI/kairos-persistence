"""Explicit, receipt-bound qualification budget adoption; never calls a provider."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from .config import PersistenceSettings
from .database import Database
from .database_target import connect_verified_database, require_database_target_url
from .source_state import CAMPAIGN_PROVIDER_CAPS, QUALIFICATION_CAMPAIGN_ID, SourceStateRepository

RECEIPT_SCHEMA = "kairos.campaign-budget-adoption.v1"
_RECEIPT_FIELDS = {
    "schema_version",
    "campaign_id",
    "source",
    "budget_microusd",
    "off_ledger_cost_microusd",
    "reconciled_through",
    "evidence_note",
}


def load_receipt(path: Path, expected_sha256: str, *, confirm_zero: bool = False) -> dict[str, object]:
    raw = path.read_bytes()
    if len(raw) > 32_768:
        raise ValueError("adoption receipt exceeds 32 KiB")
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("adoption receipt SHA-256 mismatch")
    payload = json.loads(raw)
    if not isinstance(payload, dict) or set(payload) != _RECEIPT_FIELDS:
        raise ValueError("adoption receipt requires exactly the registered fields")
    if payload["schema_version"] != RECEIPT_SCHEMA or payload["campaign_id"] != QUALIFICATION_CAMPAIGN_ID:
        raise ValueError("adoption receipt campaign/schema mismatch")
    source = payload["source"]
    if not isinstance(source, str) or source not in CAMPAIGN_PROVIDER_CAPS:
        raise ValueError("unknown campaign provider")
    budget = payload["budget_microusd"]
    historical = payload["off_ledger_cost_microusd"]
    SourceStateRepository._positive_int(budget, "budget_microusd")
    SourceStateRepository._nonnegative_int(historical, "off_ledger_cost_microusd")
    if budget > CAMPAIGN_PROVIDER_CAPS[source] or historical > 2**63 - 1:
        raise ValueError("adoption receipt cost exceeds its limit")
    if historical == 0 and not confirm_zero:
        raise ValueError("zero off-ledger spend requires explicit reconciliation confirmation")
    note = payload["evidence_note"]
    if not isinstance(note, str) or not note.strip() or len(note) > 4096:
        raise ValueError("receipt must explain the historical spend reconciliation")
    instant = payload["reconciled_through"]
    if not isinstance(instant, str):
        raise ValueError("reconciled_through must be an aware ISO timestamp")
    SourceStateRepository._aware(datetime.fromisoformat(instant))
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt-file", required=True, type=Path)
    parser.add_argument("--expected-receipt-sha256", required=True)
    parser.add_argument("--expected-database-name", required=True)
    parser.add_argument("--confirm-zero-off-ledger-spend", action="store_true")
    parser.add_argument("--confirm-paid-producers-stopped", action="store_true")
    parser.add_argument("--apply", action="store_true", help="register; default only validates the receipt")
    return parser


async def adopt(
    payload: dict[str, object],
    receipt_sha256: str,
    *,
    database: Database,
    expected_database_name: str,
) -> dict[str, object]:
    """Connect to an already migrated database; never migrate or reset it."""
    await connect_verified_database(database, expected_database_name)
    try:
        repository = SourceStateRepository(database.pool, campaign_id=QUALIFICATION_CAMPAIGN_ID)
        await repository.register_campaign(
            source=str(payload["source"]),
            budget_microusd=int(str(payload["budget_microusd"])),
            historical_cost_microusd=int(str(payload["off_ledger_cost_microusd"])),
            historical_evidence_sha256=receipt_sha256,
        )
        usage = await repository.campaign_usage(str(payload["source"]))
        return {
            "state": "REGISTERED",
            "campaign_id": usage.campaign_id,
            "source": usage.source,
            "budget_microusd": usage.budget_microusd,
            "budgeted_cost_microusd": usage.budgeted_cost_microusd,
            "remaining_microusd": max(0, usage.budget_microusd - usage.budgeted_cost_microusd),
            "receipt_sha256": receipt_sha256,
        }
    finally:
        await database.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    receipt = load_receipt(
        args.receipt_file,
        args.expected_receipt_sha256,
        confirm_zero=args.confirm_zero_off_ledger_spend,
    )
    settings = PersistenceSettings()
    require_database_target_url(settings.database_url, args.expected_database_name)
    if not args.apply:
        print(
            json.dumps(
                {
                    "state": "VALIDATED_NOT_APPLIED",
                    "campaign_id": QUALIFICATION_CAMPAIGN_ID,
                    "source": receipt["source"],
                    "receipt_sha256": args.expected_receipt_sha256,
                }
            )
        )
        return 0
    if not args.confirm_paid_producers_stopped:
        raise ValueError("stop every old paid producer before activating the cumulative campaign")
    print(
        json.dumps(
            asyncio.run(
                adopt(
                    receipt,
                    args.expected_receipt_sha256,
                    database=Database(settings),
                    expected_database_name=args.expected_database_name,
                )
            )
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
