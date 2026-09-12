# Cumulative DEV qualification budget

The fixed campaign is `kairos-dev-qualification-v1`. Its total ceilings are
OpenAI **$12**, DeepSeek **$1**, X **$2**. They do **not** reset at a calendar
month, process restart, new service identity, or a new qualification run.

There is **one authoritative paid-shadow database** for this campaign. Every
paid runtime and probe must use that same database. Do not register separate
allowances in the technical PAPER database, a second Compose project, a restored
backup, or a local probe database. The technical PAPER/canary contour makes no
paid API calls. This repository provides transactionally shared accounting
inside one PostgreSQL database; it does **not** claim cross-database budget
coordination. Deployment must preserve this single-database authority.

`SourceStateRepository(..., campaign_id=QUALIFICATION_CAMPAIGN_ID)` requires an
explicit adoption receipt before it will reserve any paid request. The LLM
runtime adapter and official X runtime/probe use this identity. All older
COMMITTED and RESERVED rows for that provider, across all services/months, are
counted directly from `source_usage_reservations`; they are not copied. X aliases
`x`, `x-api`, `x_api` share a cap. Ambiguous requests remain RESERVED until their
billing is authoritatively reconciled. A timeout is not a refund.

## Required operator procedure

1. Stop **all** paid producers/probes, including older binaries whose monthly
   reservation implementation does not use the new provider-wide lock.
2. Back up the intended database and verify its identity. Apply immutable
   migration `013_campaign_source_budgets.sql` through the normal deployment
   migration workflow; the adoption/probe CLIs never run migrations themselves.
3. Reconcile historical provider receipts and qualification reports. Existing
   database reservations are already counted. Include **only additional**
   off-ledger committed spend and conservative ceilings for ambiguous old
   probes in `off_ledger_cost_microusd`. Do not double-count the same request in
   both categories. If the evidence is incomplete, do not register a fabricated
   zero or enable paid calls; complete reconciliation first.
4. Create one local receipt per provider. Every field below is mandatory:

   ```json
   {
     "schema_version": "kairos.campaign-budget-adoption.v1",
     "campaign_id": "kairos-dev-qualification-v1",
     "source": "openai",
     "budget_microusd": 12000000,
     "off_ledger_cost_microusd": 250000,
     "reconciled_through": "2026-09-12T00:00:00Z",
     "evidence_note": "EXAMPLE ONLY: replace amount and cite reconciled receipt hashes."
   }
   ```

   The sample amount is **not** Kairos's actual historical spend. Do not copy it
   as evidence. Never include API keys or account secrets in this receipt.

5. Hash the exact receipt bytes, then validate without mutation:

   ```powershell
   python -m kairos_persistence.campaign_adoption `
     --receipt-file C:\local-evidence\openai-adoption.json `
     --expected-receipt-sha256 <actual-sha256> `
     --expected-database-name <exact-target-database>
   ```

   `KAIROS_PERSISTENCE_DATABASE_URL` must already point at the intended database
   using the normal local secret loader. It must not be put in shell history.
   Validation defaults to `VALIDATED_NOT_APPLIED` and makes no DB connection.

6. To register, add `--apply --confirm-paid-producers-stopped`. If the reconciled
   off-ledger amount is genuinely zero, also add
   `--confirm-zero-off-ledger-spend`; zero is never a default. Preserve the
   receipt and returned summary with the backup/evidence bundle.
7. Roll out only binaries sharing this campaign ledger and verify
   `campaign_usage(provider)`. No provider call is made by registration.

The provider binding is immutable: an identical retry is harmless, but a
different receipt, historical amount, campaign or cap cannot reset it. Already
exhausted historical spend can be recorded; it leaves zero spendable capacity.
Changing a registered campaign requires a separate reviewed operational plan,
not deleting rows or selecting a new campaign name.

## Compatibility and observation

Legacy generic APIs retain their monthly behavior for unregistered providers.
After registration even that interface obeys the cumulative bound; its
`monthly_budget_microusd` parameter can only impose a tighter ceiling. Monthly
usage queries/legacy Prometheus spend counters still describe a calendar month,
not remaining campaign allowance. Use `campaign_usage()` for the authoritative
campaign committed/reserved/imported totals. The fixed API budget is not a
trading-capital allocation and registration does not arm PAPER/LIVE.

X qualification retains an additional per-run hard ceiling and reports each
run's potential usage. LLM qualification retains its planned-run ceiling while
every actual inference reserves against the same provider campaign. All failed
or uncertain paid calls conservatively consume an outstanding reservation.

## Verification without API calls

Unit tests cover missing registration, month rollover, provider aliases,
immutable adoption, receipt validation and explicit zero confirmation.
`scripts/campaign_budget_integration.py` adds real PostgreSQL concurrent
admission, old committed/unknown-ACK adoption, restart, idempotent commit and
legacy monthly-bypass tests. It refuses any database not named
`kairos_budget_test_*`, and refuses nonempty test evidence rather than deleting
it. Supply `KAIROS_BUDGET_TEST_DATABASE_URL` and a separate exact confirmation
`KAIROS_BUDGET_TEST_DATABASE_NAME=kairos_budget_test_<unique-suffix>` for that
isolated database. Only `localhost`, `127.0.0.1`, `::1` and Docker `timescaledb`
hosts are allowed for the drill.

Both the drill and adoption require an explicit URL host/port and a literal
database path exactly matching the confirmed name. Query parameters (including
dbname/host/options), fragments and encoded database paths are rejected;
percent-encoded credentials remain supported. Adoption can use a remote shadow
database host, but the drill cannot. Each pool connection checks PostgreSQL
`current_database()` before any migration or registration, including the drill's
reconnection. A mismatch closes the pool without writing. Adoption validation
without `--apply` checks the receipt and URL only; it does not connect or claim
that server identity was verified.
