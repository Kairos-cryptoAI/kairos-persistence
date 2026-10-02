# kairos-persistence

Transactional PostgreSQL/TimescaleDB primitives for Kairos: event audit,
idempotent inbox processing, transactional outbox delivery and execution state.

Paid feed clients also use `SourceStateRepository` for monotonic per-source
cursors and monthly usage reservations. Capacity is reserved transactionally
before a metered request, then committed to the actual returned units or
released on a request failure. Costs use integer micro-USD amounts, so restarts
and concurrent workers cannot silently cross the configured monthly cap.

`DurableLLMUsageBudget` exposes the same ledger as a provider-wide microdollar
budget for `kairos-llm`. Text Scouts, Aggregator and Macro all write under the
shared `kairos-llm-v1/<provider>` identity, so concurrent services cannot each
spend a separate copy of the monthly OpenAI or DeepSeek allowance. One unit is
one microdollar; a provider call is admitted only after the reservation commits.

## Local development

The repository is locked with `uv` 0.12.3 and defaults to Python 3.11. The CI
suite additionally blocks on Python 3.14 compatibility.

```sh
uv sync --locked --dev
uv run ruff check .
uv run mypy kairos_persistence
uv run bandit -q -r kairos_persistence -x tests
uv run pytest -q -m "not integration"
```

The same commands are exposed through `make sync` and `make check` on systems
with Make installed. On Windows, run the `uv` commands directly.

`kairos-core` is resolved from the exact Git commit recorded in
`pyproject.toml` and `uv.lock`. Run `uv lock` deliberately when updating any
dependency and commit the resulting lock-file diff.

## Migration profiles

`Database` applies an explicit topology manifest rather than every SQL file in
the package. The default `MigrationProfile.RUNTIME` is the only profile for
DRY_RUN and PAPER databases; it deliberately omits
`017_simulator_journal.sql` and `019_simulator_book_frame_v2.sql` while still
allowing the later runtime outbox reconciliation migration. An isolated
database named `kairos_sim` or
`kairos_sim_<suffix>` must opt into `MigrationProfile.SIMULATOR` explicitly;
that profile cannot target a runtime/PAPER database, and the runtime profile
cannot target the simulator name. A database history that mixes the two
profiles is rejected before additional DDL is applied.

The simulator profile also owns `020_simulator_llm_proposals.sql` and
`021_simulator_research_decision_samples.sql`. The latter stores one immutable
`ResearchDecisionSampleV1` per campaign/arm/scheduled sample, including both
strategy and LLM outcomes when present. An evaluated no-intent requires an
evaluation receipt; `NOT_EVALUATED` and `NOT_CALLED` are distinct from that
fact. An LLM volatility alert and a failed attempted call remain separate
non-executable outcomes. A completed LLM proposal carries a separately linked
completion receipt and observed response time; a late answer cannot be paired
as if it was available at the scheduled decision. `ResearchDecisionSampleRepository` accepts exact
redelivery, rejects changed facts for the same sample, and loads bounded
integrity-checked pages. The repository does not itself verify the external
source objects named by receipt hashes. Neither table grants execution authority.

SIM-only migration `022_simulator_research_observation_schedule.sql` freezes the
three-arm roster before observations. Its published source remains unchanged.
Forward migration `023_simulator_research_baseline_lineage.sql` first blocks
sample inserts and scans every scheduled campaign, including sealed ones, for
baseline-lineage disagreements. It aborts without changing the migration
history or existing evidence if any are found; otherwise it atomically upgrades
the insert guard to require the same strategy evaluation and intent in all
matched arms. Migration `024_simulator_adaptive_candidate_protocol.sql` adds
the immutable adaptive-candidate roster and binds every new result and coverage
seal to its preregistered arm digest. All three migrations are excluded from
the runtime/PAPER profile.

Adaptive-candidate research follows a strict preregistration order:

1. Register and commit the exact `ResearchObservationScheduleV1`.
2. Register one immutable `AdaptiveCandidateProtocolV1` for that exact schedule
   with `ResearchAdaptiveCandidateProtocolRepository.register()`.
3. Before recording each `ResearchDecisionSampleV1`, set its
   `arm_protocol_digest` to the value returned by
   `ResearchAdaptiveCandidateProtocolRepository.resolve_arm_digest()` for that
   campaign and arm.
4. Seal exhaustive matched-arm coverage with
   `ResearchObservationScheduleRepository.seal_coverage()`; the seal records
   the exact `candidate_protocol_digest`.

Exact protocol re-registration is idempotent; any changed roster is rejected.
The database independently enforces that a protocol was committed before the
first result, so registering it in the same transaction as a result is invalid.
Campaigns or results created before migration 024 remain readable with absent
protocol links, but they cannot receive a retroactive adaptive protocol, more
scheduled results, or a new coverage seal. They are historical evidence, not
eligible adaptive campaigns. This simulator-only evidence layer grants no
execution authority or PAPER, alpha, or LIVE readiness.

```python
from kairos_persistence import Database, MigrationProfile

runtime_database = Database()  # explicit safe default: MigrationProfile.RUNTIME
simulator_database = Database(migration_profile=MigrationProfile.SIMULATOR)
```

This separation is a topology boundary, not a readiness grant: simulator rows
remain `SIMULATED` and cannot qualify PAPER, alpha, or LIVE.

## TimescaleDB integration tests

Start a TimescaleDB 2.28.3 / PostgreSQL 16 instance and set the test DSN:

```sh
docker run --rm --name kairos-persistence-db \
  -e POSTGRES_USER=kairos \
  -e POSTGRES_PASSWORD=kairos_test \
  -e POSTGRES_DB=kairos \
  -p 5432:5432 \
  timescale/timescaledb:2.28.3-pg16

export KAIROS_PERSISTENCE_DATABASE_URL=postgresql://kairos:kairos_test@localhost:5432/kairos
uv run python scripts/migration_smoke.py
uv run pytest -q -m integration
```

PowerShell equivalent:

```powershell
$env:KAIROS_PERSISTENCE_DATABASE_URL = "postgresql://kairos:kairos_test@localhost:5432/kairos"
uv run python scripts/migration_smoke.py
uv run pytest -q -m integration
```

## Durable runtime bus

`DurableMessageBus` is the production bridge around the normal Kairos
`MessageBus`. It starts and migrates PostgreSQL lazily, records every consumed
wire payload, claims its stable contract `message_id`, and defers the transport
ACK until inbox completion and every handler-produced outbox row commit.
Existing service loops keep the usual `subscribe` / `publish` / `ack` API.

```python
transport = build_bus(settings)
bus = DurableMessageBus(transport, service_name=settings.service_name)
```

Producer-only publishes are also committed to the outbox before the dispatcher
sends them. Every row is bound to one logical producer, and only that producer's
earliest unpublished row is leaseable. This intentionally serializes each
producer stream so retries, replicas and `SKIP LOCKED` cannot overtake a causal
predecessor. Dispatch workers also use expiring leases, bounded exponential
retry and a dead-letter terminal state. A process may crash
after Redis accepts a publish but before PostgreSQL records `published_at`; the
row is then published again. This deliberate at-least-once boundary is safe
because downstream inboxes reject a reused `message_id` with different topic or
SHA-256 payload and suppress exact completed duplicates.

## Offline maintenance writer

`OfflineDurableWriter` is deliberately narrower than `DurableMessageBus` for a
pre-approved repair of an already stopped producer. It owns no transport and
therefore never starts an outbox dispatcher, does not call `Database.migrate()`,
and holds both the durable producer advisory lease and the schema-migration lock
for its entire session. It first verifies an explicit database identity and the
exact immutable migration profile supplied by the caller. A drifted schema,
active migration or active producer fails before a write.

Each `append()` writes the audit fact and matching producer-scoped outbox row in
one transaction. Unlike the historical composite audit key, it also verifies
that a deterministic `message_id` names exactly one topic and canonical payload
across the whole audit log. Exact duplicates are idempotent; an identity conflict
rolls back the transaction. It is intended for bounded offline maintenance only,
never as a substitute for a running service or a way to dispatch a backlog.

## Atomic inbox/business/outbox processing

`AuditRepository.message_transaction()` owns one pooled connection and one
outer transaction. The inbox claim is made first. Business writes, outbox
inserts and `tx.complete()` then share a nested savepoint on that connection.
On an exception, those side effects roll back and the outer transaction records
the inbox row as `FAILED`; the original exception is re-raised after commit.

```python
async with repository.message_transaction(
    consumer="execution",
    message_id=envelope.payload["message_id"],
    topic=envelope.topic,
    payload_sha256=canonical_payload(envelope.payload)[1],
) as tx:
    if tx.claim.duplicate_completed:
        # The previous delivery committed; it is safe for the bus consumer to ACK.
        return
    if not tx.claim.claimed:
        # Another worker still owns a valid lease; do not ACK this delivery.
        return

    await tx.connection.execute("INSERT INTO domain_table ...")
    payload, payload_sha256 = canonical_payload(report.to_payload())
    await tx.enqueue_outbox(report.message_id, output_topic, payload, payload_sha256)
    await tx.complete({"report_id": report.message_id})
```

The caller must acknowledge the Redis message only after this context manager
returns successfully. A completed duplicate can be acknowledged without
repeating its side effects.

Migration application is serialized with a PostgreSQL advisory lock so all
service containers may start concurrently. The database DSN must be provided
through `KAIROS_PERSISTENCE_DATABASE_URL`; the development default is not a
production credential.

## Independent SIM research evidence and no-retry attempts

Migration `025_simulator_research_evidence.sql` belongs only to the explicit
`MigrationProfile.SIMULATOR` manifest. It is not a runtime/PAPER migration and
does not migrate, read, or repair the production database. Existing Trial 15,
frozen evaluator/protocol artifacts, and historical geometry-only coverage seals
are untouched; schedules that existed before 025 cannot be retrospectively
enrolled into the stronger evidence path.

`ResearchEvidenceRepository` is an opt-in engineering framework, not a profitable
strategy, evaluator, scientific gate, provider dispatcher, or trading authority.
Its sequence is deliberately explicit:

1. Commit a new frozen `ResearchObservationScheduleV1` and matching
   `AdaptiveCandidateProtocolV1`, then separately `enroll_campaign()` before
   adding any results. Enrollment fixes those exact schedule/protocol identities.
2. Independently append sanitized `ResearchSourceReceiptV1` JSON content (market,
   news, macro) and a `ResearchStrategyEvaluationReceiptV1`, including explicit
   zero-intent evaluations. Canonical hashes are checked on write and load;
   causal replay resolves actual stored content, not model-claimed references.
   Evaluation receipts attest which evaluator ran; storage alone does not prove
   correct strategy execution or future economic performance. JSON payloads are
   bounded and are never treated as remote paths or resource-fetch instructions.
3. Derive a stable attempt/reservation ID from the frozen campaign, arm, sample,
   route, and prompt identity. Commit `ResearchLLMAttemptStartV1` **before** any
   provider dispatch. Only `start_attempt() == True` admits a new call; `False`
   is an exact duplicate and must not dispatch. A unique arm/sample fence rejects
   a different attempt ID, including after restart. `find_attempt()` returns
   `None` only for actual absence; integrity/database errors fail closed.
4. Append one `ResearchLLMAttemptTerminalV1`: `COMPLETED` keeps the exact proposal
   and gateway completion, `FAILED` keeps the actual observed failure, and
   `UNRESOLVED` honestly keeps ambiguous dispatch/storage outcomes. A START with
   no terminal is also unresolved: it proves admission, **not** that a provider
   received the call. No automatic retry, fabricated timeout, suppressed call,
   or zero-cost assumption is permitted. Actual late timestamps are retained
   and cannot be backdated into a causal paired result.
5. `record_verified_sample()` reconstructs the existing core pairing from
   independent source/evaluation/attempt receipts and compares every result
   fact. `pending_observations()` exposes the frozen three-arm roster without
   launching calls or inferring missing outcomes. `seal_verified_coverage()`
   emits a distinct `INDEPENDENT_SOURCE_REPLAY_ONLY` engineering receipt:
   economic/PAPER qualification and live order authority are always false.

All receipts and attempt history are append-only in PostgreSQL. A legacy
geometry seal is not independently source-qualified evidence; neither seal
reports PnL or substitutes for the unchanged preregistered blind economic gates.
The optional real-PostgreSQL test requires an explicitly disposable local
`kairos_sim_test_evidence_*` database through
`KAIROS_SIM_EVIDENCE_TEST_DATABASE_URL`; it refuses other target names.

## Cumulative provider qualification budget

Runtime and metered qualification share the immutable
`kairos-dev-qualification-v1` campaign: OpenAI $12, DeepSeek $1, X $2 across
all months and services. Missing historical adoption blocks paid calls.
See [the explicit receipt/adoption procedure](docs/CAMPAIGN-BUDGET.md);
neither tests nor deployment should silently assume zero prior spend.

## Exchange-effect journal

`ExecutionJournalRepository` records each non-transactional venue mutation as
`PREPARED` before the HTTP request, then `CONFIRMED`, `RECONCILED` or `FAILED`.
The immutable request identity prevents a deterministic effect key from being
reused with different content. Every transition also appends a domain-separated
SHA-256 chained event. `recovery_required()` exposes unresolved effects so the
execution service can reconcile them before accepting another order.

PAPER effects carry an all-or-none `(environment, account_id, trade_id,
order_role)` lineage. Recovery callers must pass `environment` and `account_id`
together so a signing account never reconciles another account's venue effects.
Legacy DRY_RUN effects remain readable with all four lineage fields absent.

## PAPER trade lifecycle and recovery barrier

`TradeLifecycleRepository` persists the strict `RiskTradeDecisionV1` payload,
both the logical Binance symbol and immutable EVEDEX `venue_symbol`, deterministic
entry/protection/exit order identifiers, cumulative fills and the timeout clock.
The public state values come directly from `kairos-core`; each transition is
serialized and appended to a per-trade SHA-256 chain. A `FAILED_BLOCKED` trade is
intentionally non-terminal and continues to occupy the one-active-symbol slot.

Startup calls `begin_recovery()` before any venue reconciliation. It returns a
monotonic `recovery_epoch`; only `complete_recovery(expected_epoch=...)` for that
same epoch may unblock entries. This prevents an older process that finishes late
from clearing the barrier established by a newer restart. Day-start, peak and
latest equity are updated atomically with a monotonic reconciliation sequence.

Execution runtimes use `create_with_execution_event()` and
`transition_with_execution_event()`. Each call commits the trade row, internal
hash-chained journal, strict `TradeExecutionEventV1`, audit row and durable
outbox row in one PostgreSQL transaction. Replaying the same `fact_key` verifies
the original transition request and canonical event, returns the current trade,
and never advances the FSM twice. Startup fact audits use
`list_trades_for_scope(include_terminal=True)` so `FLAT` and `CANCELLED` trades
cannot hide a missing public lifecycle fact.

## Runtime metrics

`kairos-persistence-exporter` exposes a small Prometheus endpoint without a
Docker-socket mount. It authenticates to Redis with `KAIROS_REDIS_URL`, queries
TimescaleDB through `KAIROS_PERSISTENCE_DATABASE_URL`, and reports connectivity,
pending/dead-lettered outbox rows, processing/failed inbox rows, unresolved/failed
execution effects, and the oldest unpublished message age. The exporter performs
read-only queries and an authenticated Redis `PING`; database or Redis failure is
returned as a zero health gauge rather than fabricated healthy metrics. Its
PostgreSQL pool sets `default_transaction_read_only=on`, and startup verifies the
exact runtime migration history in a read-only transaction. It never runs
`Database.migrate()`; missing or mismatched schema history fails startup before
the metrics listener opens. This protection is specific to the observer and does
not change migration behavior for the runtime services that own schema setup.

Venue availability is derived from strict `kairos.venue.poll.v1` attempt and
terminal-outcome facts. The denominator is the full 24-hour slot count for the
latest interval and symbol-set fingerprint; missing polls, failed reads, process
downtime and an incomplete post-configuration window therefore reduce the ratio.
Separate expected, attempted, succeeded and failed gauges make the result auditable.
The PAPER account age includes only reconciled `PAPER`/`DEV` snapshots and is `-1`
when no such snapshot exists. Inbox monitoring likewise exports the oldest current
processing-attempt age and the number of rows whose recovery lease has expired.
