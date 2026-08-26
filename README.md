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
returned as a zero health gauge rather than fabricated healthy metrics.

Venue availability is derived from strict `kairos.venue.poll.v1` attempt and
terminal-outcome facts. The denominator is the full 24-hour slot count for the
latest interval and symbol-set fingerprint; missing polls, failed reads, process
downtime and an incomplete post-configuration window therefore reduce the ratio.
Separate expected, attempted, succeeded and failed gauges make the result auditable.
The PAPER account age includes only reconciled `PAPER`/`DEV` snapshots and is `-1`
when no such snapshot exists. Inbox monitoring likewise exports the oldest current
processing-attempt age and the number of rows whose recovery lease has expired.
