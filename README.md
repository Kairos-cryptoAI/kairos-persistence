# kairos-persistence

Transactional PostgreSQL/TimescaleDB primitives for Kairos: event audit,
idempotent inbox processing, transactional outbox delivery and execution state.

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
sends them. Dispatch workers use `FOR UPDATE SKIP LOCKED`, expiring leases,
bounded exponential retry and a dead-letter terminal state. A process may crash
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
