"""Minimal Prometheus exporter for durable runtime and recovery state."""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlsplit

from .config import PersistenceSettings
from .database import Database


@dataclass(frozen=True)
class RuntimeMetrics:
    persistence_up: int
    redis_up: int
    outbox_pending: int
    outbox_dead_lettered: int
    inbox_processing: int
    inbox_failed: int
    execution_prepared: int
    execution_failed: int
    oldest_outbox_age_seconds: float


RedisProbe = Callable[[str], Awaitable[bool]]

_QUERY = """
SELECT
    (SELECT count(*) FROM message_outbox
      WHERE published_at IS NULL AND dead_lettered_at IS NULL)::bigint AS outbox_pending,
    (SELECT count(*) FROM message_outbox
      WHERE dead_lettered_at IS NOT NULL)::bigint AS outbox_dead_lettered,
    (SELECT count(*) FROM message_inbox WHERE status = 'PROCESSING')::bigint AS inbox_processing,
    (SELECT count(*) FROM message_inbox WHERE status = 'FAILED')::bigint AS inbox_failed,
    (SELECT count(*) FROM execution_effects WHERE status = 'PREPARED')::bigint AS execution_prepared,
    (SELECT count(*) FROM execution_effects WHERE status = 'FAILED')::bigint AS execution_failed,
    COALESCE((SELECT EXTRACT(EPOCH FROM now() - min(created_at))
      FROM message_outbox WHERE published_at IS NULL AND dead_lettered_at IS NULL), 0)::double precision
      AS oldest_outbox_age_seconds
"""


def _resp_command(*parts: str) -> bytes:
    encoded = [part.encode("utf-8") for part in parts]
    chunks = [f"*{len(encoded)}\r\n".encode()]
    for item in encoded:
        chunks.extend((f"${len(item)}\r\n".encode(), item, b"\r\n"))
    return b"".join(chunks)


async def probe_redis(redis_url: str) -> bool:
    parsed = urlsplit(redis_url)
    if parsed.scheme not in {"redis", "rediss"} or not parsed.hostname:
        return False
    ssl = parsed.scheme == "rediss"
    writer: asyncio.StreamWriter | None = None
    try:
        reader, connected_writer = await asyncio.wait_for(
            asyncio.open_connection(parsed.hostname, parsed.port or 6379, ssl=ssl),
            timeout=3,
        )
        writer = connected_writer
        password = unquote(parsed.password or "")
        if password:
            connected_writer.write(_resp_command("AUTH", password))
            await connected_writer.drain()
            if not (await asyncio.wait_for(reader.readline(), timeout=3)).startswith(b"+OK"):
                return False
        connected_writer.write(_resp_command("PING"))
        await connected_writer.drain()
        return (await asyncio.wait_for(reader.readline(), timeout=3)).startswith(b"+PONG")
    except (OSError, TimeoutError, ValueError):
        return False
    finally:
        if writer is not None:
            writer.close()
            await writer.wait_closed()


async def collect_runtime_metrics(
    pool,
    *,
    redis_url: str,
    redis_probe: RedisProbe = probe_redis,
) -> RuntimeMetrics:
    redis_task = asyncio.ensure_future(redis_probe(redis_url))
    try:
        async with pool.acquire() as connection:
            row: Mapping[str, Any] = await connection.fetchrow(_QUERY)
        persistence_up = 1
    except Exception:
        await redis_task
        return RuntimeMetrics(0, int(redis_task.result()), 0, 0, 0, 0, 0, 0, 0)
    redis_up = int(await redis_task)
    return RuntimeMetrics(
        persistence_up=persistence_up,
        redis_up=redis_up,
        outbox_pending=int(row["outbox_pending"]),
        outbox_dead_lettered=int(row["outbox_dead_lettered"]),
        inbox_processing=int(row["inbox_processing"]),
        inbox_failed=int(row["inbox_failed"]),
        execution_prepared=int(row["execution_prepared"]),
        execution_failed=int(row["execution_failed"]),
        oldest_outbox_age_seconds=float(row["oldest_outbox_age_seconds"]),
    )


def render_prometheus(metrics: RuntimeMetrics) -> bytes:
    values = as_metric_values(metrics)
    lines: list[str] = []
    for name, help_text, metric_type, value in values:
        lines.extend(
            (
                f"# HELP {name} {help_text}",
                f"# TYPE {name} {metric_type}",
                f"{name} {value}",
            )
        )
    return ("\n".join(lines) + "\n").encode("ascii")


def as_metric_values(metrics: RuntimeMetrics) -> tuple[tuple[str, str, str, int | float], ...]:
    return (
        ("kairos_persistence_up", "TimescaleDB metrics query succeeded.", "gauge", metrics.persistence_up),
        ("kairos_redis_up", "Authenticated Redis PING succeeded.", "gauge", metrics.redis_up),
        (
            "kairos_outbox_pending",
            "Unpublished non-dead-letter outbox rows.",
            "gauge",
            metrics.outbox_pending,
        ),
        (
            "kairos_outbox_dead_lettered_total",
            "Current dead-lettered outbox rows.",
            "gauge",
            metrics.outbox_dead_lettered,
        ),
        ("kairos_inbox_processing", "Inbox rows currently processing.", "gauge", metrics.inbox_processing),
        ("kairos_inbox_failed", "Inbox rows in failed state.", "gauge", metrics.inbox_failed),
        (
            "kairos_execution_effects_prepared",
            "Execution effects awaiting confirmation or reconciliation.",
            "gauge",
            metrics.execution_prepared,
        ),
        (
            "kairos_execution_effects_failed",
            "Execution effects requiring operator review.",
            "gauge",
            metrics.execution_failed,
        ),
        (
            "kairos_outbox_oldest_age_seconds",
            "Age of the oldest unpublished outbox row.",
            "gauge",
            metrics.oldest_outbox_age_seconds,
        ),
    )


async def run_exporter(*, host: str, port: int, redis_url: str) -> None:
    database = Database(PersistenceSettings())
    await database.connect()
    await database.migrate()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request = await asyncio.wait_for(reader.readline(), timeout=5)
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=5)
                if line in {b"\r\n", b"\n", b""}:
                    break
            if not request.startswith(b"GET /metrics "):
                body = b"not found\n"
                status = b"HTTP/1.1 404 Not Found\r\n"
            else:
                body = render_prometheus(await collect_runtime_metrics(database.pool, redis_url=redis_url))
                status = b"HTTP/1.1 200 OK\r\n"
            writer.write(
                status
                + b"Content-Type: text/plain; version=0.0.4\r\n"
                + f"Content-Length: {len(body)}\r\n".encode()
                + b"Connection: close\r\n\r\n"
                + body
            )
            await writer.drain()
        except (ConnectionError, TimeoutError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, host=host, port=port)
    try:
        async with server:
            await server.serve_forever()
    finally:
        await database.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Expose Kairos durable-runtime Prometheus metrics")
    parser.add_argument("--host", default=os.getenv("KAIROS_METRICS_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("KAIROS_METRICS_PORT", "9108")))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    redis_url = os.getenv("KAIROS_REDIS_URL", "")
    if not redis_url:
        raise ValueError("KAIROS_REDIS_URL is required")
    asyncio.run(run_exporter(host=args.host, port=args.port, redis_url=redis_url))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
