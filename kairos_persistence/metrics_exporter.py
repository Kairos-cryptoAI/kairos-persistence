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
    closed_bar_gaps_24h: int = 0
    closed_bar_symbols_24h: int = 0
    closed_bar_minimum_coverage_ratio_24h: float = 0.0
    latest_closed_bar_age_seconds: float = 0.0
    venue_measurements_24h: int = 0
    venue_availability_ratio_24h: float = 0.0
    venue_blocked_24h: int = 0
    venue_p95_abs_basis_bps: float = 0.0
    venue_p95_spread_bps: float = 0.0
    venue_p95_slippage_bps: float = 0.0
    venue_max_book_age_ms: float = 0.0
    venue_max_timestamp_skew_ms: float = 0.0
    venue_p95_latency_ms: float = 0.0
    latest_venue_age_seconds: float = 0.0
    candidate_veto_24h: int = 0
    candidate_defer_24h: int = 0
    paper_active_trades: int = 0
    paper_unprotected_trades: int = 0
    paper_recovery_blocked: int = 0
    execution_p95_shortfall_bps: float = 0.0
    latest_paper_account_age_seconds: float = 0.0
    api_spend_month_usd: float = 0.0


RedisProbe = Callable[[str], Awaitable[bool]]

_QUERY = """
WITH closed_bars AS (
    SELECT payload->>'symbol' AS symbol,
           (payload->>'open_time_ms')::bigint AS open_time_ms,
           produced_at
      FROM event_audit
     WHERE topic='kairos.market.closed_bar.v1'
       AND produced_at >= now() - interval '24 hours'
), sequenced_bars AS (
    SELECT symbol, open_time_ms,
           lag(open_time_ms) OVER (PARTITION BY symbol ORDER BY open_time_ms) AS previous_open_time_ms
      FROM closed_bars
), closed_bar_coverage AS (
    SELECT symbol,
           count(DISTINCT open_time_ms)::double precision AS bar_count,
           max(produced_at) AS latest_produced_at
      FROM closed_bars
     GROUP BY symbol
), venue AS (
    SELECT produced_at,
           abs((payload->>'basis_bps')::double precision) AS abs_basis_bps,
           (payload->>'spread_bps')::double precision AS spread_bps,
           greatest(
               (payload->>'buy_slippage_bps')::double precision,
               (payload->>'sell_slippage_bps')::double precision
           ) AS slippage_bps,
           (payload->>'book_age_ms')::double precision AS book_age_ms,
           (payload->>'timestamp_skew_ms')::double precision AS timestamp_skew_ms,
           (payload->>'latency_ms')::double precision AS latency_ms,
           (payload->>'entry_allowed')::boolean AS entry_allowed
      FROM event_audit
     WHERE topic='kairos.venue.quality.v1'
       AND produced_at >= now() - interval '24 hours'
), decisions AS (
    SELECT payload->>'trade_id' AS trade_id,
           payload->'intent'->>'side' AS side,
           (payload->>'worst_entry_price')::double precision AS decision_entry_price
      FROM event_audit
     WHERE topic='kairos.risk.trade_decision.v1'
       AND payload->>'approved'='true'
), entry_fills AS (
    SELECT DISTINCT ON (payload->>'trade_id')
           payload->>'trade_id' AS trade_id,
           (payload->>'average_price')::double precision AS average_price
      FROM event_audit
     WHERE topic='kairos.execution.trade_event.v1'
       AND payload->>'event_type' IN ('ENTRY_PARTIAL_FILL', 'ENTRY_FILLED')
       AND payload->>'average_price' IS NOT NULL
       AND produced_at >= now() - interval '24 hours'
     ORDER BY payload->>'trade_id', produced_at DESC
), shortfall AS (
    SELECT abs(entry_fills.average_price - decisions.decision_entry_price)
               / decisions.decision_entry_price * 10000 AS shortfall_bps
      FROM entry_fills
      JOIN decisions USING (trade_id)
     WHERE decisions.decision_entry_price > 0
)
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
      AS oldest_outbox_age_seconds,
    (SELECT count(*) FROM sequenced_bars
      WHERE previous_open_time_ms IS NOT NULL
        AND open_time_ms <> previous_open_time_ms + 60000)::bigint AS closed_bar_gaps_24h,
    (SELECT count(*) FROM closed_bar_coverage)::bigint AS closed_bar_symbols_24h,
    COALESCE((SELECT least(1.0, min(bar_count / 1440.0)) FROM closed_bar_coverage), 0)
      ::double precision AS closed_bar_minimum_coverage_ratio_24h,
    COALESCE((SELECT EXTRACT(EPOCH FROM now() - min(latest_produced_at))
      FROM closed_bar_coverage), 0)::double precision AS latest_closed_bar_age_seconds,
    (SELECT count(*) FROM venue)::bigint AS venue_measurements_24h,
    least(1.0, (SELECT count(*) FROM venue)::double precision / 14400.0)
      AS venue_availability_ratio_24h,
    (SELECT count(*) FROM venue WHERE NOT entry_allowed)::bigint AS venue_blocked_24h,
    COALESCE((SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY abs_basis_bps) FROM venue), 0)
      ::double precision AS venue_p95_abs_basis_bps,
    COALESCE((SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY spread_bps) FROM venue), 0)
      ::double precision AS venue_p95_spread_bps,
    COALESCE((SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY slippage_bps) FROM venue), 0)
      ::double precision AS venue_p95_slippage_bps,
    COALESCE((SELECT max(book_age_ms) FROM venue), 0)
      ::double precision AS venue_max_book_age_ms,
    COALESCE((SELECT max(timestamp_skew_ms) FROM venue), 0)
      ::double precision AS venue_max_timestamp_skew_ms,
    COALESCE((SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) FROM venue), 0)
      ::double precision AS venue_p95_latency_ms,
    COALESCE((SELECT EXTRACT(EPOCH FROM now() - max(produced_at)) FROM venue), 0)
      ::double precision AS latest_venue_age_seconds,
    (SELECT count(*) FROM event_audit
      WHERE topic='kairos.aggregator.review.v1'
        AND payload->>'decision'='VETO'
        AND produced_at >= now() - interval '24 hours')::bigint AS candidate_veto_24h,
    (SELECT count(*) FROM event_audit
      WHERE topic='kairos.aggregator.review.v1'
        AND payload->>'decision'='DEFER'
        AND produced_at >= now() - interval '24 hours')::bigint AS candidate_defer_24h,
    (SELECT count(*) FROM execution_trades
      WHERE trading_mode='PAPER' AND state NOT IN ('FLAT','CANCELLED'))::bigint
      AS paper_active_trades,
    (SELECT count(*) FROM execution_trades
      WHERE trading_mode='PAPER'
        AND filled_quantity > 0
        AND stop_exchange_order_id IS NULL
        AND state NOT IN ('FLAT','CANCELLED'))::bigint AS paper_unprotected_trades,
    (SELECT count(*) FROM execution_recovery_state
      WHERE entries_blocked)::bigint AS paper_recovery_blocked,
    COALESCE((SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY shortfall_bps) FROM shortfall), 0)
      ::double precision AS execution_p95_shortfall_bps,
    COALESCE((SELECT EXTRACT(EPOCH FROM now() - max(produced_at))
      FROM event_audit WHERE topic='kairos.account.snapshot.v2'), 0)::double precision
      AS latest_paper_account_age_seconds,
    COALESCE((SELECT sum(actual_cost_microusd)::double precision / 1000000
      FROM source_usage_reservations
      WHERE status='COMMITTED'
        AND billing_month=date_trunc('month', CURRENT_DATE)::date), 0)::double precision
      AS api_spend_month_usd
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
        closed_bar_gaps_24h=int(row["closed_bar_gaps_24h"]),
        closed_bar_symbols_24h=int(row["closed_bar_symbols_24h"]),
        closed_bar_minimum_coverage_ratio_24h=float(row["closed_bar_minimum_coverage_ratio_24h"]),
        latest_closed_bar_age_seconds=float(row["latest_closed_bar_age_seconds"]),
        venue_measurements_24h=int(row["venue_measurements_24h"]),
        venue_availability_ratio_24h=float(row["venue_availability_ratio_24h"]),
        venue_blocked_24h=int(row["venue_blocked_24h"]),
        venue_p95_abs_basis_bps=float(row["venue_p95_abs_basis_bps"]),
        venue_p95_spread_bps=float(row["venue_p95_spread_bps"]),
        venue_p95_slippage_bps=float(row["venue_p95_slippage_bps"]),
        venue_max_book_age_ms=float(row["venue_max_book_age_ms"]),
        venue_max_timestamp_skew_ms=float(row["venue_max_timestamp_skew_ms"]),
        venue_p95_latency_ms=float(row["venue_p95_latency_ms"]),
        latest_venue_age_seconds=float(row["latest_venue_age_seconds"]),
        candidate_veto_24h=int(row["candidate_veto_24h"]),
        candidate_defer_24h=int(row["candidate_defer_24h"]),
        paper_active_trades=int(row["paper_active_trades"]),
        paper_unprotected_trades=int(row["paper_unprotected_trades"]),
        paper_recovery_blocked=int(row["paper_recovery_blocked"]),
        execution_p95_shortfall_bps=float(row["execution_p95_shortfall_bps"]),
        latest_paper_account_age_seconds=float(row["latest_paper_account_age_seconds"]),
        api_spend_month_usd=float(row["api_spend_month_usd"]),
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
        (
            "kairos_closed_bar_gaps_24h",
            "Gap or reorder boundaries in persisted one-minute bars over 24 hours.",
            "gauge",
            metrics.closed_bar_gaps_24h,
        ),
        (
            "kairos_closed_bar_symbols_24h",
            "Symbols with persisted closed one-minute bars over 24 hours.",
            "gauge",
            metrics.closed_bar_symbols_24h,
        ),
        (
            "kairos_closed_bar_minimum_coverage_ratio_24h",
            "Lowest per-symbol fraction of the expected 1440 closed bars over 24 hours.",
            "gauge",
            metrics.closed_bar_minimum_coverage_ratio_24h,
        ),
        (
            "kairos_closed_bar_latest_age_seconds",
            "Age of the oldest per-symbol latest closed-bar event.",
            "gauge",
            metrics.latest_closed_bar_age_seconds,
        ),
        (
            "kairos_venue_measurements_24h",
            "Persisted EVEDEX venue-quality measurements over 24 hours.",
            "gauge",
            metrics.venue_measurements_24h,
        ),
        (
            "kairos_venue_availability_ratio_24h",
            "Fraction of the expected five-symbol 30-second EVEDEX measurements over 24 hours.",
            "gauge",
            metrics.venue_availability_ratio_24h,
        ),
        (
            "kairos_venue_blocked_24h",
            "EVEDEX venue-quality measurements that denied entry over 24 hours.",
            "gauge",
            metrics.venue_blocked_24h,
        ),
        (
            "kairos_venue_p95_abs_basis_bps",
            "24-hour p95 absolute Binance-to-EVEDEX basis in basis points.",
            "gauge",
            metrics.venue_p95_abs_basis_bps,
        ),
        (
            "kairos_venue_p95_spread_bps",
            "24-hour p95 EVEDEX spread in basis points.",
            "gauge",
            metrics.venue_p95_spread_bps,
        ),
        (
            "kairos_venue_p95_slippage_bps",
            "24-hour p95 worst-side measured EVEDEX slippage in basis points.",
            "gauge",
            metrics.venue_p95_slippage_bps,
        ),
        (
            "kairos_venue_max_book_age_ms",
            "Maximum EVEDEX order-book age observed over 24 hours.",
            "gauge",
            metrics.venue_max_book_age_ms,
        ),
        (
            "kairos_venue_max_timestamp_skew_ms",
            "Maximum Binance-to-EVEDEX source timestamp skew observed over 24 hours.",
            "gauge",
            metrics.venue_max_timestamp_skew_ms,
        ),
        (
            "kairos_venue_p95_latency_ms",
            "24-hour p95 EVEDEX measurement request latency.",
            "gauge",
            metrics.venue_p95_latency_ms,
        ),
        (
            "kairos_venue_latest_age_seconds",
            "Age of the latest persisted venue-quality measurement.",
            "gauge",
            metrics.latest_venue_age_seconds,
        ),
        (
            "kairos_candidate_veto_24h",
            "Candidate reviews vetoed over 24 hours.",
            "gauge",
            metrics.candidate_veto_24h,
        ),
        (
            "kairos_candidate_defer_24h",
            "Candidate reviews deferred over 24 hours.",
            "gauge",
            metrics.candidate_defer_24h,
        ),
        (
            "kairos_paper_active_trades",
            "Non-terminal durable PAPER trades.",
            "gauge",
            metrics.paper_active_trades,
        ),
        (
            "kairos_paper_unprotected_trades",
            "Non-terminal filled PAPER trades without a reconciled stop order.",
            "gauge",
            metrics.paper_unprotected_trades,
        ),
        (
            "kairos_paper_recovery_blocked",
            "Execution environments with new PAPER entries blocked for recovery.",
            "gauge",
            metrics.paper_recovery_blocked,
        ),
        (
            "kairos_execution_p95_shortfall_bps",
            "24-hour p95 absolute entry execution shortfall in basis points.",
            "gauge",
            metrics.execution_p95_shortfall_bps,
        ),
        (
            "kairos_paper_account_latest_age_seconds",
            "Age of the latest persisted strict PAPER account snapshot.",
            "gauge",
            metrics.latest_paper_account_age_seconds,
        ),
        (
            "kairos_api_spend_month_usd",
            "Committed durable provider spend for the current billing month in USD.",
            "gauge",
            metrics.api_spend_month_usd,
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
