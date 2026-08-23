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
    latest_paper_account_age_seconds: float = -1.0
    api_spend_month_usd: float = 0.0
    execution_runtime_health_age_seconds: float = -1.0
    evedex_auth_age_seconds: float = -1.0
    evedex_auth_expires_in_seconds: float = -1.0
    evedex_local_mutation_reserve: int = -1
    evedex_local_mutation_capacity: int = -1
    evedex_local_mutation_compensation_reserve: int = -1
    evedex_local_mutation_window_seconds: float = -1.0
    evedex_venue_rate_limit_observable: int = -1
    evedex_venue_rate_limit_reserve: int = -1
    venue_poll_expected_24h: int = 0
    venue_poll_attempted_24h: int = 0
    venue_poll_succeeded_24h: int = 0
    venue_poll_failed_24h: int = 0
    oldest_inbox_processing_age_seconds: float = -1.0
    inbox_expired_processing: int = 0


RedisProbe = Callable[[str], Awaitable[bool]]

_QUERY = """
WITH bounds AS (
    SELECT (EXTRACT(EPOCH FROM now()) * 1000)::bigint AS now_ms
), closed_bars AS (
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
), venue_poll_facts AS (
    SELECT produced_at,
           payload->>'poll_id' AS poll_id,
           payload->>'config_fingerprint' AS config_fingerprint,
           payload->>'status' AS status,
           (payload->>'interval_ms')::bigint AS interval_ms,
           (payload->>'scheduled_at_ms')::bigint AS scheduled_at_ms,
           jsonb_array_length(payload->'expected_symbols')::bigint AS symbol_count
      FROM event_audit
     WHERE topic='kairos.venue.poll.v1'
       AND produced_at >= now() - interval '25 hours'
       AND payload->>'status' IN ('ATTEMPTED', 'SUCCEEDED', 'FAILED')
       AND payload->>'interval_ms' ~ '^[1-9][0-9]*$'
       AND payload->>'scheduled_at_ms' ~ '^[1-9][0-9]*$'
       AND jsonb_typeof(payload->'expected_symbols')='array'
), venue_poll_config AS (
    SELECT config_fingerprint,
           interval_ms,
           symbol_count,
           ceil(86400000.0 / interval_ms)::bigint * symbol_count AS expected_count
      FROM venue_poll_facts, bounds
     WHERE status='ATTEMPTED'
       AND interval_ms > 0
       AND symbol_count > 0
       AND scheduled_at_ms BETWEEN bounds.now_ms - 86400000 AND bounds.now_ms
     ORDER BY scheduled_at_ms DESC, produced_at DESC
     LIMIT 1
), venue_poll_attempts AS (
    SELECT DISTINCT poll_id
      FROM venue_poll_facts, venue_poll_config, bounds
     WHERE venue_poll_facts.config_fingerprint=venue_poll_config.config_fingerprint
       AND venue_poll_facts.status='ATTEMPTED'
       AND scheduled_at_ms BETWEEN bounds.now_ms - 86400000 AND bounds.now_ms
), venue_poll_terminal AS (
    SELECT venue_poll_facts.poll_id, min(venue_poll_facts.status) AS status
      FROM venue_poll_facts
      JOIN venue_poll_attempts ON venue_poll_attempts.poll_id=venue_poll_facts.poll_id
      CROSS JOIN venue_poll_config
      CROSS JOIN bounds
     WHERE venue_poll_facts.config_fingerprint=venue_poll_config.config_fingerprint
       AND venue_poll_facts.status IN ('SUCCEEDED', 'FAILED')
       AND venue_poll_facts.scheduled_at_ms BETWEEN bounds.now_ms - 86400000 AND bounds.now_ms
     GROUP BY venue_poll_facts.poll_id
    HAVING count(DISTINCT venue_poll_facts.status)=1
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
), runtime_health AS (
    SELECT *, EXTRACT(EPOCH FROM now() - observed_at)::double precision AS observation_age_seconds
      FROM execution_runtime_health
     WHERE environment='evedex-dev' AND lower(exchange)='evedex'
     ORDER BY observed_at DESC
     LIMIT 1
)
SELECT
    (SELECT count(*) FROM message_outbox
      WHERE published_at IS NULL AND dead_lettered_at IS NULL)::bigint AS outbox_pending,
    (SELECT count(*) FROM message_outbox
      WHERE dead_lettered_at IS NOT NULL)::bigint AS outbox_dead_lettered,
    (SELECT count(*) FROM message_inbox WHERE status = 'PROCESSING')::bigint AS inbox_processing,
    (SELECT count(*) FROM message_inbox WHERE status = 'FAILED')::bigint AS inbox_failed,
    COALESCE((SELECT EXTRACT(EPOCH FROM now() - min(updated_at))
      FROM message_inbox WHERE status = 'PROCESSING'), -1)::double precision
      AS oldest_inbox_processing_age_seconds,
    (SELECT count(*) FROM message_inbox
      WHERE status = 'PROCESSING' AND lease_until < now())::bigint AS inbox_expired_processing,
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
    CASE WHEN COALESCE((SELECT expected_count FROM venue_poll_config), 0) > 0
      THEN least(
        1.0,
        (SELECT count(*) FROM venue_poll_terminal WHERE status='SUCCEEDED')::double precision
          / (SELECT expected_count FROM venue_poll_config)::double precision
      )
      ELSE 0.0 END AS venue_availability_ratio_24h,
    COALESCE((SELECT expected_count FROM venue_poll_config), 0)::bigint
      AS venue_poll_expected_24h,
    (SELECT count(*) FROM venue_poll_attempts)::bigint AS venue_poll_attempted_24h,
    (SELECT count(*) FROM venue_poll_terminal WHERE status='SUCCEEDED')::bigint
      AS venue_poll_succeeded_24h,
    (SELECT count(*) FROM venue_poll_terminal WHERE status='FAILED')::bigint
      AS venue_poll_failed_24h,
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
      FROM event_audit
      WHERE topic='kairos.account.snapshot.v2'
        AND payload->>'trading_mode'='PAPER'
        AND payload->>'evedex_profile'='DEV'
        AND payload->>'reconciled'='true'), -1)::double precision
      AS latest_paper_account_age_seconds,
    COALESCE((SELECT sum(actual_cost_microusd)::double precision / 1000000
      FROM source_usage_reservations
      WHERE status='COMMITTED'
        AND billing_month=date_trunc('month', CURRENT_DATE)::date), 0)::double precision
      AS api_spend_month_usd,
    COALESCE((SELECT observation_age_seconds FROM runtime_health), -1)
      ::double precision AS execution_runtime_health_age_seconds,
    COALESCE((SELECT auth_age_ms::double precision / 1000 + observation_age_seconds
      FROM runtime_health), -1)::double precision AS evedex_auth_age_seconds,
    COALESCE((SELECT CASE WHEN auth_expires_in_ms IS NULL THEN -1
      ELSE greatest(0, auth_expires_in_ms::double precision / 1000 - observation_age_seconds)
      END FROM runtime_health), -1)::double precision AS evedex_auth_expires_in_seconds,
    COALESCE((SELECT local_mutation_reserve FROM runtime_health), -1)::bigint
      AS evedex_local_mutation_reserve,
    COALESCE((SELECT local_mutation_capacity FROM runtime_health), -1)::bigint
      AS evedex_local_mutation_capacity,
    COALESCE((SELECT local_mutation_compensation_reserve FROM runtime_health), -1)::bigint
      AS evedex_local_mutation_compensation_reserve,
    COALESCE((SELECT local_mutation_window_ms::double precision / 1000
      FROM runtime_health), -1)::double precision AS evedex_local_mutation_window_seconds,
    COALESCE((SELECT venue_rate_limit_observable::int FROM runtime_health), -1)::bigint
      AS evedex_venue_rate_limit_observable,
    COALESCE((SELECT venue_rate_limit_reserve FROM runtime_health), -1)::bigint
      AS evedex_venue_rate_limit_reserve
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
        execution_runtime_health_age_seconds=float(row["execution_runtime_health_age_seconds"]),
        evedex_auth_age_seconds=float(row["evedex_auth_age_seconds"]),
        evedex_auth_expires_in_seconds=float(row["evedex_auth_expires_in_seconds"]),
        evedex_local_mutation_reserve=int(row["evedex_local_mutation_reserve"]),
        evedex_local_mutation_capacity=int(row["evedex_local_mutation_capacity"]),
        evedex_local_mutation_compensation_reserve=int(row["evedex_local_mutation_compensation_reserve"]),
        evedex_local_mutation_window_seconds=float(row["evedex_local_mutation_window_seconds"]),
        evedex_venue_rate_limit_observable=int(row["evedex_venue_rate_limit_observable"]),
        evedex_venue_rate_limit_reserve=int(row["evedex_venue_rate_limit_reserve"]),
        venue_poll_expected_24h=int(row["venue_poll_expected_24h"]),
        venue_poll_attempted_24h=int(row["venue_poll_attempted_24h"]),
        venue_poll_succeeded_24h=int(row["venue_poll_succeeded_24h"]),
        venue_poll_failed_24h=int(row["venue_poll_failed_24h"]),
        oldest_inbox_processing_age_seconds=float(row["oldest_inbox_processing_age_seconds"]),
        inbox_expired_processing=int(row["inbox_expired_processing"]),
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
            "kairos_inbox_processing_oldest_age_seconds",
            "Age of the oldest current inbox processing attempt; -1 means none.",
            "gauge",
            metrics.oldest_inbox_processing_age_seconds,
        ),
        (
            "kairos_inbox_processing_expired",
            "Inbox processing rows whose recovery lease has expired.",
            "gauge",
            metrics.inbox_expired_processing,
        ),
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
            "Successful durable public venue polls divided by configured slots over 24 hours.",
            "gauge",
            metrics.venue_availability_ratio_24h,
        ),
        (
            "kairos_venue_poll_expected_24h",
            "Expected per-symbol public venue poll slots for the latest 24-hour configuration.",
            "gauge",
            metrics.venue_poll_expected_24h,
        ),
        (
            "kairos_venue_poll_attempted_24h",
            "Distinct durable public venue poll attempts for the latest configuration over 24 hours.",
            "gauge",
            metrics.venue_poll_attempted_24h,
        ),
        (
            "kairos_venue_poll_succeeded_24h",
            "Distinct successful public venue poll outcomes over 24 hours.",
            "gauge",
            metrics.venue_poll_succeeded_24h,
        ),
        (
            "kairos_venue_poll_failed_24h",
            "Distinct failed public venue poll outcomes over 24 hours.",
            "gauge",
            metrics.venue_poll_failed_24h,
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
            "Age of the latest reconciled EVEDEX DEV PAPER account snapshot; -1 is absent.",
            "gauge",
            metrics.latest_paper_account_age_seconds,
        ),
        (
            "kairos_api_spend_month_usd",
            "Committed durable provider spend for the current billing month in USD.",
            "gauge",
            metrics.api_spend_month_usd,
        ),
        (
            "kairos_execution_runtime_health_age_seconds",
            "Age of the latest durable EVEDEX execution runtime health snapshot; -1 is unknown.",
            "gauge",
            metrics.execution_runtime_health_age_seconds,
        ),
        (
            "kairos_evedex_auth_age_seconds",
            "Current age of the active EVEDEX authentication session; -1 is unknown.",
            "gauge",
            metrics.evedex_auth_age_seconds,
        ),
        (
            "kairos_evedex_auth_expires_in_seconds",
            "Estimated seconds until EVEDEX authentication expiry; -1 is unknown.",
            "gauge",
            metrics.evedex_auth_expires_in_seconds,
        ),
        (
            "kairos_evedex_local_mutation_reserve",
            "Remaining locally enforced EVEDEX mutation calls in the current window; -1 is unknown.",
            "gauge",
            metrics.evedex_local_mutation_reserve,
        ),
        (
            "kairos_evedex_local_mutation_capacity",
            "Configured local EVEDEX mutation capacity per window; -1 is unknown.",
            "gauge",
            metrics.evedex_local_mutation_capacity,
        ),
        (
            "kairos_evedex_local_mutation_compensation_reserve",
            "EVEDEX mutation calls reserved for cancel/close compensation; -1 is unknown.",
            "gauge",
            metrics.evedex_local_mutation_compensation_reserve,
        ),
        (
            "kairos_evedex_local_mutation_window_seconds",
            "Local EVEDEX mutation rate-limit window in seconds; -1 is unknown.",
            "gauge",
            metrics.evedex_local_mutation_window_seconds,
        ),
        (
            "kairos_evedex_venue_rate_limit_observable",
            "Whether EVEDEX reported a venue-side rate-limit reserve; -1 is unknown.",
            "gauge",
            metrics.evedex_venue_rate_limit_observable,
        ),
        (
            "kairos_evedex_venue_rate_limit_reserve",
            "Venue-reported EVEDEX request reserve; -1 is unknown or unavailable.",
            "gauge",
            metrics.evedex_venue_rate_limit_reserve,
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
