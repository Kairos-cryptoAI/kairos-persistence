import json
import os
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from kairos_persistence import metrics_exporter
from kairos_persistence.metrics_exporter import (
    _QUERY,
    RuntimeEpochServeObservation,
    RuntimeMetrics,
    _resp_command,
    collect_runtime_metrics,
    observe_runtime_epoch_serve_status,
    render_prometheus,
)

EPOCH_OWNER = "69893dc7b7c647f49967387e7efca9fb"
OBSERVED_NOW = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)
COMMON_WATERMARK_MS = 1_791_806_340_000  # 2026-10-12 11:59:00 UTC


def _serve_status(**changes):
    value = {
        "state": "RUNNING_NONTRADING",
        "owner": EPOCH_OWNER,
        "cycles": 3,
        "watermarks_ms": {
            symbol: COMMON_WATERMARK_MS for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT")
        },
        "pending_outbox": 0,
        "unresolved_inbox": 0,
        "completed_inbox": 0,
        "uncompleted_publications": 0,
        "new_publications": 0,
        "observed_at_utc": (OBSERVED_NOW - timedelta(seconds=5)).isoformat(),
        "PAPER_QUALIFIED": False,
        "ALPHA_READY": False,
        "LIVE_READY": False,
        "STRATEGY_POLICY": "REJECT_ALL",
    }
    value.update(changes)
    return value


def _observe_file(path: Path):
    return observe_runtime_epoch_serve_status(path, expected_owner=EPOCH_OWNER, now=OBSERVED_NOW)


class _Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Connection:
    def __init__(self, row=None, error=None):
        self.row = row
        self.error = error

    async def fetchrow(self, _query):
        if self.error is not None:
            raise self.error
        return self.row


class _Pool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _Acquire(self.connection)


@pytest.mark.asyncio
async def test_collects_database_and_authenticated_redis_health():
    row = {
        "outbox_pending": 2,
        "outbox_dead_lettered": 1,
        "inbox_processing": 3,
        "inbox_failed": 4,
        "oldest_inbox_processing_age_seconds": 11.0,
        "inbox_expired_processing": 1,
        "execution_prepared": 5,
        "execution_failed": 6,
        "oldest_outbox_age_seconds": 7.5,
        "closed_bar_gaps_24h": 0,
        "closed_bar_symbols_24h": 5,
        "closed_bar_minimum_coverage_ratio_24h": 1.0,
        "latest_closed_bar_age_seconds": 65.0,
        "venue_measurements_24h": 120,
        "venue_availability_ratio_24h": 0.99,
        "venue_poll_expected_24h": 14_400,
        "venue_poll_attempted_24h": 14_350,
        "venue_poll_succeeded_24h": 14_256,
        "venue_poll_failed_24h": 94,
        "venue_blocked_24h": 2,
        "venue_p95_abs_basis_bps": 3.1,
        "venue_p95_spread_bps": 4.2,
        "venue_p95_slippage_bps": 5.3,
        "venue_max_book_age_ms": 1_500,
        "venue_max_timestamp_skew_ms": 900,
        "venue_p95_latency_ms": 250,
        "latest_venue_age_seconds": 6.4,
        "candidate_veto_24h": 7,
        "candidate_defer_24h": 8,
        "paper_active_trades": 1,
        "paper_unprotected_trades": 0,
        "paper_recovery_blocked": 0,
        "execution_p95_shortfall_bps": 2.5,
        "latest_paper_account_age_seconds": 8.0,
        "api_spend_month_usd": 0.75,
        "execution_runtime_health_age_seconds": 4.0,
        "evedex_auth_age_seconds": 14.0,
        "evedex_auth_expires_in_seconds": 286.0,
        "evedex_local_mutation_reserve": 27,
        "evedex_local_mutation_capacity": 30,
        "evedex_local_mutation_compensation_reserve": 4,
        "evedex_local_mutation_window_seconds": 60.0,
        "evedex_venue_rate_limit_observable": 0,
        "evedex_venue_rate_limit_reserve": -1,
    }

    async def redis_probe(url):
        assert url == "redis://:secret@redis:6379/0"
        return True

    metrics = await collect_runtime_metrics(
        _Pool(_Connection(row=row)),
        redis_url="redis://:secret@redis:6379/0",
        redis_probe=redis_probe,
    )

    assert metrics == RuntimeMetrics(
        1,
        1,
        2,
        1,
        3,
        4,
        5,
        6,
        7.5,
        0,
        5,
        1.0,
        65.0,
        120,
        0.99,
        2,
        3.1,
        4.2,
        5.3,
        1_500,
        900,
        250,
        6.4,
        7,
        8,
        1,
        0,
        0,
        2.5,
        8.0,
        0.75,
        4.0,
        14.0,
        286.0,
        27,
        30,
        4,
        60.0,
        0,
        -1,
        14_400,
        14_350,
        14_256,
        94,
        11.0,
        1,
    )
    rendered = render_prometheus(metrics).decode()
    assert "kairos_outbox_pending 2" in rendered
    assert "kairos_execution_effects_failed 6" in rendered
    assert "kairos_closed_bar_gaps_24h 0" in rendered
    assert "kairos_closed_bar_minimum_coverage_ratio_24h 1.0" in rendered
    assert "kairos_venue_availability_ratio_24h 0.99" in rendered
    assert "kairos_venue_poll_expected_24h 14400" in rendered
    assert "kairos_venue_poll_succeeded_24h 14256" in rendered
    assert "kairos_venue_p95_spread_bps 4.2" in rendered
    assert "kairos_venue_max_book_age_ms 1500" in rendered
    assert "kairos_paper_unprotected_trades 0" in rendered
    assert "kairos_api_spend_month_usd 0.75" in rendered
    assert "kairos_evedex_auth_age_seconds 14.0" in rendered
    assert "kairos_evedex_local_mutation_reserve 27" in rendered
    assert "kairos_evedex_local_mutation_compensation_reserve 4" in rendered
    assert "kairos_evedex_venue_rate_limit_reserve -1" in rendered
    assert "kairos_inbox_processing_oldest_age_seconds 11.0" in rendered
    assert "kairos_inbox_processing_expired 1" in rendered


@pytest.mark.asyncio
async def test_database_failure_is_exposed_without_hiding_redis_health():
    async def redis_probe(_url):
        return True

    metrics = await collect_runtime_metrics(
        _Pool(_Connection(error=RuntimeError("down"))),
        redis_url="redis://redis:6379/0",
        redis_probe=redis_probe,
    )

    assert metrics.persistence_up == 0
    assert metrics.redis_up == 1
    assert metrics.outbox_pending == 0
    assert metrics.latest_paper_account_age_seconds == -1
    assert metrics.oldest_inbox_processing_age_seconds == -1


def test_operations_query_is_fail_closed_for_polling_and_paper_reconciliation():
    assert "/ 14400" not in _QUERY
    assert "topic='kairos.venue.poll.v1'" in _QUERY
    assert "payload->>'trading_mode'='PAPER'" in _QUERY
    assert "payload->>'evedex_profile'='DEV'" in _QUERY
    assert "payload->>'reconciled'='true'" in _QUERY
    assert "HAVING count(DISTINCT venue_poll_facts.status)=1" in _QUERY


def test_resp_command_does_not_embed_protocol_ambiguity():
    assert _resp_command("AUTH", "p@ss") == b"*2\r\n$4\r\nAUTH\r\n$4\r\np@ss\r\n"


def test_runtime_epoch_observation_is_disabled_by_default():
    metrics = RuntimeMetrics(1, 1, 0, 0, 0, 0, 0, 0, 0.0)
    rendered = render_prometheus(metrics).decode()
    assert "kairos_runtime_epoch_serve_status_" not in rendered
    assert "kairos_runtime_epoch_serve_status_age_seconds" not in rendered
    assert "kairos_runtime_epoch_serve_heartbeat_stale" not in rendered


def test_runtime_epoch_status_reports_fresh_and_stale_running_heartbeat():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "status.json"
        path.write_text(json.dumps(_serve_status()), encoding="utf-8")
        fresh = _observe_file(path)
        assert fresh.state == "running"
        assert fresh.age_seconds == 5

        stale_value = _serve_status(observed_at_utc=(OBSERVED_NOW - timedelta(seconds=31)).isoformat())
        path.write_text(json.dumps(stale_value), encoding="utf-8")
        stale = _observe_file(path)
        assert stale.state == "stale"
        assert stale.age_seconds == 31


def test_runtime_epoch_status_distinguishes_missing_invalid_and_failed_closed():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "status.json"
        assert _observe_file(path).state == "missing"

        path.write_text("{not-json", encoding="utf-8")
        assert _observe_file(path).state == "invalid"

        failure = {
            "state": "FAILED_CLOSED",
            "mode": "serve",
            "observed_at_utc": OBSERVED_NOW.isoformat(),
            "trading_started": False,
            "error_type": "RecoveryError",
            "reason": "postgres password=never-export-this",
        }
        path.write_text(json.dumps(failure), encoding="utf-8")
        observed = _observe_file(path)
        assert observed.state == "failed_closed"
        assert observed.age_seconds == 0

        rendered = render_prometheus(
            RuntimeMetrics(1, 1, 0, 0, 0, 0, 0, 0, 0.0, runtime_epoch_serve=observed)
        ).decode()
        assert "kairos_runtime_epoch_serve_status_failed_closed 1" in rendered
        assert "kairos_runtime_epoch_serve_heartbeat_stale 0" in rendered
        assert "never-export-this" not in rendered
        assert "postgres password" not in rendered

        failure["observed_at_utc"] = (OBSERVED_NOW - timedelta(seconds=31)).isoformat()
        path.write_text(json.dumps(failure), encoding="utf-8")
        aged_failure = _observe_file(path)
        assert aged_failure.state == "failed_closed"
        assert aged_failure.stale is True
        aged_rendered = render_prometheus(
            RuntimeMetrics(1, 1, 0, 0, 0, 0, 0, 0, 0.0, runtime_epoch_serve=aged_failure)
        ).decode()
        assert "kairos_runtime_epoch_serve_status_failed_closed 1" in aged_rendered
        assert "kairos_runtime_epoch_serve_heartbeat_stale 1" in aged_rendered


def test_runtime_epoch_status_rejects_future_naive_and_wrong_owner():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "status.json"
        for timestamp in (
            (OBSERVED_NOW + timedelta(seconds=1)).isoformat(),
            OBSERVED_NOW.replace(tzinfo=None).isoformat(),
        ):
            path.write_text(json.dumps(_serve_status(observed_at_utc=timestamp)), encoding="utf-8")
            assert _observe_file(path).state == "invalid"

        path.write_text(json.dumps(_serve_status(owner="f" * 32)), encoding="utf-8")
        assert _observe_file(path).state == "unknown_owner"

        path.write_text(json.dumps(_serve_status(owner=None)), encoding="utf-8")
        unknown = _observe_file(path)
        assert unknown.state == "unknown_owner"
        rendered = render_prometheus(
            RuntimeMetrics(1, 1, 0, 0, 0, 0, 0, 0, 0.0, runtime_epoch_serve=unknown)
        ).decode()
        assert "kairos_runtime_epoch_serve_status_unknown_owner 1" in rendered
        assert "kairos_runtime_epoch_serve_status_running 1" not in rendered


def test_runtime_epoch_status_requires_all_five_watermarks_and_nontrading_flags():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "status.json"
        invalid_cases = (
            _serve_status(watermarks_ms={"BTCUSDT": 1_800_000_000_000}),
            _serve_status(LIVE_READY=True),
            _serve_status(STRATEGY_POLICY="ALLOW"),
            _serve_status(cycles=True),
            _serve_status(unresolved_inbox=-1),
            _serve_status(pending_outbox=1),
            _serve_status(unresolved_inbox=1),
            _serve_status(uncompleted_publications=1),
            _serve_status(
                watermarks_ms={
                    **_serve_status()["watermarks_ms"],
                    "XRPUSDT": COMMON_WATERMARK_MS + 60_000,
                }
            ),
            _serve_status(
                watermarks_ms={
                    symbol: int((OBSERVED_NOW + timedelta(minutes=1)).timestamp() * 1000)
                    for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT")
                }
            ),
        )
        for value in invalid_cases:
            path.write_text(json.dumps(value), encoding="utf-8")
            assert _observe_file(path).state == "invalid"


def test_runtime_epoch_status_rejects_duplicate_json_keys():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "status.json"
        raw = json.dumps(_serve_status())
        raw = raw.replace('"cycles": 3', '"cycles": 3, "cycles": 3')
        path.write_text(raw, encoding="utf-8")
        assert _observe_file(path).state == "invalid"


def test_runtime_epoch_state_and_heartbeat_metrics_never_duplicate_names():
    base = (1, 1, 0, 0, 0, 0, 0, 0, 0.0)
    for observation in (
        RuntimeEpochServeObservation("stale", 31.0, True),
        RuntimeEpochServeObservation("failed_closed", 31.0, True),
    ):
        rendered = render_prometheus(RuntimeMetrics(*base, runtime_epoch_serve=observation)).decode()
        samples = [line.split(maxsplit=1)[0] for line in rendered.splitlines() if not line.startswith("#")]
        assert len(samples) == len(set(samples))
        assert "kairos_runtime_epoch_serve_heartbeat_stale 1" in rendered
    stale = render_prometheus(
        RuntimeMetrics(
            *base,
            runtime_epoch_serve=RuntimeEpochServeObservation("stale", 31.0, True),
        )
    ).decode()
    failed_closed = render_prometheus(
        RuntimeMetrics(
            *base,
            runtime_epoch_serve=RuntimeEpochServeObservation("failed_closed", 31.0, True),
        )
    ).decode()
    assert "kairos_runtime_epoch_serve_status_stale 1" in stale
    assert "kairos_runtime_epoch_serve_status_stale 0" in failed_closed


def test_runtime_epoch_status_rejects_file_swap_between_lstat_and_open(monkeypatch):
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        path = root / "status.json"
        replacement = root / "replacement.json"
        path.write_text(json.dumps(_serve_status()), encoding="utf-8")
        replacement.write_text(json.dumps(_serve_status()), encoding="utf-8")
        original_open = os.open

        def open_replacement(_path, flags):
            return original_open(replacement, flags)

        monkeypatch.setattr(metrics_exporter.os, "open", open_replacement)
        assert _observe_file(path).state == "invalid"


def test_runtime_epoch_status_rejects_nonregular_and_oversized_files():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        assert (
            observe_runtime_epoch_serve_status(root, expected_owner=EPOCH_OWNER, now=OBSERVED_NOW).state
            == "invalid"
        )
        path = root / "status.json"
        path.write_bytes(b" " * (32_768 + 1))
        assert _observe_file(path).state == "invalid"


@pytest.mark.asyncio
async def test_runtime_epoch_observation_requires_path_and_owner_together():
    async def redis_probe(_url):
        return True

    with pytest.raises(ValueError, match="configured together"):
        await collect_runtime_metrics(
            _Pool(_Connection(row={})),
            redis_url="redis://redis:6379/0",
            redis_probe=redis_probe,
            runtime_epoch_status_path="/not-read",
        )


@pytest.mark.asyncio
async def test_runtime_epoch_status_remains_visible_when_database_query_fails():
    async def redis_probe(_url):
        return True

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "status.json"
        observed_at = datetime.now(UTC) - timedelta(seconds=5)
        watermark_ms = int(observed_at.timestamp() // 60) * 60_000
        path.write_text(
            json.dumps(
                _serve_status(
                    observed_at_utc=observed_at.isoformat(),
                    watermarks_ms={
                        symbol: watermark_ms
                        for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT")
                    },
                )
            ),
            encoding="utf-8",
        )
        metrics = await collect_runtime_metrics(
            _Pool(_Connection(error=RuntimeError("private database diagnostic"))),
            redis_url="redis://redis:6379/0",
            redis_probe=redis_probe,
            runtime_epoch_status_path=path,
            runtime_epoch_expected_owner=EPOCH_OWNER,
        )

    assert metrics.persistence_up == 0
    assert metrics.runtime_epoch_serve is not None
    assert metrics.runtime_epoch_serve.state == "running"
    rendered = render_prometheus(metrics).decode()
    assert "kairos_runtime_epoch_serve_status_running 1" in rendered
    assert "private database diagnostic" not in rendered
