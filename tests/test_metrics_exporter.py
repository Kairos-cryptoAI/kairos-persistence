import pytest

from kairos_persistence.metrics_exporter import (
    _QUERY,
    RuntimeMetrics,
    _resp_command,
    collect_runtime_metrics,
    render_prometheus,
)


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
