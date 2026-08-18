import pytest

from kairos_persistence.metrics_exporter import (
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
        "execution_prepared": 5,
        "execution_failed": 6,
        "oldest_outbox_age_seconds": 7.5,
    }

    async def redis_probe(url):
        assert url == "redis://:secret@redis:6379/0"
        return True

    metrics = await collect_runtime_metrics(
        _Pool(_Connection(row=row)),
        redis_url="redis://:secret@redis:6379/0",
        redis_probe=redis_probe,
    )

    assert metrics == RuntimeMetrics(1, 1, 2, 1, 3, 4, 5, 6, 7.5)
    rendered = render_prometheus(metrics).decode()
    assert "kairos_outbox_pending 2" in rendered
    assert "kairos_execution_effects_failed 6" in rendered


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


def test_resp_command_does_not_embed_protocol_ambiguity():
    assert _resp_command("AUTH", "p@ss") == b"*2\r\n$4\r\nAUTH\r\n$4\r\np@ss\r\n"
