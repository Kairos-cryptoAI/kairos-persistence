from __future__ import annotations

import os
import time
from typing import Any

import httpx
import pytest

from kairos_persistence.cockpit_api import (
    COCKPIT_API_PATH,
    _database_from_environment,
    _signing_key_from_environment,
    create_app,
    sign_proxy_request,
    verify_proxy_request,
)

_KEY = b"unit-test-only-cockpit-proxy-signing-key-32-bytes"
_SNAPSHOT: dict[str, Any] = {
    "schema_version": "kairos.cockpit.snapshot.v1",
    "generated_at": "2026-09-23T08:00:00.000Z",
    "system": {
        "runtime_state": "DEGRADED",
        "trading_mode": "DRY_RUN",
        "strategy_policy": "REJECT_ALL",
        "readiness": {
            "technical_paper_ready": False,
            "paper_qualified": False,
            "alpha_ready": False,
            "live_ready": False,
        },
    },
    "markets": [],
    "decisions": [],
    "lifecycle": [],
    "tca": [],
}


class FakeReader:
    def __init__(self, value: object = _SNAPSHOT) -> None:
        self.value = value
        self.calls = 0

    async def load_snapshot(self) -> object:
        self.calls += 1
        if isinstance(self.value, Exception):
            raise self.value
        return self.value


def signed_headers(
    *, key: bytes = _KEY, principal: str = "loval", timestamp: int | None = None
) -> dict[str, str]:
    request_time = int(time.time()) if timestamp is None else timestamp
    return {
        "x-kairos-authenticated-user": principal,
        "x-kairos-auth-timestamp": str(request_time),
        "x-kairos-auth-signature": sign_proxy_request(
            key,
            principal=principal,
            timestamp=request_time,
        ),
    }


def test_proxy_signature_is_bound_to_principal_method_path_and_freshness() -> None:
    headers = signed_headers(timestamp=1_800_000_000)
    assert verify_proxy_request(headers, key=_KEY, now=1_800_000_000) == "loval"
    assert verify_proxy_request(headers, key=_KEY, now=1_800_000_031) is None
    assert (
        verify_proxy_request({**headers, "x-kairos-authenticated-user": "other"}, key=_KEY, now=1_800_000_000)
        is None
    )
    assert (
        verify_proxy_request({**headers, "x-kairos-auth-signature": "0" * 64}, key=_KEY, now=1_800_000_000)
        is None
    )


def test_runtime_secrets_are_loaded_only_from_absolute_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    key = b"unit-test-only-cockpit-proxy-signing-key-32-bytes"
    key_file = tmp_path / "proxy-signing-key"
    key_file.write_bytes(key + b"\n")
    database_file = tmp_path / "cockpit-database-url"
    database_file.write_bytes(b"postgresql://cockpit_reader:placeholder@localhost:5432/kairos\n")
    if os.name != "nt":
        key_file.chmod(0o600)
        database_file.chmod(0o600)
    monkeypatch.setenv("KAIROS_COCKPIT_PROXY_SIGNING_KEY_FILE", str(key_file))
    monkeypatch.setenv("KAIROS_COCKPIT_DATABASE_URL_FILE", str(database_file))
    monkeypatch.setenv("KAIROS_COCKPIT_PROXY_SIGNING_KEY", "must-not-be-used")
    monkeypatch.setenv("KAIROS_COCKPIT_DATABASE_URL", "postgresql://must-not-be-used")

    assert _signing_key_from_environment() == key
    database = _database_from_environment()
    assert database.settings.database_url == "postgresql://cockpit_reader:placeholder@localhost:5432/kairos"
    assert database.read_only is True


def test_runtime_secrets_fail_closed_when_file_configuration_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("KAIROS_COCKPIT_PROXY_SIGNING_KEY_FILE", raising=False)
    monkeypatch.delenv("KAIROS_COCKPIT_DATABASE_URL_FILE", raising=False)
    with pytest.raises(RuntimeError, match="secret file"):
        _signing_key_from_environment()
    with pytest.raises(RuntimeError, match="secret file"):
        _database_from_environment()


@pytest.mark.asyncio
async def test_snapshot_endpoint_requires_signed_private_ingress_and_never_enables_cors() -> None:
    reader = FakeReader()
    app = create_app(reader=reader, signing_key=_KEY)

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://cockpit.local") as client:
            unauthorized = await client.get(COCKPIT_API_PATH)
            response = await client.get(COCKPIT_API_PATH, headers=signed_headers())
    assert unauthorized.status_code == 401
    assert response.status_code == 200
    assert response.json() == _SNAPSHOT
    assert response.headers["cache-control"] == "no-store, private"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert "access-control-allow-origin" not in response.headers
    assert reader.calls == 1


@pytest.mark.asyncio
async def test_snapshot_endpoint_rejects_query_parameters_and_non_get_mutations() -> None:
    reader = FakeReader()
    app = create_app(reader=reader, signing_key=_KEY)

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://cockpit.local") as client:
            query_response = await client.get(COCKPIT_API_PATH + "?symbol=BTCUSDT", headers=signed_headers())
            post_response = await client.post(COCKPIT_API_PATH, headers=signed_headers(), json={})

    assert query_response.status_code == 400
    assert post_response.status_code == 405
    assert reader.calls == 0


@pytest.mark.asyncio
async def test_snapshot_endpoint_fails_closed_on_source_error_or_oversized_payload() -> None:
    failing_app = create_app(reader=FakeReader(RuntimeError("private connection text")), signing_key=_KEY)
    oversized_app = create_app(reader=FakeReader({"large": "x" * (2 * 1024 * 1024)}), signing_key=_KEY)

    async with failing_app.router.lifespan_context(failing_app):
        transport = httpx.ASGITransport(app=failing_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://cockpit.local") as client:
            failure = await client.get(COCKPIT_API_PATH, headers=signed_headers())
    async with oversized_app.router.lifespan_context(oversized_app):
        transport = httpx.ASGITransport(app=oversized_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://cockpit.local") as client:
            oversized = await client.get(COCKPIT_API_PATH, headers=signed_headers())

    assert failure.status_code == 503
    assert "private connection text" not in failure.text
    assert oversized.status_code == 413
