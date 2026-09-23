"""Authenticated, GET-only HTTP entry point for the mobile Cockpit snapshot."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import re
import stat
import time
from collections.abc import Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response

from .cockpit_snapshot import CockpitSnapshotRepository
from .config import PersistenceSettings
from .database import Database

COCKPIT_API_PATH = "/api/v1/cockpit/snapshot"
MAX_SNAPSHOT_BYTES = 2 * 1024 * 1024
AUTHORIZATION_WINDOW_SECONDS = 30
_SIGNATURE_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_PRINCIPAL_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9@._:+/-]{0,255}\Z")


def _canonical_request(method: str, path: str, timestamp: str, principal: str) -> bytes:
    return f"{method}\n{path}\n{timestamp}\n{principal}".encode()


def sign_proxy_request(
    key: bytes,
    *,
    principal: str,
    timestamp: int,
    method: str = "GET",
    path: str = COCKPIT_API_PATH,
) -> str:
    """Create the trusted-ingress HMAC for one authenticated, read-only request."""

    return hmac.new(
        key,
        _canonical_request(method, path, str(timestamp), principal),
        hashlib.sha256,
    ).hexdigest()


def verify_proxy_request(
    headers: Mapping[str, str],
    *,
    key: bytes,
    now: int | None = None,
) -> str | None:
    """Return a signed ingress principal or ``None`` when authorization fails.

    A private authenticated reverse proxy must remove client-supplied
    ``X-Kairos-*`` headers and replace them only after its login check.
    """

    principal = headers.get("x-kairos-authenticated-user", "")
    timestamp_text = headers.get("x-kairos-auth-timestamp", "")
    signature = headers.get("x-kairos-auth-signature", "")
    if (
        not key
        or not _PRINCIPAL_PATTERN.fullmatch(principal)
        or not timestamp_text.isascii()
        or not timestamp_text.isdecimal()
        or len(timestamp_text) > 12
        or str(int(timestamp_text)) != timestamp_text
        or not _SIGNATURE_PATTERN.fullmatch(signature)
        or any("," in value for value in (principal, timestamp_text, signature))
    ):
        return None
    timestamp = int(timestamp_text)
    current_time = int(time.time()) if now is None else now
    if abs(current_time - timestamp) > AUTHORIZATION_WINDOW_SECONDS:
        return None
    expected = sign_proxy_request(key, principal=principal, timestamp=timestamp)
    if not hmac.compare_digest(signature, expected):
        return None
    return principal


def _read_secret_file(variable: str) -> bytes:
    configured_path = os.environ.get(variable, "")
    if not configured_path:
        raise RuntimeError(f"Cockpit requires {variable} to name a mounted secret file")
    path = Path(configured_path)
    if not path.is_absolute() or path.is_symlink():
        raise RuntimeError(f"Cockpit {variable} must name an absolute regular secret file")
    try:
        metadata = path.stat()
        if not stat.S_ISREG(metadata.st_mode):
            raise RuntimeError(f"Cockpit {variable} must name an absolute regular secret file")
        if os.name != "nt" and stat.S_IMODE(metadata.st_mode) & 0o077:
            raise RuntimeError(f"Cockpit {variable} permissions must not allow group or public access")
        value = path.read_bytes().rstrip(b"\r\n")
    except OSError as exc:
        raise RuntimeError(f"Cockpit {variable} could not be read") from exc
    if not value:
        raise RuntimeError(f"Cockpit {variable} must not be empty")
    return value


def _signing_key_from_environment() -> bytes:
    key = _read_secret_file("KAIROS_COCKPIT_PROXY_SIGNING_KEY_FILE")
    if len(key) < 32:
        raise RuntimeError("Cockpit requires a proxy signing key of at least 32 bytes")
    return key


def _database_from_environment() -> Database:
    try:
        database_url = _read_secret_file("KAIROS_COCKPIT_DATABASE_URL_FILE").decode("ascii")
    except UnicodeDecodeError as exc:
        raise RuntimeError("Cockpit database URL secret must be ASCII") from exc
    settings = PersistenceSettings(
        database_url=database_url,
        pool_min_size=1,
        pool_max_size=3,
        command_timeout_s=5.0,
    )
    return Database(settings, read_only=True)


def create_app(
    *,
    reader: Any | None = None,
    signing_key: bytes | None = None,
) -> FastAPI:
    """Create the API; production startup fails closed without auth and DB config."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        key = signing_key if signing_key is not None else _signing_key_from_environment()
        if len(key) < 32:
            raise RuntimeError("Cockpit requires a proxy signing key of at least 32 bytes")
        app.state.proxy_signing_key = key
        database: Database | None = None
        if reader is None:
            database = _database_from_environment()
            await database.connect()
            try:
                await database.verify_schema()
                source = CockpitSnapshotRepository(database)
                await source.verify_read_only_access()
                app.state.snapshot_reader = source
            except BaseException:
                await database.close()
                raise
        else:
            app.state.snapshot_reader = reader
        try:
            yield
        finally:
            if database is not None:
                await database.close()

    app = FastAPI(
        title="Kairos Cockpit Read-only API",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.logger = logging.getLogger("kairos.cockpit")

    @app.get(COCKPIT_API_PATH)
    async def cockpit_snapshot(request: Request) -> Response:
        if request.url.query:
            raise HTTPException(status_code=400, detail="query parameters are not supported")
        if (
            verify_proxy_request(
                request.headers,
                key=request.app.state.proxy_signing_key,
            )
            is None
        ):
            raise HTTPException(status_code=401, detail="authenticated private ingress required")
        try:
            snapshot = await request.app.state.snapshot_reader.load_snapshot()
            body = json.dumps(snapshot, allow_nan=False, separators=(",", ":")).encode("utf-8")
        except Exception as exc:
            # Do not log exception strings: database/client errors can contain
            # connection details.  The UI gets no stale or partial fallback.
            request.app.state.logger.warning("cockpit_snapshot_unavailable error_type=%s", type(exc).__name__)
            raise HTTPException(status_code=503, detail="snapshot source unavailable") from None
        if len(body) > MAX_SNAPSHOT_BYTES:
            raise HTTPException(status_code=413, detail="snapshot exceeds the response size limit")
        return Response(
            content=body,
            media_type="application/json",
            headers={
                "Cache-Control": "no-store, private",
                "Pragma": "no-cache",
                "X-Content-Type-Options": "nosniff",
                "Referrer-Policy": "no-referrer",
            },
        )

    return app


app = create_app()


def main() -> None:
    host = os.environ.get("KAIROS_COCKPIT_API_HOST", "127.0.0.1")
    port = int(os.environ.get("KAIROS_COCKPIT_API_PORT", "8081"))
    uvicorn.run(app, host=host, port=port, access_log=False, server_header=False)


if __name__ == "__main__":
    main()
