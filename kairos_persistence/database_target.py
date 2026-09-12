"""Explicit PostgreSQL target validation for operational write commands."""

from __future__ import annotations

import re
from urllib.parse import urlsplit

from .database import Database

_DATABASE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,62}\Z")
_HOSTNAME = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?\Z")
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "timescaledb"})


class DatabaseTargetError(ValueError):
    """Unsafe or unexpected target; never include DSNs or credentials in errors."""


def require_database_target_url(
    database_url: str, expected_database_name: str, *, local_only: bool = False
) -> str:
    """Require one literal named target, without driver query overrides.

    Credentials may be percent-encoded; hosts and database paths may not.
    Requiring an explicit host and port avoids environment-selected endpoints.
    Remote hosts are permitted only for adoption, never the destructive drill.
    """
    if not isinstance(expected_database_name, str) or not _DATABASE_NAME.fullmatch(expected_database_name):
        raise DatabaseTargetError("explicit database name must be a simple PostgreSQL identifier")
    if (
        not database_url.isascii()
        or any(ord(char) <= 32 or ord(char) == 127 for char in database_url)
        or any(char in database_url for char in "\\?#")
        or re.search(r"%(?![0-9a-fA-F]{2})", database_url)
    ):
        raise DatabaseTargetError("database URL contains forbidden or ambiguous syntax")
    try:
        parsed = urlsplit(database_url)
        host, port = parsed.hostname, parsed.port
    except ValueError:
        raise DatabaseTargetError("database URL is malformed") from None
    if (
        parsed.scheme not in {"postgresql", "postgres"}
        or not host
        or (":" not in host and _HOSTNAME.fullmatch(host) is None)
        or "%" in host
        or parsed.netloc.count("@") > 1
        or port is None
        or not 1 <= port <= 65535
        or parsed.path != f"/{expected_database_name}"
    ):
        raise DatabaseTargetError(
            "database URL must name the exact expected database, host and explicit port"
        )
    if local_only and host not in _LOCAL_HOSTS:
        raise DatabaseTargetError("disposable database requires an explicitly allowed local test host")
    return expected_database_name


async def connect_verified_database(
    database: Database, expected_database_name: str, *, local_only: bool = False
) -> None:
    """Verify before connection, then check server identity before any write.

    Does not migrate or register anything. This must be called on every newly
    created/reconnected pool before the caller performs its operational writes.
    """
    require_database_target_url(database.settings.database_url, expected_database_name, local_only=local_only)
    await database.connect()
    try:
        if await database.pool.fetchval("SELECT current_database()") != expected_database_name:
            raise DatabaseTargetError("server database differs from the explicitly expected database")
    except BaseException:
        await database.close()
        raise
