"""Apply migrations once and close the pool in the same event loop."""

from __future__ import annotations

import asyncio

from kairos_persistence import Database


async def main() -> None:
    database = Database()
    await database.connect()
    try:
        await database.migrate()
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
