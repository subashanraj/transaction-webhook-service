import logging
from pathlib import Path

import asyncpg

from app.config import Settings

log = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


async def create_pool(settings: Settings, max_size: int) -> asyncpg.Pool:
    # min_size=0: no connection is opened at startup, so the app still boots
    # (and answers health checks / 503s) while the database is unreachable.
    return await asyncpg.create_pool(
        settings.database_url,
        min_size=0,
        max_size=max_size,
        statement_cache_size=settings.db_statement_cache_size,
        command_timeout=10,
        timeout=5,
    )


async def run_migrations(pool: asyncpg.Pool) -> None:
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        async with pool.acquire() as conn, conn.transaction():
            # Several instances may boot at once; serialise the DDL.
            await conn.execute("SELECT pg_advisory_xact_lock(727274)")
            await conn.execute(path.read_text(encoding="utf-8"))
        log.info("migration applied: %s", path.name)
