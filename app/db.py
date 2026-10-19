import asyncio
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path

import asyncpg

from app.config import Settings

log = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


async def _reset_connection(conn: asyncpg.Connection) -> None:
    # asyncpg's default reset (RESET ALL; UNLISTEN *; ...) is a full extra
    # round-trip on every release, doubling hot-path latency to a remote DB.
    # We never set session state, so only an abandoned transaction needs undoing.
    if conn.is_in_transaction():
        await conn.execute("ROLLBACK")


async def create_pool(settings: Settings, max_size: int) -> asyncpg.Pool:
    # min_size=0: no connection is opened at startup, so the app still boots
    # (and answers health checks / 503s) while the database is unreachable.
    # Warm connections are opened by keep_warm() instead, and never closed for
    # being idle: a fresh TLS connection to a remote DB can take ~1 s, which
    # would blow the webhook's 500 ms budget.
    return await asyncpg.create_pool(
        settings.database_url,
        min_size=0,
        max_size=max_size,
        max_inactive_connection_lifetime=0,
        reset=_reset_connection,
        statement_cache_size=settings.db_statement_cache_size,
        command_timeout=10,
        timeout=5,
    )


Prime = Callable[[asyncpg.Connection], Awaitable[None]]


async def warm_pool(pool: asyncpg.Pool, count: int, prime: Prime | None = None) -> None:
    """Hold `count` connections at once so the pool has that many open, alive and primed."""
    acquired = await asyncio.gather(*(pool.acquire() for _ in range(count)), return_exceptions=True)
    conns = [c for c in acquired if not isinstance(c, BaseException)]
    try:
        await asyncio.gather(*((prime(c) if prime else c.fetchval("SELECT 1")) for c in conns))
    finally:
        for c in conns:
            await pool.release(c)
    errors = [c for c in acquired if isinstance(c, BaseException)]
    if errors:
        raise errors[0]


async def keep_warm(pool: asyncpg.Pool, count: int, interval: float, prime: Prime | None = None) -> None:
    """Background task: keep warm connections alive, and reopen them after DB outages."""
    while True:
        await asyncio.sleep(interval)
        try:
            await warm_pool(pool, count, prime)
        except Exception as e:
            log.warning("could not warm DB pool: %s: %s", type(e).__name__, e)


async def run_migrations(pool: asyncpg.Pool) -> None:
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        async with pool.acquire() as conn, conn.transaction():
            # Several instances may boot at once; serialise the DDL.
            await conn.execute("SELECT pg_advisory_xact_lock(727274)")
            await conn.execute(path.read_text(encoding="utf-8"))
        log.info("migration applied: %s", path.name)
