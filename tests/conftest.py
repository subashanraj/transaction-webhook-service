"""Integration tests run against a real PostgreSQL (TEST_DATABASE_URL).

Processing delay is shortened so the suite runs in seconds; the logic is the
same code path as production.
"""

import asyncio
import os
import time
from contextlib import asynccontextmanager

import asyncpg
import httpx
import pytest

from app.config import Settings
from app.db import MIGRATIONS_DIR
from app.main import create_app

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "postgresql://postgres@localhost:5433/webhooks_test")

FAST = dict(
    processing_delay_seconds=0.5,
    poll_interval_seconds=0.1,
    lease_seconds=6,
    retry_base_seconds=0.05,
    retry_max_seconds=0.2,
    max_attempts=3,
    worker_concurrency=50,
    db_write_timeout_ms=2000,  # generous on CI/laptops; prod default is 400
    webhook_secret="",
)


def make_settings(**overrides) -> Settings:
    return Settings(_env_file=None, **{"database_url": TEST_DATABASE_URL, **FAST, **overrides})


def payload(tid: str, **overrides) -> dict:
    body = {
        "transaction_id": tid,
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 1500,
        "currency": "INR",
    }
    body.update(overrides)
    return body


@asynccontextmanager
async def running_app(**overrides):
    """Start the app with its lifespan (pools + worker) and yield (client, app)."""
    app = create_app(make_settings(**overrides))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client, app


@pytest.fixture
async def db():
    conn = await asyncpg.connect(TEST_DATABASE_URL)
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        await conn.execute(path.read_text(encoding="utf-8"))
    await conn.execute("TRUNCATE transactions")
    yield conn
    await conn.close()


@pytest.fixture
async def client(db):
    async with running_app() as (c, _app):
        yield c


async def wait_for_status(db, tid: str, status: str, timeout: float = 10.0) -> asyncpg.Record:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = await db.fetchrow("SELECT * FROM transactions WHERE transaction_id=$1", tid)
        if row and row["status"] == status:
            return row
        await asyncio.sleep(0.05)
    raise AssertionError(f"{tid} did not reach {status} within {timeout}s (last: {row and dict(row)})")
