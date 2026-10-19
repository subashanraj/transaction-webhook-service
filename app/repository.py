"""All SQL lives here. Every query is parameterised."""

from dataclasses import dataclass

import asyncpg

from app.schemas import TransactionWebhook

# Group commit: many webhooks in one round-trip. Ids are unique within a batch
# (Postgres rejects ON CONFLICT DO UPDATE touching a row twice in one statement),
# and sorted so concurrent batches lock rows in the same order (no deadlocks).
INSERT_BATCH = """
INSERT INTO transactions AS t
    (transaction_id, source_account, destination_account, amount, currency,
     payload_hash, webhook_count, payload_mismatch)
SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::numeric[], $5::text[],
                     $6::text[], $7::int[], $8::bool[])
ON CONFLICT (transaction_id) DO UPDATE
    SET webhook_count    = t.webhook_count + EXCLUDED.webhook_count,
        payload_mismatch = t.payload_mismatch OR EXCLUDED.payload_mismatch
                           OR t.payload_hash <> EXCLUDED.payload_hash,
        updated_at       = now()
RETURNING t.transaction_id, (t.xmax = 0) AS inserted, t.status, t.payload_hash
"""

CLAIM_BATCH = """
UPDATE transactions t
SET locked_by    = $1,
    locked_until = now() + make_interval(secs => $3),
    attempts     = attempts + 1,
    updated_at   = now()
WHERE t.transaction_id IN (
    SELECT transaction_id FROM transactions
    WHERE status = 'PROCESSING'
      AND next_attempt_at <= now()
      AND (locked_until IS NULL OR locked_until < now())
    ORDER BY next_attempt_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
RETURNING t.transaction_id, t.source_account, t.destination_account,
          t.amount, t.currency, t.attempts
"""

MARK_PROCESSED = """
UPDATE transactions
SET status = 'PROCESSED', processed_at = now(), updated_at = now(),
    locked_by = NULL, locked_until = NULL, last_error = NULL
WHERE transaction_id = $1 AND status = 'PROCESSING' AND locked_by = $2
"""

MARK_RETRY_OR_FAILED = """
UPDATE transactions
SET status          = CASE WHEN attempts >= $3 THEN 'FAILED' ELSE 'PROCESSING' END,
    next_attempt_at = now() + make_interval(
                          secs => LEAST($5::float8, $4::float8 * power(2, attempts - 1))),
    last_error      = $6,
    locked_by       = NULL,
    locked_until    = NULL,
    updated_at      = now()
WHERE transaction_id = $1 AND status = 'PROCESSING' AND locked_by = $2
RETURNING status
"""

# Graceful shutdown: hand unfinished jobs back immediately, and don't count
# the interrupted run against the retry budget.
RELEASE_LEASES = """
UPDATE transactions
SET locked_by = NULL, locked_until = NULL,
    attempts = GREATEST(attempts - 1, 0), updated_at = now()
WHERE locked_by = $1 AND status = 'PROCESSING' AND transaction_id = ANY($2::text[])
"""

GET_ONE = """
SELECT transaction_id, source_account, destination_account, amount, currency,
       status, created_at, processed_at
FROM transactions WHERE transaction_id = $1
"""

# Dashboard: most recent transactions. started_at is when the current lease was
# taken (lease expiry minus lease length), i.e. when processing actually began.
LIST_RECENT = """
SELECT transaction_id, source_account, destination_account, amount, currency,
       status, created_at, processed_at, attempts, webhook_count, payload_mismatch,
       last_error, locked_until - make_interval(secs => $3) AS started_at
FROM transactions
WHERE ($2::text IS NULL OR status = $2)
ORDER BY created_at DESC
LIMIT $1
"""

STATS = """
SELECT status, count(*) AS n, min(created_at) AS oldest, now() AS server_time
FROM transactions GROUP BY status
"""


@dataclass(frozen=True)
class InsertResult:
    inserted: bool
    status: str
    mismatch: bool


async def insert_batch(pool: asyncpg.Pool, txns: list[TransactionWebhook]) -> list[InsertResult]:
    """Insert-or-touch many webhooks in one statement; one result per input, in order."""
    hashes = [t.payload_hash() for t in txns]
    unique: dict[str, dict] = {}
    for t, h in zip(txns, hashes):
        u = unique.get(t.transaction_id)
        if u is None:
            unique[t.transaction_id] = {"t": t, "hash": h, "count": 1, "mismatch": False}
        else:
            u["count"] += 1
            u["mismatch"] |= h != u["hash"]
    batch = [unique[k] for k in sorted(unique)]

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            INSERT_BATCH,
            [u["t"].transaction_id for u in batch],
            [u["t"].source_account for u in batch],
            [u["t"].destination_account for u in batch],
            [u["t"].amount for u in batch],
            [u["t"].currency for u in batch],
            [u["hash"] for u in batch],
            [u["count"] for u in batch],
            [u["mismatch"] for u in batch],
        )
    by_id = {r["transaction_id"]: r for r in rows}

    results, seen = [], set()
    for t, h in zip(txns, hashes):
        r = by_id[t.transaction_id]
        first = t.transaction_id not in seen
        seen.add(t.transaction_id)
        results.append(
            InsertResult(
                inserted=bool(r["inserted"]) and first,  # later copies in the batch are duplicates
                status=r["status"],
                mismatch=h != r["payload_hash"],
            )
        )
    return results


async def prime_connection(conn: asyncpg.Connection) -> None:
    """Prepare the hot-path statement ahead of the first webhook (saves a round-trip)."""
    await conn.fetch(INSERT_BATCH, [], [], [], [], [], [], [], [])


async def claim_batch(pool: asyncpg.Pool, worker_id: str, limit: int, lease_seconds: float) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(CLAIM_BATCH, worker_id, limit, float(lease_seconds))


async def mark_processed(pool: asyncpg.Pool, transaction_id: str, worker_id: str) -> bool:
    async with pool.acquire() as conn:
        result = await conn.execute(MARK_PROCESSED, transaction_id, worker_id)
    return result == "UPDATE 1"


async def mark_retry_or_failed(
    pool: asyncpg.Pool,
    transaction_id: str,
    worker_id: str,
    *,
    max_attempts: int,
    base_seconds: float,
    max_seconds: float,
    error: str,
) -> str | None:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            MARK_RETRY_OR_FAILED,
            transaction_id,
            worker_id,
            max_attempts,
            float(base_seconds),
            float(max_seconds),
            error[:1000],
        )


async def release_leases(pool: asyncpg.Pool, worker_id: str, transaction_ids: list[str]) -> int:
    if not transaction_ids:
        return 0
    async with pool.acquire() as conn:
        result = await conn.execute(RELEASE_LEASES, worker_id, transaction_ids)
    return int(result.split()[-1])


async def get_transaction(pool: asyncpg.Pool, transaction_id: str) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        return await conn.fetchrow(GET_ONE, transaction_id)


async def list_recent(
    pool: asyncpg.Pool, limit: int, status: str | None, lease_seconds: float
) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(LIST_RECENT, limit, status, float(lease_seconds))


async def stats(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(STATS)
