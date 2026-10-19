"""All SQL lives here. Every query is parameterised."""

from dataclasses import dataclass

import asyncpg

from app.schemas import TransactionWebhook

INSERT_OR_TOUCH = """
INSERT INTO transactions
    (transaction_id, source_account, destination_account, amount, currency, payload_hash)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (transaction_id) DO UPDATE
    SET webhook_count    = transactions.webhook_count + 1,
        payload_mismatch = transactions.payload_mismatch
                           OR transactions.payload_hash <> EXCLUDED.payload_hash,
        updated_at       = now()
RETURNING (xmax = 0) AS inserted, status,
          (transactions.payload_hash <> $6) AS mismatch
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


@dataclass(frozen=True)
class InsertResult:
    inserted: bool
    status: str
    mismatch: bool


async def insert_or_touch(pool: asyncpg.Pool, txn: TransactionWebhook) -> InsertResult:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            INSERT_OR_TOUCH,
            txn.transaction_id,
            txn.source_account,
            txn.destination_account,
            txn.amount,
            txn.currency,
            txn.payload_hash(),
        )
    return InsertResult(inserted=row["inserted"], status=row["status"], mismatch=bool(row["mismatch"]))


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
