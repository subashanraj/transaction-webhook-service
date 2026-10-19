import asyncio
import logging
import os
import socket
import uuid
from collections.abc import Awaitable, Callable

import asyncpg

from app import repository as repo
from app.config import Settings
from app.security import mask_account

log = logging.getLogger(__name__)

Processor = Callable[[asyncpg.Record], Awaitable[None]]


class Worker:
    """Pulls PROCESSING rows from Postgres and runs them with bounded concurrency.

    The table is the queue: rows are claimed with a time-limited lease
    (FOR UPDATE SKIP LOCKED), so any number of workers/instances can run, and a
    crashed worker's jobs are picked up again once its lease expires.
    """

    def __init__(self, pool: asyncpg.Pool, settings: Settings, processor: Processor):
        self.pool = pool
        self.settings = settings
        self.processor = processor
        self.id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self._wake = asyncio.Event()
        self._in_flight: dict[str, asyncio.Task] = {}
        self._loop_task: asyncio.Task | None = None
        self._stopping = False

    def notify(self) -> None:
        """Called by the webhook handler so new work starts without waiting for the poll."""
        self._wake.set()

    @property
    def in_flight(self) -> int:
        return len(self._in_flight)

    async def start(self) -> None:
        self._loop_task = asyncio.create_task(self._run(), name="worker-loop")
        log.info("worker started id=%s concurrency=%d", self.id, self.settings.worker_concurrency)

    async def _run(self) -> None:
        error_backoff = 0.0
        while not self._stopping:
            # Clear before claiming: a notify() that lands during the claim
            # keeps the event set, so the wait below returns at once.
            self._wake.clear()
            try:
                free = self.settings.worker_concurrency - len(self._in_flight)
                if free > 0:
                    rows = await repo.claim_batch(self.pool, self.id, free, self.settings.lease_seconds)
                    for row in rows:
                        self._spawn(row)
                    if len(rows) == free:
                        self._wake.set()  # there may be more waiting
                error_backoff = 0.0
            except Exception:
                error_backoff = min(30.0, max(1.0, error_backoff * 2))
                log.exception("worker claim failed; retrying in %.0fs", error_backoff)
            try:
                await asyncio.wait_for(self._wake.wait(), self.settings.poll_interval_seconds + error_backoff)
            except TimeoutError:
                pass

    def _spawn(self, row: asyncpg.Record) -> None:
        tid = row["transaction_id"]
        log.info(
            "processing txn=%s attempt=%d %s->%s %s %s",
            tid, row["attempts"], mask_account(row["source_account"]),
            mask_account(row["destination_account"]), row["amount"], row["currency"],
        )
        task = asyncio.create_task(self._process(row), name=f"txn-{tid}")
        self._in_flight[tid] = task

    async def _process(self, row: asyncpg.Record) -> None:
        tid = row["transaction_id"]
        try:
            try:
                await self.processor(row)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await self._finalize_failure(tid, e)
            else:
                await self._finalize_success(tid)
        finally:
            self._in_flight.pop(tid, None)
            self._wake.set()  # a slot is free

    async def _finalize_success(self, tid: str) -> None:
        for attempt in range(3):
            try:
                if await repo.mark_processed(self.pool, tid, self.id):
                    log.info("processed txn=%s", tid)
                else:
                    # Lease was lost (e.g. we were too slow) and someone else owns it now.
                    log.warning("txn=%s finished but lease no longer held; leaving to new owner", tid)
                return
            except Exception:
                log.exception("could not mark txn=%s processed (try %d)", tid, attempt + 1)
                await asyncio.sleep(0.5 * (attempt + 1))
        # Lease will expire and the job will be re-run: at-least-once.
        log.error("giving up finalizing txn=%s; it will be retried after lease expiry", tid)

    async def _finalize_failure(self, tid: str, error: Exception) -> None:
        s = self.settings
        try:
            status = await repo.mark_retry_or_failed(
                self.pool, tid, self.id,
                max_attempts=s.max_attempts, base_seconds=s.retry_base_seconds,
                max_seconds=s.retry_max_seconds, error=f"{type(error).__name__}: {error}",
            )
            if status == "FAILED":
                log.error("txn=%s FAILED after max attempts: %s", tid, error)
            else:
                log.warning("txn=%s attempt failed, will retry: %s", tid, error)
        except Exception:
            log.exception("could not record failure for txn=%s; retried after lease expiry", tid)

    async def stop(self) -> None:
        """Stop claiming, cancel in-flight jobs and hand their leases back."""
        self._stopping = True
        self._wake.set()
        if self._loop_task:
            self._loop_task.cancel()
            await asyncio.gather(self._loop_task, return_exceptions=True)
        ids = list(self._in_flight)
        tasks = list(self._in_flight.values())
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if ids:
            try:
                released = await repo.release_leases(self.pool, self.id, ids)
                log.info("released %d leases on shutdown", released)
            except Exception:
                log.exception("could not release leases; they will expire on their own")
        log.info("worker stopped id=%s", self.id)
