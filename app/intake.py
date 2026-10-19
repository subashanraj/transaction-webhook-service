import asyncio
import logging

import asyncpg

from app import repository as repo
from app.repository import InsertResult
from app.schemas import TransactionWebhook

log = logging.getLogger(__name__)


class IntakeBatcher:
    """Group commit for the webhook endpoint.

    Every request still waits until *its* row is committed before getting a
    202; requests that arrive while a write is in flight are simply committed
    together in the next round-trip. With a remote database (100+ ms per
    round-trip) this is the difference between ~10 and thousands of webhooks
    per second per connection.
    """

    def __init__(self, pool: asyncpg.Pool, flushers: int, max_batch: int):
        self.pool = pool
        self.flushers = flushers
        self.max_batch = max_batch
        self._queue: asyncio.Queue[tuple[TransactionWebhook, asyncio.Future]] = asyncio.Queue()
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        self._tasks = [asyncio.create_task(self._flush_loop(), name=f"intake-{i}") for i in range(self.flushers)]

    async def submit(self, txn: TransactionWebhook) -> InsertResult:
        fut = asyncio.get_running_loop().create_future()
        self._queue.put_nowait((txn, fut))
        # If the caller times out this future is cancelled; the row may still be
        # committed by the batch, which is safe: the sender's retry is a duplicate.
        return await fut

    async def _flush_loop(self) -> None:
        while True:
            items = [await self._queue.get()]
            while len(items) < self.max_batch and not self._queue.empty():
                items.append(self._queue.get_nowait())
            live = [(t, f) for t, f in items if not f.done()]
            if not live:
                continue
            try:
                results = await repo.insert_batch(self.pool, [t for t, _ in live])
            except Exception as e:
                for _, f in live:
                    if not f.done():
                        f.set_exception(e)
                continue
            for (_, f), r in zip(live, results):
                if not f.done():
                    f.set_result(r)
            if len(live) > 1:
                log.debug("committed %d webhooks in one batch", len(live))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        while not self._queue.empty():
            _, f = self._queue.get_nowait()
            if not f.done():
                f.set_exception(RuntimeError("shutting down"))
