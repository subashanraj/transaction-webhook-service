import asyncio
import statistics
import time

from tests.conftest import payload, running_app

URL = "/v1/webhooks/transactions"


async def test_webhook_latency_under_processing_load(db):  # S28, PDF success criterion 3
    # Long processing so every accepted job stays in flight during the measurement.
    async with running_app(processing_delay_seconds=30, lease_seconds=60, worker_concurrency=500) as (c, app):
        sem = asyncio.Semaphore(50)

        async def send(i: int) -> float:
            async with sem:
                t0 = time.perf_counter()
                r = await c.post(URL, json=payload(f"txn_perf_{i}"))
                assert r.status_code == 202
                return time.perf_counter() - t0

        # First wave fills the worker with 300 in-flight 30s jobs ...
        await asyncio.gather(*(send(i) for i in range(300)))
        await asyncio.sleep(0.5)
        assert app.state.worker.in_flight > 0
        # ... second wave is measured while all of that is "processing".
        latencies = sorted(await asyncio.gather(*(send(i) for i in range(300, 600))))

    p50 = statistics.median(latencies)
    p99 = latencies[int(len(latencies) * 0.99) - 1]
    print(f"\nlatency p50={p50 * 1000:.0f}ms p99={p99 * 1000:.0f}ms max={latencies[-1] * 1000:.0f}ms")
    assert p99 < 0.5
