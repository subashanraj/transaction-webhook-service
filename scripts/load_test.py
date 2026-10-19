"""Fire many unique webhooks and report latency percentiles.

    python scripts/load_test.py --base http://127.0.0.1:8000 --count 1000 --concurrency 50

Load is spread over several client processes (--processes) because a single
Python client saturates long before the server does, and its own queueing would
show up as "latency". Both client-observed latency and server-side time (the
X-Process-Time-Ms header) are reported.
"""

import argparse
import asyncio
import multiprocessing as mp
import statistics
import time
import uuid

import httpx


async def _worker(base: str, run: str, ids: list[int], concurrency: int) -> tuple[list[float], list[float], dict]:
    sem = asyncio.Semaphore(concurrency)
    client_ms: list[float] = []
    server_ms: list[float] = []
    codes: dict[int, int] = {}
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(base_url=base, timeout=10, limits=limits) as client:
        await client.get("/")  # warm up

        async def one(i: int) -> None:
            body = {
                "transaction_id": f"txn_load_{run}_{i}",
                "source_account": "acc_user_789",
                "destination_account": "acc_merchant_456",
                "amount": (i % 5000) + 1,
                "currency": "INR",
            }
            async with sem:
                t0 = time.perf_counter()
                try:
                    r = await client.post("/v1/webhooks/transactions", json=body)
                    codes[r.status_code] = codes.get(r.status_code, 0) + 1
                    if "x-process-time-ms" in r.headers:
                        server_ms.append(float(r.headers["x-process-time-ms"]))
                except httpx.HTTPError:
                    codes[0] = codes.get(0, 0) + 1
                client_ms.append((time.perf_counter() - t0) * 1000)

        await asyncio.gather(*(one(i) for i in ids))
    return client_ms, server_ms, codes


def _proc(args: tuple) -> tuple[list[float], list[float], dict]:
    return asyncio.run(_worker(*args))


def _summary(name: str, values: list[float]) -> str:
    if not values:
        return f"{name}: n/a"
    v = sorted(values)
    pct = lambda p: v[max(0, int(len(v) * p) - 1)]
    return f"{name}: p50={statistics.median(v):.0f}ms p95={pct(0.95):.0f}ms p99={pct(0.99):.0f}ms max={v[-1]:.0f}ms"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8000", help="use 127.0.0.1, not localhost, on Windows")
    ap.add_argument("--count", type=int, default=1000)
    ap.add_argument("--concurrency", type=int, default=50, help="total in-flight requests")
    ap.add_argument("--processes", type=int, default=5)
    ap.add_argument("--sla-ms", type=float, default=500)
    args = ap.parse_args()

    run = uuid.uuid4().hex[:8]
    base = args.base.rstrip("/")
    procs = max(1, min(args.processes, args.concurrency))
    per_proc = max(1, args.concurrency // procs)
    chunks = [list(range(i, args.count, procs)) for i in range(procs)]

    t0 = time.perf_counter()
    with mp.Pool(procs) as pool:
        results = pool.map(_proc, [(base, run, c, per_proc) for c in chunks])
    elapsed = time.perf_counter() - t0

    client_ms = [x for r in results for x in r[0]]
    server_ms = [x for r in results for x in r[1]]
    codes: dict[int, int] = {}
    for r in results:
        for k, v in r[2].items():
            codes[k] = codes.get(k, 0) + v

    ok = codes.get(202, 0)
    print(f"run={run} sent={args.count} 202={ok} other={ {k: v for k, v in codes.items() if k != 202} }")
    print(f"throughput={args.count / elapsed:.0f} req/s with {procs} client processes x {per_proc}")
    print(_summary("client latency", client_ms))
    print(_summary("server time   ", server_ms))
    p99 = sorted(client_ms)[max(0, int(len(client_ms) * 0.99) - 1)]
    passed = ok == args.count and p99 < args.sla_ms
    print(f"SLA (<{args.sla_ms:.0f}ms client p99, all 202): {'PASS' if passed else 'FAIL'}")
    print(f"check later:  GET {base}/v1/transactions/txn_load_{run}_0")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
