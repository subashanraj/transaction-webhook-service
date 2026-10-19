import asyncio
import hashlib
import hmac
import json
import time

from tests.conftest import payload, running_app, wait_for_status

URL = "/v1/webhooks/transactions"


async def test_database_down_returns_503_fast(db):  # S25
    async with running_app(database_url="postgresql://postgres@127.0.0.1:1/nope", db_write_timeout_ms=400) as (c, _):
        assert (await c.get("/")).status_code == 200  # liveness unaffected
        assert (await c.get("/health/ready")).status_code == 503
        t0 = time.perf_counter()
        r = await c.post(URL, json=payload("txn_s25"))
        assert r.status_code == 503
        assert time.perf_counter() - t0 < 0.5
    # Once the DB is back, the processor's retry is accepted as a NEW transaction.
    async with running_app() as (c, _):
        r = await c.post(URL, json=payload("txn_s25"))
        assert r.status_code == 202 and r.json()["duplicate"] is False
        await wait_for_status(db, "txn_s25", "PROCESSED")


async def test_crashed_worker_lease_expires_and_job_recovers(db):  # S26
    # Simulate a worker that claimed the job and then died: lease already expired.
    await db.execute(
        """INSERT INTO transactions (transaction_id, source_account, destination_account, amount,
                                     currency, payload_hash, attempts, locked_by, locked_until)
           VALUES ('txn_s26','acc_a','acc_b',10,'INR','h',1,'dead-worker', now() - interval '1 second')"""
    )
    # And one whose lease is still valid must NOT be stolen.
    await db.execute(
        """INSERT INTO transactions (transaction_id, source_account, destination_account, amount,
                                     currency, payload_hash, attempts, locked_by, locked_until)
           VALUES ('txn_s26_live','acc_a','acc_b',10,'INR','h',1,'live-worker', now() + interval '1 hour')"""
    )
    async with running_app():
        row = await wait_for_status(db, "txn_s26", "PROCESSED")
        assert row["attempts"] == 2
        live = await db.fetchrow("SELECT * FROM transactions WHERE transaction_id='txn_s26_live'")
        assert live["status"] == "PROCESSING" and live["locked_by"] == "live-worker"


async def test_failures_retry_then_mark_failed(db):  # S27
    async with running_app(simulated_failure_rate=1.0, max_attempts=3) as (c, _):
        await c.post(URL, json=payload("txn_s27"))
        row = await wait_for_status(db, "txn_s27", "FAILED", timeout=15)
        assert row["attempts"] == 3
        assert "simulated external API failure" in row["last_error"]
        assert row["processed_at"] is None
        out = (await c.get("/v1/transactions/txn_s27")).json()[0]
        assert out["status"] == "FAILED"


async def test_transient_failures_eventually_succeed(db):
    async with running_app(simulated_failure_rate=0.5, max_attempts=10) as (c, _):
        await asyncio.gather(*(c.post(URL, json=payload(f"txn_flaky_{i}")) for i in range(20)))
        for i in range(20):
            await wait_for_status(db, f"txn_flaky_{i}", "PROCESSED", timeout=20)


async def test_graceful_shutdown_releases_leases(db):  # S29
    async with running_app(processing_delay_seconds=5, lease_seconds=60) as (c, app):
        for i in range(5):
            await c.post(URL, json=payload(f"txn_s29_{i}"))
        for _ in range(100):
            if app.state.worker.in_flight == 5:
                break
            await asyncio.sleep(0.05)
        assert app.state.worker.in_flight == 5
    rows = await db.fetch("SELECT * FROM transactions WHERE transaction_id LIKE 'txn_s29_%'")
    assert len(rows) == 5
    for r in rows:
        assert r["status"] == "PROCESSING"
        assert r["locked_by"] is None and r["locked_until"] is None
        assert r["attempts"] == 0  # interrupted run not counted
    # A fresh instance picks them up straight away (no waiting for lease expiry).
    async with running_app():
        for i in range(5):
            row = await wait_for_status(db, f"txn_s29_{i}", "PROCESSED")
            assert row["attempts"] == 1


async def test_two_instances_never_double_process(db):  # S34
    async with running_app() as (c1, a1), running_app() as (c2, a2):
        await asyncio.gather(
            *(c1.post(URL, json=payload(f"txn_s34_{i}")) for i in range(0, 40, 2)),
            *(c2.post(URL, json=payload(f"txn_s34_{i}")) for i in range(1, 40, 2)),
            # and duplicates sent to the *other* instance
            *(c2.post(URL, json=payload(f"txn_s34_{i}")) for i in range(0, 40, 2)),
        )
        for i in range(40):
            await wait_for_status(db, f"txn_s34_{i}", "PROCESSED", timeout=15)
    assert await db.fetchval("SELECT count(*) FROM transactions") == 40
    assert await db.fetchval("SELECT max(attempts) FROM transactions") == 1


async def test_signature_verification(db):  # S24
    secret = "mysecret"
    async with running_app(webhook_secret=secret) as (c, _):
        body = json.dumps(payload("txn_s24")).encode()
        sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        headers = {"Content-Type": "application/json"}

        r = await c.post(URL, content=body, headers={**headers, "X-Razorpay-Signature": "deadbeef"})
        assert r.status_code == 401
        r = await c.post(URL, content=body, headers=headers)
        assert r.status_code == 401
        assert await db.fetchval("SELECT count(*) FROM transactions") == 0

        r = await c.post(URL, content=body, headers={**headers, "X-Razorpay-Signature": sig})
        assert r.status_code == 202
