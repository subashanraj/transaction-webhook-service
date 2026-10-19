import asyncio

from tests.conftest import payload, running_app, wait_for_status

URL = "/v1/webhooks/transactions"


async def test_sequential_duplicates_processed_once(client, db):  # S2
    flags = []
    for _ in range(5):
        r = await client.post(URL, json=payload("txn_s2"))
        assert r.status_code == 202
        flags.append(r.json()["duplicate"])
    assert flags == [False, True, True, True, True]

    row = await wait_for_status(db, "txn_s2", "PROCESSED")
    assert row["webhook_count"] == 5
    assert row["attempts"] == 1
    assert len((await client.get("/v1/transactions/txn_s2")).json()) == 1


async def test_concurrent_duplicates_single_row(client, db):  # S3
    rs = await asyncio.gather(*(client.post(URL, json=payload("txn_s3")) for _ in range(30)))
    assert [r.status_code for r in rs] == [202] * 30
    assert sum(not r.json()["duplicate"] for r in rs) == 1

    row = await wait_for_status(db, "txn_s3", "PROCESSED")
    assert await db.fetchval("SELECT count(*) FROM transactions") == 1
    assert row["webhook_count"] == 30
    assert row["attempts"] == 1


async def test_duplicate_after_processed_is_not_reprocessed(client, db):  # S4
    await client.post(URL, json=payload("txn_s4"))
    first = await wait_for_status(db, "txn_s4", "PROCESSED")

    r = await client.post(URL, json=payload("txn_s4"))
    assert r.status_code == 202 and r.json()["duplicate"] is True
    await asyncio.sleep(1.5)  # longer than delay + poll: would have reprocessed by now

    after = await db.fetchrow("SELECT * FROM transactions WHERE transaction_id='txn_s4'")
    assert after["status"] == "PROCESSED"
    assert after["processed_at"] == first["processed_at"]
    assert after["attempts"] == 1


async def test_duplicate_after_failed_stays_failed(db):  # S5
    async with running_app(simulated_failure_rate=1.0) as (client, _):
        await client.post(URL, json=payload("txn_s5"))
        await wait_for_status(db, "txn_s5", "FAILED", timeout=15)
        r = await client.post(URL, json=payload("txn_s5"))
        assert r.status_code == 202 and r.json()["duplicate"] is True
        await asyncio.sleep(1)
        row = await db.fetchrow("SELECT * FROM transactions WHERE transaction_id='txn_s5'")
        assert row["status"] == "FAILED" and row["attempts"] == 3


async def test_same_id_different_payload_keeps_original(client, db):  # S19
    await client.post(URL, json=payload("txn_s19", amount=100))
    r = await client.post(URL, json=payload("txn_s19", amount=9999))
    assert r.status_code == 202 and r.json()["duplicate"] is True

    row = await wait_for_status(db, "txn_s19", "PROCESSED")
    assert str(row["amount"]) == "100.00"
    assert row["payload_mismatch"] is True
    assert row["attempts"] == 1


async def test_conflicting_payloads_in_same_batch(client, db):
    # Sent together, these are committed in one group-commit batch.
    rs = await asyncio.gather(*(client.post(URL, json=payload("txn_batch_mix", amount=a)) for a in (10, 20, 10, 30)))
    assert [r.status_code for r in rs] == [202] * 4
    assert sum(not r.json()["duplicate"] for r in rs) == 1
    row = await db.fetchrow("SELECT * FROM transactions WHERE transaction_id='txn_batch_mix'")
    assert row["webhook_count"] == 4
    assert row["payload_mismatch"] is True


async def test_identical_resend_is_not_flagged_as_mismatch(client, db):
    await client.post(URL, json=payload("txn_same", amount=150.5))
    await client.post(URL, json=payload("txn_same", amount=150.50, currency="inr"))
    row = await db.fetchrow("SELECT payload_mismatch FROM transactions WHERE transaction_id='txn_same'")
    assert row["payload_mismatch"] is False
