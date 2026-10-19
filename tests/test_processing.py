from datetime import datetime

from tests.conftest import payload, wait_for_status

URL = "/v1/webhooks/transactions"


def parse(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")


async def test_single_transaction_end_to_end(client, db):  # S1, S21
    r = await client.post(URL, json=payload("txn_s1"))
    assert r.status_code == 202
    assert r.json() == {"message": "accepted", "transaction_id": "txn_s1", "duplicate": False}

    r = await client.get("/v1/transactions/txn_s1")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, list) and len(body) == 1
    first = body[0]
    assert first["status"] == "PROCESSING"
    assert first["processed_at"] is None
    assert set(first) == {
        "transaction_id", "source_account", "destination_account",
        "amount", "currency", "status", "created_at", "processed_at",
    }

    row = await wait_for_status(db, "txn_s1", "PROCESSED")
    assert row["attempts"] == 1
    # Processing took at least the configured delay.
    assert (row["processed_at"] - row["created_at"]).total_seconds() >= 0.5

    done = (await client.get("/v1/transactions/txn_s1")).json()[0]
    assert done["status"] == "PROCESSED"
    assert done["amount"] == 1500 and done["currency"] == "INR"
    assert parse(done["processed_at"]) >= parse(done["created_at"])


async def test_decimal_amount_kept_exactly(client, db):  # S7
    r = await client.post(URL, json=payload("txn_s7", amount=150.50, currency="USD"))
    assert r.status_code == 202
    assert str(await db.fetchval("SELECT amount FROM transactions WHERE transaction_id='txn_s7'")) == "150.50"
    out = (await client.get("/v1/transactions/txn_s7")).json()[0]
    assert out["amount"] == 150.5 and out["currency"] == "USD"


async def test_many_distinct_transactions_all_processed(client, db):  # S6
    import asyncio

    rs = await asyncio.gather(*(client.post(URL, json=payload(f"txn_s6_{i}", amount=i + 1)) for i in range(60)))
    assert all(r.status_code == 202 for r in rs)
    for i in range(60):
        await wait_for_status(db, f"txn_s6_{i}", "PROCESSED", timeout=15)
    assert await db.fetchval("SELECT max(attempts) FROM transactions") == 1
