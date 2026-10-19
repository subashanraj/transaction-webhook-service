import pytest

from tests.conftest import payload

URL = "/v1/webhooks/transactions"


@pytest.mark.parametrize(
    "body",
    [
        {k: v for k, v in payload("txn_missing").items() if k != "destination_account"},  # S8
        payload("txn_type", amount="abc"),  # S9
        payload("txn_strnum", amount="1500"),  # S9: numeric string is not a number
        payload("txn_bool", amount=True),
        payload(12345),  # S9: id must be a string
        payload("txn_zero", amount=0),  # S10
        payload("txn_neg", amount=-50),  # S10
        payload("txn_dp", amount=10.999),  # S11
        payload("txn_cur1", currency="RUPEE"),  # S12
        payload("txn_cur2", currency="IN"),
        payload("txn_cur3", currency="12A"),
        payload("txn_cur4", currency=""),
        payload(""),  # S13
        payload("   "),
        payload("x" * 256),
        payload("txn abc"),
        payload("txn/../x"),
        payload("txn_₹_01"),  # S32
        payload("txn_same", destination_account="acc_user_789"),  # S14
        payload("txn_big", amount=1e20),  # S30
        payload("txn_null", amount=None),
        payload("txn_src_empty", source_account=""),
    ],
)
async def test_invalid_payloads_rejected_and_not_stored(client, db, body):
    r = await client.post(URL, json=body)
    assert r.status_code == 422, r.text
    assert await db.fetchval("SELECT count(*) FROM transactions") == 0


@pytest.mark.parametrize(
    "raw, content_type",
    [
        (b'{"transaction_id": "txn_s15",', "application/json"),  # S15 malformed
        (b"", "application/json"),  # S16 empty
        (b"[1,2,3]", "application/json"),  # not an object
        (b'{"transaction_id":"t","source_account":"a","destination_account":"b","amount":NaN,"currency":"INR"}',
         "application/json"),
        (b"hello", "text/plain"),  # S31
    ],
)
async def test_unparseable_bodies_rejected(client, db, raw, content_type):
    r = await client.post(URL, content=raw, headers={"Content-Type": content_type})
    assert r.status_code == 422, r.text
    assert await db.fetchval("SELECT count(*) FROM transactions") == 0


async def test_oversized_payload_rejected(client, db):  # S18
    body = payload("txn_s18", pad="x" * 100_000)
    r = await client.post(URL, json=body)
    assert r.status_code == 413
    assert await db.fetchval("SELECT count(*) FROM transactions") == 0


async def test_extra_fields_ignored(client, db):  # S17
    r = await client.post(URL, json=payload("txn_s17", event="payment.captured", notes={"k": "v"}))
    assert r.status_code == 202
    assert await db.fetchval("SELECT count(*) FROM transactions WHERE transaction_id='txn_s17'") == 1


async def test_currency_normalised_and_id_trimmed(client, db):  # S12, S13
    r = await client.post(URL, json=payload("  txn_trim  ", currency="inr"))
    assert r.status_code == 202
    assert r.json()["transaction_id"] == "txn_trim"
    row = await db.fetchrow("SELECT currency FROM transactions WHERE transaction_id='txn_trim'")
    assert row["currency"] == "INR"


async def test_unknown_transaction_404(client):  # S20
    r = await client.get("/v1/transactions/txn_does_not_exist")
    assert r.status_code == 404
    assert r.json() == {"detail": "transaction not found"}


async def test_wrong_method_and_unknown_route(client):  # S22, S23
    assert (await client.get(URL)).status_code == 405
    assert (await client.delete("/v1/transactions/x")).status_code == 405
    assert (await client.get("/v2/whatever")).status_code == 404
