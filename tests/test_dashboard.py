import asyncio

from tests.conftest import payload, running_app, wait_for_status

URL = "/v1/webhooks/transactions"


async def test_dashboard_page_served(client):
    r = await client.get("/dashboard")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert "Transaction Webhook Service" in r.text


async def test_how_it_works_page_served(client):
    r = await client.get("/how-it-works")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert "Run it for real" in r.text
    assert 'href="/how-it-works"' in (await client.get("/dashboard")).text


async def test_root_is_still_plain_health_json(client):
    r = await client.get("/")
    assert r.headers["content-type"].startswith("application/json")
    assert set(r.json()) == {"status", "current_time"}


async def test_list_newest_first_with_internals(client, db):
    for i in range(3):
        await client.post(URL, json=payload(f"txn_list_{i}"))
        await asyncio.sleep(1.1)  # created_at has second precision in the API
    await client.post(URL, json=payload("txn_list_2"))  # duplicate

    rows = (await client.get("/v1/transactions", params={"limit": 2})).json()
    assert [r["transaction_id"] for r in rows] == ["txn_list_2", "txn_list_1"]
    assert rows[0]["webhook_count"] == 2
    assert {"attempts", "payload_mismatch", "last_error", "started_at"} <= set(rows[0])


async def test_list_filter_and_started_at(db):
    async with running_app(processing_delay_seconds=3, lease_seconds=30) as (c, app):
        await c.post(URL, json=payload("txn_f_1"))
        for _ in range(50):
            if app.state.worker.in_flight:
                break
            await asyncio.sleep(0.05)
        rows = (await c.get("/v1/transactions", params={"status": "PROCESSING"})).json()
        assert [r["transaction_id"] for r in rows] == ["txn_f_1"]
        assert rows[0]["started_at"] is not None  # claimed: progress can be shown
        assert (await c.get("/v1/transactions", params={"status": "PROCESSED"})).json() == []


async def test_list_rejects_bad_params(client):
    assert (await client.get("/v1/transactions", params={"limit": 0})).status_code == 422
    assert (await client.get("/v1/transactions", params={"limit": 500})).status_code == 422
    assert (await client.get("/v1/transactions", params={"status": "DONE"})).status_code == 422


async def test_stats_counts(client, db):
    assert (await client.get("/v1/stats")).json()["counts"] == {"PROCESSING": 0, "PROCESSED": 0, "FAILED": 0}
    await client.post(URL, json=payload("txn_stats"))
    await wait_for_status(db, "txn_stats", "PROCESSED")
    s = (await client.get("/v1/stats")).json()
    assert s["counts"]["PROCESSED"] == 1
    assert s["oldest_pending_at"] is None
    assert s["processing_delay_seconds"] == 0.5
    assert s["server_time"].endswith("Z")


async def test_swagger_has_example_body(client):
    spec = (await client.get("/openapi.json")).json()
    example = spec["paths"]["/v1/webhooks/transactions"]["post"]["requestBody"]["content"]["application/json"]["example"]
    assert example["transaction_id"] == "txn_abc123def456"
    assert "/dashboard" not in spec["paths"]
    assert "/how-it-works" not in spec["paths"]
