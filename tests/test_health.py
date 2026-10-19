import re

ISO_Z = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


async def test_health_is_healthy_with_utc_time(client):  # S0, S35
    r = await client.get("/")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "HEALTHY"
    assert ISO_Z.match(body["current_time"])


async def test_readiness_checks_database(client):
    r = await client.get("/health/ready")
    assert r.status_code == 200
    assert r.json()["status"] == "HEALTHY"
