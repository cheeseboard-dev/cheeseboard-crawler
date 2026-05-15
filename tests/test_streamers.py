import pytest

pytestmark = pytest.mark.asyncio


async def test_register_then_list(client):
    r = await client.post("/api/v1/streamers", json={"channel_id": "ch1"})
    assert r.status_code == 200
    assert r.json()["channel_id"] == "ch1"
    r = await client.get("/api/v1/streamers")
    assert r.status_code == 200
    assert any(s["channel_id"] == "ch1" for s in r.json())
