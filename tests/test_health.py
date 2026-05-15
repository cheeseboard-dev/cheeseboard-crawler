import pytest

pytestmark = pytest.mark.asyncio


async def test_root_health(client):
    r = await client.get("/")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
