import pytest

pytestmark = pytest.mark.asyncio


async def test_jobs_list_empty(client):
    r = await client.get("/api/v1/crawl/jobs")
    assert r.status_code == 200
    assert r.json() == []


async def test_crawl_channel_sync(client):
    await client.post("/api/v1/streamers", json={"channel_id": "ch1"})
    r = await client.post("/api/v1/crawl/channel/ch1")
    assert r.status_code == 200
