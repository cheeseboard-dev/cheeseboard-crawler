import pytest

pytestmark = pytest.mark.asyncio


async def test_register_then_list(client):
    r = await client.post("/api/v1/streamers", json={"channel_id": "ch1"})
    assert r.status_code == 200
    assert r.json()["channel_id"] == "ch1"
    r = await client.get("/api/v1/streamers")
    assert r.status_code == 200
    assert any(s["channel_id"] == "ch1" for s in r.json())


async def test_bulk_register_softc_format(client):
    payload = [
        {"rank": 1, "uuid": "softc1", "name": "한동숙", "followers": "373,609"},
        {"rank": 2, "uuid": "softc2", "name": "랄로", "followers": "372,891"},
    ]
    r = await client.post("/api/v1/streamers/bulk", json=payload)
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 2
    assert body["success"] == 2
    assert body["failed"] == 0

    r = await client.get("/api/v1/streamers")
    rows = r.json()
    by_id = {s["channel_id"]: s for s in rows}
    assert by_id["softc1"]["channel_name"] == "한동숙"
    assert by_id["softc1"]["follower_count"] == 373609
    assert by_id["softc2"]["follower_count"] == 372891
