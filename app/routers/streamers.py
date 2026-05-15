from fastapi import APIRouter, Query
from pydantic import BaseModel

from app import db
from app.exceptions import StreamerNotFoundException
from app.services.chzzk_client import chzzk_client

router = APIRouter(prefix="/streamers", tags=["streamers"])


class StreamerRegisterRequest(BaseModel):
    channel_id: str


class StreamerUpdateRequest(BaseModel):
    is_active: bool


@router.post("")
async def register_streamer(request: StreamerRegisterRequest):
    channel = await chzzk_client.get_channel(request.channel_id)
    await db.upsert_streamer(channel)
    return channel


@router.get("")
async def list_streamers(active_only: bool = Query(default=False)):
    return await db.get_streamers(active_only=active_only)


@router.get("/{channel_id}/stats")
async def get_streamer_stats(channel_id: str):
    stats = await db.get_streamer_stats(channel_id)
    if stats is None:
        raise StreamerNotFoundException(channel_id)
    return stats


@router.post("/{channel_id}/refresh")
async def refresh_streamer(channel_id: str):
    if not await db.streamer_exists(channel_id):
        raise StreamerNotFoundException(channel_id)
    channel = await chzzk_client.get_channel(channel_id)
    await db.upsert_streamer(channel)
    return channel


@router.patch("/{channel_id}")
async def update_streamer(channel_id: str, request: StreamerUpdateRequest):
    updated = await db.set_streamer_active(channel_id, request.is_active)
    if not updated:
        raise StreamerNotFoundException(channel_id)
    return {"channel_id": channel_id, "is_active": request.is_active}
