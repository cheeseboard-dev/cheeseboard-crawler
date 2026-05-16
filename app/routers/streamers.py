import logging

from fastapi import APIRouter, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app import db
from app.exceptions import StreamerNotFoundException
from app.models.channel import ChannelResponse
from app.services.chzzk_client import chzzk_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/streamers", tags=["streamers"])


class StreamerRegisterRequest(BaseModel):
    channel_id: str


class StreamerUpdateRequest(BaseModel):
    is_active: bool


class StreamerBulkEntry(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    channel_id: str = Field(alias="uuid")
    channel_name: str = Field(alias="name")
    follower_count: int = Field(default=0, alias="followers")

    @field_validator("follower_count", mode="before")
    @classmethod
    def _parse_followers(cls, v: object) -> int:
        if isinstance(v, int):
            return v
        if isinstance(v, str):
            try:
                return int(v.replace(",", "").strip())
            except ValueError:
                return 0
        return 0


@router.post("")
async def register_streamer(request: StreamerRegisterRequest):
    channel = await chzzk_client.get_channel(request.channel_id)
    await db.upsert_streamer(channel)
    return channel


@router.post("/bulk")
async def register_streamers_bulk(entries: list[StreamerBulkEntry]):
    success = 0
    failed_channels: list[str] = []
    for entry in entries:
        try:
            channel = ChannelResponse(
                channel_id=entry.channel_id,
                channel_name=entry.channel_name,
                follower_count=entry.follower_count,
            )
            await db.upsert_streamer(channel)
            success += 1
        except Exception as e:
            logger.error("bulk upsert failed channel_id=%s: %s", entry.channel_id, e)
            failed_channels.append(entry.channel_id)
    return {
        "total": len(entries),
        "success": success,
        "failed": len(failed_channels),
        "failed_channels": failed_channels,
    }


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
