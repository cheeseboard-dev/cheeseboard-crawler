from typing import Literal

from fastapi import APIRouter, Query

from app.services.chzzk_client import chzzk_client

router = APIRouter(prefix="/channels", tags=["channels"])


@router.get("/{channel_id}")
async def get_channel(channel_id: str):
    return await chzzk_client.get_channel(channel_id)


@router.get("/{channel_id}/videos")
async def get_videos(
    channel_id: str,
    size: int = Query(default=30, ge=1, le=100),
    sort: Literal["LATEST", "POPULAR"] = "LATEST",
):
    return await chzzk_client.get_videos(channel_id, size=size, sort_type=sort)


@router.get("/{channel_id}/clips")
async def get_clips(
    channel_id: str,
    size: int = Query(default=50, ge=1, le=100),
    sort: Literal["LATEST", "POPULAR"] = "LATEST",
):
    return await chzzk_client.get_clips(channel_id, size=size, sort_type=sort)
