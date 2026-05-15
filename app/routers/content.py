from fastapi import APIRouter

from app import db
from app.exceptions import ClipNotFoundException, VideoNotFoundException
from app.services.chzzk_client import chzzk_client

router = APIRouter(tags=["content"])


@router.post("/videos/{video_no}/refresh")
async def refresh_video(video_no: int):
    result = await chzzk_client.get_video(video_no)
    if result is None:
        raise VideoNotFoundException(video_no)
    channel_id, video = result
    await db.upsert_videos(channel_id, [video])
    return video


@router.post("/clips/{clip_uid}/refresh")
async def refresh_clip(clip_uid: str):
    result = await chzzk_client.get_clip(clip_uid)
    if result is None:
        raise ClipNotFoundException(clip_uid)
    channel_id, clip = result
    await db.upsert_clips(channel_id, [clip])
    return clip
