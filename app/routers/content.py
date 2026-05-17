from fastapi import APIRouter

from app import db
from app.core.exceptions import ClipNotFoundException, VideoNotFoundException
from app.models.clip import ClipResponse
from app.models.video import VideoResponse
from app.schemas import ErrorResponse
from app.services.chzzk_client import chzzk_client
from app.services.es_client import es_client

router = APIRouter(tags=["content"])

_ERR_404 = {404: {"model": ErrorResponse, "description": "컨텐츠를 찾을 수 없음"}}


@router.post(
    "/videos/{video_no}/refresh",
    response_model=VideoResponse,
    summary="동영상 정보 갱신",
    responses=_ERR_404,
)
async def refresh_video(video_no: int):
    """CHZZK API에서 최신 동영상 정보를 조회해 DB와 ES 인덱스를 갱신합니다."""
    result = await chzzk_client.get_video(video_no)
    if result is None:
        raise VideoNotFoundException(video_no)
    channel_id, video = result
    await db.upsert_videos(channel_id, [video])
    channel_name = await db.get_channel_name(channel_id)
    await es_client.bulk_index_videos(channel_name, channel_id, [video])
    return video


@router.post(
    "/clips/{clip_uid}/refresh",
    response_model=ClipResponse,
    summary="클립 정보 갱신",
    responses=_ERR_404,
)
async def refresh_clip(clip_uid: str):
    """CHZZK API에서 최신 클립 정보를 조회해 DB와 ES 인덱스를 갱신합니다."""
    result = await chzzk_client.get_clip(clip_uid)
    if result is None:
        raise ClipNotFoundException(clip_uid)
    channel_id, clip = result
    await db.upsert_clips(channel_id, [clip])
    channel_name = await db.get_channel_name(channel_id)
    await es_client.bulk_index_clips(channel_name, channel_id, [clip])
    return clip
