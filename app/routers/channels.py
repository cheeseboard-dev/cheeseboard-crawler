from typing import Literal

from fastapi import APIRouter, Query

from app.models.channel import ChannelResponse
from app.models.clip import ClipResponse
from app.models.video import VideoResponse
from app.schemas import ErrorResponse
from app.services.chzzk_client import chzzk_client

router = APIRouter(prefix="/channels", tags=["channels"])

_ERR_404 = {404: {"model": ErrorResponse, "description": "채널을 찾을 수 없음"}}


@router.get(
    "/{channel_id}",
    response_model=ChannelResponse,
    summary="채널 정보 조회",
    responses=_ERR_404,
)
async def get_channel(channel_id: str):
    """CHZZK API에서 채널 정보를 실시간으로 조회합니다."""
    return await chzzk_client.get_channel(channel_id)


@router.get(
    "/{channel_id}/videos",
    response_model=list[VideoResponse],
    summary="채널 동영상 목록 조회",
    responses=_ERR_404,
)
async def get_videos(
    channel_id: str,
    size: int = Query(default=30, ge=1, le=100, description="페이지당 항목 수"),
    sort: Literal["LATEST", "POPULAR"] = Query(default="LATEST", description="정렬 기준"),
):
    """CHZZK API에서 채널의 동영상 목록을 조회합니다. DB가 아닌 원본 API를 직접 호출합니다."""
    return await chzzk_client.get_videos(channel_id, size=size, sort_type=sort)


@router.get(
    "/{channel_id}/clips",
    response_model=list[ClipResponse],
    summary="채널 클립 목록 조회",
    responses=_ERR_404,
)
async def get_clips(
    channel_id: str,
    size: int = Query(default=50, ge=1, le=100, description="페이지당 항목 수"),
    sort: Literal["LATEST", "POPULAR"] = Query(default="LATEST", description="정렬 기준"),
):
    """CHZZK API에서 채널의 클립 목록을 조회합니다. DB가 아닌 원본 API를 직접 호출합니다."""
    return await chzzk_client.get_clips(channel_id, size=size, sort_type=sort)
