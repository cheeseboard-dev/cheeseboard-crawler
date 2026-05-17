import csv
import io
import logging

from fastapi import APIRouter, Query, UploadFile
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app import db
from app.core.exceptions import StreamerNotFoundException
from app.models.channel import ChannelResponse
from app.queue import enqueue_channels
from app.schemas import (
    BulkRegisterResponse,
    ErrorResponse,
    JobStartedResponse,
    StreamerActiveResponse,
    StreamerRow,
    StreamerStats,
)
from app.services import crawler
from app.services.chzzk_client import chzzk_client
from app.services.crawler import CrawlScope

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/streamers", tags=["streamers"])

_ERR_404 = {404: {"model": ErrorResponse, "description": "스트리머를 찾을 수 없음"}}


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


@router.post(
    "",
    response_model=ChannelResponse,
    status_code=201,
    summary="스트리머 등록",
    responses={404: {"model": ErrorResponse, "description": "CHZZK에서 채널을 찾을 수 없음"}},
)
async def register_streamer(request: StreamerRegisterRequest):
    """CHZZK API에서 채널 정보를 조회한 뒤 DB에 스트리머로 등록합니다."""
    channel = await chzzk_client.get_channel(request.channel_id)
    await db.upsert_streamer(channel)
    return channel


@router.post(
    "/bulk",
    response_model=BulkRegisterResponse,
    status_code=201,
    summary="스트리머 일괄 등록",
)
async def register_streamers_bulk(entries: list[StreamerBulkEntry]):
    """
    CSV에서 추출한 스트리머 목록을 일괄 등록합니다.

    CHZZK API 호출 없이 전달된 정보를 그대로 저장합니다.
    각 항목의 필드명은 CSV 컬럼명(uuid, name, followers)을 그대로 사용합니다.
    """
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


@router.get(
    "",
    response_model=list[StreamerRow],
    summary="스트리머 목록 조회",
)
async def list_streamers(
    active_only: bool = Query(
        default=False, description="true이면 is_active=true인 스트리머만 반환"
    ),
):
    """DB에 등록된 스트리머 목록을 반환합니다."""
    return await db.get_streamers(active_only=active_only)


@router.get(
    "/{channel_id}/stats",
    response_model=StreamerStats,
    summary="스트리머 통계 조회",
    responses=_ERR_404,
)
async def get_streamer_stats(channel_id: str):
    """스트리머의 수집된 동영상/클립 수, 최신 컨텐츠 날짜 등 통계를 반환합니다."""
    stats = await db.get_streamer_stats(channel_id)
    if stats is None:
        raise StreamerNotFoundException(channel_id)
    return stats


@router.post(
    "/{channel_id}/refresh",
    response_model=ChannelResponse,
    summary="스트리머 정보 갱신",
    responses=_ERR_404,
)
async def refresh_streamer(channel_id: str):
    """CHZZK API에서 최신 채널 정보를 조회해 DB를 갱신합니다."""
    if not await db.streamer_exists(channel_id):
        raise StreamerNotFoundException(channel_id)
    channel = await chzzk_client.get_channel(channel_id)
    await db.upsert_streamer(channel)
    return channel


@router.patch(
    "/{channel_id}",
    response_model=StreamerActiveResponse,
    summary="스트리머 활성 상태 변경",
    responses=_ERR_404,
)
async def update_streamer(channel_id: str, request: StreamerUpdateRequest):
    """
    스트리머의 `is_active` 값을 변경합니다.

    `is_active=true`인 스트리머만 정기 스케줄러(증분 크롤, 주간 재조정)에 포함됩니다.
    """
    updated = await db.set_streamer_active(channel_id, request.is_active)
    if not updated:
        raise StreamerNotFoundException(channel_id)
    return {"channel_id": channel_id, "is_active": request.is_active}


@router.post(
    "/import-csv",
    response_model=JobStartedResponse,
    status_code=202,
    summary="CSV 파일로 스트리머 일괄 등록 (CHZZK 크롤)",
)
async def import_streamers_csv(file: UploadFile):
    """
    CSV에서 `channel_id`(UUID)만 읽어 CHZZK API에서 채널 정보를 직접 크롤합니다.

    헤더 컬럼명이 `channel_id`이거나 첫 번째 컬럼을 UUID로 사용합니다.
    처리는 백그라운드로 실행되며 즉시 `job_id`를 반환합니다.
    진행 상황은 `GET /crawl/jobs/{job_id}`로 확인합니다.
    """
    content = await file.read()
    text = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))

    channel_ids: list[str] = []
    for row in reader:
        cid = (row.get("channel_id") or next(iter(row.values()), "") or "").strip()
        if cid:
            channel_ids.append(cid)

    job = await crawler.create_job(channel_ids, job_type="initial", triggered_by="user")
    await enqueue_channels(channel_ids, str(job["job_id"]), CrawlScope.FULL)
    return {**job, "status": "queued"}
