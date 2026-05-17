from datetime import datetime

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app import db
from app.config import settings
from app.exceptions import InvalidRequestException
from app.queue import (
    enqueue_channels,
    enqueue_home_clips_poll,
    enqueue_home_videos_poll,
    enqueue_live_crawl,
)
from app.schemas import CrawlChannelResponse, ErrorResponse, JobResponse, JobStartedResponse
from app.services import crawler
from app.services.crawler import CrawlJobType, CrawlMode, CrawlScope

router = APIRouter(prefix="/crawl", tags=["crawl"])

_PAGES_DESC = "양의 정수 = 최대 페이지, 0 = 무제한, 생략 = 기본값"
_ERR_404 = {404: {"model": ErrorResponse, "description": "잡을 찾을 수 없음"}}
_ERR_409 = {409: {"model": ErrorResponse, "description": "같은 타입의 잡이 이미 실행 중"}}


def _normalize_pages(v: int | None) -> int | None:
    return None if v == 0 else v


class ChannelListRequest(BaseModel):
    channel_ids: list[str]
    since: datetime | None = None
    max_video_pages: int | None = None
    max_clip_pages: int | None = None


@router.post(
    "/channel/{channel_id}",
    response_model=CrawlChannelResponse,
    summary="단일 채널 즉시 크롤",
    responses={404: {"model": ErrorResponse, "description": "채널을 찾을 수 없음"}},
)
async def crawl_single_channel(
    channel_id: str,
    since: datetime | None = Query(default=None, description="이 시각 이후 컨텐츠만 수집"),
    mode: CrawlMode = Query(default="full", description="full / streamers_only"),
    max_video_pages: int = Query(
        default=settings.default_video_pages, ge=0, description=_PAGES_DESC
    ),
    max_clip_pages: int = Query(default=settings.default_clip_pages, ge=0, description=_PAGES_DESC),
):
    """단일 채널을 즉시 크롤합니다 (동기, 결과 바로 반환)."""
    effective_since = None if mode == "streamers_only" else since
    return await crawler.crawl_channel(
        channel_id,
        since=effective_since,
        mode=mode,
        max_video_pages=_normalize_pages(max_video_pages),
        max_clip_pages=_normalize_pages(max_clip_pages),
    )


@router.post(
    "/bulk",
    response_model=JobStartedResponse,
    status_code=202,
    summary="다수 채널 큐 크롤",
    responses=_ERR_409,
)
async def crawl_bulk(
    request: ChannelListRequest,
    scope: CrawlScope = Query(
        default=CrawlScope.FULL, description="full / videos / clips / streamers_only"
    ),
):
    """여러 채널을 큐에 등록합니다. 같은 타입의 잡이 실행 중이면 409를 반환합니다."""
    if scope == CrawlScope.VIDEOS:
        job_type: CrawlJobType = "user_videos"
    elif scope == CrawlScope.CLIPS:
        job_type = "user_clips"
    else:
        job_type = "user_bulk"
    job = await crawler.create_job(request.channel_ids, job_type=job_type)
    default_video = None if request.since else settings.default_video_pages
    default_clip = None if request.since else settings.default_clip_pages
    await enqueue_channels(
        request.channel_ids,
        str(job["job_id"]),
        scope,
        since=request.since,
        max_video_pages=_normalize_pages(request.max_video_pages)
        if request.max_video_pages is not None
        else default_video,
        max_clip_pages=_normalize_pages(request.max_clip_pages)
        if request.max_clip_pages is not None
        else default_clip,
    )
    return {**job, "status": "queued"}


@router.post(
    "/live",
    response_model=JobStartedResponse,
    status_code=202,
    summary="라이브 채널 큐 크롤",
    responses=_ERR_409,
)
async def crawl_live(
    min_viewers: int = Query(default=100, ge=1, description="최소 시청자 수 필터"),
    since: datetime | None = Query(default=None, description="이 시각 이후 컨텐츠만 수집"),
    mode: CrawlMode = Query(default="full", description="full / streamers_only"),
    max_video_pages: int = Query(
        default=settings.default_video_pages, ge=0, description=_PAGES_DESC
    ),
    max_clip_pages: int = Query(default=settings.default_clip_pages, ge=0, description=_PAGES_DESC),
):
    """현재 라이브 중인 채널을 큐에 등록합니다."""
    effective_since = None if mode == "streamers_only" else since
    job = await crawler.create_live_job(job_type="user_live")
    await enqueue_live_crawl(
        str(job["job_id"]),
        min_viewers,
        effective_since,
        mode,
        _normalize_pages(max_video_pages),
        _normalize_pages(max_clip_pages),
    )
    return {"job_id": job["job_id"], "status": "queued", "total": 0}


@router.get("/jobs", response_model=list[JobResponse], summary="크롤 잡 목록 조회")
async def list_jobs(limit: int = Query(default=10, ge=1, le=100)):
    return await crawler.get_jobs(limit=limit)


@router.get(
    "/jobs/{job_id}",
    response_model=JobResponse,
    summary="크롤 잡 상태 조회",
    responses=_ERR_404,
)
async def get_job_status(job_id: str):
    return await crawler.get_job(job_id)


@router.post(
    "/jobs/{job_id}/retry",
    response_model=JobStartedResponse,
    status_code=202,
    summary="실패 채널 재시도",
    responses={**_ERR_404, **_ERR_409},
)
async def retry_failed_channels(job_id: str):
    """이전 잡에서 실패한 채널들만 추려 새 잡으로 재크롤합니다."""
    job, channel_ids = await crawler.prepare_retry_job(job_id)
    await enqueue_channels(channel_ids, str(job["job_id"]), CrawlScope.FULL)
    return {**job, "status": "queued"}


@router.post(
    "/pending",
    response_model=JobStartedResponse,
    status_code=202,
    summary="미크롤 스트리머 전체 크롤",
    responses=_ERR_409,
)
async def crawl_pending():
    """is_initial_crawled=False인 활성 스트리머를 큐에 등록합니다."""
    channel_ids = await db.get_uncrawled_channel_ids()
    if not channel_ids:
        raise InvalidRequestException("초기 크롤이 필요한 스트리머가 없습니다.")
    job = await crawler.create_job(channel_ids, job_type="initial", triggered_by="user")
    await enqueue_channels(channel_ids, str(job["job_id"]), CrawlScope.FULL)
    return {**job, "status": "queued"}


@router.post(
    "/trigger/hot-clips",
    status_code=202,
    summary="홈 인기 클립 수집 수동 트리거",
)
async def trigger_hot_clips():
    """home_clips_poll 잡을 큐에 등록합니다. 이미 대기 중이면 무시됩니다."""
    await enqueue_home_clips_poll(triggered_by="user")
    return {"status": "queued"}


@router.post(
    "/trigger/latest-videos",
    status_code=202,
    summary="홈 최신 영상 수집 수동 트리거",
)
async def trigger_latest_videos():
    """home_videos_poll 잡을 큐에 등록합니다. 이미 대기 중이면 무시됩니다."""
    await enqueue_home_videos_poll(triggered_by="user")
    return {"status": "queued"}
