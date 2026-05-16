from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, Query
from pydantic import BaseModel

from app.config import settings
from app.schemas import CrawlChannelResponse, ErrorResponse, JobResponse, JobStartedResponse
from app.services import crawler
from app.services.crawler import CrawlJobType, CrawlMode, CrawlScope
from app.services.scheduler import run_hot_clips_poll, run_latest_videos_poll

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
    since: datetime | None = Query(
        default=None, description="이 시각 이후 컨텐츠만 수집 (None이면 전체)"
    ),
    mode: CrawlMode = Query(
        default="full", description="full: 채널+영상+클립 / streamers_only: 채널 정보만"
    ),
    max_video_pages: int = Query(
        default=settings.default_video_pages, ge=0, description=_PAGES_DESC
    ),
    max_clip_pages: int = Query(default=settings.default_clip_pages, ge=0, description=_PAGES_DESC),
):
    """
    단일 채널을 즉시 크롤합니다 (동기, 결과 바로 반환).

    `mode=streamers_only`이면 채널 메타데이터만 수집하고 영상/클립은 건너뜁니다.
    """
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
    summary="다수 채널 백그라운드 크롤",
    responses=_ERR_409,
)
async def crawl_bulk(
    request: ChannelListRequest,
    background_tasks: BackgroundTasks,
    scope: CrawlScope = Query(
        default=CrawlScope.FULL, description="full / videos / clips / streamers_only"
    ),
):
    """
    여러 채널을 백그라운드 잡으로 크롤합니다 (비동기, 즉시 job_id 반환).

    `scope`로 수집 대상을 제한할 수 있습니다. 같은 타입의 잡이 실행 중이면 409를 반환합니다.
    진행 상황은 `GET /crawl/jobs/{job_id}`로 확인합니다.
    """
    if scope == CrawlScope.VIDEOS:
        job_type: CrawlJobType = "user_videos"
    elif scope == CrawlScope.CLIPS:
        job_type = "user_clips"
    else:
        job_type = "user_bulk"
    job = await crawler.create_job(request.channel_ids, job_type=job_type)
    background_tasks.add_task(
        crawler.run_crawl,
        job["job_id"],
        request.channel_ids,
        scope,
        request.since,
        _normalize_pages(request.max_video_pages)
        if request.max_video_pages is not None
        else settings.default_video_pages,
        _normalize_pages(request.max_clip_pages)
        if request.max_clip_pages is not None
        else settings.default_clip_pages,
    )
    return {**job, "status": "started"}


@router.post(
    "/live",
    response_model=JobStartedResponse,
    status_code=202,
    summary="라이브 채널 백그라운드 크롤",
    responses=_ERR_409,
)
async def crawl_live(
    background_tasks: BackgroundTasks,
    min_viewers: int = Query(default=100, ge=1, description="최소 시청자 수 필터"),
    since: datetime | None = Query(
        default=None,
        description="이 시각 이후 컨텐츠만 수집 (None이면 전체)",
    ),
    mode: CrawlMode = Query(default="full", description="full | streamers_only"),
    max_video_pages: int = Query(
        default=settings.default_video_pages, ge=0, description=_PAGES_DESC
    ),
    max_clip_pages: int = Query(default=settings.default_clip_pages, ge=0, description=_PAGES_DESC),
):
    """
    현재 라이브 중인 채널을 백그라운드로 크롤합니다.

    `min_viewers` 이상 시청자가 있는 채널만 대상으로 합니다.
    최대 `max_live_pages` 페이지까지 탐색합니다 (설정값: 200페이지).
    """
    effective_since = None if mode == "streamers_only" else since
    job = await crawler.create_live_job(job_type="user_live")
    background_tasks.add_task(
        crawler.run_live_crawl,
        job["job_id"],
        min_viewers,
        effective_since,
        mode,
        _normalize_pages(max_video_pages),
        _normalize_pages(max_clip_pages),
    )
    return {"job_id": job["job_id"], "status": "started", "total": 0}


@router.get(
    "/jobs",
    response_model=list[JobResponse],
    summary="크롤 잡 목록 조회",
)
async def list_jobs(
    limit: int = Query(default=10, ge=1, le=100, description="반환할 최대 잡 수"),
):
    """최근 크롤 잡 목록을 시작 시각 내림차순으로 반환합니다."""
    return await crawler.get_jobs(limit=limit)


@router.get(
    "/jobs/{job_id}",
    response_model=JobResponse,
    summary="크롤 잡 상태 조회",
    responses=_ERR_404,
)
async def get_job_status(job_id: str):
    """특정 크롤 잡의 현재 상태와 진행률(성공/실패 채널 수)을 반환합니다."""
    return await crawler.get_job(job_id)


@router.post(
    "/jobs/{job_id}/retry",
    response_model=JobStartedResponse,
    status_code=202,
    summary="실패 채널 재시도",
    responses={**_ERR_404, **_ERR_409},
)
async def retry_failed_channels(job_id: str, background_tasks: BackgroundTasks):
    """
    이전 잡에서 실패한 채널들만 추려 새 잡으로 재크롤합니다.

    실패 채널이 없으면 400을 반환합니다.
    """
    job, channel_ids = await crawler.prepare_retry_job(job_id)
    background_tasks.add_task(
        crawler.run_crawl,
        job["job_id"],
        channel_ids,
        scope=CrawlScope.FULL,
        since=None,
    )
    return {**job, "status": "started"}


@router.post(
    "/trigger/hot-clips",
    status_code=202,
    summary="홈 인기 클립 수집 수동 트리거",
)
async def trigger_hot_clips(background_tasks: BackgroundTasks):
    """스케줄러의 `hot_clips_poll` 잡을 즉시 실행합니다. 이미 실행 중이면 건너뜁니다."""
    background_tasks.add_task(run_hot_clips_poll)
    return {"status": "triggered"}


@router.post(
    "/trigger/latest-videos",
    status_code=202,
    summary="홈 최신 영상 수집 수동 트리거",
)
async def trigger_latest_videos(background_tasks: BackgroundTasks):
    """스케줄러의 `latest_videos_poll` 잡을 즉시 실행합니다. 이미 실행 중이면 건너뜁니다."""
    background_tasks.add_task(run_latest_videos_poll)
    return {"status": "triggered"}
