from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, Query
from pydantic import BaseModel

from app.config import settings
from app.services import crawler
from app.services.crawler import CrawlJobType, CrawlMode, CrawlScope

router = APIRouter(prefix="/crawl", tags=["crawl"])

_PAGES_DESC = "양의 정수 = 최대 페이지, 0 = 무제한, 생략 = 기본값"


def _normalize_pages(v: int | None) -> int | None:
    return None if v == 0 else v


class ChannelListRequest(BaseModel):
    channel_ids: list[str]
    since: datetime | None = None
    max_video_pages: int | None = None
    max_clip_pages: int | None = None


@router.post("/channel/{channel_id}")
async def crawl_single_channel(
    channel_id: str,
    since: datetime | None = Query(default=None),
    mode: CrawlMode = Query(default="full"),
    max_video_pages: int = Query(
        default=settings.default_video_pages, ge=0, description=_PAGES_DESC
    ),
    max_clip_pages: int = Query(default=settings.default_clip_pages, ge=0, description=_PAGES_DESC),
):
    effective_since = None if mode == "streamers_only" else since
    return await crawler.crawl_channel(
        channel_id,
        since=effective_since,
        mode=mode,
        max_video_pages=_normalize_pages(max_video_pages),
        max_clip_pages=_normalize_pages(max_clip_pages),
    )


@router.post("/bulk")
async def crawl_bulk(
    request: ChannelListRequest,
    background_tasks: BackgroundTasks,
    scope: CrawlScope = Query(default=CrawlScope.FULL),
):
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


@router.post("/live")
async def crawl_live(
    background_tasks: BackgroundTasks,
    min_viewers: int = Query(default=100, ge=1),
    since: datetime | None = Query(
        default=None,
        description="Video/clip crawl cutoff. None이면 컷오프 없음 (전체 페이지).",
    ),
    mode: CrawlMode = Query(default="full", description="full | streamers_only"),
    max_video_pages: int = Query(
        default=settings.default_video_pages, ge=0, description=_PAGES_DESC
    ),
    max_clip_pages: int = Query(default=settings.default_clip_pages, ge=0, description=_PAGES_DESC),
):
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
    return {"job_id": job["job_id"], "status": "started", "mode": mode}


@router.get("/jobs")
async def list_jobs(limit: int = Query(default=10, ge=1, le=100)):
    return await crawler.get_jobs(limit=limit)


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    return await crawler.get_job(job_id)


@router.post("/jobs/{job_id}/retry")
async def retry_failed_channels(job_id: str, background_tasks: BackgroundTasks):
    job, channel_ids = await crawler.prepare_retry_job(job_id)
    background_tasks.add_task(
        crawler.run_crawl,
        job["job_id"],
        channel_ids,
        scope=CrawlScope.FULL,
        since=None,
    )
    return {**job, "status": "started"}
