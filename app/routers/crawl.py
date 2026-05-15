from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, Query
from pydantic import BaseModel

from app.services import crawler
from app.services.crawler import CrawlMode

router = APIRouter(prefix="/crawl", tags=["crawl"])


class ChannelListRequest(BaseModel):
    channel_ids: list[str]
    since: datetime | None = None


@router.post("/channel/{channel_id}")
async def crawl_single_channel(
    channel_id: str,
    since: datetime | None = Query(default=None),
    mode: CrawlMode = Query(default="full"),
):
    effective_since = None if mode == "streamers_only" else since
    return await crawler.crawl_channel(channel_id, since=effective_since, mode=mode)


@router.post("/bulk")
async def crawl_bulk(request: ChannelListRequest, background_tasks: BackgroundTasks):
    job = await crawler.create_job(request.channel_ids, job_type="user_bulk")
    background_tasks.add_task(
        crawler.run_bulk_crawl,
        job["job_id"],
        request.channel_ids,
        request.since,
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
):
    effective_since = None if mode == "streamers_only" else since
    job = await crawler.create_live_job(job_type="user_live")
    background_tasks.add_task(
        crawler.run_live_crawl,
        job["job_id"],
        min_viewers,
        effective_since,
        mode,
    )
    return {"job_id": job["job_id"], "status": "started", "mode": mode}


@router.post("/videos")
async def crawl_videos(request: ChannelListRequest, background_tasks: BackgroundTasks):
    job = await crawler.create_job(request.channel_ids, job_type="user_videos")
    background_tasks.add_task(
        crawler.run_videos_crawl,
        job["job_id"],
        request.channel_ids,
        request.since,
    )
    return {**job, "status": "started"}


@router.post("/clips")
async def crawl_clips(request: ChannelListRequest, background_tasks: BackgroundTasks):
    job = await crawler.create_job(request.channel_ids, job_type="user_clips")
    background_tasks.add_task(
        crawler.run_clips_crawl,
        job["job_id"],
        request.channel_ids,
        request.since,
    )
    return {**job, "status": "started"}


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
        crawler.run_bulk_crawl,
        job["job_id"],
        channel_ids,
        None,
    )
    return {**job, "status": "started"}
