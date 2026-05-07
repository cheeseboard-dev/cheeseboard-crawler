from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Query
from pydantic import BaseModel

from app.services import crawler

router = APIRouter(prefix="/crawl", tags=["crawl"])


class BulkCrawlRequest(BaseModel):
    channel_ids: List[str]


@router.post("/channel/{channel_id}")
async def crawl_single_channel(channel_id: str):
    return await crawler.crawl_channel(channel_id)


@router.post("/bulk")
async def crawl_bulk(request: BulkCrawlRequest, background_tasks: BackgroundTasks):
    job = crawler.create_job(request.channel_ids)
    background_tasks.add_task(crawler.run_bulk_crawl, job.job_id, request.channel_ids)
    return {"job_id": job.job_id, "total": len(request.channel_ids), "status": "started"}


@router.post("/live")
async def crawl_live(
    background_tasks: BackgroundTasks,
    min_viewers: int = Query(default=100, ge=1),
):
    job = crawler.create_live_job()
    background_tasks.add_task(crawler.run_live_crawl, job.job_id, min_viewers)
    return {"job_id": job.job_id, "status": "started"}


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    return crawler.get_job(job_id).to_dict()
