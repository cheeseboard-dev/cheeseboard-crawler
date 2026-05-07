from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel

from app.config import settings
from app.services import crawler

router = APIRouter(prefix="/crawl", tags=["crawl"])


class BulkCrawlRequest(BaseModel):
    channel_ids: Optional[List[str]] = None
    use_csv: bool = False


@router.post("/channel/{channel_id}")
async def crawl_single_channel(channel_id: str):
    return await crawler.crawl_channel(channel_id)


@router.post("/bulk")
async def crawl_bulk(request: BulkCrawlRequest, background_tasks: BackgroundTasks):
    channel_ids = list(request.channel_ids or [])
    if request.use_csv:
        csv_ids = crawler.load_channel_ids_from_csv(settings.streamers_csv_path)
        channel_ids = list(set(channel_ids + csv_ids))

    job = crawler.create_job(channel_ids)
    background_tasks.add_task(crawler.run_bulk_crawl, job.job_id, channel_ids)
    return {"job_id": job.job_id, "total": len(channel_ids), "status": "started"}


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    return crawler.get_job(job_id).to_dict()
