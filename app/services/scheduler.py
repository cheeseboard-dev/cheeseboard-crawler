from __future__ import annotations

import logging
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app import db
from app.services import crawler

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


async def _run_scheduled_crawl(job_type: crawler.CrawlJobType) -> None:
    if await db.has_running_job_of_type(job_type):
        logger.warning("scheduled %s crawl skipped: job already running", job_type)
        return

    channel_ids = await db.get_active_channel_ids()
    job_id = await db.insert_crawl_job(
        job_type,
        total_streamers=len(channel_ids),
        triggered_by="scheduler",
    )

    if not channel_ids:
        await db.update_crawl_job(job_id, status="done", total_streamers=0)
        logger.info("scheduled %s crawl skipped: no active streamers", job_type)
        return

    if job_type == "incremental":
        await crawler.run_crawl(
            job_id,
            channel_ids,
            scope=crawler.CrawlScope.FULL,
            since=datetime.now() - timedelta(hours=3),
            max_video_pages=3,
            max_clip_pages=3,
        )
    else:
        await crawler.run_crawl(
            job_id,
            channel_ids,
            scope=crawler.CrawlScope.FULL,
            since=None,
            max_video_pages=None,
            max_clip_pages=None,
        )


async def run_incremental_crawl() -> None:
    await _run_scheduled_crawl("incremental")


async def run_full_crawl() -> None:
    await _run_scheduled_crawl("full")


def start_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler and _scheduler.running:
        return _scheduler

    scheduler = AsyncIOScheduler(timezone="Asia/Seoul")
    scheduler.add_job(
        run_incremental_crawl,
        "interval",
        hours=3,
        id="incremental_crawl",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        run_full_crawl,
        "cron",
        day_of_week="sun",
        hour=3,
        minute=0,
        id="weekly_full_crawl",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    _scheduler = scheduler
    logger.info("crawl scheduler started")
    return scheduler


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("crawl scheduler stopped")
    _scheduler = None
