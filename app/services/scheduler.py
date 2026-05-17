from __future__ import annotations

import logging
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app import db
from app.config import settings
from app.queue import (
    enqueue_channels,
    enqueue_clips_watermark_batch,
    enqueue_home_clips_poll,
    enqueue_home_videos_poll,
)
from app.services.crawler import CrawlScope

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


async def run_hot_clips_poll() -> None:
    await enqueue_home_clips_poll(triggered_by="scheduler")


async def run_latest_videos_poll() -> None:
    await enqueue_home_videos_poll(triggered_by="scheduler")


async def run_channel_clips_incremental_job() -> None:
    if await db.has_running_job_of_type("incremental"):
        logger.warning("channel_clips_incremental skipped: already running")
        return

    channel_ids = await db.get_active_channel_ids()
    job_id = await db.insert_crawl_job(
        "incremental",
        total_streamers=len(channel_ids),
        triggered_by="scheduler",
    )
    if not channel_ids:
        await db.update_crawl_job(job_id, status="done", total_streamers=0)
        logger.info("channel_clips_incremental skipped: no active streamers")
        return

    await enqueue_clips_watermark_batch(
        channel_ids,
        job_id,
        max_pages=1,
        min_read_count=100,
    )

    uncrawled = await db.get_uncrawled_channel_ids()
    if uncrawled:
        logger.info("uncrawled streamers found, enqueueing initial crawl count=%d", len(uncrawled))
        initial_job_id = await db.insert_crawl_job(
            "initial", total_streamers=len(uncrawled), triggered_by="scheduler"
        )
        await enqueue_channels(
            uncrawled,
            initial_job_id,
            CrawlScope.FULL,
            max_video_pages=settings.default_video_pages,
            max_clip_pages=settings.default_clip_pages,
        )


async def run_weekly_reconciliation() -> None:
    if await db.has_running_job_of_type("full"):
        logger.warning("weekly_reconciliation skipped: already running")
        return

    channel_ids = await db.get_active_channel_ids()
    job_id = await db.insert_crawl_job(
        "full",
        total_streamers=len(channel_ids),
        triggered_by="scheduler",
    )
    if not channel_ids:
        await db.update_crawl_job(job_id, status="done", total_streamers=0)
        logger.info("weekly_reconciliation skipped: no active streamers")
        return

    since = datetime.now() - timedelta(days=30)
    await enqueue_channels(
        channel_ids,
        job_id,
        CrawlScope.FULL,
        since=since,
        max_video_pages=10,
        max_clip_pages=5,
    )


def start_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler and _scheduler.running:
        return _scheduler

    scheduler = AsyncIOScheduler(timezone="Asia/Seoul")
    scheduler.add_job(
        run_hot_clips_poll,
        "interval",
        hours=1,
        id="hot_clips_poll",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        run_latest_videos_poll,
        "interval",
        hours=1,
        id="latest_videos_poll",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        run_channel_clips_incremental_job,
        "interval",
        hours=3,
        id="channel_clips_incremental",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        run_weekly_reconciliation,
        "cron",
        day_of_week="sun",
        hour=3,
        minute=0,
        id="weekly_reconciliation",
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
