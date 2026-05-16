from __future__ import annotations

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app import db
from app.services import crawler
from app.services.chzzk_client import chzzk_client
from app.services.crawler import CrawlScope

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


async def _crawl_new_streamers(channel_ids: list[str]) -> None:
    """홈 피드에서 새로 발견된 스트리머의 전체 컨텐츠를 크롤합니다."""
    logger.info("new streamers discovered, starting initial crawl count=%d", len(channel_ids))
    job_id = await db.insert_crawl_job(
        "initial", total_streamers=len(channel_ids), triggered_by="scheduler"
    )
    await crawler.run_crawl(job_id, channel_ids, scope=CrawlScope.FULL)


async def run_hot_clips_poll(triggered_by: str = "scheduler") -> None:
    if await db.has_running_job_of_type("hot_clips"):
        logger.warning("hot_clips_poll skipped: already running")
        return

    job_id = await db.insert_crawl_job("hot_clips", total_streamers=0, triggered_by=triggered_by)
    try:
        entries: list = []
        cursor: str | None = None
        for _ in range(3):
            page_entries, cursor = await chzzk_client.get_home_popular_clips(
                filter_type="WITHIN_1_DAY",
                next_cursor=cursor,
                size=30,
            )
            entries.extend(page_entries)
            if not cursor:
                break
        stats = await crawler.ingest_home_clips(entries, min_read_count=100)
        await db.update_crawl_job(
            job_id,
            status="done",
            total_streamers=stats.get("channels_seen", 0),
            success_count=stats.get("clips_upserted", 0),
        )
        logger.info("hot_clips_poll done %s", stats)
        new_ids = stats.get("new_channel_ids") or []
        if new_ids:
            asyncio.create_task(_crawl_new_streamers(list(new_ids)))
    except Exception as e:
        logger.exception("hot_clips_poll failed")
        await db.update_crawl_job(job_id, status="failed", error_msg=str(e))


async def run_latest_videos_poll(triggered_by: str = "scheduler") -> None:
    if await db.has_running_job_of_type("latest_videos"):
        logger.warning("latest_videos_poll skipped: already running")
        return

    job_id = await db.insert_crawl_job(
        "latest_videos",
        total_streamers=0,
        triggered_by=triggered_by,
    )
    try:
        entries: list = []
        cursor: tuple[int, int] | None = None
        for _ in range(2):
            page_entries, cursor = await chzzk_client.get_home_videos(
                sort_type="LATEST",
                cursor_publish_date_at=cursor[0] if cursor else None,
                cursor_read_count=cursor[1] if cursor else None,
                size=30,
            )
            entries.extend(page_entries)
            if not cursor:
                break
        stats = await crawler.ingest_home_videos(entries)
        await db.update_crawl_job(
            job_id,
            status="done",
            total_streamers=stats.get("channels_seen", 0),
            success_count=stats.get("videos_upserted", 0),
        )
        logger.info("latest_videos_poll done %s", stats)
        new_ids = stats.get("new_channel_ids") or []
        if new_ids:
            asyncio.create_task(_crawl_new_streamers(list(new_ids)))
    except Exception as e:
        logger.exception("latest_videos_poll failed")
        await db.update_crawl_job(job_id, status="failed", error_msg=str(e))


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

    await crawler.run_channel_clips_incremental(
        job_id,
        channel_ids,
        max_pages=1,
        min_read_count=100,
    )

    uncrawled = await db.get_uncrawled_channel_ids()
    if uncrawled:
        logger.info("uncrawled streamers found, scheduling initial crawl count=%d", len(uncrawled))
        asyncio.create_task(_crawl_new_streamers(uncrawled))


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

    await crawler.run_crawl(
        job_id,
        channel_ids,
        scope=crawler.CrawlScope.FULL,
        since=None,
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
