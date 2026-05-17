from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from arq.connections import RedisSettings

from app import db
from app.config import settings
from app.notifier import send_alert
from app.services.chzzk_client import chzzk_client
from app.services.crawler import (
    CrawlScope,
    crawl_channel,
    ingest_home_clips,
    ingest_home_videos,
)
from app.services.es_client import es_client

logger = logging.getLogger(__name__)


# ── 잡 완료 처리 ──────────────────────────────────────────────────────────────


async def _finalize_job(job_id: str) -> None:
    row = await db.get_crawl_job(job_id)
    if not row:
        return
    total = row.get("total_streamers") or 0
    success = row.get("success_count") or 0
    failed = row.get("failed_count") or 0
    status = "done" if failed == 0 else "done"  # partial failure도 done으로 처리
    await db.update_crawl_job(job_id, status=status)
    logger.info("job=%s finished success=%d failed=%d", job_id, success, failed)
    if total <= 0:
        return
    failure_rate = failed / max(total, 1)
    if failure_rate >= 0.3:
        await send_alert(
            title="크롤 잡 실패율 임계 초과",
            message=f"job_id={job_id} total={total} success={success} failed={failed} rate={failure_rate:.1%}",
            level="warning",
        )


# ── Worker 함수들 ─────────────────────────────────────────────────────────────


async def channel_crawl(
    ctx: dict[str, Any],
    *,
    channel_id: str,
    job_id: str,
    scope: str,
    since_iso: str | None = None,
    max_video_pages: int | None = None,
    max_clip_pages: int | None = None,
) -> None:
    since = datetime.fromisoformat(since_iso) if since_iso else None
    scope_enum = CrawlScope(scope)

    try:
        if scope_enum == CrawlScope.FULL:
            await crawl_channel(
                channel_id,
                since=since,
                mode="full",
                max_video_pages=max_video_pages,
                max_clip_pages=max_clip_pages,
            )
        elif scope_enum == CrawlScope.STREAMERS_ONLY:
            await crawl_channel(channel_id, mode="streamers_only")
        elif scope_enum == CrawlScope.VIDEOS:
            videos = await chzzk_client.get_videos(
                channel_id, since=since, max_pages=max_video_pages
            )
            await db.upsert_videos(channel_id, videos)
            if videos:
                channel_name = await db.get_channel_name(channel_id)
                await es_client.bulk_index_videos(channel_name, channel_id, videos)
        elif scope_enum == CrawlScope.CLIPS:
            clips = await chzzk_client.get_clips(channel_id, since=since, max_pages=max_clip_pages)
            await db.upsert_clips(channel_id, clips)
            if clips:
                channel_name = await db.get_channel_name(channel_id)
                await es_client.bulk_index_clips(channel_name, channel_id, clips)
        done = await db.increment_job_progress(job_id, success=True)
        logger.info("channel_crawl done channel=%s job=%s", channel_id, job_id)
    except Exception as e:
        logger.error("channel_crawl failed channel=%s job=%s: %s", channel_id, job_id, e)
        done = await db.increment_job_progress(job_id, success=False, failed_channel=channel_id)

    if done:
        await _finalize_job(job_id)


async def channel_clips_watermark(
    ctx: dict[str, Any],
    *,
    channel_id: str,
    job_id: str,
    max_pages: int,
    min_read_count: int,
) -> None:
    try:
        last_seen = await db.get_channel_clip_watermark(channel_id)
        clips = await chzzk_client.get_clips(channel_id, since=last_seen, max_pages=max_pages)
        clips = [c for c in clips if c.read_count >= min_read_count]
        await db.upsert_clips(channel_id, clips)
        if clips:
            channel_name = await db.get_channel_name(channel_id)
            await es_client.bulk_index_clips(channel_name, channel_id, clips)
        done = await db.increment_job_progress(job_id, success=True)
        logger.info("clips_watermark done channel=%s job=%s", channel_id, job_id)
    except Exception as e:
        logger.error("clips_watermark failed channel=%s job=%s: %s", channel_id, job_id, e)
        done = await db.increment_job_progress(job_id, success=False, failed_channel=channel_id)

    if done:
        await _finalize_job(job_id)


async def home_clips_poll(
    ctx: dict[str, Any],
    *,
    triggered_by: str = "scheduler",
) -> None:
    from app.queue import enqueue_channels
    from app.services.chzzk_client import chzzk_client as client

    job_id = await db.insert_crawl_job("hot_clips", total_streamers=0, triggered_by=triggered_by)
    try:
        entries: list = []
        cursor: str | None = None
        for _ in range(3):
            page_entries, cursor = await client.get_home_popular_clips(
                filter_type="WITHIN_1_DAY",
                next_cursor=cursor,
                size=30,
            )
            entries.extend(page_entries)
            if not cursor:
                break
        stats = await ingest_home_clips(entries, min_read_count=100)
        await db.update_crawl_job(
            job_id,
            status="done",
            total_streamers=stats.get("channels_seen", 0),
            success_count=stats.get("clips_upserted", 0),
        )
        logger.info("home_clips_poll done %s", stats)
        new_ids = stats.get("new_channel_ids") or []
        if new_ids:
            initial_job_id = await db.insert_crawl_job(
                "initial", total_streamers=len(new_ids), triggered_by="scheduler"
            )
            await enqueue_channels(
                new_ids,
                initial_job_id,
                CrawlScope.FULL,
                max_video_pages=settings.default_video_pages,
                max_clip_pages=settings.default_clip_pages,
            )
    except Exception as e:
        logger.exception("home_clips_poll failed")
        await db.update_crawl_job(job_id, status="failed", error_msg=str(e))


async def home_videos_poll(
    ctx: dict[str, Any],
    *,
    triggered_by: str = "scheduler",
) -> None:
    from app.queue import enqueue_channels
    from app.services.chzzk_client import chzzk_client as client

    job_id = await db.insert_crawl_job(
        "latest_videos", total_streamers=0, triggered_by=triggered_by
    )
    try:
        entries: list = []
        cursor: tuple[int, int] | None = None
        for _ in range(2):
            page_entries, cursor = await client.get_home_videos(
                sort_type="LATEST",
                cursor_publish_date_at=cursor[0] if cursor else None,
                cursor_read_count=cursor[1] if cursor else None,
                size=30,
            )
            entries.extend(page_entries)
            if not cursor:
                break
        stats = await ingest_home_videos(entries)
        await db.update_crawl_job(
            job_id,
            status="done",
            total_streamers=stats.get("channels_seen", 0),
            success_count=stats.get("videos_upserted", 0),
        )
        logger.info("home_videos_poll done %s", stats)
        new_ids = stats.get("new_channel_ids") or []
        if new_ids:
            initial_job_id = await db.insert_crawl_job(
                "initial", total_streamers=len(new_ids), triggered_by="scheduler"
            )
            await enqueue_channels(
                new_ids,
                initial_job_id,
                CrawlScope.FULL,
                max_video_pages=settings.default_video_pages,
                max_clip_pages=settings.default_clip_pages,
            )
    except Exception as e:
        logger.exception("home_videos_poll failed")
        await db.update_crawl_job(job_id, status="failed", error_msg=str(e))


async def live_crawl(
    ctx: dict[str, Any],
    *,
    job_id: str,
    min_viewers: int,
    since_iso: str | None = None,
    mode: str = "full",
    max_video_pages: int | None = None,
    max_clip_pages: int | None = None,
) -> None:
    from app.services.chzzk_client import LiveCursor

    since = datetime.fromisoformat(since_iso) if since_iso else None
    scope = CrawlScope.FULL if mode == "full" else CrawlScope.STREAMERS_ONLY
    total = 0

    try:
        cursor: LiveCursor | None = None
        for page_num in range(settings.max_live_pages):
            channel_ids, cursor = await chzzk_client.get_live_page(
                min_viewers=min_viewers,
                cursor_viewer_count=cursor["viewer_count"] if cursor else None,
                cursor_live_id=cursor["live_id"] if cursor else None,
            )
            if not channel_ids:
                break
            total += len(channel_ids)
            await db.update_crawl_job(job_id, total_streamers=total)
            logger.info("live page=%d channels=%d", page_num, len(channel_ids))

            from app.queue import enqueue_channels

            await enqueue_channels(
                channel_ids,
                job_id,
                scope,
                since=since,
                max_video_pages=max_video_pages,
                max_clip_pages=max_clip_pages,
            )
            if cursor is None:
                break

        if total == 0:
            await db.update_crawl_job(job_id, status="done", total_streamers=0)
    except Exception as e:
        logger.exception("live_crawl failed job=%s", job_id)
        await db.update_crawl_job(job_id, status="failed", error_msg=str(e))


# ── Worker 설정 ───────────────────────────────────────────────────────────────


async def startup(ctx: dict[str, Any]) -> None:
    from app.log_config import setup_logging

    setup_logging()
    await db.init_pool()
    await chzzk_client.start()
    await es_client.start()
    logger.info("arq worker started")


async def shutdown(ctx: dict[str, Any]) -> None:
    await es_client.stop()
    await chzzk_client.stop()
    await db.close_pool()
    logger.info("arq worker stopped")


class WorkerSettings:
    functions = [
        channel_crawl,
        channel_clips_watermark,
        home_clips_poll,
        home_videos_poll,
        live_crawl,
    ]
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    max_jobs = 3
    on_startup = startup
    on_shutdown = shutdown
