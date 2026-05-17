from __future__ import annotations

import logging
from datetime import datetime

from arq import ArqRedis, create_pool
from arq.connections import RedisSettings

from app.core.config import settings

logger = logging.getLogger(__name__)

_pool: ArqRedis | None = None


async def get_pool() -> ArqRedis:
    global _pool
    if _pool is None:
        _pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def enqueue_channel_crawl(
    channel_id: str,
    job_id: str,
    scope: str,
    since: datetime | None = None,
    max_video_pages: int | None = None,
    max_clip_pages: int | None = None,
) -> None:
    pool = await get_pool()
    await pool.enqueue_job(
        "channel_crawl",
        channel_id=channel_id,
        job_id=job_id,
        scope=scope,
        since_iso=since.isoformat() if since else None,
        max_video_pages=max_video_pages,
        max_clip_pages=max_clip_pages,
    )


async def enqueue_channels(
    channel_ids: list[str],
    job_id: str,
    scope: str,
    since: datetime | None = None,
    max_video_pages: int | None = None,
    max_clip_pages: int | None = None,
) -> None:
    for channel_id in channel_ids:
        await enqueue_channel_crawl(
            channel_id,
            job_id,
            scope,
            since=since,
            max_video_pages=max_video_pages,
            max_clip_pages=max_clip_pages,
        )
    logger.info(
        "enqueued %d channel crawl jobs job_id=%s scope=%s", len(channel_ids), job_id, scope
    )


async def enqueue_clips_watermark(
    channel_id: str,
    job_id: str,
    max_pages: int,
    min_read_count: int,
) -> None:
    pool = await get_pool()
    await pool.enqueue_job(
        "channel_clips_watermark",
        channel_id=channel_id,
        job_id=job_id,
        max_pages=max_pages,
        min_read_count=min_read_count,
    )


async def enqueue_clips_watermark_batch(
    channel_ids: list[str],
    job_id: str,
    max_pages: int,
    min_read_count: int,
) -> None:
    for channel_id in channel_ids:
        await enqueue_clips_watermark(channel_id, job_id, max_pages, min_read_count)
    logger.info("enqueued %d clips watermark jobs job_id=%s", len(channel_ids), job_id)


async def enqueue_home_clips_poll(triggered_by: str = "scheduler") -> None:
    pool = await get_pool()
    await pool.enqueue_job(
        "home_clips_poll",
        triggered_by=triggered_by,
        _job_id="home_clips_poll_singleton",
    )


async def enqueue_home_videos_poll(triggered_by: str = "scheduler") -> None:
    pool = await get_pool()
    await pool.enqueue_job(
        "home_videos_poll",
        triggered_by=triggered_by,
        _job_id="home_videos_poll_singleton",
    )


async def enqueue_live_crawl(
    job_id: str,
    min_viewers: int,
    since: datetime | None,
    mode: str,
    max_video_pages: int | None,
    max_clip_pages: int | None,
) -> None:
    pool = await get_pool()
    await pool.enqueue_job(
        "live_crawl",
        job_id=job_id,
        min_viewers=min_viewers,
        since_iso=since.isoformat() if since else None,
        mode=mode,
        max_video_pages=max_video_pages,
        max_clip_pages=max_clip_pages,
    )
