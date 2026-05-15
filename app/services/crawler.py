from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from app import db
from app.config import settings
from app.exceptions import (
    CrawlJobConflictException,
    CrawlJobNotFoundException,
    InvalidRequestException,
)
from app.models.channel import ChannelInfo
from app.models.clip import Clip
from app.models.video import Video
from app.services.chzzk_client import LiveCursor, chzzk_client

logger = logging.getLogger(__name__)

CrawlMode = Literal["full", "streamers_only"]
CrawlJobType = Literal[
    "initial",
    "incremental",
    "full",
    "user_triggered",
    "user_bulk",
    "user_videos",
    "user_clips",
    "user_live",
    "user_retry",
]
TriggeredBy = Literal["scheduler", "user", "admin"]


@dataclass
class CrawlJobProgress:
    job_id: str
    total: int
    success_count: int = 0
    failed_count: int = 0
    failed_channels: list[str] = field(default_factory=list)


class ChannelCrawlResult:
    def __init__(self, channel: ChannelInfo, videos: list[Video], clips: list[Clip]) -> None:
        self.channel = channel
        self.videos = videos
        self.clips = clips
        self.crawled_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def to_dict(self) -> dict[str, object]:
        return {
            "channel": self.channel.model_dump(),
            "videos": [v.model_dump() for v in self.videos],
            "clips": [c.model_dump() for c in self.clips],
            "crawled_at": self.crawled_at,
        }


async def crawl_channel(
    channel_id: str,
    since: datetime | None = None,
    mode: CrawlMode = "full",
    max_video_pages: int | None = settings.default_video_pages,
    max_clip_pages: int | None = settings.default_clip_pages,
) -> ChannelCrawlResult:
    videos: list[Video] = []
    clips: list[Clip] = []

    if mode == "streamers_only":
        channel = await chzzk_client.get_channel(channel_id)
    else:
        channel, videos, clips = await asyncio.gather(
            chzzk_client.get_channel(channel_id),
            chzzk_client.get_videos(channel_id, since=since, max_pages=max_video_pages),
            chzzk_client.get_clips(channel_id, since=since, max_pages=max_clip_pages),
        )

    await db.upsert_streamer(channel)
    if videos:
        v_count = await db.upsert_videos(channel_id, videos)
        logger.info("upserted channel=%s videos=%d", channel_id, v_count)
    if clips:
        c_count = await db.upsert_clips(channel_id, clips)
        logger.info("upserted channel=%s clips=%d", channel_id, c_count)

    return ChannelCrawlResult(channel=channel, videos=videos, clips=clips)


async def _sync_job_progress(progress: CrawlJobProgress) -> None:
    await db.update_crawl_job(
        progress.job_id,
        total_streamers=progress.total,
        success_count=progress.success_count,
        failed_count=progress.failed_count,
        failed_channels=progress.failed_channels,
    )


async def _crawl_channel_safe(
    progress: CrawlJobProgress,
    channel_id: str,
    since: datetime | None = None,
    mode: CrawlMode = "full",
    max_video_pages: int | None = settings.default_video_pages,
    max_clip_pages: int | None = settings.default_clip_pages,
) -> None:
    try:
        await crawl_channel(
            channel_id,
            since=since,
            mode=mode,
            max_video_pages=max_video_pages,
            max_clip_pages=max_clip_pages,
        )
        progress.success_count += 1
    except Exception as e:
        logger.error("channel crawl failed channel=%s: %s", channel_id, e)
        progress.failed_count += 1
        progress.failed_channels.append(channel_id)
    await _sync_job_progress(progress)


async def _finish_job(progress: CrawlJobProgress, error_msg: str | None = None) -> None:
    await db.update_crawl_job(
        progress.job_id,
        status="failed" if error_msg else "done",
        total_streamers=progress.total,
        success_count=progress.success_count,
        failed_count=progress.failed_count,
        error_msg=error_msg,
        failed_channels=progress.failed_channels,
    )
    logger.info(
        "job=%s finished status=%s success=%d failed=%d",
        progress.job_id,
        "failed" if error_msg else "done",
        progress.success_count,
        progress.failed_count,
    )


async def run_bulk_crawl(
    job_id: str,
    channel_ids: list[str],
    since: datetime | None = None,
    mode: CrawlMode = "full",
    max_video_pages: int | None = settings.default_video_pages,
    max_clip_pages: int | None = settings.default_clip_pages,
) -> None:
    progress = CrawlJobProgress(job_id=job_id, total=len(channel_ids))
    logger.info("bulk crawl started job=%s total=%d mode=%s", job_id, len(channel_ids), mode)
    try:
        await asyncio.gather(
            *[
                _crawl_channel_safe(
                    progress,
                    cid,
                    since=since,
                    mode=mode,
                    max_video_pages=max_video_pages,
                    max_clip_pages=max_clip_pages,
                )
                for cid in channel_ids
            ]
        )
        await _finish_job(progress)
    except Exception as e:
        logger.exception("bulk crawl failed job=%s", job_id)
        await _finish_job(progress, error_msg=str(e))


async def run_live_crawl(
    job_id: str,
    min_viewers: int,
    since: datetime | None = None,
    mode: CrawlMode = "full",
) -> None:
    progress = CrawlJobProgress(job_id=job_id, total=0)
    logger.info("live crawl started job=%s min_viewers=%d mode=%s", job_id, min_viewers, mode)
    try:
        cursor: LiveCursor | None = None
        for page_num in range(settings.max_live_pages):
            channel_ids, cursor = await chzzk_client.get_live_page(
                min_viewers=min_viewers,
                cursor_viewer_count=cursor["viewer_count"] if cursor else None,
                cursor_live_id=cursor["live_id"] if cursor else None,
            )
            if not channel_ids:
                logger.info("live crawl stopped page=%d empty", page_num)
                break
            progress.total += len(channel_ids)
            await _sync_job_progress(progress)
            logger.info("live page=%d channels=%d", page_num, len(channel_ids))
            await asyncio.gather(
                *[_crawl_channel_safe(progress, cid, since=since, mode=mode) for cid in channel_ids]
            )
            if cursor is None:
                logger.info("live crawl reached last page page=%d", page_num)
                break
        await _finish_job(progress)
    except Exception as e:
        logger.exception("live crawl failed job=%s", job_id)
        await _finish_job(progress, error_msg=str(e))


async def _crawl_videos_safe(
    progress: CrawlJobProgress,
    channel_id: str,
    since: datetime | None,
) -> None:
    try:
        videos = await chzzk_client.get_videos(channel_id, since=since)
        await db.upsert_videos(channel_id, videos)
        progress.success_count += 1
    except Exception as e:
        logger.error("videos crawl failed channel=%s: %s", channel_id, e)
        progress.failed_count += 1
        progress.failed_channels.append(channel_id)
    await _sync_job_progress(progress)


async def _crawl_clips_safe(
    progress: CrawlJobProgress,
    channel_id: str,
    since: datetime | None,
) -> None:
    try:
        clips = await chzzk_client.get_clips(channel_id, since=since)
        await db.upsert_clips(channel_id, clips)
        progress.success_count += 1
    except Exception as e:
        logger.error("clips crawl failed channel=%s: %s", channel_id, e)
        progress.failed_count += 1
        progress.failed_channels.append(channel_id)
    await _sync_job_progress(progress)


async def run_videos_crawl(job_id: str, channel_ids: list[str], since: datetime | None) -> None:
    progress = CrawlJobProgress(job_id=job_id, total=len(channel_ids))
    logger.info("videos crawl started job=%s total=%d", job_id, len(channel_ids))
    try:
        await asyncio.gather(*[_crawl_videos_safe(progress, cid, since) for cid in channel_ids])
        await _finish_job(progress)
    except Exception as e:
        logger.exception("videos crawl failed job=%s", job_id)
        await _finish_job(progress, error_msg=str(e))


async def run_clips_crawl(job_id: str, channel_ids: list[str], since: datetime | None) -> None:
    progress = CrawlJobProgress(job_id=job_id, total=len(channel_ids))
    logger.info("clips crawl started job=%s total=%d", job_id, len(channel_ids))
    try:
        await asyncio.gather(*[_crawl_clips_safe(progress, cid, since) for cid in channel_ids])
        await _finish_job(progress)
    except Exception as e:
        logger.exception("clips crawl failed job=%s", job_id)
        await _finish_job(progress, error_msg=str(e))


async def create_job(
    channel_ids: list[str],
    job_type: CrawlJobType = "user_triggered",
    triggered_by: TriggeredBy = "user",
) -> dict[str, object]:
    if not channel_ids:
        raise InvalidRequestException("channel_ids must not be empty.")
    if await db.has_running_job_of_type(job_type):
        raise CrawlJobConflictException(job_type)
    job_id = await db.insert_crawl_job(
        job_type,
        total_streamers=len(channel_ids),
        triggered_by=triggered_by,
    )
    return {"job_id": job_id, "total": len(channel_ids), "status": "running"}


async def create_live_job(job_type: CrawlJobType = "user_triggered") -> dict[str, object]:
    if await db.has_running_job_of_type(job_type):
        raise CrawlJobConflictException(job_type)
    job_id = await db.insert_crawl_job(
        job_type,
        total_streamers=0,
        triggered_by="user",
    )
    return {"job_id": job_id, "total": 0, "status": "running"}


async def prepare_retry_job(original_job_id: str) -> tuple[dict[str, object], list[str]]:
    try:
        row = await db.get_crawl_job(original_job_id)
    except ValueError:
        raise CrawlJobNotFoundException(original_job_id) from None
    if row is None:
        raise CrawlJobNotFoundException(original_job_id)
    failed_channels = row.get("failed_channels") or []
    if not failed_channels:
        raise InvalidRequestException("재실행할 실패 채널이 없습니다.")
    job = await create_job(failed_channels, job_type="user_retry")
    return job, failed_channels


def _serialize_job(row: dict) -> dict[str, object]:
    result = dict(row)
    result["id"] = str(result["id"])
    result["job_id"] = result["id"]
    result["total"] = result.get("total_streamers")
    result["processed"] = result.get("success_count")
    result["failed"] = result.get("failed_count")
    return result


async def get_job(job_id: str) -> dict[str, object]:
    try:
        row = await db.get_crawl_job(job_id)
    except ValueError:
        row = None
    if not row:
        raise CrawlJobNotFoundException(job_id)
    return _serialize_job(row)


async def get_jobs(limit: int = 10) -> list[dict[str, object]]:
    rows = await db.get_crawl_jobs(limit=limit)
    return [_serialize_job(r) for r in rows]
