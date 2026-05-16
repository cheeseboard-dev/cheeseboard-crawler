from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Literal

import app.models.clip as clip_models
import app.models.video as video_models
from app import db
from app.config import settings
from app.exceptions import (
    CrawlJobConflictException,
    CrawlJobNotFoundException,
    InvalidRequestException,
)
from app.models.channel import ChannelResponse
from app.notifier import send_alert
from app.services.chzzk_client import LiveCursor, chzzk_client
from app.services.es_client import es_client


class CrawlScope(StrEnum):
    FULL = "full"
    VIDEOS = "videos"
    CLIPS = "clips"
    STREAMERS_ONLY = "streamers_only"


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
    def __init__(
        self,
        channel: ChannelResponse,
        videos: list[video_models.VideoResponse],
        clips: list[clip_models.ClipResponse],
    ) -> None:
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
    videos: list[video_models.VideoResponse] = []
    clips: list[clip_models.ClipResponse] = []

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
        await es_client.bulk_index_videos(channel.channel_name, channel_id, videos)
    if clips:
        c_count = await db.upsert_clips(channel_id, clips)
        logger.info("upserted channel=%s clips=%d", channel_id, c_count)
        await es_client.bulk_index_clips(channel.channel_name, channel_id, clips)

    return ChannelCrawlResult(channel=channel, videos=videos, clips=clips)


async def upsert_stub_streamer_if_missing(channel: ChannelResponse) -> bool:
    if await db.streamer_exists(channel.channel_id):
        return False
    await db.upsert_streamer(channel)
    return True


async def ingest_home_clips(
    entries: list[tuple[clip_models.ClipResponse, ChannelResponse]],
    min_read_count: int = 100,
) -> dict[str, int]:
    channels_by_id: dict[str, ChannelResponse] = {}
    channels_new = 0
    grouped: defaultdict[str, list[clip_models.ClipResponse]] = defaultdict(list)
    for clip, channel in entries:
        channels_by_id[channel.channel_id] = channel
        if await upsert_stub_streamer_if_missing(channel):
            channels_new += 1
        if clip.read_count < min_read_count:
            continue
        grouped[channel.channel_id].append(clip)
    clips_upserted = 0
    for channel_id, clips in grouped.items():
        clips_upserted += await db.upsert_clips(channel_id, clips)
        ch = channels_by_id[channel_id]
        await es_client.bulk_index_clips(ch.channel_name, channel_id, clips)
    return {
        "channels_seen": len(channels_by_id),
        "channels_new": channels_new,
        "clips_upserted": clips_upserted,
    }


async def ingest_home_videos(
    entries: list[tuple[video_models.VideoResponse, ChannelResponse]],
) -> dict[str, int]:
    channels_by_id: dict[str, ChannelResponse] = {}
    channels_new = 0
    grouped: defaultdict[str, list[video_models.VideoResponse]] = defaultdict(list)
    for video, channel in entries:
        channels_by_id[channel.channel_id] = channel
        if await upsert_stub_streamer_if_missing(channel):
            channels_new += 1
        grouped[channel.channel_id].append(video)
    videos_upserted = 0
    for channel_id, videos in grouped.items():
        videos_upserted += await db.upsert_videos(channel_id, videos)
        ch = channels_by_id[channel_id]
        await es_client.bulk_index_videos(ch.channel_name, channel_id, videos)
    return {
        "channels_seen": len(channels_by_id),
        "channels_new": channels_new,
        "videos_upserted": videos_upserted,
    }


async def _sync_job_progress(progress: CrawlJobProgress) -> None:
    await db.update_crawl_job(
        progress.job_id,
        total_streamers=progress.total,
        success_count=progress.success_count,
        failed_count=progress.failed_count,
        failed_channels=progress.failed_channels,
    )


async def _crawl_one_safe(
    progress: CrawlJobProgress,
    channel_id: str,
    scope: CrawlScope,
    since: datetime | None = None,
    max_video_pages: int | None = settings.default_video_pages,
    max_clip_pages: int | None = settings.default_clip_pages,
) -> None:
    try:
        if scope == CrawlScope.FULL:
            await crawl_channel(
                channel_id,
                since,
                mode="full",
                max_video_pages=max_video_pages,
                max_clip_pages=max_clip_pages,
            )
        elif scope == CrawlScope.STREAMERS_ONLY:
            await crawl_channel(channel_id, since=None, mode="streamers_only")
        elif scope == CrawlScope.VIDEOS:
            videos = await chzzk_client.get_videos(
                channel_id, since=since, max_pages=max_video_pages
            )
            await db.upsert_videos(channel_id, videos)
            if videos:
                channel_name = await db.get_channel_name(channel_id)
                await es_client.bulk_index_videos(channel_name, channel_id, videos)
        elif scope == CrawlScope.CLIPS:
            clips = await chzzk_client.get_clips(channel_id, since=since, max_pages=max_clip_pages)
            await db.upsert_clips(channel_id, clips)
            if clips:
                channel_name = await db.get_channel_name(channel_id)
                await es_client.bulk_index_clips(channel_name, channel_id, clips)
        progress.success_count += 1
    except Exception as e:
        logger.error("crawl failed channel=%s scope=%s: %s", channel_id, scope.value, e)
        progress.failed_count += 1
        progress.failed_channels.append(channel_id)
    await _sync_job_progress(progress)


async def _crawl_one_clips_watermark(
    progress: CrawlJobProgress,
    channel_id: str,
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
        progress.success_count += 1
    except Exception as e:
        logger.error("crawl failed channel=%s scope=%s: %s", channel_id, CrawlScope.CLIPS.value, e)
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
    job_id = progress.job_id
    if progress.total <= 0:
        return
    elif error_msg is not None:
        await send_alert(
            title="크롤 잡 실행 실패",
            message=f"job_id={job_id} error={error_msg}",
            level="error",
        )
    else:
        failure_rate = progress.failed_count / max(progress.total, 1)
        if failure_rate >= 0.3:
            await send_alert(
                title="크롤 잡 실패율 임계 초과",
                message=(
                    f"job_id={job_id} total={progress.total} success={progress.success_count} "
                    f"failed={progress.failed_count} rate={failure_rate:.1%}"
                ),
                level="warning",
            )


async def run_crawl(
    job_id: str,
    channel_ids: list[str],
    scope: CrawlScope = CrawlScope.FULL,
    since: datetime | None = None,
    max_video_pages: int | None = settings.default_video_pages,
    max_clip_pages: int | None = settings.default_clip_pages,
) -> None:
    progress = CrawlJobProgress(job_id=job_id, total=len(channel_ids))
    logger.info("crawl started job=%s total=%d scope=%s", job_id, len(channel_ids), scope.value)
    try:
        await asyncio.gather(
            *[
                _crawl_one_safe(progress, cid, scope, since, max_video_pages, max_clip_pages)
                for cid in channel_ids
            ]
        )
        await _finish_job(progress)
    except Exception as e:
        logger.exception("crawl failed job=%s", job_id)
        await _finish_job(progress, error_msg=str(e))


async def run_channel_clips_incremental(
    job_id: str,
    channel_ids: list[str],
    max_pages: int,
    min_read_count: int,
) -> None:
    progress = CrawlJobProgress(job_id=job_id, total=len(channel_ids))
    logger.info(
        "crawl started job=%s total=%d scope=%s",
        job_id,
        len(channel_ids),
        CrawlScope.CLIPS.value,
    )
    try:
        await asyncio.gather(
            *[
                _crawl_one_clips_watermark(progress, cid, max_pages, min_read_count)
                for cid in channel_ids
            ]
        )
        await _finish_job(progress)
    except Exception as e:
        logger.exception("crawl failed job=%s", job_id)
        await _finish_job(progress, error_msg=str(e))


async def run_live_crawl(
    job_id: str,
    min_viewers: int,
    since: datetime | None = None,
    mode: CrawlMode = "full",
    max_video_pages: int | None = settings.default_video_pages,
    max_clip_pages: int | None = settings.default_clip_pages,
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
                *[
                    _crawl_one_safe(
                        progress,
                        cid,
                        CrawlScope.FULL if mode == "full" else CrawlScope.STREAMERS_ONLY,
                        since,
                        max_video_pages,
                        max_clip_pages,
                    )
                    for cid in channel_ids
                ]
            )
            if cursor is None:
                logger.info("live crawl reached last page page=%d", page_num)
                break
        await _finish_job(progress)
    except Exception as e:
        logger.exception("live crawl failed job=%s", job_id)
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
