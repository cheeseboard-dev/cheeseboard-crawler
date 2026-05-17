from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime
from enum import StrEnum
from typing import Literal, TypedDict

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
from app.services.chzzk_client import chzzk_client
from app.services.es_client import es_client


class CrawlScope(StrEnum):
    FULL = "full"
    VIDEOS = "videos"
    CLIPS = "clips"
    STREAMERS_ONLY = "streamers_only"


class ClipIngestStats(TypedDict):
    channels_seen: int
    channels_new: int
    new_channel_ids: list[str]
    clips_upserted: int


class VideoIngestStats(TypedDict):
    channels_seen: int
    channels_new: int
    new_channel_ids: list[str]
    videos_upserted: int


logger = logging.getLogger(__name__)

CrawlMode = Literal["full", "streamers_only"]
CrawlJobType = Literal[
    "initial",
    "incremental",
    "full",
    "hot_clips",
    "latest_videos",
    "user_bulk",
    "user_videos",
    "user_clips",
    "user_live",
    "user_retry",
]
TriggeredBy = Literal["scheduler", "user", "admin"]


# ── 단건 채널 크롤 (동기 API용 + worker 내부 공유) ───────────────────────────


async def crawl_channel(
    channel_id: str,
    since: datetime | None = None,
    mode: CrawlMode = "full",
    max_video_pages: int | None = settings.default_video_pages,
    max_clip_pages: int | None = settings.default_clip_pages,
) -> dict[str, object]:
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

    if mode == "full":
        await db.set_initial_crawled(channel_id)

    return {
        "channel": channel.model_dump(),
        "videos_count": len(videos),
        "clips_count": len(clips),
    }


# ── 홈 피드 수집 (worker에서 호출) ───────────────────────────────────────────


async def upsert_stub_streamer_if_missing(channel: ChannelResponse) -> bool:
    if await db.streamer_exists(channel.channel_id):
        return False
    await db.upsert_streamer(channel)
    return True


async def ingest_home_clips(
    entries: list[tuple[clip_models.ClipResponse, ChannelResponse]],
    min_read_count: int = 100,
) -> ClipIngestStats:
    channels_by_id: dict[str, ChannelResponse] = {}
    new_channel_ids: list[str] = []
    grouped: defaultdict[str, list[clip_models.ClipResponse]] = defaultdict(list)
    for clip, channel in entries:
        channels_by_id[channel.channel_id] = channel
        if await upsert_stub_streamer_if_missing(channel):
            new_channel_ids.append(channel.channel_id)
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
        "channels_new": len(new_channel_ids),
        "new_channel_ids": new_channel_ids,
        "clips_upserted": clips_upserted,
    }


async def ingest_home_videos(
    entries: list[tuple[video_models.VideoResponse, ChannelResponse]],
) -> VideoIngestStats:
    channels_by_id: dict[str, ChannelResponse] = {}
    new_channel_ids: list[str] = []
    grouped: defaultdict[str, list[video_models.VideoResponse]] = defaultdict(list)
    for video, channel in entries:
        channels_by_id[channel.channel_id] = channel
        if await upsert_stub_streamer_if_missing(channel):
            new_channel_ids.append(channel.channel_id)
        grouped[channel.channel_id].append(video)
    videos_upserted = 0
    for channel_id, videos in grouped.items():
        videos_upserted += await db.upsert_videos(channel_id, videos)
        ch = channels_by_id[channel_id]
        await es_client.bulk_index_videos(ch.channel_name, channel_id, videos)
    return {
        "channels_seen": len(channels_by_id),
        "channels_new": len(new_channel_ids),
        "new_channel_ids": new_channel_ids,
        "videos_upserted": videos_upserted,
    }


# ── 잡 관리 헬퍼 ─────────────────────────────────────────────────────────────


async def create_job(
    channel_ids: list[str],
    job_type: CrawlJobType = "user_bulk",
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


async def create_live_job(job_type: CrawlJobType = "user_live") -> dict[str, object]:
    if await db.has_running_job_of_type(job_type):
        raise CrawlJobConflictException(job_type)
    job_id = await db.insert_crawl_job(job_type, total_streamers=0, triggered_by="user")
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
