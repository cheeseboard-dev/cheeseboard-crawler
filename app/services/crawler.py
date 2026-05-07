"""크롤 오케스트레이션: 채널별 수집 → PostgreSQL upsert."""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Literal

from app import db
from app.exceptions import CrawlJobNotFoundException, InvalidRequestException
from app.models.channel import ChannelInfo
from app.models.clip import Clip
from app.models.video import Video
from app.services.chzzk_client import LiveCursor, chzzk_client

logger = logging.getLogger(__name__)

CrawlMode = Literal["full", "streamers_only"]


# ── In-memory job 상태 ────────────────────────────────────────────────────────


class CrawlJob:
    def __init__(self, job_id: str, total: int) -> None:
        self.job_id = job_id
        self.status: str = "running"
        self.total = total
        self.processed = 0
        self.failed = 0
        self.started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.finished_at: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "total": self.total,
            "processed": self.processed,
            "failed": self.failed,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


_jobs: dict[str, CrawlJob] = {}


# ── 채널 단건 수집 ────────────────────────────────────────────────────────────


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
) -> ChannelCrawlResult:
    videos: list[Video] = []
    clips: list[Clip] = []

    if mode == "streamers_only":
        channel = await chzzk_client.get_channel(channel_id)
    else:
        channel, videos, clips = await asyncio.gather(
            chzzk_client.get_channel(channel_id),
            chzzk_client.get_videos(channel_id, since=since),
            chzzk_client.get_clips(channel_id, since=since),
        )

    try:
        await db.upsert_streamer(channel)
        if videos:
            v_count = await db.upsert_videos(channel_id, videos)
            logger.info("upserted channel=%s videos=%d", channel_id, v_count)
        if clips:
            c_count = await db.upsert_clips(channel_id, clips)
            logger.info("upserted channel=%s clips=%d", channel_id, c_count)
    except Exception as e:
        logger.warning("DB upsert 실패 (channel=%s): %s", channel_id, e)

    return ChannelCrawlResult(channel=channel, videos=videos, clips=clips)


# ── 벌크 / 라이브 크롤 공통 ──────────────────────────────────────────────────


async def _crawl_channel_safe(
    job: CrawlJob,
    channel_id: str,
    since: datetime | None = None,
    mode: CrawlMode = "full",
) -> None:
    try:
        await crawl_channel(channel_id, since=since, mode=mode)
        job.processed += 1
    except Exception as e:
        logger.error("채널 크롤 실패 channel=%s: %s", channel_id, e)
        job.failed += 1


async def _finish_job(job: CrawlJob, db_job_id: str | None) -> None:
    job.status = "done"
    job.finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("job=%s 완료 — processed=%d failed=%d", job.job_id, job.processed, job.failed)
    if db_job_id:
        try:
            await db.finish_crawl_job(db_job_id, success=job.processed, failed=job.failed)
        except Exception as e:
            logger.warning("crawl_jobs 업데이트 실패: %s", e)


async def run_bulk_crawl(
    job_id: str,
    channel_ids: list[str],
    since: datetime | None = None,
    mode: CrawlMode = "full",
) -> None:
    job = _jobs[job_id]
    logger.info("bulk crawl 시작 — job=%s total=%d mode=%s", job_id, len(channel_ids), mode)

    db_job_id: str | None = None
    try:
        db_job_id = await db.create_crawl_job(
            mode, total_streamers=len(channel_ids), triggered_by="user"
        )
    except Exception as e:
        logger.warning("crawl_jobs 생성 실패: %s", e)

    await asyncio.gather(
        *[_crawl_channel_safe(job, cid, since=since, mode=mode) for cid in channel_ids]
    )
    await _finish_job(job, db_job_id)


# ── 라이브 크롤 ───────────────────────────────────────────────────────────────

_MAX_LIVE_PAGES = 200


async def run_live_crawl(
    job_id: str,
    min_viewers: int,
    since: datetime | None = None,
    mode: CrawlMode = "full",
) -> None:
    job = _jobs[job_id]
    logger.info("live crawl 시작 — job=%s min_viewers=%d mode=%s", job_id, min_viewers, mode)

    db_job_id: str | None = None
    try:
        db_job_id = await db.create_crawl_job(mode, total_streamers=0, triggered_by="user")
    except Exception as e:
        logger.warning("crawl_jobs 생성 실패: %s", e)

    cursor: LiveCursor | None = None
    for page_num in range(_MAX_LIVE_PAGES):
        channel_ids, cursor = await chzzk_client.get_live_page(
            min_viewers=min_viewers,
            cursor_viewer_count=cursor["viewer_count"] if cursor else None,
            cursor_live_id=cursor["live_id"] if cursor else None,
        )
        if not channel_ids:
            logger.info("live crawl 종료 — page=%d 채널 없음", page_num)
            break
        job.total += len(channel_ids)
        logger.info("live page=%d 채널 %d개 수집", page_num, len(channel_ids))
        await asyncio.gather(
            *[_crawl_channel_safe(job, cid, since=since, mode=mode) for cid in channel_ids]
        )
        if cursor is None:
            logger.info("live crawl 종료 — 마지막 페이지 도달 (page=%d)", page_num)
            break

    await _finish_job(job, db_job_id)


# ── videos / clips 단독 크롤 ─────────────────────────────────────────────────


async def _crawl_videos_safe(job: CrawlJob, channel_id: str, since: datetime | None) -> None:
    try:
        videos = await chzzk_client.get_videos(channel_id, since=since)
        await db.upsert_videos(channel_id, videos)
        job.processed += 1
    except Exception as e:
        logger.error("videos 크롤 실패 channel=%s: %s", channel_id, e)
        job.failed += 1


async def _crawl_clips_safe(job: CrawlJob, channel_id: str, since: datetime | None) -> None:
    try:
        clips = await chzzk_client.get_clips(channel_id, since=since)
        await db.upsert_clips(channel_id, clips)
        job.processed += 1
    except Exception as e:
        logger.error("clips 크롤 실패 channel=%s: %s", channel_id, e)
        job.failed += 1


async def run_videos_crawl(job_id: str, channel_ids: list[str], since: datetime | None) -> None:
    job = _jobs[job_id]
    logger.info("videos crawl 시작 — job=%s total=%d", job_id, len(channel_ids))

    db_job_id: str | None = None
    try:
        db_job_id = await db.create_crawl_job(
            "videos_only", total_streamers=len(channel_ids), triggered_by="user"
        )
    except Exception as e:
        logger.warning("crawl_jobs 생성 실패: %s", e)

    await asyncio.gather(*[_crawl_videos_safe(job, cid, since) for cid in channel_ids])
    await _finish_job(job, db_job_id)


async def run_clips_crawl(job_id: str, channel_ids: list[str], since: datetime | None) -> None:
    job = _jobs[job_id]
    logger.info("clips crawl 시작 — job=%s total=%d", job_id, len(channel_ids))

    db_job_id: str | None = None
    try:
        db_job_id = await db.create_crawl_job(
            "clips_only", total_streamers=len(channel_ids), triggered_by="user"
        )
    except Exception as e:
        logger.warning("crawl_jobs 생성 실패: %s", e)

    await asyncio.gather(*[_crawl_clips_safe(job, cid, since) for cid in channel_ids])
    await _finish_job(job, db_job_id)


# ── job 관리 ──────────────────────────────────────────────────────────────────


def create_job(channel_ids: list[str]) -> CrawlJob:
    if not channel_ids:
        raise InvalidRequestException("channel_ids가 비어 있습니다.")
    job_id = str(uuid.uuid4())[:8]
    job = CrawlJob(job_id, total=len(channel_ids))
    _jobs[job_id] = job
    return job


def create_live_job() -> CrawlJob:
    job_id = str(uuid.uuid4())[:8]
    job = CrawlJob(job_id, total=0)
    _jobs[job_id] = job
    return job


def get_job(job_id: str) -> CrawlJob:
    job = _jobs.get(job_id)
    if not job:
        raise CrawlJobNotFoundException(job_id)
    return job
