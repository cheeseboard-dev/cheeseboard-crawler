"""크롤 오케스트레이션: 채널별 수집 → PostgreSQL upsert."""
from __future__ import annotations

import asyncio
import csv
import logging
import uuid
from datetime import datetime
from typing import Dict, List, Optional

from app import db
from app.exceptions import CrawlJobNotFoundException, InvalidRequestException
from app.models.channel import ChannelInfo
from app.models.clip import Clip
from app.models.video import Video
from app.services.chzzk_client import chzzk_client

logger = logging.getLogger(__name__)


# ── In-memory job 상태 (crawl_jobs 테이블 미러) ───────────────────────────────

class CrawlJob:
    def __init__(self, job_id: str, total: int) -> None:
        self.job_id = job_id
        self.status = "running"
        self.total = total
        self.processed = 0
        self.failed = 0
        self.started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.finished_at: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "total": self.total,
            "processed": self.processed,
            "failed": self.failed,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


_jobs: Dict[str, CrawlJob] = {}


# ── 채널 단건 수집 ────────────────────────────────────────────────────────────

class ChannelCrawlResult:
    def __init__(self, channel: ChannelInfo, videos: List[Video], clips: List[Clip]) -> None:
        self.channel = channel
        self.videos = videos
        self.clips = clips
        self.crawled_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def to_dict(self) -> dict:
        return {
            "channel": self.channel.model_dump(),
            "videos": [v.model_dump() for v in self.videos],
            "clips": [c.model_dump() for c in self.clips],
            "crawled_at": self.crawled_at,
        }


async def crawl_channel(channel_id: str) -> ChannelCrawlResult:
    channel, videos, clips = await asyncio.gather(
        chzzk_client.get_channel(channel_id),
        chzzk_client.get_videos(channel_id),
        chzzk_client.get_clips(channel_id),
    )

    try:
        await db.upsert_streamer(channel)
        v_count = await db.upsert_videos(channel_id, videos)
        c_count = await db.upsert_clips(channel_id, clips)
        logger.info(
            "upserted channel=%s videos=%d clips=%d", channel_id, v_count, c_count
        )
    except Exception as e:
        logger.warning("DB upsert 실패 (channel=%s): %s", channel_id, e)

    return ChannelCrawlResult(channel=channel, videos=videos, clips=clips)


# ── 벌크 크롤 ─────────────────────────────────────────────────────────────────

async def run_bulk_crawl(job_id: str, channel_ids: List[str]) -> None:
    job = _jobs[job_id]

    # crawl_jobs 테이블에도 기록
    db_job_id: Optional[str] = None
    try:
        db_job_id = await db.create_crawl_job(
            "incremental", total_streamers=len(channel_ids), triggered_by="user"
        )
    except Exception as e:
        logger.warning("crawl_jobs 생성 실패: %s", e)

    for channel_id in channel_ids:
        try:
            await crawl_channel(channel_id)
            job.processed += 1
        except Exception as e:
            logger.error("채널 크롤 실패 channel=%s: %s", channel_id, e)
            job.failed += 1

    job.status = "done"
    job.finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if db_job_id:
        try:
            await db.finish_crawl_job(
                db_job_id, success=job.processed, failed=job.failed
            )
        except Exception as e:
            logger.warning("crawl_jobs 업데이트 실패: %s", e)


# ── CSV 로드 / job 관리 ───────────────────────────────────────────────────────

def load_channel_ids_from_csv(csv_path: str) -> List[str]:
    ids: List[str] = []
    try:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                cid = row.get("channelId") or row.get("channel_id")
                if cid:
                    ids.append(cid.strip())
    except FileNotFoundError:
        logger.warning("CSV 파일 없음: %s", csv_path)
    return ids


def create_job(channel_ids: List[str]) -> CrawlJob:
    if not channel_ids:
        raise InvalidRequestException(
            "channel_ids를 직접 지정하거나 use_csv=true로 CSV에서 읽어야 합니다."
        )
    job_id = str(uuid.uuid4())[:8]
    job = CrawlJob(job_id, total=len(channel_ids))
    _jobs[job_id] = job
    return job


def get_job(job_id: str) -> CrawlJob:
    job = _jobs.get(job_id)
    if not job:
        raise CrawlJobNotFoundException(job_id)
    return job
