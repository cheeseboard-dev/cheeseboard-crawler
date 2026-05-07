import asyncio
import csv
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from app.config import settings
from app.exceptions import CrawlJobNotFoundException, InvalidRequestException
from app.models.channel import ChannelInfo
from app.models.clip import Clip
from app.models.video import Video
from app.services.chzzk_client import chzzk_client


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
    return ChannelCrawlResult(channel=channel, videos=videos, clips=clips)


async def run_bulk_crawl(job_id: str, channel_ids: List[str]) -> None:
    job = _jobs[job_id]
    output_dir = Path(settings.output_dir)
    output_dir.mkdir(exist_ok=True)

    for channel_id in channel_ids:
        try:
            result = await crawl_channel(channel_id)
            out_file = output_dir / f"{channel_id}.json"
            out_file.write_text(
                json.dumps(result.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
            )
            job.processed += 1
        except Exception:
            job.failed += 1

    job.status = "done"
    job.finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    summary_file = output_dir / f"job_{job_id}.json"
    summary_file.write_text(
        json.dumps(job.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
    )


def load_channel_ids_from_csv(csv_path: str) -> List[str]:
    ids: List[str] = []
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cid = row.get("channelId") or row.get("channel_id")
                if cid:
                    ids.append(cid.strip())
    except FileNotFoundError:
        pass
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
