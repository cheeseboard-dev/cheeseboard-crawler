"""asyncpg 커넥션 풀 + 테이블별 upsert 함수."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime

import asyncpg

from app.config import settings
from app.models.channel import ChannelInfo
from app.models.clip import Clip
from app.models.video import Video

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


async def init_pool() -> None:
    global _pool
    _pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=5)


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialized")
    return _pool


# ── 날짜 파싱 ──────────────────────────────────────────────────────────────────


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(value.split("+")[0].strip(), fmt)
        except ValueError:
            continue
    return None


# ── upsert: streamers ──────────────────────────────────────────────────────────


async def upsert_streamer(channel: ChannelInfo) -> None:
    pool = get_pool()
    await pool.execute(
        """
        INSERT INTO streamers
            (channel_id, channel_name, profile_image_url, follower_count,
             updated_at, last_crawled_at)
        VALUES ($1, $2, $3, $4, NOW(), NOW())
        ON CONFLICT (channel_id) DO UPDATE SET
            channel_name      = EXCLUDED.channel_name,
            profile_image_url = EXCLUDED.profile_image_url,
            follower_count    = EXCLUDED.follower_count,
            updated_at        = NOW(),
            last_crawled_at   = NOW()
        """,
        channel.channel_id,
        channel.channel_name,
        channel.profile_image_url,
        channel.follower_count,
    )


# ── upsert: videos ────────────────────────────────────────────────────────────


async def upsert_videos(channel_id: str, videos: list[Video]) -> int:
    if not videos:
        return 0
    pool = get_pool()
    rows = [
        (
            v.video_no,
            v.video_id,
            channel_id,
            v.title,
            v.category if v.category != "미지정" else None,
            v.tags or [],
            v.read_count,
            v.duration,
            _parse_dt(v.published_at),
            v.thumbnail_url,
        )
        for v in videos
    ]
    await pool.executemany(
        """
        INSERT INTO videos
            (video_no, video_id, channel_id, title, category, tags,
             read_count, duration, published_at, thumbnail_url, last_refreshed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
        ON CONFLICT (video_no) DO UPDATE SET
            title             = EXCLUDED.title,
            category          = EXCLUDED.category,
            tags              = EXCLUDED.tags,
            read_count        = EXCLUDED.read_count,
            duration          = EXCLUDED.duration,
            thumbnail_url     = EXCLUDED.thumbnail_url,
            last_refreshed_at = NOW()
        """,
        rows,
    )
    return len(rows)


# ── upsert: clips ─────────────────────────────────────────────────────────────


async def upsert_clips(channel_id: str, clips: list[Clip]) -> int:
    if not clips:
        return 0
    pool = get_pool()

    # origin_video_id FK 사전 검증: DB에 없는 video_id는 NULL로 처리
    candidate_ids = list({c.origin_video_id for c in clips if c.origin_video_id})
    known_video_ids: set[str] = set()
    if candidate_ids:
        rows_v = await pool.fetch(
            "SELECT video_id FROM videos WHERE video_id = ANY($1::varchar[])",
            candidate_ids,
        )
        known_video_ids = {r["video_id"] for r in rows_v}

    rows = [
        (
            c.clip_uid,
            channel_id,
            c.origin_video_id if c.origin_video_id in known_video_ids else None,
            c.title,
            c.read_count,
            c.duration,
            _parse_dt(c.created_at),
            c.thumbnail_url,
        )
        for c in clips
    ]
    await pool.executemany(
        """
        INSERT INTO clips
            (clip_uid, channel_id, origin_video_id, title,
             read_count, duration, created_at, thumbnail_url, last_refreshed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
        ON CONFLICT (clip_uid) DO UPDATE SET
            title             = EXCLUDED.title,
            read_count        = EXCLUDED.read_count,
            duration          = EXCLUDED.duration,
            thumbnail_url     = EXCLUDED.thumbnail_url,
            last_refreshed_at = NOW()
        """,
        rows,
    )
    return len(rows)


# ── crawl_jobs ────────────────────────────────────────────────────────────────


async def create_crawl_job(job_type: str, total_streamers: int, triggered_by: str) -> str:
    job_id = str(uuid.uuid4())
    pool = get_pool()
    await pool.execute(
        """
        INSERT INTO crawl_jobs
            (id, job_type, started_at, status, total_streamers, triggered_by)
        VALUES ($1, $2, NOW(), 'running', $3, $4)
        """,
        job_id,
        job_type,
        total_streamers,
        triggered_by,
    )
    return job_id


async def finish_crawl_job(
    job_id: str,
    *,
    success: int,
    failed: int,
    error_msg: str | None = None,
) -> None:
    status = "failed" if error_msg else "done"
    pool = get_pool()
    await pool.execute(
        """
        UPDATE crawl_jobs
        SET finished_at   = NOW(),
            status        = $2,
            success_count = $3,
            failed_count  = $4,
            error_msg     = $5
        WHERE id = $1
        """,
        job_id,
        status,
        success,
        failed,
        error_msg,
    )


async def get_crawl_jobs(limit: int = 10) -> list[dict]:
    pool = get_pool()
    rows = await pool.fetch(
        """
        SELECT id, job_type, started_at, finished_at, status,
               total_streamers, success_count, failed_count, triggered_by, error_msg
        FROM crawl_jobs
        ORDER BY started_at DESC
        LIMIT $1
        """,
        limit,
    )
    return [dict(r) for r in rows]


async def get_crawl_job(job_id: str) -> dict | None:
    pool = get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM crawl_jobs WHERE id = $1",
        job_id,
    )
    return dict(row) if row else None
