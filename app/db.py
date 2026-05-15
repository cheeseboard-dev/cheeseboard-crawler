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


async def streamer_exists(channel_id: str) -> bool:
    pool = get_pool()
    row = await pool.fetchrow(
        "SELECT 1 FROM streamers WHERE channel_id = $1 LIMIT 1",
        channel_id,
    )
    return row is not None


async def upsert_streamer(channel: ChannelInfo) -> None:
    pool = get_pool()
    await pool.execute(
        """
        INSERT INTO streamers
            (channel_id, channel_name, profile_image_url, follower_count,
             updated_at, last_crawled_at, last_refreshed_at)
        VALUES ($1, $2, $3, $4, NOW(), NOW(), NOW())
        ON CONFLICT (channel_id) DO UPDATE SET
            channel_name        = EXCLUDED.channel_name,
            profile_image_url   = EXCLUDED.profile_image_url,
            follower_count      = EXCLUDED.follower_count,
            updated_at          = NOW(),
            last_crawled_at     = NOW(),
            last_refreshed_at   = NOW()
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
    videos = [v for v in videos if v.video_id is not None]
    if not videos:
        return 0
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


async def insert_crawl_job(
    job_type: str,
    total_streamers: int | None = None,
    triggered_by: str | None = None,
) -> str:
    pool = get_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO crawl_jobs
            (job_type, total_streamers, triggered_by)
        VALUES ($1, $2, $3)
        RETURNING id
        """,
        job_type,
        total_streamers,
        triggered_by,
    )
    return str(row["id"])


async def has_running_job_of_type(job_type: str) -> bool:
    pool = get_pool()
    row = await pool.fetchrow(
        "SELECT 1 FROM crawl_jobs WHERE job_type = $1 AND status = 'running' LIMIT 1",
        job_type,
    )
    return row is not None


async def update_crawl_job(
    job_id: str,
    *,
    status: str | None = None,
    total_streamers: int | None = None,
    success_count: int | None = None,
    failed_count: int | None = None,
    error_msg: str | None = None,
    failed_channels: list[str] | None = None,
) -> None:
    job_uuid = uuid.UUID(job_id)
    pool = get_pool()
    await pool.execute(
        """
        UPDATE crawl_jobs
        SET finished_at     = CASE
                                  WHEN $2::varchar IN ('done', 'failed') THEN NOW()
                                  ELSE finished_at
                              END,
            status          = COALESCE($2, status),
            total_streamers = COALESCE($3, total_streamers),
            success_count   = COALESCE($4, success_count),
            failed_count    = COALESCE($5, failed_count),
            error_msg       = COALESCE($6, error_msg),
            failed_channels = COALESCE($7::text[], failed_channels)
        WHERE id = $1
        """,
        job_uuid,
        status,
        total_streamers,
        success_count,
        failed_count,
        error_msg,
        failed_channels,
    )


async def create_crawl_job(job_type: str, total_streamers: int, triggered_by: str) -> str:
    return await insert_crawl_job(
        job_type,
        total_streamers=total_streamers,
        triggered_by=triggered_by,
    )


async def finish_crawl_job(
    job_id: str,
    *,
    success: int,
    failed: int,
    error_msg: str | None = None,
) -> None:
    await update_crawl_job(
        job_id,
        status="failed" if error_msg else "done",
        success_count=success,
        failed_count=failed,
        error_msg=error_msg,
    )


async def get_crawl_jobs(limit: int = 10) -> list[dict]:
    pool = get_pool()
    rows = await pool.fetch(
        """
        SELECT id, job_type, started_at, finished_at, status,
               total_streamers, success_count, failed_count, triggered_by, error_msg,
               failed_channels
        FROM crawl_jobs
        ORDER BY started_at DESC
        LIMIT $1
        """,
        limit,
    )
    return [dict(r) for r in rows]


async def get_streamers(active_only: bool = False) -> list[dict]:
    pool = get_pool()
    query = (
        "SELECT * FROM streamers WHERE is_active = TRUE ORDER BY channel_name"
        if active_only
        else "SELECT * FROM streamers ORDER BY channel_name"
    )
    rows = await pool.fetch(query)
    return [dict(r) for r in rows]


async def get_streamer_stats(channel_id: str) -> dict | None:
    pool = get_pool()
    row = await pool.fetchrow(
        """
        SELECT s.channel_id,
               s.channel_name,
               s.is_active,
               s.follower_count,
               COALESCE(v.video_count, 0) AS video_count,
               COALESCE(c.clip_count, 0) AS clip_count,
               v.latest_video_published_at,
               c.latest_clip_created_at,
               s.updated_at AS last_crawled_at
        FROM streamers s
        LEFT JOIN (
            SELECT channel_id,
                   COUNT(*) AS video_count,
                   MAX(published_at) AS latest_video_published_at
            FROM videos
            GROUP BY channel_id
        ) v ON v.channel_id = s.channel_id
        LEFT JOIN (
            SELECT channel_id,
                   COUNT(*) AS clip_count,
                   MAX(created_at) AS latest_clip_created_at
            FROM clips
            GROUP BY channel_id
        ) c ON c.channel_id = s.channel_id
        WHERE s.channel_id = $1
        """,
        channel_id,
    )
    return dict(row) if row else None


async def get_active_channel_ids() -> list[str]:
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT channel_id FROM streamers WHERE is_active = TRUE ORDER BY channel_name"
    )
    return [r["channel_id"] for r in rows]


async def set_streamer_active(channel_id: str, is_active: bool) -> bool:
    pool = get_pool()
    result: str = await pool.execute(
        "UPDATE streamers SET is_active = $2 WHERE channel_id = $1",
        channel_id,
        is_active,
    )
    return result == "UPDATE 1"


async def cleanup_stale_jobs() -> int:
    pool = get_pool()
    result = await pool.execute(
        """
        UPDATE crawl_jobs
        SET status      = 'failed',
            finished_at = NOW(),
            error_msg   = 'server restarted while job was running'
        WHERE status = 'running'
        """
    )
    return int(result.split()[-1])


async def get_crawl_job(job_id: str) -> dict | None:
    pool = get_pool()
    row = await pool.fetchrow(
        """
        SELECT id, job_type, started_at, finished_at, status,
               total_streamers, success_count, failed_count, triggered_by, error_msg,
               failed_channels
        FROM crawl_jobs
        WHERE id = $1
        """,
        uuid.UUID(job_id),
    )
    return dict(row) if row else None
