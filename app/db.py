from __future__ import annotations

import logging
import uuid
from datetime import datetime

from sqlalchemy import Text, func, select
from sqlalchemy import update as sa_update
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import insert as pg_insert

import app.models.clip as clip_models
import app.models.video as video_models
from app.models.channel import ChannelResponse
from app.orm import Clip, CrawlJob, Streamer, Video
from app.orm import session as orm_session

logger = logging.getLogger(__name__)

init_pool = orm_session.init_engine
close_pool = orm_session.close_engine


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(value.split("+")[0].strip(), fmt)
        except ValueError:
            continue
    return None


def _category_or_none(value: str) -> str | None:
    if value == "\ubbf8\uc9c0\uc815":
        return None
    return value


async def streamer_exists(channel_id: str) -> bool:
    async with orm_session.get_session() as session:
        result = await session.execute(
            select(Streamer.channel_id).where(Streamer.channel_id == channel_id).limit(1)
        )
        return result.scalar_one_or_none() is not None


async def get_channel_video_watermark(channel_id: str) -> datetime | None:
    async with orm_session.get_session() as session:
        result = await session.execute(
            select(func.max(Video.published_at)).where(Video.channel_id == channel_id)
        )
        row: datetime | None = result.scalar_one_or_none()
        return row


async def get_channel_clip_watermark(channel_id: str) -> datetime | None:
    async with orm_session.get_session() as session:
        result = await session.execute(
            select(func.max(Clip.created_at)).where(Clip.channel_id == channel_id)
        )
        row: datetime | None = result.scalar_one_or_none()
        return row


async def upsert_streamer(channel: ChannelResponse) -> None:
    values = {
        "channel_id": channel.channel_id,
        "channel_name": channel.channel_name,
        "profile_image_url": channel.profile_image_url,
        "follower_count": channel.follower_count,
        "updated_at": func.now(),
        "last_crawled_at": func.now(),
        "last_refreshed_at": func.now(),
    }
    stmt = pg_insert(Streamer).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Streamer.channel_id],
        set_={
            "channel_name": stmt.excluded.channel_name,
            "profile_image_url": stmt.excluded.profile_image_url,
            "follower_count": stmt.excluded.follower_count,
            "updated_at": func.now(),
            "last_crawled_at": func.now(),
            "last_refreshed_at": func.now(),
            # is_initial_crawled 은 여기서 건드리지 않음 — set_initial_crawled()로만 변경
        },
    )
    async with orm_session.get_session() as session:
        await session.execute(stmt)
        await session.commit()


async def upsert_videos(channel_id: str, videos: list[video_models.VideoResponse]) -> int:
    videos = [v for v in videos if v.video_id is not None]
    if not videos:
        return 0
    rows = [
        {
            "video_no": v.video_no,
            "video_id": v.video_id,
            "channel_id": channel_id,
            "title": v.title,
            "category": _category_or_none(v.category),
            "tags": v.tags or [],
            "read_count": v.read_count,
            "duration": v.duration,
            "published_at": _parse_dt(v.published_at),
            "thumbnail_url": v.thumbnail_url,
            "last_refreshed_at": func.now(),
        }
        for v in videos
    ]
    stmt = pg_insert(Video).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Video.video_no],
        set_={
            "title": stmt.excluded.title,
            "category": stmt.excluded.category,
            "tags": stmt.excluded.tags,
            "read_count": stmt.excluded.read_count,
            "duration": stmt.excluded.duration,
            "thumbnail_url": stmt.excluded.thumbnail_url,
            "last_refreshed_at": func.now(),
        },
    )
    async with orm_session.get_session() as session:
        await session.execute(stmt)
        await session.commit()
    return len(rows)


async def upsert_clips(channel_id: str, clips: list[clip_models.ClipResponse]) -> int:
    if not clips:
        return 0
    candidate_ids = list({c.origin_video_id for c in clips if c.origin_video_id})
    async with orm_session.get_session() as session:
        known_video_ids: set[str] = set()
        if candidate_ids:
            rows_v = await session.execute(
                select(Video.video_id).where(Video.video_id.in_(candidate_ids))
            )
            known_video_ids = set(rows_v.scalars().all())
        nullified = sum(
            1 for c in clips if c.origin_video_id and c.origin_video_id not in known_video_ids
        )
        if nullified:
            logger.debug(
                "upsert_clips: %d clip(s) with unresolved origin_video_id stored without link channel=%s",
                nullified,
                channel_id,
            )
        rows = [
            {
                "clip_uid": c.clip_uid,
                "channel_id": channel_id,
                "origin_video_id": c.origin_video_id
                if c.origin_video_id in known_video_ids
                else None,
                "title": c.title,
                "read_count": c.read_count,
                "duration": c.duration,
                "created_at": _parse_dt(c.created_at),
                "thumbnail_url": c.thumbnail_url,
                "last_refreshed_at": func.now(),
            }
            for c in clips
        ]
        if not rows:
            return 0
        stmt = pg_insert(Clip).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=[Clip.clip_uid],
            set_={
                "title": stmt.excluded.title,
                "read_count": stmt.excluded.read_count,
                "duration": stmt.excluded.duration,
                "thumbnail_url": stmt.excluded.thumbnail_url,
                "last_refreshed_at": func.now(),
            },
        )
        await session.execute(stmt)
        await session.commit()
        return len(rows)


async def insert_crawl_job(
    job_type: str,
    total_streamers: int | None = None,
    triggered_by: str | None = None,
) -> str:
    stmt = (
        pg_insert(CrawlJob)
        .values(
            job_type=job_type,
            total_streamers=total_streamers,
            triggered_by=triggered_by,
        )
        .returning(CrawlJob.id)
    )
    async with orm_session.get_session() as session:
        result = await session.execute(stmt)
        await session.commit()
        return str(result.scalar_one())


async def has_running_job_of_type(job_type: str) -> bool:
    async with orm_session.get_session() as session:
        result = await session.execute(
            select(CrawlJob.id)
            .where(CrawlJob.job_type == job_type, CrawlJob.status == "running")
            .limit(1)
        )
        return result.scalar_one_or_none() is not None


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
    values: dict[str, object] = {}
    if status is not None:
        values["status"] = status
        if status in ("done", "failed"):
            values["finished_at"] = func.now()
    if total_streamers is not None:
        values["total_streamers"] = total_streamers
    if success_count is not None:
        values["success_count"] = success_count
    if failed_count is not None:
        values["failed_count"] = failed_count
    if error_msg is not None:
        values["error_msg"] = error_msg
    if failed_channels is not None:
        values["failed_channels"] = failed_channels
    if not values:
        return
    stmt = sa_update(CrawlJob).where(CrawlJob.id == uuid.UUID(job_id)).values(**values)
    async with orm_session.get_session() as session:
        await session.execute(stmt)
        await session.commit()


async def get_crawl_jobs(limit: int = 10) -> list[dict]:
    stmt = (
        select(
            CrawlJob.id,
            CrawlJob.job_type,
            CrawlJob.started_at,
            CrawlJob.finished_at,
            CrawlJob.status,
            CrawlJob.total_streamers,
            CrawlJob.success_count,
            CrawlJob.failed_count,
            CrawlJob.triggered_by,
            CrawlJob.error_msg,
            CrawlJob.failed_channels,
        )
        .order_by(CrawlJob.started_at.desc())
        .limit(limit)
    )
    async with orm_session.get_session() as session:
        result = await session.execute(stmt)
        return [dict(row) for row in result.mappings().all()]


async def get_crawl_job(job_id: str) -> dict | None:
    stmt = select(
        CrawlJob.id,
        CrawlJob.job_type,
        CrawlJob.started_at,
        CrawlJob.finished_at,
        CrawlJob.status,
        CrawlJob.total_streamers,
        CrawlJob.success_count,
        CrawlJob.failed_count,
        CrawlJob.triggered_by,
        CrawlJob.error_msg,
        CrawlJob.failed_channels,
    ).where(CrawlJob.id == uuid.UUID(job_id))
    async with orm_session.get_session() as session:
        result = await session.execute(stmt)
        row = result.mappings().one_or_none()
        return dict(row) if row else None


async def cleanup_stale_jobs() -> int:
    stmt = (
        sa_update(CrawlJob)
        .where(CrawlJob.status == "running")
        .values(
            status="failed",
            finished_at=func.now(),
            error_msg="server restarted while job was running",
        )
    )
    async with orm_session.get_session() as session:
        result = await session.execute(stmt)
        await session.commit()
        return int(result.rowcount or 0)


async def get_streamers(active_only: bool = False) -> list[dict]:
    stmt = select(*Streamer.__table__.c).order_by(Streamer.channel_name)
    if active_only:
        stmt = stmt.where(Streamer.is_active.is_(True))
    async with orm_session.get_session() as session:
        result = await session.execute(stmt)
        return [dict(row) for row in result.mappings().all()]


async def get_streamer_stats(channel_id: str) -> dict | None:
    video_stats = (
        select(
            Video.channel_id,
            func.count().label("video_count"),
            func.max(Video.published_at).label("latest_video_published_at"),
        )
        .group_by(Video.channel_id)
        .subquery()
    )
    clip_stats = (
        select(
            Clip.channel_id,
            func.count().label("clip_count"),
            func.max(Clip.created_at).label("latest_clip_created_at"),
        )
        .group_by(Clip.channel_id)
        .subquery()
    )
    stmt = (
        select(
            Streamer.channel_id,
            Streamer.channel_name,
            Streamer.is_active,
            Streamer.follower_count,
            func.coalesce(video_stats.c.video_count, 0).label("video_count"),
            func.coalesce(clip_stats.c.clip_count, 0).label("clip_count"),
            video_stats.c.latest_video_published_at,
            clip_stats.c.latest_clip_created_at,
            Streamer.updated_at.label("last_crawled_at"),
        )
        .outerjoin(video_stats, video_stats.c.channel_id == Streamer.channel_id)
        .outerjoin(clip_stats, clip_stats.c.channel_id == Streamer.channel_id)
        .where(Streamer.channel_id == channel_id)
    )
    async with orm_session.get_session() as session:
        result = await session.execute(stmt)
        row = result.mappings().one_or_none()
        return dict(row) if row else None


async def get_channel_name(channel_id: str) -> str:
    async with orm_session.get_session() as session:
        result = await session.execute(
            select(Streamer.channel_name).where(Streamer.channel_id == channel_id).limit(1)
        )
        return result.scalar_one_or_none() or channel_id


async def get_active_channel_ids() -> list[str]:
    stmt = (
        select(Streamer.channel_id)
        .where(Streamer.is_active.is_(True))
        .order_by(Streamer.channel_name)
    )
    async with orm_session.get_session() as session:
        result = await session.execute(stmt)
        return list(result.scalars().all())


async def set_streamer_active(channel_id: str, is_active: bool) -> bool:
    stmt = sa_update(Streamer).where(Streamer.channel_id == channel_id).values(is_active=is_active)
    async with orm_session.get_session() as session:
        result = await session.execute(stmt)
        await session.commit()
        return bool(result.rowcount == 1)


async def set_initial_crawled(channel_id: str) -> None:
    stmt = (
        sa_update(Streamer).where(Streamer.channel_id == channel_id).values(is_initial_crawled=True)
    )
    async with orm_session.get_session() as session:
        await session.execute(stmt)
        await session.commit()


async def increment_job_progress(
    job_id: str,
    *,
    success: bool,
    failed_channel: str | None = None,
) -> bool:
    """성공/실패 카운트를 원자적으로 증가. 모든 채널이 처리됐으면 True 반환."""
    if success:
        stmt = (
            sa_update(CrawlJob)
            .where(CrawlJob.id == uuid.UUID(job_id))
            .values(success_count=CrawlJob.success_count + 1)
            .returning(CrawlJob.success_count, CrawlJob.failed_count, CrawlJob.total_streamers)
        )
    else:
        stmt = (
            sa_update(CrawlJob)
            .where(CrawlJob.id == uuid.UUID(job_id))
            .values(failed_count=CrawlJob.failed_count + 1)
            .returning(CrawlJob.success_count, CrawlJob.failed_count, CrawlJob.total_streamers)
        )
    async with orm_session.get_session() as session:
        result = await session.execute(stmt)
        if not success and failed_channel:
            await session.execute(
                sa_update(CrawlJob)
                .where(CrawlJob.id == uuid.UUID(job_id))
                .values(
                    failed_channels=func.array_append(
                        func.coalesce(CrawlJob.failed_channels, func.cast([], ARRAY(Text))),
                        failed_channel,
                    )
                )
            )
        await session.commit()
        row = result.one()
        total = row.total_streamers or 0
        if total <= 0:
            return False
        return bool((row.success_count + row.failed_count) >= total)


async def get_uncrawled_channel_ids() -> list[str]:
    """초기 전체 크롤이 완료되지 않은 활성 스트리머 ID 목록을 반환합니다."""
    stmt = (
        select(Streamer.channel_id)
        .where(Streamer.is_active.is_(True), Streamer.is_initial_crawled.is_(False))
        .order_by(Streamer.channel_name)
    )
    async with orm_session.get_session() as session:
        result = await session.execute(stmt)
        return list(result.scalars().all())
