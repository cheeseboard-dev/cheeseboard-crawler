from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy import update as sa_update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.channel import ChannelResponse
from app.orm import Clip, Streamer, Video
from app.orm import session as orm_session


async def streamer_exists(channel_id: str) -> bool:
    async with orm_session.get_session() as session:
        result = await session.execute(
            select(Streamer.channel_id).where(Streamer.channel_id == channel_id).limit(1)
        )
        return result.scalar_one_or_none() is not None


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
        },
    )
    async with orm_session.get_session() as session:
        await session.execute(stmt)
        await session.commit()


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


async def get_uncrawled_channel_ids() -> list[str]:
    stmt = (
        select(Streamer.channel_id)
        .where(Streamer.is_active.is_(True), Streamer.is_initial_crawled.is_(False))
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
