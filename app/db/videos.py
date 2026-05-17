from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

import app.models.video as video_models
from app.db.pool import _category_or_none, _parse_dt
from app.orm import Video
from app.orm import session as orm_session


async def get_channel_video_watermark(channel_id: str) -> datetime | None:
    async with orm_session.get_session() as session:
        result = await session.execute(
            select(func.max(Video.published_at)).where(Video.channel_id == channel_id)
        )
        row: datetime | None = result.scalar_one_or_none()
        return row


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
