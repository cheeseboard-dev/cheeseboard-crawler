from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

import app.models.clip as clip_models
from app.db.pool import _parse_dt
from app.orm import Clip, Video
from app.orm import session as orm_session

logger = logging.getLogger(__name__)


async def get_channel_clip_watermark(channel_id: str) -> datetime | None:
    async with orm_session.get_session() as session:
        result = await session.execute(
            select(func.max(Clip.created_at)).where(Clip.channel_id == channel_id)
        )
        row: datetime | None = result.scalar_one_or_none()
        return row


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
