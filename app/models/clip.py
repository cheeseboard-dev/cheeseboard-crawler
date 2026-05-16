from __future__ import annotations

from pydantic import BaseModel


class ClipResponse(BaseModel):
    clip_uid: str
    title: str
    created_at: str | None = None
    read_count: int = 0
    duration: int = 0
    thumbnail_url: str | None = None
    origin_video_id: str | None = None
    link: str
