from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class Clip(BaseModel):
    clip_uid: str
    title: str
    created_at: Optional[str] = None
    read_count: int = 0
    duration: int = 0
    thumbnail_url: Optional[str] = None
    origin_video_id: Optional[str] = None
    link: str
