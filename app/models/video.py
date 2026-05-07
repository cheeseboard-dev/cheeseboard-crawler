from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class Video(BaseModel):
    video_no: int
    video_id: str
    title: str
    category: str = "미지정"
    tags: list[str] = Field(default_factory=list)
    published_at: Optional[str] = None
    read_count: int = 0
    duration: int = 0
    thumbnail_url: Optional[str] = None
    link: str
