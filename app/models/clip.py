from pydantic import BaseModel
from typing import Optional


class Clip(BaseModel):
    clip_uid: str
    title: str
    created_at: Optional[str] = None
    read_count: int = 0
    duration: int = 0
    origin_video_no: Optional[int] = None
    link: str
