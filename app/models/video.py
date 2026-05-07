from pydantic import BaseModel
from typing import List, Optional


class Video(BaseModel):
    video_no: int
    title: str
    category: str = "미지정"
    tags: List[str] = []
    published_at: Optional[str] = None
    read_count: int = 0
    duration: int = 0
    link: str
