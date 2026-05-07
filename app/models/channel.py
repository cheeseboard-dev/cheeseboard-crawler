from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class ChannelInfo(BaseModel):
    channel_id: str
    channel_name: str
    profile_image_url: Optional[str] = None
    follower_count: int = 0
    is_live: bool = False
