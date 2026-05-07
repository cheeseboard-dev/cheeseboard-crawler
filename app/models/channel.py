from __future__ import annotations

from pydantic import BaseModel


class ChannelInfo(BaseModel):
    channel_id: str
    channel_name: str
    profile_image_url: str | None = None
    follower_count: int = 0
    is_live: bool = False
