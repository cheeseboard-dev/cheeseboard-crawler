from pydantic import BaseModel
from typing import Optional


class ChannelInfo(BaseModel):
    channel_id: str
    channel_name: str
    channel_image_url: Optional[str] = None
    follower_count: int = 0
    is_live: bool = False
