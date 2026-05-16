from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from app.models.channel import ChannelResponse
from app.models.clip import ClipResponse
from app.models.video import VideoResponse


class ErrorResponse(BaseModel):
    error: str
    message: str


class CrawlChannelResponse(BaseModel):
    channel: ChannelResponse
    videos: list[VideoResponse]
    clips: list[ClipResponse]
    crawled_at: str


class JobStartedResponse(BaseModel):
    job_id: str
    status: str
    total: int


class JobResponse(BaseModel):
    job_id: str
    job_type: str | None = None
    status: str
    total: int | None = None
    processed: int | None = None
    failed: int | None = None
    triggered_by: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_msg: str | None = None
    failed_channels: list[str] | None = None


class BulkRegisterResponse(BaseModel):
    total: int
    success: int
    failed: int
    failed_channels: list[str]


class StreamerRow(BaseModel):
    channel_id: str
    channel_name: str
    profile_image_url: str | None = None
    follower_count: int = 0
    is_active: bool = True
    is_initial_crawled: bool = False
    updated_at: datetime | None = None
    last_crawled_at: datetime | None = None
    last_refreshed_at: datetime | None = None


class StreamerStats(BaseModel):
    channel_id: str
    channel_name: str
    is_active: bool
    follower_count: int
    video_count: int
    clip_count: int
    latest_video_published_at: datetime | None = None
    latest_clip_created_at: datetime | None = None
    last_crawled_at: datetime | None = None


class StreamerActiveResponse(BaseModel):
    channel_id: str
    is_active: bool
