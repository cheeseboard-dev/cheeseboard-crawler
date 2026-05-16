from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from app.orm.base import Base


class Clip(Base):
    __tablename__ = "clips"

    clip_uid: Mapped[str] = mapped_column(String(20), primary_key=True)
    channel_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("streamers.channel_id"), nullable=False
    )
    origin_video_id: Mapped[str | None] = mapped_column(String(40), ForeignKey("videos.video_id"))
    title: Mapped[str] = mapped_column(Text, nullable=False)
    read_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    duration: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime | None] = mapped_column(DateTime)
    thumbnail_url: Mapped[str | None] = mapped_column(Text)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime)
