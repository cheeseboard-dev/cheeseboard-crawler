from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from app.orm.base import Base


class Video(Base):
    __tablename__ = "videos"

    video_no: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    video_id: Mapped[str] = mapped_column(String(40), nullable=False, unique=True)
    channel_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("streamers.channel_id"), nullable=False
    )
    title: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str | None] = mapped_column(String(100))
    tags: Mapped[list[str] | None] = mapped_column(ARRAY(Text))
    read_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    duration: Mapped[int | None] = mapped_column(Integer)
    published_at: Mapped[datetime | None] = mapped_column(DateTime)
    thumbnail_url: Mapped[str | None] = mapped_column(Text)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime)
