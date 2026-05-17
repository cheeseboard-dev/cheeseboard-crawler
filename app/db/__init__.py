from app.db.clips import get_channel_clip_watermark, upsert_clips
from app.db.jobs import (
    cleanup_stale_jobs,
    get_crawl_job,
    get_crawl_jobs,
    has_running_job_of_type,
    increment_job_progress,
    insert_crawl_job,
    update_crawl_job,
)
from app.db.pool import close_pool, init_pool
from app.db.streamers import (
    get_active_channel_ids,
    get_channel_name,
    get_streamer_stats,
    get_streamers,
    get_uncrawled_channel_ids,
    set_initial_crawled,
    set_streamer_active,
    streamer_exists,
    upsert_streamer,
)
from app.db.videos import get_channel_video_watermark, upsert_videos

__all__ = [
    "init_pool",
    "close_pool",
    "streamer_exists",
    "upsert_streamer",
    "get_streamers",
    "get_streamer_stats",
    "get_channel_name",
    "get_active_channel_ids",
    "get_uncrawled_channel_ids",
    "set_streamer_active",
    "set_initial_crawled",
    "upsert_videos",
    "get_channel_video_watermark",
    "upsert_clips",
    "get_channel_clip_watermark",
    "insert_crawl_job",
    "has_running_job_of_type",
    "update_crawl_job",
    "get_crawl_jobs",
    "get_crawl_job",
    "cleanup_stale_jobs",
    "increment_job_progress",
]
