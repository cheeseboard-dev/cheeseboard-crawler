from __future__ import annotations

import logging
from typing import Any

from elasticsearch import AsyncElasticsearch, NotFoundError
from elasticsearch.helpers import async_bulk

import app.models.clip as clip_models
import app.models.video as video_models
from app.config import settings

logger = logging.getLogger(__name__)

INDEX_VIDEOS = "cheeseboard_videos"
INDEX_CLIPS = "cheeseboard_clips"

_ANALYSIS: dict[str, Any] = {
    "tokenizer": {
        "nori_mixed": {"type": "nori_tokenizer", "decompound_mode": "mixed"},
    },
    "analyzer": {
        "korean": {
            "type": "custom",
            "tokenizer": "nori_mixed",
            "filter": ["lowercase"],
        }
    },
}

_VIDEO_SETTINGS: dict[str, Any] = {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "analysis": _ANALYSIS,
}

_VIDEO_MAPPINGS: dict[str, Any] = {
    "properties": {
        "video_no": {"type": "long"},
        "video_id": {"type": "keyword"},
        "channel_id": {"type": "keyword"},
        "channel_name": {"type": "keyword"},
        "title": {"type": "text", "analyzer": "korean"},
        "category": {"type": "keyword"},
        "tags": {"type": "keyword"},
        "read_count": {"type": "integer"},
        "duration": {"type": "integer"},
        "published_at": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis",
        },
        "thumbnail_url": {"type": "keyword", "index": False},
    }
}

_CLIP_SETTINGS: dict[str, Any] = {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "analysis": _ANALYSIS,
}

_CLIP_MAPPINGS: dict[str, Any] = {
    "properties": {
        "clip_uid": {"type": "keyword"},
        "channel_id": {"type": "keyword"},
        "channel_name": {"type": "keyword"},
        "title": {"type": "text", "analyzer": "korean"},
        "read_count": {"type": "integer"},
        "duration": {"type": "integer"},
        "created_at": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis",
        },
        "thumbnail_url": {"type": "keyword", "index": False},
        "origin_video_id": {"type": "keyword"},
    }
}


class EsClient:
    def __init__(self) -> None:
        self._client: AsyncElasticsearch | None = None

    @property
    def enabled(self) -> bool:
        return bool(settings.elasticsearch_url)

    async def start(self) -> None:
        if not self.enabled:
            logger.info("elasticsearch_url not set — ES indexing disabled")
            return
        self._client = AsyncElasticsearch(settings.elasticsearch_url)
        await self._ensure_indices()
        logger.info("Elasticsearch client started: %s", settings.elasticsearch_url)

    async def stop(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None

    async def _ensure_indices(self) -> None:
        assert self._client
        for index, idx_settings, idx_mappings in [
            (INDEX_VIDEOS, _VIDEO_SETTINGS, _VIDEO_MAPPINGS),
            (INDEX_CLIPS, _CLIP_SETTINGS, _CLIP_MAPPINGS),
        ]:
            try:
                await self._client.indices.get(index=index)
            except NotFoundError:
                await self._client.indices.create(
                    index=index,
                    settings=idx_settings,
                    mappings=idx_mappings,
                )
                logger.info("Created ES index: %s", index)

    async def bulk_index_videos(
        self,
        channel_name: str,
        channel_id: str,
        videos: list[video_models.VideoResponse],
    ) -> int:
        if self._client is None or not videos:
            return 0
        try:
            actions = [
                {
                    "_op_type": "index",
                    "_index": INDEX_VIDEOS,
                    "_id": str(v.video_no),
                    "_source": {
                        "video_no": v.video_no,
                        "video_id": v.video_id,
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "title": v.title,
                        "category": v.category,
                        "tags": v.tags or [],
                        "read_count": v.read_count,
                        "duration": v.duration,
                        "published_at": v.published_at,
                        "thumbnail_url": v.thumbnail_url,
                    },
                }
                for v in videos
                if v.video_no is not None
            ]
            result = await async_bulk(self._client, actions, raise_on_error=False)
            ok: int = result[0]
            errors: list[Any] = result[1] if isinstance(result[1], list) else []
            if errors:
                logger.warning(
                    "ES index videos partial errors channel=%s count=%d", channel_id, len(errors)
                )
            return ok
        except Exception as e:
            logger.error("ES bulk_index_videos failed channel=%s: %s", channel_id, e)
            return 0

    async def bulk_index_clips(
        self,
        channel_name: str,
        channel_id: str,
        clips: list[clip_models.ClipResponse],
    ) -> int:
        if self._client is None or not clips:
            return 0
        try:
            actions = [
                {
                    "_op_type": "index",
                    "_index": INDEX_CLIPS,
                    "_id": c.clip_uid,
                    "_source": {
                        "clip_uid": c.clip_uid,
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "title": c.title,
                        "read_count": c.read_count,
                        "duration": c.duration,
                        "created_at": c.created_at,
                        "thumbnail_url": c.thumbnail_url,
                        "origin_video_id": c.origin_video_id,
                    },
                }
                for c in clips
            ]
            result = await async_bulk(self._client, actions, raise_on_error=False)
            ok: int = result[0]
            errors: list[Any] = result[1] if isinstance(result[1], list) else []
            if errors:
                logger.warning(
                    "ES index clips partial errors channel=%s count=%d", channel_id, len(errors)
                )
            return ok
        except Exception as e:
            logger.error("ES bulk_index_clips failed channel=%s: %s", channel_id, e)
            return 0


es_client = EsClient()
