from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime
from typing import Any, TypedDict

import certifi
import httpx

import app.models.clip as clip_models
import app.models.video as video_models
from app.config import settings
from app.exceptions import ChannelNotFoundException, ChzzkAPIException
from app.models.channel import ChannelResponse

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://chzzk.naver.com",
    "Referer": "https://chzzk.naver.com/",
    "Sec-Ch-Ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}


class LiveCursor(TypedDict):
    viewer_count: int
    live_id: int


class ChzzkClient:
    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None
        self._semaphore: asyncio.Semaphore | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            headers=_HEADERS,
            follow_redirects=True,
            timeout=settings.request_timeout,
            verify=certifi.where(),
        )
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_requests)

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()

    def _parse_date(self, date_val: Any) -> str:
        try:
            if isinstance(date_val, str):
                try:
                    dt = datetime.strptime(date_val, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    dt = datetime.strptime(date_val.split("+")[0].strip(), "%Y-%m-%dT%H:%M:%S")
            elif isinstance(date_val, int | float):
                if date_val > 1e11:
                    date_val /= 1000
                dt = datetime.fromtimestamp(date_val)
            else:
                dt = datetime.now()
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    async def _fetch(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
        assert self._semaphore is not None and self._client is not None
        async with self._semaphore:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            for attempt in range(settings.retry_count):
                try:
                    res = await self._client.get(url, params=params)
                    if res.status_code == 200:
                        result: dict[str, Any] = res.json()
                        return result
                    logger.warning(
                        "HTTP %d (attempt %d/%d): %s",
                        res.status_code,
                        attempt + 1,
                        settings.retry_count,
                        url,
                    )
                    await asyncio.sleep(2**attempt)
                except Exception as e:
                    logger.warning(
                        "요청 오류 (attempt %d/%d): %s — %s",
                        attempt + 1,
                        settings.retry_count,
                        url,
                        e,
                    )
                    if attempt < settings.retry_count - 1:
                        await asyncio.sleep(2**attempt)
            raise ChzzkAPIException(
                f"CHZZK API 요청 실패 (재시도 {settings.retry_count}회 초과): {url}"
            )

    async def get_channel(self, channel_id: str) -> ChannelResponse:
        url = f"{settings.chzzk_base_url}/channels/{channel_id}"
        data = await self._fetch(url)
        c = (data or {}).get("content", {})
        if not c.get("channelId"):
            raise ChannelNotFoundException(channel_id)
        return ChannelResponse(
            channel_id=c["channelId"],
            channel_name=c["channelName"],
            profile_image_url=c.get("channelImageUrl"),
            follower_count=c.get("followerCount", 0),
            is_live=c.get("openLive", False),
        )

    async def get_videos(
        self,
        channel_id: str,
        since: datetime | None = None,
        size: int = 30,
        sort_type: str = "LATEST",
        max_pages: int | None = settings.default_video_pages,
    ) -> list[video_models.VideoResponse]:
        url = f"{settings.chzzk_base_url}/channels/{channel_id}/videos"
        since_str = since.strftime("%Y-%m-%d %H:%M:%S") if since else None
        result: list[video_models.VideoResponse] = []
        page = 0

        while max_pages is None or page < max_pages:
            data = await self._fetch(url, {"size": size, "sortType": sort_type, "page": page})
            if not data:
                break
            items = data.get("content", {}).get("data", [])
            if not items:
                break
            cutoff_hit = False
            for v in items:
                try:
                    date_key = "publishDateAt" if "publishDateAt" in v else "publishDate"
                    published_str = self._parse_date(v.get(date_key))
                    if since_str and published_str and published_str < since_str:
                        cutoff_hit = True
                        break
                    result.append(
                        video_models.VideoResponse(
                            video_no=v["videoNo"],
                            video_id=v.get("videoId"),
                            title=v["videoTitle"],
                            category=v.get("videoCategoryValue", "미지정"),
                            tags=v.get("tags", []),
                            published_at=published_str,
                            read_count=v.get("readCount", 0),
                            duration=v.get("duration", 0),
                            thumbnail_url=v.get("thumbnailImageUrl"),
                            link=f"https://chzzk.naver.com/video/{v['videoNo']}",
                        )
                    )
                except Exception as e:
                    logger.warning("video parse failed (video_no=%s): %s", v.get("videoNo"), e)
            if cutoff_hit or len(items) < size:
                break
            page += 1

        return result

    async def get_video(self, video_no: int) -> tuple[str, video_models.VideoResponse] | None:
        url = f"{settings.chzzk_base_url}/videos/{video_no}"
        data = await self._fetch(url)
        content = (data or {}).get("content")
        if not content:
            return None
        channel_id = content.get("channelId") or (content.get("channel") or {}).get("channelId")
        if not channel_id:
            return None
        date_key = "publishDateAt" if "publishDateAt" in content else "publishDate"
        published_str = self._parse_date(content.get(date_key))
        video = video_models.VideoResponse(
            video_no=content["videoNo"],
            video_id=content.get("videoId"),
            title=content["videoTitle"],
            category=content.get("videoCategoryValue", "미지정"),
            tags=content.get("tags", []),
            published_at=published_str,
            read_count=content.get("readCount", 0),
            duration=content.get("duration", 0),
            thumbnail_url=content.get("thumbnailImageUrl"),
            link=f"https://chzzk.naver.com/video/{content['videoNo']}",
        )
        return channel_id, video

    async def get_clips(
        self,
        channel_id: str,
        since: datetime | None = None,
        size: int = 50,
        sort_type: str = "LATEST",
        max_pages: int | None = settings.default_clip_pages,
    ) -> list[clip_models.ClipResponse]:
        url = f"{settings.chzzk_base_url}/channels/{channel_id}/clips"
        since_str = since.strftime("%Y-%m-%d %H:%M:%S") if since else None
        result: list[clip_models.ClipResponse] = []
        cursor: str | None = None
        page = 0

        while max_pages is None or page < max_pages:
            params: dict[str, Any] = {"size": size, "sortType": sort_type}
            if cursor:
                params["clipUID"] = cursor
            data = await self._fetch(url, params)
            if not data:
                break
            items = data.get("content", {}).get("data", [])
            if not items:
                break
            cutoff_hit = False
            for c in items:
                try:
                    created_str = self._parse_date(c.get("createdDate"))
                    if since_str and created_str and created_str < since_str:
                        cutoff_hit = True
                        break
                    result.append(
                        clip_models.ClipResponse(
                            clip_uid=c["clipUID"],
                            title=c["clipTitle"],
                            created_at=created_str,
                            read_count=c.get("readCount", 0),
                            duration=c.get("duration", 0),
                            thumbnail_url=c.get("thumbnailImageUrl"),
                            origin_video_id=c.get("videoId"),
                            link=f"https://chzzk.naver.com/clips/{c['clipUID']}",
                        )
                    )
                except Exception as e:
                    logger.warning("clip parse failed (clip_uid=%s): %s", c.get("clipUID"), e)
            if cutoff_hit or len(items) < size:
                break
            cursor = items[-1]["clipUID"]
            page += 1

        return result

    async def get_clip(self, clip_uid: str) -> tuple[str, clip_models.ClipResponse] | None:
        url = f"{settings.chzzk_base_url}/clips/{clip_uid}"
        data = await self._fetch(url)
        content = (data or {}).get("content")
        if not content:
            return None
        channel_id = content.get("channelId") or (content.get("channel") or {}).get("channelId")
        if not channel_id:
            return None
        created_str = self._parse_date(content.get("createdDate"))
        clip = clip_models.ClipResponse(
            clip_uid=content["clipUID"],
            title=content["clipTitle"],
            created_at=created_str,
            read_count=content.get("readCount", 0),
            duration=content.get("duration", 0),
            thumbnail_url=content.get("thumbnailImageUrl"),
            origin_video_id=content.get("videoId"),
            link=f"https://chzzk.naver.com/clips/{content['clipUID']}",
        )
        return channel_id, clip

    async def get_live_page(
        self,
        min_viewers: int = 100,
        size: int = 50,
        cursor_viewer_count: int | None = None,
        cursor_live_id: int | None = None,
    ) -> tuple[list[str], LiveCursor | None]:
        url = f"{settings.chzzk_base_url}/lives"
        params: dict[str, Any] = {"size": size, "sortType": "POPULAR"}
        if cursor_viewer_count is not None and cursor_live_id is not None:
            params["concurrentUserCount"] = cursor_viewer_count
            params["liveId"] = cursor_live_id

        data = await self._fetch(url, params)
        items = (data or {}).get("content", {}).get("data", [])
        if not items:
            return [], None

        channel_ids: list[str] = []
        last_item: dict[str, Any] | None = None
        for item in items:
            if item.get("concurrentUserCount", 0) < min_viewers:
                logger.debug("live page stopped below viewer threshold: %d", min_viewers)
                return channel_ids, None
            channel_id = (item.get("channel") or {}).get("channelId")
            if channel_id:
                channel_ids.append(channel_id)
            last_item = item

        next_cursor: LiveCursor | None = None
        if last_item:
            next_cursor = LiveCursor(
                viewer_count=last_item["concurrentUserCount"],
                live_id=last_item["liveId"],
            )
        return channel_ids, next_cursor


chzzk_client = ChzzkClient()
