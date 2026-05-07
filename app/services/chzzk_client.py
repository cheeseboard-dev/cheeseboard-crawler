from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime
from typing import Any, Optional, TypedDict

import certifi
import httpx

from app.config import settings
from app.exceptions import ChannelNotFoundException, ChzzkAPIException
from app.models.channel import ChannelInfo
from app.models.clip import Clip
from app.models.video import Video

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
        self._client: Optional[httpx.AsyncClient] = None
        self._semaphore: Optional[asyncio.Semaphore] = None

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
            elif isinstance(date_val, (int, float)):
                if date_val > 1e11:
                    date_val /= 1000
                dt = datetime.fromtimestamp(date_val)
            else:
                dt = datetime.now()
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    async def _fetch(self, url: str, params: Optional[dict[str, Any]] = None) -> Optional[dict[str, Any]]:
        assert self._semaphore is not None and self._client is not None
        async with self._semaphore:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            for attempt in range(settings.retry_count):
                try:
                    res = await self._client.get(url, params=params)
                    if res.status_code == 200:
                        return res.json()
                    logger.warning("HTTP %d (attempt %d/%d): %s", res.status_code, attempt + 1, settings.retry_count, url)
                    await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    logger.warning("요청 오류 (attempt %d/%d): %s — %s", attempt + 1, settings.retry_count, url, e)
                    if attempt < settings.retry_count - 1:
                        await asyncio.sleep(2 ** attempt)
            raise ChzzkAPIException(f"CHZZK API 요청 실패 (재시도 {settings.retry_count}회 초과): {url}")

    async def get_channel(self, channel_id: str) -> ChannelInfo:
        url = f"{settings.chzzk_base_url}/channels/{channel_id}"
        data = await self._fetch(url)
        c = (data or {}).get("content", {})
        if not c.get("channelId"):
            raise ChannelNotFoundException(channel_id)
        return ChannelInfo(
            channel_id=c["channelId"],
            channel_name=c["channelName"],
            profile_image_url=c.get("channelImageUrl"),
            follower_count=c.get("followerCount", 0),
            is_live=c.get("openLive", False),
        )

    async def get_videos(
        self, channel_id: str, size: int = 30, sort_type: str = "LATEST"
    ) -> list[Video]:
        url = f"{settings.chzzk_base_url}/channels/{channel_id}/videos"
        data = await self._fetch(url, {"size": size, "sortType": sort_type})
        if not data:
            return []
        result: list[Video] = []
        for v in data.get("content", {}).get("data", []):
            try:
                date_key = "publishDateAt" if "publishDateAt" in v else "publishDate"
                result.append(Video(
                    video_no=v["videoNo"],
                    video_id=v["videoId"],
                    title=v["videoTitle"],
                    category=v.get("videoCategoryValue", "미지정"),
                    tags=v.get("tags", []),
                    published_at=self._parse_date(v.get(date_key)),
                    read_count=v.get("readCount", 0),
                    duration=v.get("duration", 0),
                    thumbnail_url=v.get("thumbnailImageUrl"),
                    link=f"https://chzzk.naver.com/video/{v['videoNo']}",
                ))
            except Exception as e:
                logger.warning("video 파싱 스킵 (video_no=%s): %s", v.get("videoNo"), e)
        return result

    async def get_clips(
        self, channel_id: str, size: int = 50, sort_type: str = "LATEST"
    ) -> list[Clip]:
        url = f"{settings.chzzk_base_url}/channels/{channel_id}/clips"
        data = await self._fetch(url, {"size": size, "sortType": sort_type})
        if not data:
            return []
        result: list[Clip] = []
        for c in data.get("content", {}).get("data", []):
            try:
                result.append(Clip(
                    clip_uid=c["clipUID"],
                    title=c["clipTitle"],
                    created_at=self._parse_date(c.get("createdDate")),
                    read_count=c.get("readCount", 0),
                    duration=c.get("duration", 0),
                    thumbnail_url=c.get("thumbnailImageUrl"),
                    origin_video_id=c.get("videoId"),
                    link=f"https://chzzk.naver.com/clips/{c['clipUID']}",
                ))
            except Exception as e:
                logger.warning("clip 파싱 스킵 (clip_uid=%s): %s", c.get("clipUID"), e)
        return result

    async def get_live_page(
        self,
        min_viewers: int = 100,
        size: int = 50,
        cursor_viewer_count: Optional[int] = None,
        cursor_live_id: Optional[int] = None,
    ) -> tuple[list[str], Optional[LiveCursor]]:
        """라이브 채널 1페이지를 fetch해 channel_id 목록과 다음 커서를 반환."""
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
        last_item: Optional[dict[str, Any]] = None
        for item in items:
            if item.get("concurrentUserCount", 0) < min_viewers:
                logger.debug("시청자 기준 미달 — 페이지 종료 (기준: %d명)", min_viewers)
                return channel_ids, None
            channel_id = (item.get("channel") or {}).get("channelId")
            if channel_id:
                channel_ids.append(channel_id)
            last_item = item

        next_cursor: Optional[LiveCursor] = None
        if last_item:
            next_cursor = LiveCursor(
                viewer_count=last_item["concurrentUserCount"],
                live_id=last_item["liveId"],
            )
        return channel_ids, next_cursor


chzzk_client = ChzzkClient()
