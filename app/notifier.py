import logging

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


async def send_alert(title: str, message: str, level: str = "warning") -> None:
    if settings.discord_webhook_url == "":
        return

    color_map = {
        "info": 0x3498DB,
        "warning": 0xF39C12,
        "error": 0xE74C3C,
    }
    payload = {
        "embeds": [
            {
                "title": title,
                "description": message,
                "color": color_map.get(level, 0x95A5A6),
            }
        ]
    }
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(settings.discord_webhook_url, json=payload)
    except Exception as e:
        logger.warning("discord alert failed: %s", e)
