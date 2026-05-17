import hashlib
import hmac

from fastapi import Header

from app.core.config import settings
from app.core.exceptions import InvalidApiKeyException


async def require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")):
    if not settings.api_key_hash:
        return None
    if not x_api_key:
        raise InvalidApiKeyException()
    incoming_hash = hashlib.sha256(x_api_key.encode()).hexdigest()
    if not hmac.compare_digest(incoming_hash, settings.api_key_hash):
        raise InvalidApiKeyException()
    return None
