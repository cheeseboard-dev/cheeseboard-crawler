from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def _to_async_url(url: str) -> str:
    if url.startswith("postgresql+asyncpg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


async def init_engine() -> None:
    global _engine, _session_factory
    if _engine is not None:
        return
    _engine = create_async_engine(_to_async_url(settings.database_url), pool_size=5, max_overflow=0)
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)


async def close_engine() -> None:
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None


def get_session() -> AsyncSession:
    if _session_factory is None:
        raise RuntimeError("DB engine not initialized")
    return _session_factory()
