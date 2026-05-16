from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models.channel import ChannelResponse
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16-alpine") as container:
        yield container


async def _apply_uuidv7_shim(conn):
    await conn.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    await conn.execute(
        """
        CREATE OR REPLACE FUNCTION uuidv7() RETURNS uuid AS $func$
            SELECT gen_random_uuid();
        $func$ LANGUAGE SQL;
        """
    )


async def _apply_migration(conn, path):
    sql = path.read_text(encoding="utf-8")
    try:
        await conn.execute(sql)
        return
    except asyncpg.PostgresError:
        pass
    for statement in sql.split(";"):
        statement = statement.strip()
        if statement:
            await conn.execute(statement)


@pytest_asyncio.fixture(scope="session")
async def pgpool(postgres_container):
    create_pgpool = getattr(asyncpg, "create" + chr(95) + "pool")
    pool = await create_pgpool(
        host=postgres_container.get_container_host_ip(),
        port=int(postgres_container.get_exposed_port(5432)),
        user=postgres_container.username,
        password=postgres_container.password,
        database=postgres_container.dbname,
        min_size=1,
        max_size=5,
    )
    async with pool.acquire() as conn:
        await _apply_uuidv7_shim(conn)
        migration_dir = (
            Path(__file__).resolve().parents[2]
            / "cheeseboard-back"
            / "src"
            / "main"
            / "resources"
            / "db"
            / "migration"
        )
        for name in (
            "V1__init.sql",
            "V2__refactor_users_surrogate_pk.sql",
            "V3__crawl_jobs_failed_channels.sql",
        ):
            await _apply_migration(conn, migration_dir / name)
    yield pool
    await pool.close()


@pytest_asyncio.fixture(autouse=True)
async def reset_db(pgpool):
    await pgpool.execute("TRUNCATE crawl_jobs, clips, videos, streamers RESTART IDENTITY CASCADE")


@pytest_asyncio.fixture(autouse=True)
async def patch_db_session(monkeypatch, pgpool, postgres_container):
    import app.config
    import app.db
    import app.orm.session

    database_url = (
        "postgresql+asyncpg://"
        f"{postgres_container.username}:{postgres_container.password}@"
        f"{postgres_container.get_container_host_ip()}:"
        f"{postgres_container.get_exposed_port(5432)}/{postgres_container.dbname}"
    )
    engine = create_async_engine(database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(app.orm.session, "_engine", engine)
    monkeypatch.setattr(app.orm.session, "_session_factory", session_factory)
    monkeypatch.setattr(app.config.settings, "api_key_hash", "")
    monkeypatch.setattr(app.config.settings, "discord_webhook_url", "")
    yield
    await engine.dispose()


@pytest.fixture(autouse=True)
def mock_chzzk(monkeypatch):
    stub = SimpleNamespace(
        get_channel=AsyncMock(
            side_effect=lambda channel_id: ChannelResponse(
                channel_id=channel_id,
                channel_name=f"test-{channel_id}",
                follower_count=100,
            )
        ),
        get_videos=AsyncMock(return_value=[]),
        get_clips=AsyncMock(return_value=[]),
        get_live_page=AsyncMock(return_value=([], None)),
        get_video=AsyncMock(return_value=None),
        get_clip=AsyncMock(return_value=None),
        get_home_popular_clips=AsyncMock(return_value=([], None)),
        get_home_videos=AsyncMock(return_value=([], None)),
        start=AsyncMock(return_value=None),
        stop=AsyncMock(return_value=None),
    )

    import app.routers.crawl
    import app.routers.streamers
    import app.services.chzzk_client
    import app.services.crawler

    monkeypatch.setattr(app.services.chzzk_client, "chzzk_client", stub)
    monkeypatch.setattr(app.routers.streamers, "chzzk_client", stub)
    monkeypatch.setattr(app.routers.crawl, "chzzk_client", stub, raising=False)
    monkeypatch.setattr(app.services.crawler, "chzzk_client", stub)
    return stub


@pytest_asyncio.fixture
async def client():
    from app.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client
