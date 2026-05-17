from __future__ import annotations

import uuid

from sqlalchemy import Text, func, select
from sqlalchemy import update as sa_update
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.orm import CrawlJob
from app.orm import session as orm_session


async def insert_crawl_job(
    job_type: str,
    total_streamers: int | None = None,
    triggered_by: str | None = None,
) -> str:
    stmt = (
        pg_insert(CrawlJob)
        .values(
            job_type=job_type,
            total_streamers=total_streamers,
            triggered_by=triggered_by,
        )
        .returning(CrawlJob.id)
    )
    async with orm_session.get_session() as session:
        result = await session.execute(stmt)
        await session.commit()
        return str(result.scalar_one())


async def has_running_job_of_type(job_type: str) -> bool:
    async with orm_session.get_session() as session:
        result = await session.execute(
            select(CrawlJob.id)
            .where(CrawlJob.job_type == job_type, CrawlJob.status == "running")
            .limit(1)
        )
        return result.scalar_one_or_none() is not None


async def update_crawl_job(
    job_id: str,
    *,
    status: str | None = None,
    total_streamers: int | None = None,
    success_count: int | None = None,
    failed_count: int | None = None,
    error_msg: str | None = None,
    failed_channels: list[str] | None = None,
) -> None:
    values: dict[str, object] = {}
    if status is not None:
        values["status"] = status
        if status in ("done", "failed"):
            values["finished_at"] = func.now()
    if total_streamers is not None:
        values["total_streamers"] = total_streamers
    if success_count is not None:
        values["success_count"] = success_count
    if failed_count is not None:
        values["failed_count"] = failed_count
    if error_msg is not None:
        values["error_msg"] = error_msg
    if failed_channels is not None:
        values["failed_channels"] = failed_channels
    if not values:
        return
    stmt = sa_update(CrawlJob).where(CrawlJob.id == uuid.UUID(job_id)).values(**values)
    async with orm_session.get_session() as session:
        await session.execute(stmt)
        await session.commit()


async def get_crawl_jobs(limit: int = 10) -> list[dict]:
    stmt = (
        select(
            CrawlJob.id,
            CrawlJob.job_type,
            CrawlJob.started_at,
            CrawlJob.finished_at,
            CrawlJob.status,
            CrawlJob.total_streamers,
            CrawlJob.success_count,
            CrawlJob.failed_count,
            CrawlJob.triggered_by,
            CrawlJob.error_msg,
            CrawlJob.failed_channels,
        )
        .order_by(CrawlJob.started_at.desc())
        .limit(limit)
    )
    async with orm_session.get_session() as session:
        result = await session.execute(stmt)
        return [dict(row) for row in result.mappings().all()]


async def get_crawl_job(job_id: str) -> dict | None:
    stmt = select(
        CrawlJob.id,
        CrawlJob.job_type,
        CrawlJob.started_at,
        CrawlJob.finished_at,
        CrawlJob.status,
        CrawlJob.total_streamers,
        CrawlJob.success_count,
        CrawlJob.failed_count,
        CrawlJob.triggered_by,
        CrawlJob.error_msg,
        CrawlJob.failed_channels,
    ).where(CrawlJob.id == uuid.UUID(job_id))
    async with orm_session.get_session() as session:
        result = await session.execute(stmt)
        row = result.mappings().one_or_none()
        return dict(row) if row else None


async def cleanup_stale_jobs() -> int:
    stmt = (
        sa_update(CrawlJob)
        .where(CrawlJob.status == "running")
        .values(
            status="failed",
            finished_at=func.now(),
            error_msg="server restarted while job was running",
        )
    )
    async with orm_session.get_session() as session:
        result = await session.execute(stmt)
        await session.commit()
        return int(result.rowcount or 0)


async def increment_job_progress(
    job_id: str,
    *,
    success: bool,
    failed_channel: str | None = None,
) -> bool:
    """성공/실패 카운트를 원자적으로 증가. 모든 채널이 처리됐으면 True 반환."""
    if success:
        stmt = (
            sa_update(CrawlJob)
            .where(CrawlJob.id == uuid.UUID(job_id))
            .values(success_count=CrawlJob.success_count + 1)
            .returning(CrawlJob.success_count, CrawlJob.failed_count, CrawlJob.total_streamers)
        )
    else:
        stmt = (
            sa_update(CrawlJob)
            .where(CrawlJob.id == uuid.UUID(job_id))
            .values(failed_count=CrawlJob.failed_count + 1)
            .returning(CrawlJob.success_count, CrawlJob.failed_count, CrawlJob.total_streamers)
        )
    async with orm_session.get_session() as session:
        result = await session.execute(stmt)
        if not success and failed_channel:
            await session.execute(
                sa_update(CrawlJob)
                .where(CrawlJob.id == uuid.UUID(job_id))
                .values(
                    failed_channels=func.array_append(
                        func.coalesce(CrawlJob.failed_channels, func.cast([], ARRAY(Text))),
                        failed_channel,
                    )
                )
            )
        await session.commit()
        row = result.one()
        total = row.total_streamers or 0
        if total <= 0:
            return False
        return bool((row.success_count + row.failed_count) >= total)
