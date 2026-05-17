from __future__ import annotations

from datetime import datetime

from app.orm import session as orm_session

init_pool = orm_session.init_engine
close_pool = orm_session.close_engine


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(value.split("+")[0].strip(), fmt)
        except ValueError:
            continue
    return None


def _category_or_none(value: str) -> str | None:
    if value == "미지정":
        return None
    return value
