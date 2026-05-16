import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from app import db
from app.auth import require_api_key
from app.config import settings
from app.exception_handlers import cheeseboard_exception_handler, unhandled_exception_handler
from app.exceptions import CheeseBoardException
from app.log_config import setup_logging
from app.routers import channels, content, crawl, streamers
from app.services.chzzk_client import chzzk_client
from app.services.scheduler import start_scheduler, stop_scheduler

setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await chzzk_client.start()
    scheduler_started = False
    try:
        await db.init_pool()
        logging.getLogger(__name__).info("DB pool initialized")
        cleaned = await db.cleanup_stale_jobs()
        if cleaned:
            logging.getLogger(__name__).warning("stale running jobs cleaned up: %d", cleaned)
        start_scheduler()
        scheduler_started = True
    except Exception as e:
        logging.getLogger(__name__).warning("DB pool initialization failed: %s", e)
    yield
    if scheduler_started:
        stop_scheduler()
    await chzzk_client.stop()
    await db.close_pool()


app = FastAPI(
    title=settings.app_name,
    description="CHZZK VOD/clip crawler server",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_exception_handler(CheeseBoardException, cheeseboard_exception_handler)
app.add_exception_handler(Exception, unhandled_exception_handler)

app.include_router(channels.router, prefix="/api/v1", dependencies=[Depends(require_api_key)])
app.include_router(content.router, prefix="/api/v1", dependencies=[Depends(require_api_key)])
app.include_router(crawl.router, prefix="/api/v1", dependencies=[Depends(require_api_key)])
app.include_router(streamers.router, prefix="/api/v1", dependencies=[Depends(require_api_key)])


@app.get("/")
async def root():
    return {"service": settings.app_name, "status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=settings.port, reload=settings.debug)
