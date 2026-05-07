import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import db
from app.config import settings
from app.exception_handlers import cheeseboard_exception_handler, unhandled_exception_handler
from app.exceptions import CheeseBoardException
from app.routers import channels, crawl
from app.services.chzzk_client import chzzk_client

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await chzzk_client.start()
    try:
        await db.init_pool()
        logging.getLogger(__name__).info("DB pool initialized")
    except Exception as e:
        logging.getLogger(__name__).warning("DB pool 초기화 실패 (DB 없이 기동): %s", e)
    yield
    await chzzk_client.stop()
    await db.close_pool()


app = FastAPI(
    title=settings.app_name,
    description="CHZZK VOD/클립 크롤링 서버",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_exception_handler(CheeseBoardException, cheeseboard_exception_handler)
app.add_exception_handler(Exception, unhandled_exception_handler)

app.include_router(channels.router, prefix="/api/v1")
app.include_router(crawl.router, prefix="/api/v1")


@app.get("/")
async def root():
    return {"service": settings.app_name, "status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=settings.port, reload=settings.debug)
