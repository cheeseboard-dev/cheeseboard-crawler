from fastapi import Request
from fastapi.responses import JSONResponse

from app.exceptions import CheeseBoardException


async def cheeseboard_exception_handler(request: Request, exc: CheeseBoardException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.code, "message": exc.message},
    )


async def unhandled_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "error": "INTERNAL_SERVER_ERROR",
            "message": "서버 내부 오류가 발생했습니다.",
        },
    )
