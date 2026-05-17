import logging
import sys
from typing import Any

from app.core.config import settings

JsonFormatter: Any
try:
    from pythonjsonlogger import json as _json_logger

    JsonFormatter = _json_logger.JsonFormatter
except ImportError:
    from pythonjsonlogger.jsonlogger import JsonFormatter as _LegacyJsonFormatter

    JsonFormatter = _LegacyJsonFormatter


def setup_logging() -> None:
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    if settings.log_format == "json":
        formatter: logging.Formatter = JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s",
            rename_fields={"asctime": "timestamp", "levelname": "level", "name": "logger"},
        )
    else:
        formatter = logging.Formatter("%(levelname)s %(name)s: %(message)s")
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(logging.INFO)
