"""Structured (JSON-lines) logging shared by every component."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

_RESERVED = set(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Anything passed through ``extra=`` becomes a top-level field.
        payload.update({k: v for k, v in record.__dict__.items() if k not in _RESERVED})
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure(level: str | None = None) -> None:
    """Install the JSON handler on the root logger (idempotent)."""
    level = (level or os.environ.get("QC_LOG_LEVEL", "INFO")).upper()
    root = logging.getLogger()
    if any(isinstance(h.formatter, JsonFormatter) for h in root.handlers):
        root.setLevel(level)
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.handlers[:] = [handler]
    root.setLevel(level)
    for noisy in ("py4j", "urllib3", "kafka", "botocore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get(name: str) -> logging.Logger:
    return logging.getLogger(name)
