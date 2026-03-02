"""Centralized structured logging configuration for pipeline components."""

from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path
from typing import Any

_CONSOLE_HANDLER_NAME = "ana_console_handler"
_FILE_HANDLER_NAME = "ana_daily_file_handler"
_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
_EVENT_FIELDS_ORDER = (
    "job_name",
    "step",
    "run_id",
    "status",
    "mode",
    "source",
    "data_inicial",
    "data_final",
    "window_source",
    "dry_run",
    "force",
    "processed",
    "inserted",
    "existing",
    "records_in",
    "records_out",
    "invalid",
    "duration_ms",
    "error",
    "next_run_utc",
    "sleep_s",
)


class _AppLoggerFilter(logging.Filter):
    """Filter that keeps only project loggers in file output."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.name.startswith("app.")


def _resolve_logs_dir() -> Path:
    """Resolve directory where daily log files are written."""
    env_value = os.environ.get("APP_LOG_DIR", "").strip()
    if env_value:
        return Path(env_value)
    # src/app/core/logging_setup.py -> repository root
    return Path(__file__).resolve().parents[3] / "logs"


def _ensure_console_handler(
    root_logger: logging.Logger,
    formatter: logging.Formatter,
    *,
    level: int,
) -> None:
    """Create or update the dedicated console handler."""
    handler = next(
        (
            h
            for h in root_logger.handlers
            if h.get_name() == _CONSOLE_HANDLER_NAME
            and isinstance(h, logging.StreamHandler)
            and not isinstance(h, logging.FileHandler)
        ),
        None,
    )
    if handler is None:
        handler = logging.StreamHandler()
        handler.set_name(_CONSOLE_HANDLER_NAME)
        root_logger.addHandler(handler)

    handler.setLevel(level)
    handler.setFormatter(formatter)


def _ensure_daily_file_handler(
    root_logger: logging.Logger,
    formatter: logging.Formatter,
    *,
    level: int,
) -> None:
    """Create or rotate the daily file handler for project logs."""
    logs_dir = _resolve_logs_dir()
    logs_dir.mkdir(parents=True, exist_ok=True)
    today_path = logs_dir / f"ana_pipeline_{date.today().isoformat()}.log"

    current = next(
        (
            h
            for h in root_logger.handlers
            if h.get_name() == _FILE_HANDLER_NAME and isinstance(h, logging.FileHandler)
        ),
        None,
    )

    if current is not None:
        current_path = Path(current.baseFilename)
        if current_path != today_path:
            root_logger.removeHandler(current)
            current.close()
            current = None

    if current is None:
        current = logging.FileHandler(today_path, encoding="utf-8")
        current.set_name(_FILE_HANDLER_NAME)
        current.addFilter(_AppLoggerFilter())
        root_logger.addHandler(current)

    current.setLevel(level)
    current.setFormatter(formatter)


def configure_structured_logging(log_level: str = "INFO") -> None:
    """Configure root logger with console and daily file handlers.

    Args:
        log_level: Logging level name, for example ``INFO`` or ``DEBUG``.

    Side Effects:
        Mutates global logging handlers and creates the logs directory if missing.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)
    formatter = logging.Formatter(_LOG_FORMAT)
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    _ensure_console_handler(root_logger, formatter, level=level)
    _ensure_daily_file_handler(root_logger, formatter, level=level)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def format_log_event(**fields: Any) -> str:
    """Render event fields in a deterministic ``key=value`` sequence.

    Args:
        **fields: Arbitrary structured event fields.

    Returns:
        str: Serialized event string with known keys first and extras sorted.
    """
    parts: list[str] = []
    for key in _EVENT_FIELDS_ORDER:
        if key not in fields:
            continue
        value = fields[key]
        if value is None:
            continue
        if isinstance(value, bool):
            rendered = "true" if value else "false"
        else:
            rendered = str(value)
        parts.append(f"{key}={rendered}")

    extra_keys = sorted(key for key in fields if key not in _EVENT_FIELDS_ORDER)
    for key in extra_keys:
        value = fields[key]
        if value is None:
            continue
        parts.append(f"{key}={value}")

    return " | ".join(parts)
