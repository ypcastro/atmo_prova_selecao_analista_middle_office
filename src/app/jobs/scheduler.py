"""Simple fixed-interval scheduler for repeated ANA extraction runs."""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timedelta, timezone

from app.core.config import load_settings
from app.core.logging_setup import configure_structured_logging, format_log_event
from app.jobs.extract_job import run_once


def compute_next_run(last_run_utc: datetime, interval_s: int) -> datetime:
    """Compute next run based on the previous scheduled run timestamp."""
    if last_run_utc.tzinfo is None:
        last_run_utc = last_run_utc.replace(tzinfo=timezone.utc)

    safe_interval = max(1, int(interval_s))
    return last_run_utc + timedelta(seconds=safe_interval)


def _configure_logging() -> logging.Logger:
    configure_structured_logging("INFO")
    return logging.getLogger("app.jobs.scheduler")


def _log_event(logger: logging.Logger, level: int, **fields: object) -> None:
    logger.log(level, format_log_event(**fields))


def main_loop() -> None:
    """Run extraction indefinitely at configured intervals.

    Side Effects:
        Executes job runs, writes logs, and sleeps between ticks forever.
    """
    logger = _configure_logging()
    s = load_settings()
    interval = max(1, int(s.pipeline_interval_seconds))

    _log_event(
        logger,
        logging.INFO,
        job_name="scheduler",
        step="start",
        interval_s=interval,
        mode=s.ana_mode,
    )

    last_run = datetime.now(timezone.utc) - timedelta(seconds=interval)
    while True:
        tick_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc)
        next_run = compute_next_run(last_run, interval)
        sleep_s = (next_run - now).total_seconds()
        if sleep_s > 0:
            _log_event(
                logger,
                logging.DEBUG,
                job_name="scheduler",
                run_id=tick_id,
                step="sleep",
                sleep_s=round(sleep_s, 2),
                next_run_utc=next_run.isoformat(),
            )
            time.sleep(min(sleep_s, interval))

        try:
            run_once()
            _log_event(
                logger,
                logging.INFO,
                job_name="scheduler",
                run_id=tick_id,
                step="tick_success",
                next_run_utc=next_run.isoformat(),
            )
        except Exception as exc:
            _log_event(
                logger,
                logging.ERROR,
                job_name="scheduler",
                run_id=tick_id,
                step="tick_fail",
                error=str(exc),
                next_run_utc=next_run.isoformat(),
            )

        # Keep schedule anchored to expected cadence to reduce drift.
        last_run = next_run


if __name__ == "__main__":
    main_loop()
