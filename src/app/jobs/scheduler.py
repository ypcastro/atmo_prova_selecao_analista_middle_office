from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from app.core.config import load_settings
from app.jobs.extract_job import run_once


def compute_next_run(last_run_utc: datetime, interval_s: int) -> datetime:
    """TODO (Q7): calcular próxima execução sem drift grosseiro."""
    return last_run_utc + timedelta(seconds=interval_s)

def main_loop() -> None:
    s = load_settings()
    interval = max(1, int(s.pipeline_interval_seconds))

    last_run = datetime.now(timezone.utc) - timedelta(seconds=interval)
    while True:
        now = datetime.now(timezone.utc)
        next_run = compute_next_run(last_run, interval)
        sleep_s = (next_run - now).total_seconds()
        if sleep_s > 0:
            time.sleep(min(sleep_s, interval))

        try:
            run_once()
        except Exception:
            pass

        last_run = datetime.now(timezone.utc)


if __name__ == "__main__":
    main_loop()
