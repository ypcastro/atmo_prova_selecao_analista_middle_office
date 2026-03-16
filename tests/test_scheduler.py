from datetime import datetime, timezone
from app.jobs.scheduler import compute_next_run


def test_compute_next_run_no_drift():
    last = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    nxt = compute_next_run(last, 60)
    assert (nxt - last).total_seconds() == 60
