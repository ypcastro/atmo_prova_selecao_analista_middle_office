from datetime import date, datetime, timezone
from pathlib import Path

from app.core.config import Settings
from app.core.pipeline_io import PipelineIO
from app.jobs.extract_job import RunOptions, _resolve_window, main, run_once


def _prepare_snapshot(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    snapshot_src = Path("data") / "ana_snapshot.html"
    snapshot_dst = data_dir / "ana_snapshot.html"
    snapshot_dst.write_text(snapshot_src.read_text(encoding="utf-8"), encoding="utf-8")


def test_cli_dry_run_does_not_write_db(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    _prepare_snapshot(data_dir)

    monkeypatch.setenv("APP_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ANA_MODE", "snapshot")
    monkeypatch.setenv("ANA_RESERVATORIO", "19091")
    monkeypatch.setenv("ANA_DATA_INICIAL", "2025-10-01")
    monkeypatch.setenv("ANA_DATA_FINAL", "2025-10-07")

    exit_code = main(["--dry-run", "--log-level", "INFO"])
    assert exit_code == 0

    db_path = data_dir / "out" / "ana.db"
    assert not db_path.exists()

    checkpoint = PipelineIO(data_dir).read_checkpoint()
    assert checkpoint is not None
    assert checkpoint["status"] == "dry_run"


def test_resolve_window_uses_watermark_unless_force(tmp_path):
    data_dir = tmp_path / "data"
    io = PipelineIO(data_dir)
    io.write_watermark("live:19091", "2025-01-15")

    settings = Settings(
        data_dir=data_dir,
        ana_mode="live",
        pipeline_interval_seconds=60,
        ana_reservatorio=19091,
        ana_data_inicial=date(2025, 1, 1),
        ana_data_final=date(2025, 1, 31),
    )

    start, end, key, source = _resolve_window(
        settings,
        io,
        RunOptions(),
        now_utc=datetime(2025, 1, 20, 12, 0, tzinfo=timezone.utc),
    )
    assert start == date(2025, 1, 16)
    assert end == date(2025, 1, 20)
    assert key == "live:19091"
    assert source == "watermark"

    start_force, end_force, _, source_force = _resolve_window(
        settings,
        io,
        RunOptions(force=True),
        now_utc=datetime(2025, 1, 20, 12, 0, tzinfo=timezone.utc),
    )
    assert start_force == date(2025, 1, 1)
    assert end_force == date(2025, 1, 31)
    assert source_force == "settings"


def test_run_once_logs_structured_events(monkeypatch, tmp_path, caplog):
    data_dir = tmp_path / "data"
    _prepare_snapshot(data_dir)

    monkeypatch.setenv("APP_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ANA_MODE", "snapshot")

    with caplog.at_level("INFO", logger="app.jobs.extract_job"):
        payload = run_once(options=RunOptions(dry_run=True, log_level="INFO"))

    assert payload["status"] == "dry_run"

    messages = [
        record.message
        for record in caplog.records
        if record.name == "app.jobs.extract_job"
    ]
    assert any("run_id=" in message for message in messages)
    assert any("step=start" in message for message in messages)
    assert any("step=finish" in message for message in messages)


def test_run_once_writes_daily_log_file(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    logs_dir = tmp_path / "logs"
    _prepare_snapshot(data_dir)

    monkeypatch.setenv("APP_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ANA_MODE", "snapshot")
    monkeypatch.setenv("APP_LOG_DIR", str(logs_dir))

    payload = run_once(options=RunOptions(dry_run=True, log_level="INFO"))
    assert payload["status"] == "dry_run"

    log_files = list(logs_dir.glob("ana_pipeline_*.log"))
    assert len(log_files) == 1

    content = log_files[0].read_text(encoding="utf-8")
    assert "step=start" in content
    assert "step=finish" in content
