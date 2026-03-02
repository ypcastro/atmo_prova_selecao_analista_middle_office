import tempfile
from pathlib import Path

from app.core.pipeline_io import PipelineIO


def test_checkpoint_roundtrip():
    with tempfile.TemporaryDirectory() as td:
        data_dir = Path(td)
        (data_dir / "out").mkdir(parents=True, exist_ok=True)

        io = PipelineIO(data_dir)
        io.write_checkpoint(
            status="success", inserted=1, existing=2, meta={"mode": "snapshot"}
        )
        ck = io.read_checkpoint()

        assert ck["status"] == "success"
        assert ck["inserted"] == 1
        assert ck["existing"] == 2
        assert ck["meta"]["mode"] == "snapshot"


def test_watermark_roundtrip():
    with tempfile.TemporaryDirectory() as td:
        data_dir = Path(td)
        io = PipelineIO(data_dir)

        io.write_watermark("live:19091", "2025-01-15")
        assert io.read_watermark_value("live:19091") == "2025-01-15"

        all_watermarks = io.read_watermarks()
        assert all_watermarks is not None
        assert "live:19091" in all_watermarks
        assert all_watermarks["live:19091"]["value"] == "2025-01-15"
