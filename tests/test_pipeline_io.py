import tempfile
from pathlib import Path

from app.core.pipeline_io import PipelineIO


def test_checkpoint_roundtrip():
    with tempfile.TemporaryDirectory() as td:
        data_dir = Path(td)
        (data_dir / "out").mkdir(parents=True, exist_ok=True)

        io = PipelineIO(data_dir)
        io.write_checkpoint(status="success", inserted=1, existing=2, meta={"mode": "snapshot"})
        ck = io.read_checkpoint()

        assert ck["status"] == "success"
        assert ck["inserted"] == 1
        assert ck["existing"] == 2
        assert ck["meta"]["mode"] == "snapshot"
