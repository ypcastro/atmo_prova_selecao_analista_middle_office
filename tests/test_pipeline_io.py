"""Testes Q2 — pipeline_io.py"""

import json
import os
import tempfile
from pathlib import Path

import pytest

from app.core.pipeline_io import PipelineIO


@pytest.fixture
def tmp_io(tmp_path):
    """PipelineIO com diretório temporário."""
    return PipelineIO(base_dir=str(tmp_path))


class TestPipelineIO:
    def test_save_raw_html_creates_file(self, tmp_io, tmp_path):
        path = tmp_io.save_raw_html("<html>test</html>", "run_001")
        assert path.exists()
        assert path.read_text(encoding="utf-8") == "<html>test</html>"

    def test_save_raw_html_in_raw_dir(self, tmp_io, tmp_path):
        path = tmp_io.save_raw_html("content", "run_001")
        assert "raw" in str(path)

    def test_save_normalized_json(self, tmp_io):
        records = [{"record_id": "19091-2025-10-01", "cota_m": 615.22}]
        path = tmp_io.save_normalized_json(records, "run_001")
        assert path.exists()
        loaded = json.loads(path.read_text(encoding="utf-8"))
        assert loaded[0]["record_id"] == "19091-2025-10-01"

    def test_save_checkpoint_success(self, tmp_io):
        path = tmp_io.save_checkpoint(
            run_id="run_001",
            success=True,
            inserted=5,
            existing=2,
        )
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["success"] is True
        assert data["inserted"] == 5
        assert data["existing"] == 2
        assert data["error"] is None

    def test_save_checkpoint_failure(self, tmp_io):
        path = tmp_io.save_checkpoint(
            run_id="run_fail",
            success=False,
            error="Erro de teste",
        )
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["success"] is False
        assert data["error"] == "Erro de teste"

    def test_latest_checkpoint_created(self, tmp_io):
        tmp_io.save_checkpoint("run_001", success=True, inserted=3)
        cp = tmp_io.load_latest_checkpoint()
        assert cp is not None
        assert cp["run_id"] == "run_001"
        assert cp["inserted"] == 3

    def test_latest_checkpoint_overwritten(self, tmp_io):
        tmp_io.save_checkpoint("run_001", success=True, inserted=3)
        tmp_io.save_checkpoint("run_002", success=True, inserted=7)
        cp = tmp_io.load_latest_checkpoint()
        assert cp["run_id"] == "run_002"

    def test_no_checkpoint_returns_none(self, tmp_io):
        assert tmp_io.load_latest_checkpoint() is None

    def test_safe_write_no_tmp_left(self, tmp_io, tmp_path):
        """Arquivo .tmp não deve existir após escrita bem-sucedida."""
        tmp_io.save_raw_html("content", "run_safe")
        tmp_files = list((tmp_path / "raw").glob("*.tmp"))
        assert tmp_files == []

    def test_directories_created_automatically(self, tmp_path):
        nested = tmp_path / "deeply" / "nested"
        io = PipelineIO(base_dir=str(nested))
        # Apenas instanciar deve criar os diretórios
        assert (nested / "raw").exists()
        assert (nested / "normalized").exists()
        assert (nested / "checkpoints").exists()
