"""Testes Q7 — jobs/extract_job.py e análise"""

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from app.analysis.ana_analysis import compute_analysis
from app.jobs.extract_job import run_once


@pytest.fixture(autouse=True)
def set_env(tmp_path, monkeypatch):
    """Configura ambiente para modo snapshot com diretórios temporários."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    # Copia snapshot para tmp
    snapshot_src = Path("data/ana_snapshot.html")
    if snapshot_src.exists():
        (data_dir / "ana_snapshot.html").write_bytes(snapshot_src.read_bytes())

    monkeypatch.setenv("APP_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ANA_MODE", "snapshot")
    monkeypatch.setenv("ANA_RESERVATORIO", "19091")
    monkeypatch.setenv("ANA_DATA_INICIAL", "2025-10-01")
    monkeypatch.setenv("ANA_DATA_FINAL", "2025-10-07")


class TestRunOnce:
    def test_returns_success(self, tmp_path):
        db_path = str(tmp_path / "data" / "test.db")
        base_dir = str(tmp_path / "data")
        result = run_once(db_path=db_path, base_dir=base_dir)
        assert result["success"] is True

    def test_returns_run_id(self, tmp_path):
        result = run_once(
            db_path=str(tmp_path / "data" / "test.db"),
            base_dir=str(tmp_path / "data"),
        )
        assert "run_id" in result
        assert len(result["run_id"]) > 0

    def test_inserts_records(self, tmp_path):
        result = run_once(
            db_path=str(tmp_path / "data" / "test.db"),
            base_dir=str(tmp_path / "data"),
        )
        assert result["inserted"] > 0

    def test_idempotent_second_run(self, tmp_path):
        db = str(tmp_path / "data" / "test.db")
        bd = str(tmp_path / "data")
        r1 = run_once(db_path=db, base_dir=bd)
        r2 = run_once(db_path=db, base_dir=bd)
        assert r2["inserted"] == 0
        assert r2["existing"] == r1["inserted"]

    def test_saves_checkpoint(self, tmp_path):
        bd = str(tmp_path / "data")
        run_once(db_path=str(tmp_path / "data" / "test.db"), base_dir=bd)
        latest = Path(bd) / "checkpoints" / "latest.json"
        assert latest.exists()
        cp = json.loads(latest.read_text())
        assert cp["success"] is True

    def test_saves_raw_html(self, tmp_path):
        bd = str(tmp_path / "data")
        run_once(db_path=str(tmp_path / "data" / "test.db"), base_dir=bd)
        raw_files = list((Path(bd) / "raw").glob("*.html"))
        assert len(raw_files) == 1

    def test_saves_normalized_json(self, tmp_path):
        bd = str(tmp_path / "data")
        run_once(db_path=str(tmp_path / "data" / "test.db"), base_dir=bd)
        norm_files = list((Path(bd) / "normalized").glob("*.json"))
        assert len(norm_files) == 1

    def test_error_on_missing_snapshot(self, tmp_path, monkeypatch):
        bad_dir = tmp_path / "empty"
        bad_dir.mkdir()
        monkeypatch.setenv("APP_DATA_DIR", str(bad_dir))
        result = run_once(
            db_path=str(bad_dir / "test.db"),
            base_dir=str(bad_dir),
        )
        assert result["success"] is False
        assert result["error"] is not None


class TestComputeAnalysis:
    _RECORDS = [
        {
            "record_id": "19091-2025-10-01",
            "data_iso": "2025-10-01",
            "cota_m": 615.22,
            "afluencia_m3s": 1234.56,
            "defluencia_m3s": 980.0,
            "volume_util_pct": 72.3,
        },
        {
            "record_id": "19091-2025-10-07",
            "data_iso": "2025-10-07",
            "cota_m": 616.10,
            "afluencia_m3s": 1000.0,
            "defluencia_m3s": 900.0,
            "volume_util_pct": 75.0,
        },
    ]

    def test_total_records(self):
        result = compute_analysis(self._RECORDS)
        assert result["total_records"] == 2

    def test_periodo(self):
        result = compute_analysis(self._RECORDS)
        assert result["periodo"]["inicio"] == "2025-10-01"
        assert result["periodo"]["fim"] == "2025-10-07"

    def test_tendencia_cota_subindo(self):
        result = compute_analysis(self._RECORDS)
        assert result["tendencia_cota"] == "subindo"

    def test_tendencia_cota_descendo(self):
        records = [
            {"data_iso": "2025-10-01", "cota_m": 616.0, "afluencia_m3s": None, "defluencia_m3s": None, "volume_util_pct": None},
            {"data_iso": "2025-10-07", "cota_m": 614.0, "afluencia_m3s": None, "defluencia_m3s": None, "volume_util_pct": None},
        ]
        result = compute_analysis(records)
        assert result["tendencia_cota"] == "descendo"

    def test_balanco_hidrico(self):
        result = compute_analysis(self._RECORDS)
        # Média de (1234.56-980.0) e (1000.0-900.0) = (254.56+100)/2 = 177.28
        assert result["balanco_hidrico_medio_m3s"] == pytest.approx(177.28)

    def test_metricas_keys(self):
        result = compute_analysis(self._RECORDS)
        for k in ("cota_m", "afluencia_m3s", "defluencia_m3s", "volume_util_pct"):
            assert k in result["metricas"]

    def test_interpretacao_not_empty(self):
        result = compute_analysis(self._RECORDS)
        assert len(result["interpretacao"]) > 10

    def test_empty_records(self):
        result = compute_analysis([])
        assert result["total_records"] == 0
        assert result["tendencia_cota"] == "sem dados"
