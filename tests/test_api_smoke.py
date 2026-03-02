from pathlib import Path

from fastapi.testclient import TestClient

from app.api.main import app


def _prepare_snapshot(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    src = Path("data") / "ana_snapshot.html"
    dst = data_dir / "ana_snapshot.html"
    dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")


def test_health(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    _prepare_snapshot(data_dir)
    monkeypatch.setenv("APP_DATA_DIR", str(data_dir))
    client = TestClient(app)

    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_required_endpoints_smoke(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    _prepare_snapshot(data_dir)

    monkeypatch.setenv("APP_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ANA_MODE", "snapshot")
    monkeypatch.setenv("ANA_RESERVATORIO", "19091")
    monkeypatch.setenv("ANA_DATA_INICIAL", "2025-10-01")
    monkeypatch.setenv("ANA_DATA_FINAL", "2025-10-07")

    client = TestClient(app)

    assert client.post("/extract/ana").status_code == 200
    assert client.get("/ana/checkpoint").status_code == 200
    assert client.get("/ana/analysis").status_code == 200

    list_resp = client.get("/ana/medicoes", params={"limit": 10})
    assert list_resp.status_code == 200
    rows = list_resp.json()
    assert isinstance(rows, list)
    assert len(rows) > 0

    record_id = rows[0]["record_id"]
    assert client.get(f"/ana/medicoes/{record_id}").status_code == 200
    assert client.get("/ana/medicoes/__does_not_exist__").status_code == 404


def test_list_medicoes_applies_filters_before_limit(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    _prepare_snapshot(data_dir)

    monkeypatch.setenv("APP_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ANA_MODE", "snapshot")

    client = TestClient(app)
    assert client.post("/extract/ana").status_code == 200

    resp = client.get("/ana/medicoes", params={"reservatorio_id": 19091, "limit": 2})
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) == 2
    assert all(row["reservatorio_id"] == 19091 for row in rows)
