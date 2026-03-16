import pytest
from app.core.transforms import dedupe, normalize_record, validate_record
from datetime import datetime

def test_dedupe_preserves_first():
    rows = [{"record_id": "A", "x": 1}, {"record_id": "A", "x": 2}, {"record_id": "B", "x": 3}]
    assert dedupe(rows, "record_id") == [{"record_id": "A", "x": 1}, {"record_id": "B", "x": 3}]


def test_normalize_builds_iso_and_record_id():
    r = normalize_record({"reservatorio_id": "19091", "reservatorio": " X ", "data_medicao": "01/10/2025"})
    assert r["record_id"] == "19091-2025-10-01"
    assert r["reservatorio_id"] == 19091
    assert r["reservatorio"] == "X"
    assert r["data_medicao"] == "2025-10-01"


def test_validate_raises_on_missing_required():
    with pytest.raises(ValueError):
        validate_record({"record_id": None})
