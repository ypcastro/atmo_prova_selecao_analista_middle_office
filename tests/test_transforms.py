"""Testes Q4 — core/transforms.py"""

import pytest

from app.core.transforms import deduplicar, normalize_record, validate_record

_VALID_RAW = {
    "record_id": "19091-2025-10-01",
    "reservatorio_id": "19091",
    "data_hora": "2025-10-01T00:00:00",
    "data_iso": "2025-10-01",
    "cota_m": 615.22,
    "afluencia_m3s": 1234.56,
    "defluencia_m3s": 980.0,
    "vazao_vertida_m3s": 0.0,
    "vazao_turbinada_m3s": 980.0,
    "nivel_montante_m": 615.22,
    "volume_util_pct": 72.3,
}


class TestNormalizeRecord:
    def test_returns_canonical_fields(self):
        norm = normalize_record(_VALID_RAW)
        assert "record_id" in norm
        assert "reservatorio_id" in norm
        assert "data_hora" in norm
        assert "cota_m" in norm

    def test_string_float_converted(self):
        raw = {**_VALID_RAW, "cota_m": "615,22", "afluencia_m3s": "1.234,56"}
        norm = normalize_record(raw)
        assert norm["cota_m"] == pytest.approx(615.22)
        assert norm["afluencia_m3s"] == pytest.approx(1234.56)

    def test_absent_value_becomes_none(self):
        raw = {**_VALID_RAW, "cota_m": "—"}
        norm = normalize_record(raw)
        assert norm["cota_m"] is None

    def test_none_value_stays_none(self):
        raw = {**_VALID_RAW, "defluencia_m3s": None}
        norm = normalize_record(raw)
        assert norm["defluencia_m3s"] is None

    def test_date_string_preserved(self):
        norm = normalize_record(_VALID_RAW)
        assert "2025-10-01" in norm["data_hora"]

    def test_does_not_modify_original(self):
        original = dict(_VALID_RAW)
        normalize_record(_VALID_RAW)
        assert _VALID_RAW == original

    def test_record_id_built_from_parts_if_missing(self):
        raw = {**_VALID_RAW}
        del raw["record_id"]
        norm = normalize_record(raw)
        assert norm["record_id"] == "19091-2025-10-01"


class TestValidateRecord:
    def test_valid_record_no_exception(self):
        norm = normalize_record(_VALID_RAW)
        validate_record(norm)  # deve passar sem erro

    def test_missing_record_id_raises(self):
        raw = {**_VALID_RAW, "record_id": None}
        norm = normalize_record(raw)
        norm["record_id"] = None
        with pytest.raises(ValueError, match="record_id"):
            validate_record(norm)

    def test_missing_reservatorio_raises(self):
        raw = {**_VALID_RAW, "reservatorio_id": ""}
        norm = normalize_record(raw)
        with pytest.raises(ValueError, match="reservatorio_id"):
            validate_record(norm)

    def test_missing_data_hora_raises(self):
        norm = normalize_record(_VALID_RAW)
        norm["data_hora"] = None
        with pytest.raises(ValueError, match="data_hora"):
            validate_record(norm)

    def test_invalid_volume_raises(self):
        raw = {**_VALID_RAW, "volume_util_pct": 150.0}
        norm = normalize_record(raw)
        with pytest.raises(ValueError, match="volume_util_pct"):
            validate_record(norm)

    def test_negative_volume_raises(self):
        raw = {**_VALID_RAW, "volume_util_pct": -1.0}
        norm = normalize_record(raw)
        with pytest.raises(ValueError, match="volume_util_pct"):
            validate_record(norm)

    def test_negative_cota_raises(self):
        raw = {**_VALID_RAW, "cota_m": -10.0}
        norm = normalize_record(raw)
        with pytest.raises(ValueError, match="cota_m"):
            validate_record(norm)

    def test_none_optional_fields_valid(self):
        raw = {**_VALID_RAW, "defluencia_m3s": None, "vazao_vertida_m3s": None}
        norm = normalize_record(raw)
        validate_record(norm)  # campos opcionais None são válidos


class TestDeduplicar:
    def test_removes_duplicates(self):
        records = [
            {"record_id": "A", "val": 1},
            {"record_id": "B", "val": 2},
            {"record_id": "A", "val": 99},  # duplicata
        ]
        result = deduplicar(records)
        assert len(result) == 2
        ids = [r["record_id"] for r in result]
        assert "A" in ids
        assert "B" in ids

    def test_keeps_first_occurrence(self):
        records = [
            {"record_id": "A", "val": 1},
            {"record_id": "A", "val": 2},
        ]
        result = deduplicar(records)
        assert result[0]["val"] == 1

    def test_empty_list(self):
        assert deduplicar([]) == []

    def test_no_duplicates_unchanged(self):
        records = [{"record_id": "A"}, {"record_id": "B"}, {"record_id": "C"}]
        result = deduplicar(records)
        assert len(result) == 3

    def test_complexity_is_linear(self):
        """Verifica que o algoritmo é O(n) via uso de dict."""
        N = 10_000
        records = [{"record_id": str(i % 100), "i": i} for i in range(N)]
        result = deduplicar(records)
        assert len(result) == 100
