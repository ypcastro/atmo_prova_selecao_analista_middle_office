from __future__ import annotations

from app.core.enrichment import classify_hydrologic_status, enrich_record


def test_classify_hydrologic_status_thresholds():
    assert classify_hydrologic_status(None) == "indefinido"
    assert classify_hydrologic_status(20.0) == "critico"
    assert classify_hydrologic_status(35.0) == "atencao"
    assert classify_hydrologic_status(60.0) == "normal"
    assert classify_hydrologic_status(85.0) == "alto"


def test_enrich_record_merges_metadata_and_derives_balance():
    base = {
        "record_id": "19091-2025-10-01",
        "reservatorio_id": 19091,
        "reservatorio": "SANTA BRANCA",
        "data_medicao": "2025-10-01",
        "afluencia_m3s": 112.65,
        "defluencia_m3s": 124.00,
        "volume_util_pct": 37.22,
    }
    metadata = {
        19091: {
            "uf": "SP",
            "subsistema": None,
        }
    }

    enriched = enrich_record(base, metadata)

    assert enriched["uf"] == "SP"
    assert (
        enriched["balanco_vazao_m3s"] == base["afluencia_m3s"] - base["defluencia_m3s"]
    )
    assert enriched["situacao_hidrologica"] == "atencao"
