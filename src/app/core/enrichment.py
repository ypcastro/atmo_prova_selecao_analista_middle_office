from __future__ import annotations

from typing import Any

from app.core.subsystems import infer_subsistema_from_uf_text

_TEXT_META_FIELDS = (
    "uf",
    "subsistema",
)


def classify_hydrologic_status(volume_util_pct: float | None) -> str:
    """Classify hydrologic status based on useful volume percentage."""
    if volume_util_pct is None:
        return "indefinido"
    if volume_util_pct >= 80.0:
        return "alto"
    if volume_util_pct >= 50.0:
        return "normal"
    if volume_util_pct >= 30.0:
        return "atencao"
    return "critico"


def enrich_record(
    row: dict[str, Any],
    metadata_by_reservoir: dict[int, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Enrich one normalized record with optional metadata and derived metrics."""
    enriched = dict(row)
    metadata_by_reservoir = metadata_by_reservoir or {}

    reservoir_id = enriched.get("reservatorio_id")
    meta = (
        metadata_by_reservoir.get(reservoir_id, {})
        if isinstance(reservoir_id, int)
        else {}
    )

    for field in _TEXT_META_FIELDS:
        value = enriched.get(field)
        if value in (None, ""):
            enriched[field] = meta.get(field)

    afluencia = enriched.get("afluencia_m3s")
    defluencia = enriched.get("defluencia_m3s")
    if isinstance(afluencia, (int, float)) and isinstance(defluencia, (int, float)):
        enriched["balanco_vazao_m3s"] = float(afluencia) - float(defluencia)
    else:
        enriched["balanco_vazao_m3s"] = None

    if enriched.get("situacao_hidrologica") in (None, ""):
        volume_util = enriched.get("volume_util_pct")
        if isinstance(volume_util, (int, float)):
            enriched["situacao_hidrologica"] = classify_hydrologic_status(
                float(volume_util)
            )
        else:
            enriched["situacao_hidrologica"] = classify_hydrologic_status(None)

    uf = enriched.get("uf")
    if isinstance(uf, str):
        enriched["uf"] = uf.strip().upper() or None

    subsistema = enriched.get("subsistema")
    if isinstance(subsistema, str):
        subsistema = subsistema.strip().upper() or None
    if subsistema in (None, ""):
        subsistema = infer_subsistema_from_uf_text(enriched.get("uf"))
    enriched["subsistema"] = subsistema

    return enriched
