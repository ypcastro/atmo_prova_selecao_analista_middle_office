from __future__ import annotations

from typing import Any


def _avg(records: list[dict[str, Any]], field: str) -> float | None:
    values: list[float] = []
    for record in records:
        value = record.get(field)
        if isinstance(value, (int, float)):
            values.append(float(value))
    if not values:
        return None
    return sum(values) / len(values)


def run_analysis(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute simple serializable metrics over persisted records."""
    if not records:
        return {
            "total_records": 0,
            "reservatorios_count": 0,
            "date_min": None,
            "date_max": None,
            "avg_volume_util_pct": None,
            "avg_afluencia_m3s": None,
            "avg_balanco_vazao_m3s": None,
            "by_uf": {},
            "by_situacao_hidrologica": {},
        }

    dates = sorted(str(r["data_medicao"]) for r in records if r.get("data_medicao"))
    reservatorios = {
        r.get("reservatorio_id")
        for r in records
        if r.get("reservatorio_id") is not None
    }
    by_uf: dict[str, int] = {}
    by_status: dict[str, int] = {}

    for record in records:
        uf = str(record.get("uf") or "NA").upper()
        by_uf[uf] = by_uf.get(uf, 0) + 1

        status = str(record.get("situacao_hidrologica") or "indefinido").lower()
        by_status[status] = by_status.get(status, 0) + 1

    return {
        "total_records": len(records),
        "reservatorios_count": len(reservatorios),
        "date_min": dates[0] if dates else None,
        "date_max": dates[-1] if dates else None,
        "avg_volume_util_pct": _avg(records, "volume_util_pct"),
        "avg_afluencia_m3s": _avg(records, "afluencia_m3s"),
        "avg_balanco_vazao_m3s": _avg(records, "balanco_vazao_m3s"),
        "by_uf": by_uf,
        "by_situacao_hidrologica": by_status,
    }
