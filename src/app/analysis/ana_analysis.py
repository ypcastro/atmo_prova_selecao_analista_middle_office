from __future__ import annotations

from typing import Any


def run_analysis(records: list[dict[str, Any]]) -> dict[str, Any]:
    """TODO (Q7): calcular métricas simples e retornar dict serializável."""
    if not records:
        return {"count":0}
    return {
        "count":len(records),
        "reservatorios": list(set(r['reservatorio'] for r in records)),
        "datas":sorted({r["data_medicao"] for r in records if r.get("data_medicao")})
    }
