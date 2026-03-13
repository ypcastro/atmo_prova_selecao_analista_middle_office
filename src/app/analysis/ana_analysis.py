"""
Q7 — Análise dos dados ANA: métricas + interpretação.

compute_analysis(records) → dict com estatísticas e interpretação textual.
"""

import statistics
from typing import Any, Dict, List, Optional


def _stats(values: List[float]) -> Dict[str, Optional[float]]:
    """Calcula estatísticas básicas de uma lista de valores não-nulos."""
    clean = [v for v in values if v is not None]
    if not clean:
        return {"min": None, "max": None, "mean": None, "median": None, "count": 0}
    return {
        "min": round(min(clean), 4),
        "max": round(max(clean), 4),
        "mean": round(statistics.mean(clean), 4),
        "median": round(statistics.median(clean), 4),
        "count": len(clean),
    }


def compute_analysis(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calcula métricas agregadas e gera interpretação textual dos dados ANA.

    Métricas computadas:
      - cota_m: nível do reservatório
      - afluencia_m3s: vazão afluente
      - defluencia_m3s: vazão defluente
      - volume_util_pct: volume útil
      - balanço hídrico médio (afluência − defluência)
      - tendência de cota (subindo/descendo/estável)
      - interpretação textual

    Args:
        records: lista de registros normalizados do banco.

    Returns:
        Dict com métricas, balanço e interpretação.
    """
    if not records:
        return {
            "total_records": 0,
            "periodo": None,
            "metricas": {},
            "balanco_hidrico_medio": None,
            "tendencia_cota": "sem dados",
            "interpretacao": "Nenhum dado disponível para análise.",
        }

    # Ordena por data para tendência
    sorted_recs = sorted(records, key=lambda r: r.get("data_iso", ""))

    cotas = [r.get("cota_m") for r in sorted_recs]
    afluencias = [r.get("afluencia_m3s") for r in sorted_recs]
    defluencias = [r.get("defluencia_m3s") for r in sorted_recs]
    volumes = [r.get("volume_util_pct") for r in sorted_recs]

    # Balanço hídrico: afluência − defluência (quando ambos disponíveis)
    balancos = [
        a - d
        for a, d in zip(afluencias, defluencias)
        if a is not None and d is not None
    ]
    balanco_medio = round(statistics.mean(balancos), 4) if balancos else None

    # Tendência de cota: compara primeira e última cota válida
    cotas_validas = [c for c in cotas if c is not None]
    if len(cotas_validas) >= 2:
        delta = cotas_validas[-1] - cotas_validas[0]
        if delta > 0.1:
            tendencia = "subindo"
        elif delta < -0.1:
            tendencia = "descendo"
        else:
            tendencia = "estável"
    else:
        tendencia = "indeterminada"
        delta = None

    # Período coberto
    datas = [r.get("data_iso") for r in sorted_recs if r.get("data_iso")]
    periodo = {"inicio": datas[0], "fim": datas[-1]} if datas else None

    # Interpretação textual
    interpretacao_partes = []

    if periodo:
        interpretacao_partes.append(
            f"Período analisado: {periodo['inicio']} a {periodo['fim']} "
            f"({len(records)} medições)."
        )

    s_cota = _stats(cotas)
    if s_cota["count"] > 0:
        interpretacao_partes.append(
            f"Cota do reservatório variou de {s_cota['min']} m a {s_cota['max']} m "
            f"(média {s_cota['mean']} m). Tendência: {tendencia}."
        )

    s_vol = _stats(volumes)
    if s_vol["count"] > 0:
        vol_medio = s_vol["mean"]
        nivel_str = (
            "crítico (< 30%)" if vol_medio < 30
            else "alerta (30-50%)" if vol_medio < 50
            else "confortável (> 50%)"
        )
        interpretacao_partes.append(
            f"Volume útil médio: {vol_medio}% — nível {nivel_str}."
        )

    if balanco_medio is not None:
        sinal = "positivo (acumulando)" if balanco_medio > 0 else "negativo (perdendo)"
        interpretacao_partes.append(
            f"Balanço hídrico médio: {balanco_medio:.2f} m³/s ({sinal})."
        )

    interpretacao = " ".join(interpretacao_partes) or "Dados insuficientes para interpretação."

    return {
        "total_records": len(records),
        "periodo": periodo,
        "metricas": {
            "cota_m": s_cota,
            "afluencia_m3s": _stats(afluencias),
            "defluencia_m3s": _stats(defluencias),
            "volume_util_pct": s_vol,
        },
        "balanco_hidrico_medio_m3s": balanco_medio,
        "tendencia_cota": tendencia,
        "variacao_cota_m": round(delta, 4) if delta is not None else None,
        "interpretacao": interpretacao,
    }
