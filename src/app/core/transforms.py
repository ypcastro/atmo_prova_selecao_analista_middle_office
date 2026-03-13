"""
Q4 — Normalização, validação e deduplicação de registros.

normalize_record(raw)  → dict com tipos e nomes canônicos
validate_record(record) → levanta ValueError se regras mínimas falharem
deduplicar(records)    → O(n) por record_id, mantém primeira ocorrência
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.parsing import parse_date_mixed, safe_float_ptbr

# Campos numéricos opcionais (podem ser None = dado ausente)
_OPTIONAL_FLOATS = {
    "cota_m",
    "afluencia_m3s",
    "defluencia_m3s",
    "vazao_vertida_m3s",
    "vazao_turbinada_m3s",
    "nivel_montante_m",
    "volume_util_pct",
}

# Intervalo aceitável para volume útil (%)
_VOLUME_MIN, _VOLUME_MAX = 0.0, 100.0
# Cota mínima razoável para evitar dados corrompidos (m)
_COTA_MIN = 0.0


def normalize_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza um registro bruto para schema canônico.

    - Garante que data_hora seja string ISO-8601
    - Converte campos numéricos via safe_float_ptbr quando ainda strings
    - Preenche campos ausentes com None
    - Retorna novo dict sem modificar o original
    """
    record: Dict[str, Any] = {}

    # --- reservatorio_id ---
    record["reservatorio_id"] = str(raw.get("reservatorio_id", "")).strip() or None

    # --- data_hora ---
    dh = raw.get("data_hora")
    if isinstance(dh, datetime):
        record["data_hora"] = dh.isoformat()
        record["data_iso"] = dh.date().isoformat()
    elif isinstance(dh, str):
        parsed = parse_date_mixed(dh)
        record["data_hora"] = parsed.isoformat() if parsed else dh
        record["data_iso"] = parsed.date().isoformat() if parsed else raw.get("data_iso")
    else:
        record["data_hora"] = None
        record["data_iso"] = raw.get("data_iso")

    # --- record_id ---
    record["record_id"] = raw.get("record_id") or (
        f"{record['reservatorio_id']}-{record['data_iso']}"
        if record.get("reservatorio_id") and record.get("data_iso")
        else None
    )

    # --- campos numéricos ---
    for field in _OPTIONAL_FLOATS:
        val = raw.get(field)
        if val is None or val == "":
            record[field] = None
        elif isinstance(val, (int, float)):
            import math
            record[field] = None if (math.isnan(val) or math.isinf(val)) else float(val)
        else:
            record[field] = safe_float_ptbr(str(val))

    return record


def validate_record(record: Dict[str, Any]) -> None:
    """
    Valida regras mínimas de integridade.

    Levanta ValueError com mensagem descritiva se:
      - record_id ausente
      - data_hora ausente ou inválida
      - reservatorio_id ausente
      - volume_util_pct fora de [0, 100]
      - cota_m negativa
    """
    rid = record.get("record_id")
    if not rid:
        raise ValueError(f"record_id ausente: {record}")

    if not record.get("reservatorio_id"):
        raise ValueError(f"reservatorio_id ausente em {rid}")

    if not record.get("data_hora"):
        raise ValueError(f"data_hora ausente em {rid}")

    # Tenta parsear para confirmar que a string é uma data válida
    if isinstance(record["data_hora"], str):
        dt = parse_date_mixed(record["data_hora"])
        if dt is None:
            raise ValueError(f"data_hora inválida em {rid}: {record['data_hora']!r}")

    vol = record.get("volume_util_pct")
    if vol is not None and not (_VOLUME_MIN <= vol <= _VOLUME_MAX):
        raise ValueError(
            f"volume_util_pct fora do intervalo [0,100] em {rid}: {vol}"
        )

    cota = record.get("cota_m")
    if cota is not None and cota < _COTA_MIN:
        raise ValueError(f"cota_m negativa em {rid}: {cota}")


def deduplicar(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove duplicatas por record_id em O(n).
    Mantém a primeira ocorrência de cada record_id.
    """
    seen: dict = {}
    for rec in records:
        rid = rec.get("record_id")
        if rid and rid not in seen:
            seen[rid] = rec
        elif not rid:
            # Registros sem record_id são mantidos (não podemos deduplicar)
            seen[id(rec)] = rec
    return list(seen.values())
