from __future__ import annotations

from datetime import date
from typing import Any

from app.core.parsing import parse_date_mixed, safe_float_ptbr

_NUMERIC_ALIASES: dict[str, tuple[str, ...]] = {
    "cota_m": ("cota_m", "cota"),
    "afluencia_m3s": ("afluencia_m3s", "afluencia"),
    "defluencia_m3s": ("defluencia_m3s", "defluencia"),
    "vazao_vertida_m3s": ("vazao_vertida_m3s", "vazao_vertida"),
    "vazao_turbinada_m3s": ("vazao_turbinada_m3s", "vazao_turbinada"),
    "vazao_natural_m3s": ("vazao_natural_m3s", "vazao_natural"),
    "volume_util_pct": ("volume_util_pct", "volume_util"),
    "vazao_incremental_m3s": ("vazao_incremental_m3s", "vazao_incremental"),
}

_TEXT_ALIASES: dict[str, tuple[str, ...]] = {
    "uf": ("uf", "estado"),
    "subsistema": ("subsistema",),
}


def dedupe(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    """Deduplicate preserving the first row for each key value."""
    seen: set[Any] = set()
    output: list[dict[str, Any]] = []

    for row in rows:
        value = row.get(key)
        if value in seen:
            continue
        seen.add(value)
        output.append(row)

    return output


def _pick_first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def normalize_record(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize record types and canonical field names."""
    reservatorio_id_raw = _pick_first(
        row,
        "reservatorio_id",
        "codigo_reservatorio",
        "codigo_do_reservatorio",
    )
    reservatorio_raw = _pick_first(row, "reservatorio", "nome_reservatorio")
    data_raw = _pick_first(row, "data_medicao", "data_da_medicao", "data")

    reservatorio_id = (
        int(str(reservatorio_id_raw).strip())
        if reservatorio_id_raw is not None
        else None
    )
    reservatorio = str(reservatorio_raw).strip() if reservatorio_raw is not None else ""
    data_iso = (
        parse_date_mixed(str(data_raw)).isoformat() if data_raw is not None else ""
    )

    record_id_raw = _pick_first(row, "record_id")
    if record_id_raw is not None:
        record_id = str(record_id_raw).strip()
    elif reservatorio_id is not None and data_iso:
        record_id = f"{reservatorio_id}-{data_iso}"
    else:
        record_id = None

    normalized: dict[str, Any] = {
        "record_id": record_id,
        "reservatorio_id": reservatorio_id,
        "reservatorio": reservatorio,
        "data_medicao": data_iso,
    }

    for target, aliases in _NUMERIC_ALIASES.items():
        value = _pick_first(row, *aliases)
        normalized[target] = safe_float_ptbr(value)

    for target, aliases in _TEXT_ALIASES.items():
        value = _pick_first(row, *aliases)
        if value is None:
            normalized[target] = None
            continue
        text_value = str(value).strip()
        if not text_value:
            normalized[target] = None
            continue
        if target in {"uf", "subsistema"}:
            normalized[target] = text_value.upper()
        else:
            normalized[target] = text_value

    normalized["balanco_vazao_m3s"] = None
    normalized["situacao_hidrologica"] = None

    return normalized


def validate_record(row: dict[str, Any]) -> None:
    """Validate minimal schema and required fields."""
    required_fields = ("record_id", "reservatorio_id", "reservatorio", "data_medicao")
    for field in required_fields:
        value = row.get(field)
        if value in (None, ""):
            raise ValueError(f"missing required field: {field}")

    if not isinstance(row["record_id"], str):
        raise ValueError("record_id must be str")
    if not isinstance(row["reservatorio_id"], int):
        raise ValueError("reservatorio_id must be int")
    if not isinstance(row["reservatorio"], str):
        raise ValueError("reservatorio must be str")
    if not isinstance(row["data_medicao"], str):
        raise ValueError("data_medicao must be str")

    # Ensures ISO date string.
    date.fromisoformat(row["data_medicao"])

    expected_record_id = f"{row['reservatorio_id']}-{row['data_medicao']}"
    if row["record_id"] != expected_record_id:
        raise ValueError("record_id does not match reservatorio_id and data_medicao")
