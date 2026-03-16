from __future__ import annotations

from typing import Any
from datetime import datetime, timezone

def dedupe(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    """TODO (Q4): deduplicar O(n) preservando primeira ocorrência."""
    seen = set()
    deduped = []
    for row in rows:
        value = row.get(key)
        if value and value not in seen:
            seen.add(value)
            deduped.append(row)
    return deduped

def normalize_record(row: dict[str, Any]) -> dict[str, Any]:
    """TODO (Q4): normalizar schema (tipos e nomes canônicos)."""
    reservatorio_id = int(str(row.get("reservatorio_id","")).strip())
    reservatorio = str(row.get("reservatorio", "")).strip()

    data_raw = str(row.get("data_medicao", "")).strip()
    data_iso = datetime.strptime(data_raw, "%d/%m/%Y").date().isoformat()

    record_id = f"{reservatorio_id}-{data_iso}"
    return {
        "record_id": record_id,
        "reservatorio_id": reservatorio_id,
        "reservatorio": reservatorio,
        "data_medicao": data_iso
    }
def validate_record(row: dict[str, Any]) -> None:
    """TODO (Q4): validar schema e regras mínimas."""
    required_fields = ["record_id", "reservatorio_id", "reservatorio", "data_medicao"]
    for field in required_fields:
        if not row.get(field):
            raise ValueError(f"Missing required field: '{field}'")