from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

_META_FIELDS = (
    "uf",
    "subsistema",
)


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def load_reservoir_metadata(data_dir: Path) -> dict[int, dict[str, Any]]:
    """Load optional reservoir metadata from data/reservatorios_metadata.csv."""
    path = data_dir / "reservatorios_metadata.csv"
    if not path.exists():
        return {}

    metadata: dict[int, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            rid_raw = row.get("reservatorio_id")
            rid_clean = _clean_text(rid_raw)
            if rid_clean is None:
                continue

            try:
                reservatorio_id = int(rid_clean)
            except ValueError:
                continue

            entry: dict[str, Any] = {}
            for field in _META_FIELDS:
                value = _clean_text(row.get(field))
                if field in {"uf", "subsistema"} and value is not None:
                    value = value.upper()
                entry[field] = value

            metadata[reservatorio_id] = entry

    return metadata
