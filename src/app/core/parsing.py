from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any


def parse_date_mixed(s: str) -> date:
    """Parse date strings in common BR and ISO formats."""
    text = str(s).strip()
    if not text:
        raise ValueError("empty date")

    for fmt in ("%d/%m/%Y", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    iso_text = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(iso_text).date()
    except ValueError as exc:
        raise ValueError(f"unsupported date format: {s!r}") from exc


def safe_float_ptbr(x: Any) -> float | None:
    """Parse a pt-BR number safely, returning None for missing/non-finite tokens."""
    if x is None:
        return None

    if isinstance(x, (int, float)) and not isinstance(x, bool):
        value = float(x)
        return value if math.isfinite(value) else None

    text = str(x).strip()
    if not text:
        return None

    lowered = text.lower()
    if lowered in {"na", "nan", "none", "null", "inf", "+inf", "-inf"}:
        return None
    if text in {"—", "â€”", "-", "--"}:
        return None

    if "," in text:
        text = text.replace(".", "").replace(",", ".")

    try:
        value = float(text)
    except ValueError:
        return None

    return value if math.isfinite(value) else None
