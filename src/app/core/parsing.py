from __future__ import annotations

from datetime import date, datetime
from typing import Any


def parse_date_mixed(s: str) -> date:
    # """TODO (Q1): parse datas em formatos mistos (ver README)."""
    if not s or s.strip() in ("—", "NaN", "inf"):
        return None
    s = s.strip()
    # DD/MM/YYYY, YYYY/MM/DDTHH:MM:SS
    if '/' in s:
        for fmt in ("%d/%m/%Y", "%d/%m/%Y %H:%M:%S"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
    # ISO: YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS
    try:
        normalized = s.replace('Z','+00:00')
        return datetime.fromisoformat(normalized).date()    
    except ValueError:
        return None
    
    


def safe_float_ptbr(x: Any) -> float | None:
    # """TODO (Q1): converter números pt-BR e tokens ausentes."""
    if x is None:
        return None
    x = str(x).strip()
    if x in ("—", "NaN", "inf", ""):
        return None
    try:
        if "," in x:
            x = x.replace('.', '').replace(',', '.')
        return float(x)
    except ValueError: 
        return None
