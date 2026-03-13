"""
Q1 — Parsing robusto de datas e números pt-BR.

Suporta:
  - parse_date_mixed(): DD/MM/YYYY, DD/MM/YYYY HH:MM:SS, ISO 8601 c/ e s/ timezone
  - safe_float_ptbr(): 1.234,56 | 1234,56 | 1234.56 → float; tokens ausentes → None
"""

import math
import re
from datetime import datetime, timezone
from typing import Optional

# Tokens que representam valor ausente/inválido
_ABSENT_TOKENS: frozenset = frozenset(
    {"", "—", "-", "--", "nan", "inf", "-inf", "+inf", "none", "null", "n/a", "nd", "s/d"}
)

# Formatos de data ordenados do mais específico ao mais genérico
_DATE_FORMATS = [
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%d",
]

# Regex para strip de timezone offset (+HH:MM, -HH:MM) ou Z no final
_TZ_SUFFIX_RE = re.compile(r"[+-]\d{2}:\d{2}$")


def parse_date_mixed(value: str) -> Optional[datetime]:
    """
    Faz parse de string de data em múltiplos formatos.

    Formatos suportados:
      - DD/MM/YYYY
      - DD/MM/YYYY HH:MM:SS
      - YYYY-MM-DD
      - YYYY-MM-DDTHH:MM:SS          (sem timezone)
      - YYYY-MM-DDTHH:MM:SSZ         (UTC — convertido para naive)
      - YYYY-MM-DDTHH:MM:SS+HH:MM   (offset removido, retorna naive local)

    Retorna datetime naive ou None se não reconhecer.
    """
    if value is None:
        return None
    v = value.strip()
    if not v:
        return None

    # Remove offset de timezone para normalizar antes dos formatos
    cleaned = _TZ_SUFFIX_RE.sub("", v).rstrip("Z").strip()

    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue

    return None


def safe_float_ptbr(value) -> Optional[float]:
    """
    Converte string numérica no padrão pt-BR para float.

    Regras:
      - "1.234,56" → 1234.56  (ponto = milhar, vírgula = decimal)
      - "1234,56"  → 1234.56
      - "1234.56"  → 1234.56  (aceita EN também)
      - tokens ausentes ("", "—", "NaN", "inf" etc.) → None
      - valores infinitos ou NaN após conversão → None

    Retorna float ou None.
    """
    if value is None:
        return None

    v = str(value).strip()

    if v.lower() in _ABSENT_TOKENS:
        return None

    # Formato pt-BR com separador de milhar e decimal em vírgula
    if "." in v and "," in v:
        # ex: 1.234,56 → remove ponto, troca vírgula por ponto
        v = v.replace(".", "").replace(",", ".")
    elif "," in v:
        # ex: 1234,56 → troca vírgula por ponto
        v = v.replace(",", ".")
    # else: já em formato EN (1234.56) — não precisa de alteração

    try:
        result = float(v)
    except (ValueError, TypeError):
        return None

    if math.isnan(result) or math.isinf(result):
        return None

    return result
