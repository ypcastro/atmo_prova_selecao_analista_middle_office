from __future__ import annotations

import re

_UF_TO_SUBSYSTEM = {
    # SE/CO
    "DF": "SE/CO",
    "ES": "SE/CO",
    "GO": "SE/CO",
    "MG": "SE/CO",
    "MS": "SE/CO",
    "MT": "SE/CO",
    "RJ": "SE/CO",
    "SP": "SE/CO",
    # Sul
    "PR": "SUL",
    "RS": "SUL",
    "SC": "SUL",
    # Nordeste
    "AL": "NE",
    "BA": "NE",
    "CE": "NE",
    "MA": "NE",
    "PB": "NE",
    "PE": "NE",
    "PI": "NE",
    "RN": "NE",
    "SE": "NE",
    # Norte
    "AC": "NORTE",
    "AM": "NORTE",
    "AP": "NORTE",
    "PA": "NORTE",
    "RO": "NORTE",
    "RR": "NORTE",
    "TO": "NORTE",
}


def infer_subsistema_from_uf_text(uf_text: str | None) -> str | None:
    """Infer SIN subsystem from one or many UFs."""
    if uf_text is None:
        return None

    tokens = [
        token.strip().upper() for token in re.split(r"[,;/]+", uf_text) if token.strip()
    ]
    if not tokens:
        return None

    subsystems = {
        value for value in (_UF_TO_SUBSYSTEM.get(token) for token in tokens) if value
    }
    if not subsystems:
        return None
    if len(subsystems) == 1:
        return next(iter(subsystems))
    return "MULTI"
