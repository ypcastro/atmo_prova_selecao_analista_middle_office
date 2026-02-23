from __future__ import annotations

from typing import Any


def dedupe(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    """TODO (Q4): deduplicar O(n) preservando primeira ocorrência."""
    raise NotImplementedError


def normalize_record(row: dict[str, Any]) -> dict[str, Any]:
    """TODO (Q4): normalizar schema (tipos e nomes canônicos)."""
    raise NotImplementedError


def validate_record(row: dict[str, Any]) -> None:
    """TODO (Q4): validar schema e regras mínimas."""
    raise NotImplementedError
