from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class UpsertResult:
    inserted: int
    existing: int


def init_db(db_path: Path) -> sqlite3.Connection:
    """TODO (Q6): criar schema SQLite e retornar conexão."""
    raise NotImplementedError


def upsert_many(con: sqlite3.Connection, rows: Iterable[dict[str, Any]]) -> UpsertResult:
    """TODO (Q6): inserir idempotente e retornar contagens."""
    raise NotImplementedError


def fetch_records(con: sqlite3.Connection, *, limit: int = 1000) -> list[dict[str, Any]]:
    """TODO (Q6): listar registros."""
    raise NotImplementedError


def fetch_by_id(con: sqlite3.Connection, record_id: str) -> dict[str, Any] | None:
    """TODO (Q6): buscar por record_id."""
    raise NotImplementedError
