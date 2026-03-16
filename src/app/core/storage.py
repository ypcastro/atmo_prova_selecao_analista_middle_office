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
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("""
        CREATE TABLE IF NOT EXISTS reservatorios (
            record_id TEXT PRIMARY KEY,
            reservatorio_id INTEGER,
            reservatorio TEXT,
            data_medicao TEXT
        )
    """)
    con.commit()
    return con


def upsert_many(con: sqlite3.Connection, rows: Iterable[dict[str, Any]]) -> UpsertResult:
    """TODO (Q6): inserir idempotente e retornar contagens."""
    inserted = 0 
    existing = 0
    for row in rows:
        cur = con.execute("""
            SELECT 1 FROM reservatorios WHERE record_id = ?
        """, (row['record_id'],))
        if cur.fetchone():
            existing += 1
        else:
            con.execute("INSERT INTO reservatorios (record_id, reservatorio_id, reservatorio, data_medicao) VALUES (?, ?, ?, ?)",
                (row['record_id'], row['reservatorio_id'], row['reservatorio'], row['data_medicao'])
            )
            inserted += 1
        con.commit()
    return UpsertResult(inserted=inserted, existing=existing)



def fetch_records(con: sqlite3.Connection, *, limit: int = 1000) -> list[dict[str, Any]]:
    """TODO (Q6): listar registros."""
    cur = con.execute("SELECT * from reservatorios Limit ?", (limit,))
    return [dict(row) for row in cur.fetchall()]
        

def fetch_by_id(con: sqlite3.Connection, record_id: str) -> dict[str, Any] | None:
    """TODO (Q6): buscar por record_id."""
    cur = con.execute("SELECT * from reservatorios WHERE record_id = ?", (record_id,))
    row = cur.fetchone()
    return dict(row) if row else None