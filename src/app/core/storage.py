"""
Q6 — Persistência idempotente com SQLite.

init_db()     → cria schema se não existir
upsert_many() → INSERT OR IGNORE idempotente; retorna {inserted, existing}
fetch_records() → lista todos os registros
fetch_by_id()   → busca por record_id
"""

import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_DEFAULT_DB_PATH = "data/ana.db"

# DDL da tabela principal
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ana_medicoes (
    record_id           TEXT PRIMARY KEY,
    reservatorio_id     TEXT NOT NULL,
    data_hora           TEXT NOT NULL,
    data_iso            TEXT NOT NULL,
    cota_m              REAL,
    afluencia_m3s       REAL,
    defluencia_m3s      REAL,
    vazao_vertida_m3s   REAL,
    vazao_turbinada_m3s REAL,
    nivel_montante_m    REAL,
    volume_util_pct     REAL,
    created_at          TEXT DEFAULT (datetime('now')),
    raw_json            TEXT
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_ana_data_iso
    ON ana_medicoes(data_iso);
"""


def _db_path() -> str:
    return os.environ.get("APP_DATA_DIR", "data") + "/ana.db"


def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    """Abre (ou cria) conexão SQLite com configurações recomendadas."""
    path = db_path or _db_path()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")   # Write-Ahead Log para concorrência
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(db_path: Optional[str] = None) -> sqlite3.Connection:
    """
    Inicializa o banco de dados criando tabelas e índices se não existirem.
    Retorna a conexão aberta.
    """
    conn = get_connection(db_path)
    with conn:
        conn.execute(_CREATE_TABLE_SQL)
        conn.execute(_CREATE_INDEX_SQL)
    logger.info("DB inicializado: %s", db_path or _db_path())
    return conn


def _record_to_tuple(record: Dict[str, Any]) -> Tuple:
    """Converte dict de registro para tupla de INSERT."""
    return (
        record.get("record_id"),
        record.get("reservatorio_id"),
        record.get("data_hora"),
        record.get("data_iso"),
        record.get("cota_m"),
        record.get("afluencia_m3s"),
        record.get("defluencia_m3s"),
        record.get("vazao_vertida_m3s"),
        record.get("vazao_turbinada_m3s"),
        record.get("nivel_montante_m"),
        record.get("volume_util_pct"),
        json.dumps(record, ensure_ascii=False, default=str),
    )


_INSERT_SQL = """
INSERT OR IGNORE INTO ana_medicoes (
    record_id, reservatorio_id, data_hora, data_iso,
    cota_m, afluencia_m3s, defluencia_m3s,
    vazao_vertida_m3s, vazao_turbinada_m3s,
    nivel_montante_m, volume_util_pct, raw_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def upsert_many(
    records: List[Dict[str, Any]],
    conn: Optional[sqlite3.Connection] = None,
    db_path: Optional[str] = None,
) -> Dict[str, int]:
    """
    Insere registros de forma idempotente usando INSERT OR IGNORE.

    - Registros já existentes (mesmo record_id) são silenciosamente ignorados.
    - Retorna dict com contagens: {"inserted": N, "existing": M}

    Args:
        records: lista de registros normalizados
        conn: conexão existente (opcional)
        db_path: caminho do banco (usado se conn=None)
    """
    if not records:
        return {"inserted": 0, "existing": 0}

    should_close = conn is None
    if conn is None:
        conn = init_db(db_path)

    try:
        total = len(records)
        count_before = conn.execute("SELECT COUNT(*) FROM ana_medicoes").fetchone()[0]

        tuples = [_record_to_tuple(r) for r in records]
        with conn:
            conn.executemany(_INSERT_SQL, tuples)

        count_after = conn.execute("SELECT COUNT(*) FROM ana_medicoes").fetchone()[0]
        inserted = count_after - count_before
        existing = total - inserted

        logger.info("upsert_many: inserted=%d, existing=%d", inserted, existing)
        return {"inserted": inserted, "existing": existing}
    finally:
        if should_close:
            conn.close()


def fetch_records(
    conn: Optional[sqlite3.Connection] = None,
    db_path: Optional[str] = None,
    reservatorio_id: Optional[str] = None,
    limit: int = 1000,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Retorna lista de registros, com paginação e filtro opcional por reservatório.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection(db_path)

    try:
        if reservatorio_id:
            rows = conn.execute(
                """SELECT * FROM ana_medicoes
                   WHERE reservatorio_id = ?
                   ORDER BY data_iso ASC
                   LIMIT ? OFFSET ?""",
                (reservatorio_id, limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT * FROM ana_medicoes
                   ORDER BY data_iso ASC
                   LIMIT ? OFFSET ?""",
                (limit, offset),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        if should_close:
            conn.close()


def fetch_by_id(
    record_id: str,
    conn: Optional[sqlite3.Connection] = None,
    db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Busca um registro por record_id. Retorna None se não encontrado."""
    should_close = conn is None
    if conn is None:
        conn = get_connection(db_path)

    try:
        row = conn.execute(
            "SELECT * FROM ana_medicoes WHERE record_id = ?", (record_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        if should_close:
            conn.close()
