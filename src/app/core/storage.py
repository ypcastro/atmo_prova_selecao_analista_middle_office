from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class UpsertResult:
    inserted: int
    existing: int


_COLUMNS = (
    "record_id",
    "reservatorio_id",
    "reservatorio",
    "data_medicao",
    "cota_m",
    "afluencia_m3s",
    "defluencia_m3s",
    "vazao_vertida_m3s",
    "vazao_turbinada_m3s",
    "vazao_natural_m3s",
    "volume_util_pct",
    "vazao_incremental_m3s",
    "uf",
    "subsistema",
    "balanco_vazao_m3s",
    "situacao_hidrologica",
)


_EXTRA_SCHEMA_COLUMNS = {
    "uf": "TEXT",
    "subsistema": "TEXT",
    "balanco_vazao_m3s": "REAL",
    "situacao_hidrologica": "TEXT",
}

_RESERVOIR_COLUMNS = (
    "reservatorio_id",
    "reservatorio",
    "estado_codigo_ana",
    "estado_nome",
    "uf",
    "subsistema",
    "source",
    "updated_at_utc",
)


def _create_medicoes_table(
    con: sqlite3.Connection, table_name: str = "ana_medicoes"
) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            record_id TEXT PRIMARY KEY,
            reservatorio_id INTEGER NOT NULL,
            reservatorio TEXT NOT NULL,
            data_medicao TEXT NOT NULL,
            cota_m REAL,
            afluencia_m3s REAL,
            defluencia_m3s REAL,
            vazao_vertida_m3s REAL,
            vazao_turbinada_m3s REAL,
            vazao_natural_m3s REAL,
            volume_util_pct REAL,
            vazao_incremental_m3s REAL,
            uf TEXT,
            subsistema TEXT,
            balanco_vazao_m3s REAL,
            situacao_hidrologica TEXT
        )
        """)


def _create_reservatorios_table(
    con: sqlite3.Connection, table_name: str = "ana_reservatorios"
) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            reservatorio_id INTEGER PRIMARY KEY,
            reservatorio TEXT NOT NULL,
            estado_codigo_ana INTEGER,
            estado_nome TEXT,
            uf TEXT,
            subsistema TEXT,
            source TEXT,
            updated_at_utc TEXT
        )
        """)


def _existing_columns(con: sqlite3.Connection, table_name: str) -> list[str]:
    return [
        str(row["name"])
        for row in con.execute(f"PRAGMA table_info({table_name})").fetchall()
    ]


def _rebuild_table_if_has_legacy_columns(
    con: sqlite3.Connection,
    *,
    table_name: str,
    target_columns: tuple[str, ...],
    create_table_fn,
) -> None:
    existing_columns = _existing_columns(con, table_name)
    legacy_columns = [
        column for column in existing_columns if column not in target_columns
    ]
    if not legacy_columns:
        return

    temp_table = f"{table_name}__tmp"
    con.execute(f"DROP TABLE IF EXISTS {temp_table}")
    create_table_fn(con, temp_table)

    common_columns = [column for column in target_columns if column in existing_columns]
    if common_columns:
        cols_sql = ", ".join(common_columns)
        con.execute(f"""
            INSERT INTO {temp_table} ({cols_sql})
            SELECT {cols_sql}
            FROM {table_name}
            """)

    con.execute(f"DROP TABLE {table_name}")
    con.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")


def _ensure_columns(con: sqlite3.Connection) -> None:
    existing = {
        str(row["name"])
        for row in con.execute("PRAGMA table_info(ana_medicoes)").fetchall()
    }
    for column, column_type in _EXTRA_SCHEMA_COLUMNS.items():
        if column in existing:
            continue
        con.execute(f"ALTER TABLE ana_medicoes ADD COLUMN {column} {column_type}")


def _ensure_reservoir_columns(con: sqlite3.Connection) -> None:
    expected = {
        "estado_codigo_ana": "INTEGER",
        "estado_nome": "TEXT",
        "uf": "TEXT",
        "subsistema": "TEXT",
        "source": "TEXT",
        "updated_at_utc": "TEXT",
    }
    existing = {
        str(row["name"])
        for row in con.execute("PRAGMA table_info(ana_reservatorios)").fetchall()
    }
    for column, column_type in expected.items():
        if column in existing:
            continue
        con.execute(f"ALTER TABLE ana_reservatorios ADD COLUMN {column} {column_type}")


def init_db(db_path: Path) -> sqlite3.Connection:
    """Create SQLite schema and return a ready connection."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    _create_medicoes_table(con)
    _create_reservatorios_table(con)
    _ensure_columns(con)
    _ensure_reservoir_columns(con)
    _rebuild_table_if_has_legacy_columns(
        con,
        table_name="ana_medicoes",
        target_columns=_COLUMNS,
        create_table_fn=_create_medicoes_table,
    )
    _rebuild_table_if_has_legacy_columns(
        con,
        table_name="ana_reservatorios",
        target_columns=_RESERVOIR_COLUMNS,
        create_table_fn=_create_reservatorios_table,
    )
    con.commit()
    return con


def upsert_many(
    con: sqlite3.Connection, rows: Iterable[dict[str, Any]]
) -> UpsertResult:
    """Insert rows idempotently and return inserted/existing counts."""
    insert_sql = """
        INSERT OR IGNORE INTO ana_medicoes (
            record_id,
            reservatorio_id,
            reservatorio,
            data_medicao,
            cota_m,
            afluencia_m3s,
            defluencia_m3s,
            vazao_vertida_m3s,
            vazao_turbinada_m3s,
            vazao_natural_m3s,
            volume_util_pct,
            vazao_incremental_m3s,
            uf,
            subsistema,
            balanco_vazao_m3s,
            situacao_hidrologica
        ) VALUES (
            :record_id,
            :reservatorio_id,
            :reservatorio,
            :data_medicao,
            :cota_m,
            :afluencia_m3s,
            :defluencia_m3s,
            :vazao_vertida_m3s,
            :vazao_turbinada_m3s,
            :vazao_natural_m3s,
            :volume_util_pct,
            :vazao_incremental_m3s,
            :uf,
            :subsistema,
            :balanco_vazao_m3s,
            :situacao_hidrologica
        )
    """
    update_sql = """
        UPDATE ana_medicoes
        SET
            reservatorio_id = :reservatorio_id,
            reservatorio = :reservatorio,
            data_medicao = :data_medicao,
            cota_m = :cota_m,
            afluencia_m3s = :afluencia_m3s,
            defluencia_m3s = :defluencia_m3s,
            vazao_vertida_m3s = :vazao_vertida_m3s,
            vazao_turbinada_m3s = :vazao_turbinada_m3s,
            vazao_natural_m3s = :vazao_natural_m3s,
            volume_util_pct = :volume_util_pct,
            vazao_incremental_m3s = :vazao_incremental_m3s,
            uf = :uf,
            subsistema = :subsistema,
            balanco_vazao_m3s = :balanco_vazao_m3s,
            situacao_hidrologica = :situacao_hidrologica
        WHERE record_id = :record_id
    """

    inserted = 0
    existing = 0

    for row in rows:
        payload = {column: row.get(column) for column in _COLUMNS}
        cursor = con.execute(insert_sql, payload)
        if cursor.rowcount == 1:
            inserted += 1
        else:
            existing += 1
            con.execute(update_sql, payload)

    con.commit()
    return UpsertResult(inserted=inserted, existing=existing)


def fetch_records(
    con: sqlite3.Connection,
    *,
    limit: int = 1000,
    uf: str | None = None,
    reservatorio_id: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch records from storage with deterministic ordering."""
    safe_limit = max(1, int(limit))
    columns = ",\n            ".join(_COLUMNS)
    where_clauses: list[str] = []
    params: list[Any] = []

    if uf:
        where_clauses.append("UPPER(COALESCE(uf, '')) = UPPER(?)")
        params.append(uf)
    if reservatorio_id is not None:
        where_clauses.append("reservatorio_id = ?")
        params.append(int(reservatorio_id))

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)
    params.append(safe_limit)

    cursor = con.execute(
        f"""
        SELECT
            {columns}
        FROM ana_medicoes
        {where_sql}
        ORDER BY data_medicao ASC, record_id ASC
        LIMIT ?
        """,
        tuple(params),
    )
    return [dict(row) for row in cursor.fetchall()]


def fetch_by_id(con: sqlite3.Connection, record_id: str) -> dict[str, Any] | None:
    """Fetch one record by record_id."""
    columns = ",\n            ".join(_COLUMNS)
    cursor = con.execute(
        f"""
        SELECT
            {columns}
        FROM ana_medicoes
        WHERE record_id = ?
        """,
        (record_id,),
    )
    row = cursor.fetchone()
    return dict(row) if row is not None else None


def upsert_reservoir_catalog(
    con: sqlite3.Connection,
    rows: Iterable[dict[str, Any]],
) -> UpsertResult:
    """Upsert structured reservoir catalog rows."""
    now_utc = datetime.now(timezone.utc).isoformat()
    inserted = 0
    existing = 0
    insert_sql = """
        INSERT OR IGNORE INTO ana_reservatorios (
            reservatorio_id,
            reservatorio,
            estado_codigo_ana,
            estado_nome,
            uf,
            subsistema,
            source,
            updated_at_utc
        ) VALUES (
            :reservatorio_id,
            :reservatorio,
            :estado_codigo_ana,
            :estado_nome,
            :uf,
            :subsistema,
            :source,
            :updated_at_utc
        )
    """
    update_sql = """
        UPDATE ana_reservatorios
        SET
            reservatorio = :reservatorio,
            estado_codigo_ana = :estado_codigo_ana,
            estado_nome = :estado_nome,
            uf = :uf,
            subsistema = :subsistema,
            source = :source,
            updated_at_utc = :updated_at_utc
        WHERE reservatorio_id = :reservatorio_id
    """

    for row in rows:
        payload = {column: row.get(column) for column in _RESERVOIR_COLUMNS}
        payload["updated_at_utc"] = payload.get("updated_at_utc") or now_utc
        cursor = con.execute(insert_sql, payload)
        if cursor.rowcount == 1:
            inserted += 1
        else:
            existing += 1
            con.execute(update_sql, payload)

    con.commit()
    return UpsertResult(inserted=inserted, existing=existing)


def fetch_reservoir_catalog(
    con: sqlite3.Connection,
    *,
    limit: int = 2000,
    uf: str | None = None,
) -> list[dict[str, Any]]:
    """List reservoirs from the structured catalog table."""
    safe_limit = max(1, int(limit))
    columns = ",\n                ".join(_RESERVOIR_COLUMNS)
    if uf:
        rows = con.execute(
            f"""
            SELECT
                {columns}
            FROM ana_reservatorios
            WHERE UPPER(COALESCE(uf, '')) = UPPER(?)
            ORDER BY reservatorio ASC
            LIMIT ?
            """,
            (uf, safe_limit),
        ).fetchall()
    else:
        rows = con.execute(
            f"""
            SELECT
                {columns}
            FROM ana_reservatorios
            ORDER BY reservatorio ASC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def fetch_reservoir_metadata_map(con: sqlite3.Connection) -> dict[int, dict[str, Any]]:
    """Return catalog metadata indexed by reservatorio_id for enrichment."""
    rows = con.execute("""
        SELECT
            reservatorio_id,
            uf,
            subsistema
        FROM ana_reservatorios
        """).fetchall()

    output: dict[int, dict[str, Any]] = {}
    for row in rows:
        rid = row["reservatorio_id"]
        if rid is None:
            continue
        output[int(rid)] = {
            "uf": row["uf"],
            "subsistema": row["subsistema"],
        }
    return output


def refresh_measurement_metadata_from_catalog(con: sqlite3.Connection) -> int:
    """Backfill metadata fields in ana_medicoes from ana_reservatorios when missing."""
    cursor = con.execute("""
        UPDATE ana_medicoes
        SET
            uf = COALESCE(ana_medicoes.uf, (
                SELECT c.uf
                FROM ana_reservatorios c
                WHERE c.reservatorio_id = ana_medicoes.reservatorio_id
            )),
            subsistema = COALESCE(ana_medicoes.subsistema, (
                SELECT c.subsistema
                FROM ana_reservatorios c
                WHERE c.reservatorio_id = ana_medicoes.reservatorio_id
            ))
        WHERE ana_medicoes.reservatorio_id IN (
            SELECT reservatorio_id
            FROM ana_reservatorios
        )
        """)
    con.commit()
    return int(cursor.rowcount)
