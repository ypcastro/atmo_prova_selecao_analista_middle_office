"""FastAPI application exposing ANA pipeline operational endpoints."""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from app.ana.catalog import sync_catalog_to_db
from app.analysis.ana_analysis import run_analysis
from app.core.config import load_settings
from app.core.pipeline_io import PipelineIO
from app.core.storage import (
    fetch_by_id,
    fetch_records,
    fetch_reservoir_catalog,
    init_db,
)
from app.jobs.extract_job import run_once

app = FastAPI(
    title="ANA Pipeline Challenge",
    version="0.1.0",
    description=(
        "HTTP interface to run extraction jobs, query measurements, and inspect "
        "pipeline operational artifacts."
    ),
)


@app.get(
    "/health",
    summary="Health check",
    description="Validate API process and database initialization path.",
)
def health() -> dict[str, str]:
    """Return service health status after opening the configured SQLite DB.

    Returns:
        dict[str, str]: Status payload with ``status=ok`` when initialization works.
    """
    s = load_settings()
    con = init_db(s.db_path)
    con.close()
    return {"status": "ok"}


@app.post(
    "/extract/ana",
    summary="Run extraction",
    description="Execute one ANA extraction cycle using current environment settings.",
)
def extract_ana() -> dict[str, object]:
    """Trigger one extraction run and return execution counters.

    Returns:
        dict[str, object]: Job result with status, processed count, and run metadata.
    """
    return run_once()


@app.get(
    "/ana/checkpoint",
    summary="Read checkpoint",
    description="Fetch latest pipeline checkpoint saved under data/out/checkpoint.json.",
)
def checkpoint() -> dict[str, object]:
    """Return the last persisted checkpoint for the extraction job.

    Raises:
        HTTPException: If no checkpoint file is available yet.

    Returns:
        dict[str, object]: Stored checkpoint payload.
    """
    s = load_settings()
    io = PipelineIO(s.data_dir)
    ck = io.read_checkpoint()
    if not ck:
        raise HTTPException(status_code=404, detail="checkpoint not found")
    return ck


@app.get(
    "/ana/medicoes",
    summary="List measurements",
    description="List stored ANA measurements with optional UF/reservoir filters.",
)
def list_medicoes(
    limit: int = Query(100, ge=1, le=1000),
    uf: str | None = Query(None),
    reservatorio_id: int | None = Query(None),
) -> list[dict[str, object]]:
    """Return measurement rows from SQLite with deterministic ordering.

    Args:
        limit: Maximum number of rows to return.
        uf: Optional UF filter.
        reservatorio_id: Optional reservoir id filter.

    Returns:
        list[dict[str, object]]: Measurement rows sorted by date and record id.
    """
    s = load_settings()
    con = init_db(s.db_path)
    try:
        rows = fetch_records(
            con,
            limit=limit,
            uf=uf.strip().upper() if uf else None,
            reservatorio_id=reservatorio_id,
        )
    finally:
        con.close()

    return rows


@app.get(
    "/ana/medicoes/{record_id}",
    summary="Get measurement by id",
    description="Fetch one measurement row by canonical record_id.",
)
def get_medicao(record_id: str) -> dict[str, object]:
    """Return one measurement row.

    Args:
        record_id: Canonical primary key in the format ``{reservatorio_id}-{date}``.

    Raises:
        HTTPException: If the record does not exist.

    Returns:
        dict[str, object]: Matching measurement row.
    """
    s = load_settings()
    con = init_db(s.db_path)
    try:
        row = fetch_by_id(con, record_id)
    finally:
        con.close()

    if row is None:
        raise HTTPException(status_code=404, detail="record not found")
    return row


@app.get(
    "/ana/analysis",
    summary="Run aggregate analysis",
    description="Compute aggregate hydrology indicators from stored measurements.",
)
def analysis() -> dict[str, object]:
    """Compute analysis payload from persisted data.

    Returns:
        dict[str, object]: Aggregate statistics produced by ``run_analysis``.
    """
    s = load_settings()
    con = init_db(s.db_path)
    try:
        rows = fetch_records(con, limit=100000)
    finally:
        con.close()

    return run_analysis(rows)


@app.post(
    "/ana/reservatorios/sync",
    summary="Sync reservoir catalog",
    description="Fetch ANA reservoir catalog and upsert it into SQLite.",
)
def sync_reservatorios() -> dict[str, object]:
    """Synchronize ANA reservoir catalog into the local database.

    Returns:
        dict[str, object]: Sync status with inserted/existing counters.
    """
    s = load_settings()
    return sync_catalog_to_db(db_path=s.db_path, data_dir=s.data_dir)


@app.get(
    "/ana/reservatorios",
    summary="List reservoir catalog",
    description="List persisted reservoir catalog rows with optional UF filter.",
)
def list_reservatorios(
    limit: int = Query(1000, ge=1, le=10000),
    uf: str | None = Query(None),
) -> list[dict[str, object]]:
    """Return catalog rows from ``ana_reservatorios``.

    Args:
        limit: Maximum number of rows returned.
        uf: Optional UF filter.

    Returns:
        list[dict[str, object]]: Catalog rows.
    """
    s = load_settings()
    con = init_db(s.db_path)
    try:
        return fetch_reservoir_catalog(con, limit=limit, uf=uf)
    finally:
        con.close()
