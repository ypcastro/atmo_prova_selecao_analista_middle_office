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

app = FastAPI(title="ANA Pipeline Challenge", version="0.1.0")


@app.get("/health")
def health():
    s = load_settings()
    con = init_db(s.db_path)
    con.close()
    return {"status": "ok"}


@app.post("/extract/ana")
def extract_ana():
    return run_once()


@app.get("/ana/checkpoint")
def checkpoint():
    s = load_settings()
    io = PipelineIO(s.data_dir)
    ck = io.read_checkpoint()
    if not ck:
        raise HTTPException(status_code=404, detail="checkpoint not found")
    return ck


@app.get("/ana/medicoes")
def list_medicoes(
    limit: int = Query(100, ge=1, le=1000),
    uf: str | None = Query(None),
    reservatorio_id: int | None = Query(None),
):
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


@app.get("/ana/medicoes/{record_id}")
def get_medicao(record_id: str):
    s = load_settings()
    con = init_db(s.db_path)
    try:
        row = fetch_by_id(con, record_id)
    finally:
        con.close()

    if row is None:
        raise HTTPException(status_code=404, detail="record not found")
    return row


@app.get("/ana/analysis")
def analysis():
    s = load_settings()
    con = init_db(s.db_path)
    try:
        rows = fetch_records(con, limit=100000)
    finally:
        con.close()

    return run_analysis(rows)


@app.post("/ana/reservatorios/sync")
def sync_reservatorios():
    s = load_settings()
    return sync_catalog_to_db(db_path=s.db_path, data_dir=s.data_dir)


@app.get("/ana/reservatorios")
def list_reservatorios(
    limit: int = Query(1000, ge=1, le=10000),
    uf: str | None = Query(None),
):
    s = load_settings()
    con = init_db(s.db_path)
    try:
        return fetch_reservoir_catalog(con, limit=limit, uf=uf)
    finally:
        con.close()
