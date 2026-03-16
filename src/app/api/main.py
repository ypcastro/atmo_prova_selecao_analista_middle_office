from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from app.core.storage import init_db
from app.core.config import load_settings
from app.jobs.extract_job import run_once
from app.core.pipeline_io import PipelineIO


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
def list_medicoes(limit: int = Query(100, ge=1, le=1000)):
    """TODO (Q7): listar registros do banco."""
    con = init_db(load_settings().db_path)
    try:
        return fetch_records(con, limit=limit)
    finally:
        con.close()


@app.get("/ana/medicoes/{record_id}")
def get_medicao(record_id: str):
    """TODO (Q7): buscar por record_id."""
    con = init_db(load_settings().db_path)
    try: 
        row = fetch_by_id(con, record_id)
    finally:
        con.close()
    if not row:
        raise HTTPException(status_code=404, detail="record not found")
    return row


@app.get("/ana/analysis")
def analysis():
    """TODO (Q7): rodar análise com dados persistidos."""
    con = init_db(load_settings().db_path)
    try:
        rows = fetch_records(con, limit=1000)
    finally:
        con.close()
    return run_analysis(rows)
