from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from app.core.config import load_settings
from app.jobs.extract_job import run_once
from app.core.pipeline_io import PipelineIO


app = FastAPI(title="ANA Pipeline Challenge", version="0.1.0")


@app.get("/health")
def health():
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
    raise NotImplementedError


@app.get("/ana/medicoes/{record_id}")
def get_medicao(record_id: str):
    """TODO (Q7): buscar por record_id."""
    raise NotImplementedError


@app.get("/ana/analysis")
def analysis():
    """TODO (Q7): rodar análise com dados persistidos."""
    raise NotImplementedError
