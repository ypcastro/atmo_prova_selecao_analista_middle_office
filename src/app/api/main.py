"""
Q7 — API FastAPI.

Endpoints:
  POST /extract/ana              → dispara 1 rodada do pipeline
  GET  /ana/medicoes             → lista medições (paginável)
  GET  /ana/medicoes/{record_id} → medição por ID
  GET  /ana/checkpoint           → último checkpoint
  GET  /ana/analysis             → análise dos dados
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from app.analysis.ana_analysis import compute_analysis
from app.core.pipeline_io import PipelineIO
from app.core.storage import fetch_by_id, fetch_records, init_db
from app.jobs.extract_job import run_once
from app.jobs.scheduler import start_scheduler

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ------------------------------------------------------------------ #
# Lifespan: inicializa DB e scheduler ao subir a API                  #
# ------------------------------------------------------------------ #

_scheduler = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    logger.info("API iniciando — inicializando banco de dados...")
    init_db()
    logger.info("Iniciando scheduler em background...")
    _scheduler = start_scheduler()
    yield
    logger.info("API encerrando...")
    if _scheduler:
        _scheduler.stop()


app = FastAPI(
    title="ANA Pipeline API",
    description="Pipeline de medições hidrológicas da ANA (Agência Nacional de Águas)",
    version="1.0.0",
    lifespan=lifespan,
)


# ------------------------------------------------------------------ #
# Endpoints                                                            #
# ------------------------------------------------------------------ #


@app.post(
    "/extract/ana",
    summary="Dispara uma rodada de extração do pipeline ANA",
    tags=["Pipeline"],
)
async def trigger_extraction() -> Dict[str, Any]:
    """
    Executa synchronously uma rodada completa:
    extração → parse → normalização → validação → persistência.

    Retorna o resultado da rodada com contagens e status.
    """
    result = run_once()
    status_code = 200 if result["success"] else 500
    return JSONResponse(content=result, status_code=status_code)


@app.get(
    "/ana/medicoes",
    summary="Lista medições armazenadas",
    tags=["Medições"],
)
async def list_medicoes(
    reservatorio_id: Optional[str] = Query(None, description="Filtrar por reservatório"),
    limit: int = Query(100, ge=1, le=1000, description="Máximo de registros"),
    offset: int = Query(0, ge=0, description="Offset para paginação"),
) -> List[Dict[str, Any]]:
    """
    Retorna lista paginada de medições.
    Filtro opcional por reservatorio_id.
    """
    records = fetch_records(
        reservatorio_id=reservatorio_id,
        limit=limit,
        offset=offset,
    )
    return records


@app.get(
    "/ana/medicoes/{record_id}",
    summary="Busca medição por ID",
    tags=["Medições"],
)
async def get_medicao(record_id: str) -> Dict[str, Any]:
    """
    Retorna medição específica pelo record_id.
    Formato: {reservatorio_id}-{data_iso}, ex: 19091-2025-10-01
    """
    record = fetch_by_id(record_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Medição não encontrada: {record_id}")
    return record


@app.get(
    "/ana/checkpoint",
    summary="Retorna o último checkpoint de execução",
    tags=["Pipeline"],
)
async def get_checkpoint() -> Dict[str, Any]:
    """
    Retorna o checkpoint da última rodada do pipeline:
    run_id, timestamp, success, inserted, existing, error.
    """
    io = PipelineIO()
    checkpoint = io.load_latest_checkpoint()
    if checkpoint is None:
        raise HTTPException(
            status_code=404,
            detail="Nenhum checkpoint encontrado. Execute /extract/ana primeiro.",
        )
    return checkpoint


@app.get(
    "/ana/analysis",
    summary="Análise dos dados armazenados",
    tags=["Análise"],
)
async def get_analysis(
    reservatorio_id: Optional[str] = Query(None, description="Filtrar por reservatório"),
) -> Dict[str, Any]:
    """
    Retorna análise estatística e interpretação dos dados armazenados:
    - Métricas de cota, afluência, defluência, volume útil
    - Balanço hídrico médio
    - Tendência de cota
    - Interpretação textual
    """
    records = fetch_records(reservatorio_id=reservatorio_id, limit=10000)
    analysis = compute_analysis(records)
    return analysis


@app.get("/health", tags=["Infra"])
async def health() -> Dict[str, str]:
    """Healthcheck simples."""
    return {"status": "ok"}
