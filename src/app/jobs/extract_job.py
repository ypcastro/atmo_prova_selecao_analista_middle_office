"""
Q7 — Job de extração: orquestra extract → parse → normalize → validate → upsert.

run_once() executa uma rodada completa do pipeline e salva artefatos/checkpoint.
"""

import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from app.ana.parser import parse_ana_records
from app.core.pipeline_io import PipelineIO
from app.core.storage import init_db, upsert_many
from app.core.transforms import deduplicar, normalize_record, validate_record

logger = logging.getLogger(__name__)


def _get_html(mode: str, reservatorio_id: str, data_inicial: str, data_final: str) -> str:
    """
    Obtém HTML da ANA conforme o modo configurado.
    - snapshot: lê arquivo local data/ana_snapshot.html
    - live: faz requisição HTTP (bônus)
    """
    if mode == "live":
        from app.ana.client import fetch_ana_html
        return fetch_ana_html(
            reservatorio_id=reservatorio_id,
            data_inicial=data_inicial,
            data_final=data_final,
        )
    else:
        # Modo snapshot (padrão — reprodutível)
        data_dir = os.environ.get("APP_DATA_DIR", "data")
        snapshot_path = Path(data_dir) / "ana_snapshot.html"
        if not snapshot_path.exists():
            raise FileNotFoundError(f"Snapshot não encontrado: {snapshot_path}")
        return snapshot_path.read_text(encoding="utf-8")


def run_once(
    db_path: Optional[str] = None,
    base_dir: Optional[str] = None,
) -> Dict:
    """
    Executa uma rodada completa do pipeline ANA:
      1. Extrai HTML (snapshot ou live)
      2. Faz parse dos registros
      3. Normaliza (tipos, nomes canônicos)
      4. Valida (regras mínimas)
      5. Deduplica
      6. Persiste no SQLite (idempotente)
      7. Salva artefatos e checkpoint

    Retorna dict com resultado: {run_id, success, inserted, existing, error, records_count}
    """
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]
    mode = os.environ.get("ANA_MODE", "snapshot")
    res_id = os.environ.get("ANA_RESERVATORIO", "19091")
    data_ini = os.environ.get("ANA_DATA_INICIAL", "2025-10-01")
    data_fim = os.environ.get("ANA_DATA_FINAL", "2025-10-07")

    io = PipelineIO(base_dir)
    conn = init_db(db_path)

    logger.info("[%s] Iniciando pipeline (mode=%s, res=%s)", run_id, mode, res_id)

    try:
        # 1. Extração
        html = _get_html(mode, res_id, data_ini, data_fim)
        io.save_raw_html(html, run_id)
        logger.info("[%s] HTML extraído (%d bytes)", run_id, len(html))

        # 2. Parse
        raw_records = parse_ana_records(html, reservatorio_id=res_id)
        logger.info("[%s] %d registros parseados", run_id, len(raw_records))

        # 3. Normalização
        normalized = [normalize_record(r) for r in raw_records]

        # 4. Validação (registros inválidos são descartados com log)
        valid = []
        for rec in normalized:
            try:
                validate_record(rec)
                valid.append(rec)
            except ValueError as exc:
                logger.warning("[%s] Registro inválido descartado: %s", run_id, exc)

        # 5. Deduplicação
        unique = deduplicar(valid)
        logger.info("[%s] %d registros válidos após dedup", run_id, len(unique))

        # Salva JSON normalizado
        io.save_normalized_json(unique, run_id)

        # 6. Persistência
        counts = upsert_many(unique, conn=conn)

        # 7. Checkpoint de sucesso
        io.save_checkpoint(
            run_id=run_id,
            success=True,
            inserted=counts["inserted"],
            existing=counts["existing"],
        )

        result = {
            "run_id": run_id,
            "success": True,
            "records_count": len(unique),
            "inserted": counts["inserted"],
            "existing": counts["existing"],
            "error": None,
        }
        logger.info("[%s] Pipeline concluído: %s", run_id, result)
        return result

    except Exception as exc:
        logger.error("[%s] Pipeline falhou: %s", run_id, exc, exc_info=True)
        io.save_checkpoint(run_id=run_id, success=False, error=str(exc))
        return {
            "run_id": run_id,
            "success": False,
            "records_count": 0,
            "inserted": 0,
            "existing": 0,
            "error": str(exc),
        }
    finally:
        conn.close()
