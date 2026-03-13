"""
Q2 — Estruturar I/O do pipeline (artefatos operacionais).

PipelineIO gerencia:
  - raw HTML  → data/raw/{run_id}.html
  - JSON normalizado → data/normalized/{run_id}.json
  - checkpoint → data/checkpoints/{run_id}.json + latest.json

Escrita segura via arquivo temporário + rename atômico.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


class PipelineIO:
    """Gerencia persistência de artefatos de cada rodada do pipeline."""

    def __init__(self, base_dir: Optional[str] = None) -> None:
        root = Path(base_dir or os.environ.get("APP_DATA_DIR", "data"))
        self.raw_dir = root / "raw"
        self.normalized_dir = root / "normalized"
        self.checkpoint_dir = root / "checkpoints"

        for d in (self.raw_dir, self.normalized_dir, self.checkpoint_dir):
            d.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ #
    #  Escrita segura                                                       #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _safe_write_text(path: Path, content: str) -> None:
        """
        Escreve `content` em `path` de forma atômica:
        1. grava em arquivo .tmp
        2. faz rename para o destino final
        Evita arquivos parcialmente escritos em caso de falha.
        """
        tmp = path.with_suffix(path.suffix + ".tmp")
        try:
            tmp.write_text(content, encoding="utf-8")
            tmp.replace(path)       # rename atômico no mesmo FS
        except Exception:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            raise

    # ------------------------------------------------------------------ #
    #  API pública                                                          #
    # ------------------------------------------------------------------ #

    def save_raw_html(self, html: str, run_id: str) -> Path:
        """Salva HTML bruto da extração."""
        path = self.raw_dir / f"{run_id}.html"
        self._safe_write_text(path, html)
        return path

    def save_normalized_json(self, records: List[Dict[str, Any]], run_id: str) -> Path:
        """Salva registros normalizados como JSON."""
        path = self.normalized_dir / f"{run_id}.json"
        content = json.dumps(records, ensure_ascii=False, default=str, indent=2)
        self._safe_write_text(path, content)
        return path

    def save_checkpoint(
        self,
        run_id: str,
        success: bool,
        inserted: int = 0,
        existing: int = 0,
        error: Optional[str] = None,
    ) -> Path:
        """
        Salva checkpoint da execução.
        Também sobrescreve data/checkpoints/latest.json para consulta rápida.
        """
        checkpoint: Dict[str, Any] = {
            "run_id": run_id,
            "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "success": success,
            "inserted": inserted,
            "existing": existing,
            "error": error,
        }
        content = json.dumps(checkpoint, ensure_ascii=False, indent=2)

        path = self.checkpoint_dir / f"{run_id}.json"
        self._safe_write_text(path, content)

        latest = self.checkpoint_dir / "latest.json"
        self._safe_write_text(latest, content)

        return path

    def load_latest_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Carrega o checkpoint mais recente, ou None se não existir."""
        latest = self.checkpoint_dir / "latest.json"
        if not latest.exists():
            return None
        return json.loads(latest.read_text(encoding="utf-8"))

    def load_normalized_json(self, run_id: str) -> Optional[List[Dict[str, Any]]]:
        """Carrega registros normalizados de uma rodada específica."""
        path = self.normalized_dir / f"{run_id}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
