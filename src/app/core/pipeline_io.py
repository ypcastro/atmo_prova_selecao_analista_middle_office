from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json
from datetime import datetime, timezone

@dataclass(frozen=True)
class PipelineIO:
    """TODO (Q2): I/O de pipeline com artefatos operacionais."""

    data_dir: Path

    def write_raw_html(self, *, source: str, html: str) -> Path:
        if not html:
            return None
        path = self.data_dir / "raw" / f"{source}.html"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(html)
        return path

    def write_normalized_json(self, *, source: str, rows: list[dict[str, Any]]) -> Path:
        path = self.data_dir / "normalized" / f"{source}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(rows,ensure_ascii=False, indent=2), encoding="utf-8")
        return path


    def write_checkpoint(
        self,
        *,
        status: str,
        inserted: int = 0,
        existing: int = 0,
        error: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> Path:
        payload ={
            "status": status,
            "inserted": inserted,
            "existing": existing,
            "error": error,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "meta": meta or {},
        }
        path = self.data_dir / "checkpoint.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def read_checkpoint(self) -> dict[str, Any] | None:
        path = self.data_dir / "checkpoint.json"
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

