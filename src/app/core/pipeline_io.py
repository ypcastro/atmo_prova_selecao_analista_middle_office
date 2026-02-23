from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PipelineIO:
    """TODO (Q2): I/O de pipeline com artefatos operacionais."""

    data_dir: Path

    def write_raw_html(self, *, source: str, html: str) -> Path:
        raise NotImplementedError

    def write_normalized_json(self, *, source: str, rows: list[dict[str, Any]]) -> Path:
        raise NotImplementedError

    def write_checkpoint(
        self,
        *,
        status: str,
        inserted: int = 0,
        existing: int = 0,
        error: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> Path:
        raise NotImplementedError

    def read_checkpoint(self) -> dict[str, Any] | None:
        raise NotImplementedError
