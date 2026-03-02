from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PipelineIO:
    """Operational artifact I/O for the pipeline."""

    data_dir: Path

    @property
    def _out_dir(self) -> Path:
        return self.data_dir / "out"

    @property
    def _checkpoint_path(self) -> Path:
        return self._out_dir / "checkpoint.json"

    @property
    def _watermark_path(self) -> Path:
        return self._out_dir / "watermark.json"

    def _safe_source(self, source: str) -> str:
        safe = re.sub(r"[^a-zA-Z0-9._-]+", "-", source.strip())
        return safe.strip("-") or "unknown"

    def _now_stamp(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    def _atomic_write_text(self, path: Path, content: str) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(path)
        return path

    def _atomic_write_json(self, path: Path, payload: dict[str, Any]) -> Path:
        content = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        return self._atomic_write_text(path, content)

    def write_raw_html(self, *, source: str, html: str) -> Path:
        raw_dir = self._out_dir / "raw"
        path = raw_dir / f"{self._now_stamp()}_{self._safe_source(source)}.html"
        return self._atomic_write_text(path, html)

    def write_normalized_json(self, *, source: str, rows: list[dict[str, Any]]) -> Path:
        normalized_dir = self._out_dir / "normalized"
        path = normalized_dir / f"{self._now_stamp()}_{self._safe_source(source)}.json"
        payload = {
            "source": source,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "rows": rows,
        }
        return self._atomic_write_json(path, payload)

    def write_checkpoint(
        self,
        *,
        status: str,
        inserted: int = 0,
        existing: int = 0,
        error: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> Path:
        payload = {
            "status": status,
            "inserted": int(inserted),
            "existing": int(existing),
            "error": error,
            "meta": meta or {},
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
        return self._atomic_write_json(self._checkpoint_path, payload)

    def read_checkpoint(self) -> dict[str, Any] | None:
        path = self._checkpoint_path
        if not path.exists():
            return None

        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None

    def write_watermark(self, key: str, value: str) -> Path:
        key_clean = key.strip()
        if not key_clean:
            raise ValueError("watermark key cannot be empty")
        value_clean = value.strip()
        if not value_clean:
            raise ValueError("watermark value cannot be empty")

        payload = self.read_watermarks() or {}
        payload[key_clean] = {
            "value": value_clean,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        return self._atomic_write_json(self._watermark_path, payload)

    def read_watermarks(self) -> dict[str, Any] | None:
        path = self._watermark_path
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
        return data if isinstance(data, dict) else None

    def read_watermark_value(self, key: str) -> str | None:
        payload = self.read_watermarks()
        if not payload:
            return None
        entry = payload.get(key)
        if not isinstance(entry, dict):
            return None
        value = entry.get("value")
        if not isinstance(value, str):
            return None
        value_clean = value.strip()
        return value_clean or None
