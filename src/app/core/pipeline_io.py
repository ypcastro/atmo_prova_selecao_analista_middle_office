"""Utilities to persist and read pipeline operational artifacts."""

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
        """Return the root output directory."""
        return self.data_dir / "out"

    @property
    def _checkpoint_path(self) -> Path:
        """Return checkpoint JSON path."""
        return self._out_dir / "checkpoint.json"

    @property
    def _watermark_path(self) -> Path:
        """Return watermark JSON path."""
        return self._out_dir / "watermark.json"

    def _safe_source(self, source: str) -> str:
        """Sanitize a source label so it can be safely used in filenames.

        Args:
            source: Raw source string.

        Returns:
            str: Filename-safe source token.
        """
        safe = re.sub(r"[^a-zA-Z0-9._-]+", "-", source.strip())
        return safe.strip("-") or "unknown"

    def _now_stamp(self) -> str:
        """Return UTC timestamp suitable for artifact filenames."""
        return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    def _atomic_write_text(self, path: Path, content: str) -> Path:
        """Write text atomically using a temp file and replace operation.

        Args:
            path: Final destination path.
            content: UTF-8 text payload.

        Returns:
            Path: Final written path.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(path)
        return path

    def _atomic_write_json(self, path: Path, payload: dict[str, Any]) -> Path:
        """Serialize JSON and persist it atomically.

        Args:
            path: Final destination path.
            payload: JSON-serializable dictionary payload.

        Returns:
            Path: Final written path.
        """
        content = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        return self._atomic_write_text(path, content)

    def write_raw_html(self, *, source: str, html: str) -> Path:
        """Persist raw HTML artifact for traceability.

        Args:
            source: Extraction source label.
            html: Raw HTML content.

        Returns:
            Path: Written artifact path under ``out/raw``.
        """
        raw_dir = self._out_dir / "raw"
        path = raw_dir / f"{self._now_stamp()}_{self._safe_source(source)}.html"
        return self._atomic_write_text(path, html)

    def write_normalized_json(self, *, source: str, rows: list[dict[str, Any]]) -> Path:
        """Persist normalized rows as JSON artifact.

        Args:
            source: Extraction source label.
            rows: Normalized measurement rows.

        Returns:
            Path: Written artifact path under ``out/normalized``.
        """
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
        """Write extraction checkpoint file.

        Args:
            status: Job status, usually ``success``, ``dry_run``, or ``fail``.
            inserted: Number of inserted records.
            existing: Number of existing records.
            error: Optional error message.
            meta: Extra metadata payload.

        Returns:
            Path: Checkpoint file path.
        """
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
        """Read checkpoint JSON if present and valid.

        Returns:
            dict[str, Any] | None: Checkpoint payload or ``None``.
        """
        path = self._checkpoint_path
        if not path.exists():
            return None

        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None

    def write_watermark(self, key: str, value: str) -> Path:
        """Write or update a watermark entry.

        Args:
            key: Watermark key, for example ``live:19119``.
            value: Last processed value, usually ISO date string.

        Raises:
            ValueError: If key or value is empty after trimming.

        Returns:
            Path: Watermark file path.
        """
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
        """Read watermark payload if present and valid.

        Returns:
            dict[str, Any] | None: Watermark map keyed by watermark id.
        """
        path = self._watermark_path
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
        return data if isinstance(data, dict) else None

    def read_watermark_value(self, key: str) -> str | None:
        """Read a single watermark value by key.

        Args:
            key: Watermark key.

        Returns:
            str | None: Stored watermark value, or ``None`` if missing/invalid.
        """
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
