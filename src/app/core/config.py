from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    ana_mode: str
    pipeline_interval_seconds: int
    ana_reservatorio: int
    ana_data_inicial: date
    ana_data_final: date

    @property
    def db_path(self) -> Path:
        return self.data_dir / "out" / "ana.db"


def load_settings() -> Settings:
    data_dir = Path(os.environ.get("APP_DATA_DIR", "data"))
    ana_mode = os.environ.get("ANA_MODE", "snapshot").strip().lower()
    interval = int(os.environ.get("PIPELINE_INTERVAL_SECONDS", "60"))

    reservatorio = int(os.environ.get("ANA_RESERVATORIO", "19091"))
    di = date.fromisoformat(os.environ.get("ANA_DATA_INICIAL", "2025-10-01"))
    df = date.fromisoformat(os.environ.get("ANA_DATA_FINAL", "2025-10-07"))

    return Settings(
        data_dir=data_dir,
        ana_mode=ana_mode,
        pipeline_interval_seconds=interval,
        ana_reservatorio=reservatorio,
        ana_data_inicial=di,
        ana_data_final=df,
    )
