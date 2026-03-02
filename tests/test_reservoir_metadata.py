from __future__ import annotations

import tempfile
from pathlib import Path

from app.core.reservoir_metadata import load_reservoir_metadata


def test_load_reservoir_metadata_csv():
    with tempfile.TemporaryDirectory() as td:
        data_dir = Path(td)
        csv_path = data_dir / "reservatorios_metadata.csv"
        csv_path.write_text(
            (
                "reservatorio_id,reservatorio,uf,subsistema\n"
                "19091,SANTA BRANCA,sp,SE/CO\n"
            ),
            encoding="utf-8",
        )

        metadata = load_reservoir_metadata(data_dir)

        assert 19091 in metadata
        assert metadata[19091]["uf"] == "SP"
        assert metadata[19091]["subsistema"] == "SE/CO"
