from __future__ import annotations

import tempfile
from pathlib import Path

from app.core.storage import fetch_reservoir_catalog, init_db, upsert_reservoir_catalog


def test_upsert_and_fetch_reservoir_catalog():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "ana.db"
        con = init_db(db)
        try:
            rows = [
                {
                    "reservatorio_id": 19091,
                    "reservatorio": "SANTA BRANCA",
                    "estado_codigo_ana": 26,
                    "estado_nome": "São Paulo",
                    "uf": "SP",
                    "subsistema": "SE/CO",
                    "source": "test",
                    "updated_at_utc": "2026-03-01T00:00:00+00:00",
                }
            ]
            res1 = upsert_reservoir_catalog(con, rows)
            assert res1.inserted == 1
            assert res1.existing == 0

            rows[0]["subsistema"] = "SE"
            res2 = upsert_reservoir_catalog(con, rows)
            assert res2.inserted == 0
            assert res2.existing == 1

            all_rows = fetch_reservoir_catalog(con, limit=10)
            assert len(all_rows) == 1
            assert all_rows[0]["reservatorio_id"] == 19091
            assert all_rows[0]["uf"] == "SP"
            assert all_rows[0]["subsistema"] == "SE"

            sp_rows = fetch_reservoir_catalog(con, limit=10, uf="SP")
            assert len(sp_rows) == 1
        finally:
            con.close()
