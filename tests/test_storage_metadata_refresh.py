import tempfile
from pathlib import Path

from app.core.storage import (
    fetch_by_id,
    init_db,
    refresh_measurement_metadata_from_catalog,
    upsert_many,
    upsert_reservoir_catalog,
)


def test_refresh_measurement_metadata_from_catalog():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "ana.db"
        con = init_db(db)
        try:
            upsert_many(
                con,
                [
                    {
                        "record_id": "19004-2025-01-01",
                        "reservatorio_id": 19004,
                        "reservatorio": "FURNAS",
                        "data_medicao": "2025-01-01",
                    }
                ],
            )
            before = fetch_by_id(con, "19004-2025-01-01")
            assert before is not None
            assert before["uf"] is None
            assert before["subsistema"] is None

            upsert_reservoir_catalog(
                con,
                [
                    {
                        "reservatorio_id": 19004,
                        "reservatorio": "FURNAS",
                        "estado_codigo_ana": 14,
                        "estado_nome": "Minas Gerais",
                        "uf": "MG",
                        "subsistema": "SE/CO",
                        "source": "test",
                        "updated_at_utc": "2026-03-01T00:00:00+00:00",
                    }
                ],
            )

            changed = refresh_measurement_metadata_from_catalog(con)
            assert changed >= 1

            after = fetch_by_id(con, "19004-2025-01-01")
            assert after is not None
            assert after["uf"] == "MG"
            assert after["subsistema"] == "SE/CO"
        finally:
            con.close()
