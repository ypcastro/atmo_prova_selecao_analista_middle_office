import tempfile
from pathlib import Path

from app.core.storage import init_db, upsert_many, fetch_records


def test_upsert_idempotent():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "ana.db"
        con = init_db(db)
        try:
            rows = [
                {
                    "record_id": "A",
                    "reservatorio_id": 1,
                    "reservatorio": "X",
                    "data_medicao": "2025-01-01",
                },
                {
                    "record_id": "A",
                    "reservatorio_id": 1,
                    "reservatorio": "X",
                    "data_medicao": "2025-01-01",
                },
                {
                    "record_id": "B",
                    "reservatorio_id": 1,
                    "reservatorio": "X",
                    "data_medicao": "2025-01-02",
                },
            ]
            res1 = upsert_many(con, rows)
            assert res1.inserted == 2
            assert res1.existing == 1

            res2 = upsert_many(con, rows)
            assert res2.inserted == 0
            assert res2.existing == 3

            all_rows = fetch_records(con, limit=10)
            assert len(all_rows) == 2
        finally:
            con.close()


def test_fetch_records_filters_before_limit():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "ana.db"
        con = init_db(db)
        try:
            rows = [
                {"record_id": "A", "reservatorio_id": 1, "reservatorio": "X", "data_medicao": "2025-01-01", "uf": "SP"},
                {"record_id": "B", "reservatorio_id": 1, "reservatorio": "X", "data_medicao": "2025-01-02", "uf": "SP"},
                {"record_id": "C", "reservatorio_id": 2, "reservatorio": "Y", "data_medicao": "2025-01-03", "uf": "MG"},
            ]
            upsert_many(con, rows)

            filtered = fetch_records(con, limit=1, uf="SP")
            assert len(filtered) == 1
            assert filtered[0]["uf"] == "SP"

            by_id = fetch_records(con, limit=5, reservatorio_id=2)
            assert len(by_id) == 1
            assert by_id[0]["record_id"] == "C"
        finally:
            con.close()
