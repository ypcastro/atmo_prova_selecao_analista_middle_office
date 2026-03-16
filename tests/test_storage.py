import tempfile
from pathlib import Path

from app.core.storage import init_db, upsert_many, fetch_records


def test_upsert_idempotent():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "ana.db"
        con = init_db(db)
        try:
            rows = [
                {"record_id": "A", "reservatorio_id": 1, "reservatorio": "X", "data_medicao": "2025-01-01"},
                {"record_id": "A", "reservatorio_id": 1, "reservatorio": "X", "data_medicao": "2025-01-01"},
                {"record_id": "B", "reservatorio_id": 1, "reservatorio": "X", "data_medicao": "2025-01-02"},
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
