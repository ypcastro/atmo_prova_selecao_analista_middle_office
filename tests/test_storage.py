"""Testes Q6 — core/storage.py"""

import sqlite3

import pytest

from app.core.storage import fetch_by_id, fetch_records, init_db, upsert_many

_RECORDS = [
    {
        "record_id": "19091-2025-10-01",
        "reservatorio_id": "19091",
        "data_hora": "2025-10-01T00:00:00",
        "data_iso": "2025-10-01",
        "cota_m": 615.22,
        "afluencia_m3s": 1234.56,
        "defluencia_m3s": 980.0,
        "vazao_vertida_m3s": 0.0,
        "vazao_turbinada_m3s": 980.0,
        "nivel_montante_m": 615.22,
        "volume_util_pct": 72.3,
    },
    {
        "record_id": "19091-2025-10-02",
        "reservatorio_id": "19091",
        "data_hora": "2025-10-02T00:00:00",
        "data_iso": "2025-10-02",
        "cota_m": 615.45,
        "afluencia_m3s": 1300.10,
        "defluencia_m3s": 1000.0,
        "vazao_vertida_m3s": None,
        "vazao_turbinada_m3s": 1000.0,
        "nivel_montante_m": 615.45,
        "volume_util_pct": 73.1,
    },
]


@pytest.fixture
def db(tmp_path):
    """Banco SQLite em memória / tmp para cada teste."""
    db_path = str(tmp_path / "test_ana.db")
    conn = init_db(db_path=db_path)
    yield conn, db_path
    conn.close()


class TestInitDb:
    def test_creates_table(self, db):
        conn, _ = db
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='ana_medicoes'"
        ).fetchone()
        assert row is not None

    def test_creates_index(self, db):
        conn, _ = db
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_ana_data_iso'"
        ).fetchone()
        assert row is not None

    def test_idempotent_second_call(self, db):
        conn, db_path = db
        # Segunda chamada não deve lançar erro
        init_db(db_path=db_path)


class TestUpsertMany:
    def test_insert_new_records(self, db):
        conn, db_path = db
        result = upsert_many(_RECORDS, conn=conn)
        assert result["inserted"] == 2
        assert result["existing"] == 0

    def test_idempotent_second_upsert(self, db):
        conn, db_path = db
        upsert_many(_RECORDS, conn=conn)
        result = upsert_many(_RECORDS, conn=conn)
        assert result["inserted"] == 0
        assert result["existing"] == 2

    def test_partial_overlap(self, db):
        conn, _ = db
        upsert_many([_RECORDS[0]], conn=conn)
        result = upsert_many(_RECORDS, conn=conn)
        assert result["inserted"] == 1
        assert result["existing"] == 1

    def test_empty_list_returns_zero(self, db):
        conn, _ = db
        result = upsert_many([], conn=conn)
        assert result == {"inserted": 0, "existing": 0}

    def test_none_optional_fields_stored(self, db):
        conn, _ = db
        upsert_many([_RECORDS[1]], conn=conn)
        row = conn.execute(
            "SELECT vazao_vertida_m3s FROM ana_medicoes WHERE record_id=?",
            (_RECORDS[1]["record_id"],),
        ).fetchone()
        assert row[0] is None


class TestFetchRecords:
    def test_fetch_all(self, db):
        conn, db_path = db
        upsert_many(_RECORDS, conn=conn)
        records = fetch_records(conn=conn)
        assert len(records) == 2

    def test_fetch_by_reservatorio(self, db):
        conn, _ = db
        extra = {**_RECORDS[0], "record_id": "99999-2025-10-01", "reservatorio_id": "99999"}
        upsert_many(_RECORDS + [extra], conn=conn)
        records = fetch_records(conn=conn, reservatorio_id="19091")
        assert len(records) == 2
        assert all(r["reservatorio_id"] == "19091" for r in records)

    def test_pagination_limit(self, db):
        conn, _ = db
        upsert_many(_RECORDS, conn=conn)
        records = fetch_records(conn=conn, limit=1)
        assert len(records) == 1

    def test_pagination_offset(self, db):
        conn, _ = db
        upsert_many(_RECORDS, conn=conn)
        r1 = fetch_records(conn=conn, limit=1, offset=0)
        r2 = fetch_records(conn=conn, limit=1, offset=1)
        assert r1[0]["record_id"] != r2[0]["record_id"]

    def test_empty_db_returns_empty_list(self, db):
        conn, _ = db
        assert fetch_records(conn=conn) == []


class TestFetchById:
    def test_returns_correct_record(self, db):
        conn, _ = db
        upsert_many(_RECORDS, conn=conn)
        rec = fetch_by_id("19091-2025-10-01", conn=conn)
        assert rec is not None
        assert rec["cota_m"] == pytest.approx(615.22)

    def test_missing_id_returns_none(self, db):
        conn, _ = db
        assert fetch_by_id("nao-existe", conn=conn) is None

    def test_record_has_all_fields(self, db):
        conn, _ = db
        upsert_many(_RECORDS, conn=conn)
        rec = fetch_by_id("19091-2025-10-01", conn=conn)
        for field in ("record_id", "reservatorio_id", "data_hora", "cota_m"):
            assert field in rec
