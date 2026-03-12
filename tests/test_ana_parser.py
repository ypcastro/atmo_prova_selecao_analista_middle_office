"""Testes Q3 — ana/parser.py"""

import pytest

from app.ana.parser import parse_ana_records

# HTML base com 3 registros
_SNAPSHOT_HTML = """
<table>
  <tr>
    <th>Data</th>
    <th>Cota (m)</th>
    <th>Afluência (m³/s)</th>
    <th>Defluência (m³/s)</th>
    <th>Volume Útil (%)</th>
  </tr>
  <tr><td>01/10/2025</td><td>615,22</td><td>1.234,56</td><td>980,00</td><td>72,30</td></tr>
  <tr><td>02/10/2025</td><td>615,45</td><td>1.300,10</td><td>1.000,00</td><td>73,10</td></tr>
  <tr><td>03/10/2025</td><td>615,80</td><td>1.450,20</td><td>1.100,00</td><td>74,50</td></tr>
</table>
"""

# HTML com valores ausentes (—)
_HTML_WITH_ABSENT = """
<table>
  <tr>
    <th>Data</th><th>Cota (m)</th><th>Afluência (m³/s)</th><th>Defluência (m³/s)</th>
  </tr>
  <tr><td>05/10/2025</td><td>615,60</td><td>950,40</td><td>—</td></tr>
</table>
"""

# HTML com headers em variação (case + espaços)
_HTML_VARIANT_HEADERS = """
<table>
  <tr>
    <th>  DATA  </th><th>COTA (M)</th><th>AFLUÊNCIA (M³/S)</th>
  </tr>
  <tr><td>06/10/2025</td><td>615,30</td><td>870,00</td></tr>
</table>
"""

# HTML com registros duplicados
_HTML_DUPLICATE = """
<table>
  <tr><th>Data</th><th>Cota (m)</th></tr>
  <tr><td>01/10/2025</td><td>615,22</td></tr>
  <tr><td>01/10/2025</td><td>999,99</td></tr>
</table>
"""


class TestParseAnaRecords:
    def test_returns_correct_count(self):
        records = parse_ana_records(_SNAPSHOT_HTML, reservatorio_id="19091")
        assert len(records) == 3

    def test_record_id_format(self):
        records = parse_ana_records(_SNAPSHOT_HTML, reservatorio_id="19091")
        for r in records:
            assert r["record_id"].startswith("19091-")
            assert "-2025-10-" in r["record_id"]

    def test_fields_parsed(self):
        records = parse_ana_records(_SNAPSHOT_HTML, reservatorio_id="19091")
        r = records[0]
        assert r["cota_m"] == pytest.approx(615.22)
        assert r["afluencia_m3s"] == pytest.approx(1234.56)
        assert r["defluencia_m3s"] == pytest.approx(980.0)
        assert r["volume_util_pct"] == pytest.approx(72.30)

    def test_absent_values_are_none(self):
        records = parse_ana_records(_HTML_WITH_ABSENT, reservatorio_id="19091")
        assert len(records) == 1
        assert records[0]["defluencia_m3s"] is None

    def test_variant_headers_tolerated(self):
        records = parse_ana_records(_HTML_VARIANT_HEADERS, reservatorio_id="19091")
        assert len(records) == 1
        assert records[0]["cota_m"] == pytest.approx(615.30)
        assert records[0]["afluencia_m3s"] == pytest.approx(870.0)

    def test_deduplication_keeps_first(self):
        records = parse_ana_records(_HTML_DUPLICATE, reservatorio_id="19091")
        assert len(records) == 1
        assert records[0]["cota_m"] == pytest.approx(615.22)

    def test_empty_table_returns_empty_list(self):
        html = "<table><tr><th>Data</th></tr></table>"
        records = parse_ana_records(html, reservatorio_id="19091")
        assert records == []

    def test_no_table_returns_empty_list(self):
        records = parse_ana_records("<html><body>sem tabela</body></html>", "19091")
        assert records == []

    def test_reservatorio_id_in_record(self):
        records = parse_ana_records(_SNAPSHOT_HTML, reservatorio_id="99999")
        for r in records:
            assert r["reservatorio_id"] == "99999"

    def test_data_iso_format(self):
        records = parse_ana_records(_SNAPSHOT_HTML, reservatorio_id="19091")
        for r in records:
            assert len(r["data_iso"]) == 10
            assert r["data_iso"][4] == "-"

    def test_full_snapshot_html(self):
        """Testa o snapshot oficial do projeto."""
        from pathlib import Path
        snap = Path("data/ana_snapshot.html")
        if snap.exists():
            html = snap.read_text(encoding="utf-8")
            records = parse_ana_records(html, reservatorio_id="19091")
            assert len(records) >= 7
