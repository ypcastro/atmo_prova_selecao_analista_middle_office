from pathlib import Path
from app.ana.parser import parse_ana_records


def test_parse_snapshot_extracts_rows_and_dedupes():
    html = Path("data/ana_snapshot.html").read_text(encoding="utf-8")
    rows = parse_ana_records(html)
    assert len(rows) == 7
    assert rows[0]["record_id"].startswith("19091-2025-10-")


def test_parser_tolerates_header_variation():
    html = Path("data/ana_snapshot.html").read_text(encoding="utf-8")
    html2 = html.replace("Afluência (m³/s)", "Afluencia (m3/s)")
    rows = parse_ana_records(html2)
    assert len(rows) == 7
