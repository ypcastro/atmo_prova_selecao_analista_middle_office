from __future__ import annotations

from pathlib import Path

from app.core.config import load_settings
from app.core.pipeline_io import PipelineIO
from app.ana.parser import parse_ana_records
from app.core.storage import init_db, upsert_many
from app.core.transforms import normalize_record, validate_record


def run_once() -> dict[str, int]:
    """TODO (Q7): executar 1 rodada extract→parse→normalize→validate→upsert + artefatos/checkpoint."""
    s = load_settings()
    print(f"Running extract job with settings: {s}")

    io = PipelineIO(s.data_dir)
    PATH = 'data/ana_snapshot.html'
    html_path = Path(PATH)
    html = html_path.read_text(encoding="utf-8")
    io.write_raw_html(source='ana', html=html)

    parsed = parse_ana_records(html)

    normalized = []

    for row in parsed: 
        n = normalize_records(row)
        validate_records(n)
        normalized.append(n)
        io.write_normalized_records(source='ana', records=normalized)
        con = init_db(s.db_path)

    try:
        res = upsert_many(con, normalized)
        io.write_checkpoint(
            status = 'success',
            inserted=res.inserted,
            existing=res.existing,
        )
        return {
            'inserted': res.inserted,
            'existing': res.existing,
        }
    except Exception as e:
        io.write_checkpoint(
            status = 'fail',
            error=str(e),
        )
        raise e
    finally:
        con.close()
