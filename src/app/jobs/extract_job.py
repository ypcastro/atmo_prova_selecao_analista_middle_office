from __future__ import annotations

import argparse
import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from app.ana.client import build_ana_url, fetch_ana_html
from app.ana.parser import parse_ana_records
from app.core.config import Settings, load_settings
from app.core.enrichment import enrich_record
from app.core.logging_setup import configure_structured_logging, format_log_event
from app.core.pipeline_io import PipelineIO
from app.core.reservoir_metadata import load_reservoir_metadata
from app.core.storage import (
    fetch_reservoir_metadata_map,
    init_db,
    refresh_measurement_metadata_from_catalog,
    upsert_many,
)
from app.core.transforms import dedupe, normalize_record, validate_record


@dataclass(frozen=True)
class RunOptions:
    dry_run: bool = False
    log_level: str = "INFO"
    since: date | None = None
    until: date | None = None
    force: bool = False


def _configure_logging(log_level: str) -> None:
    configure_structured_logging(log_level)


def _log_event(logger: logging.Logger, level: int, **fields: Any) -> None:
    logger.log(level, format_log_event(**fields))


def _parse_date_arg(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from exc


def _resolve_window(
    settings: Settings,
    io: PipelineIO,
    options: RunOptions,
    *,
    now_utc: datetime,
) -> tuple[date, date, str | None, str]:
    data_inicial = settings.ana_data_inicial
    data_final = settings.ana_data_final
    watermark_key: str | None = None
    source = "settings"

    if options.since is not None:
        data_inicial = options.since
        source = "cli"
    if options.until is not None:
        data_final = options.until
        source = "cli"

    if (
        settings.ana_mode == "live"
        and options.since is None
        and options.until is None
        and not options.force
    ):
        watermark_key = f"live:{settings.ana_reservatorio}"
        watermark_value = io.read_watermark_value(watermark_key)
        if watermark_value is not None:
            watermark_date = date.fromisoformat(watermark_value)
            data_inicial = watermark_date + timedelta(days=1)
            data_final = now_utc.date()
            source = "watermark"

    if data_inicial > data_final:
        data_inicial = data_final

    return data_inicial, data_final, watermark_key, source


def _load_html(
    settings: Settings, *, data_inicial: date, data_final: date
) -> tuple[str, str]:
    if settings.ana_mode == "live":
        url = build_ana_url(
            reservatorio=settings.ana_reservatorio,
            data_inicial=data_inicial,
            data_final=data_final,
        )
        return "live", fetch_ana_html(url=url)

    snapshot_path = settings.data_dir / "ana_snapshot.html"
    html = snapshot_path.read_text(encoding="utf-8")
    return "snapshot", html


def _max_data_medicao(rows: list[dict[str, Any]]) -> str | None:
    values = [str(row["data_medicao"]) for row in rows if row.get("data_medicao")]
    if not values:
        return None
    return max(values)


def run_once(options: RunOptions | None = None) -> dict[str, Any]:
    """Run one full extraction cycle and persist artifacts/checkpoint."""
    options = options or RunOptions()
    _configure_logging(options.log_level)
    logger = logging.getLogger("app.jobs.extract_job")
    run_id = uuid.uuid4().hex
    job_name = "extract_job"
    started_at = time.perf_counter()

    settings = load_settings()
    io = PipelineIO(settings.data_dir)
    source = settings.ana_mode
    now_utc = datetime.now(timezone.utc)
    data_inicial, data_final, watermark_key, window_source = _resolve_window(
        settings,
        io,
        options,
        now_utc=now_utc,
    )
    con = None if options.dry_run else init_db(settings.db_path)

    _log_event(
        logger,
        logging.INFO,
        job_name=job_name,
        run_id=run_id,
        step="start",
        mode=settings.ana_mode,
        dry_run=options.dry_run,
        data_inicial=data_inicial.isoformat(),
        data_final=data_final.isoformat(),
        window_source=window_source,
        force=options.force,
    )

    try:
        source, html = _load_html(
            settings,
            data_inicial=data_inicial,
            data_final=data_final,
        )
        _log_event(
            logger,
            logging.INFO,
            job_name=job_name,
            run_id=run_id,
            step="fetch_html",
            source=source,
            records_in=0,
            records_out=0,
            invalid=0,
        )

        raw_path = io.write_raw_html(source=source, html=html)
        catalog_metadata = fetch_reservoir_metadata_map(con) if con is not None else {}
        csv_metadata = load_reservoir_metadata(settings.data_dir)
        metadata_by_reservoir = _merge_metadata(catalog_metadata, csv_metadata)

        parse_started = time.perf_counter()
        parsed_rows = parse_ana_records(html)
        invalid_rows = 0
        normalized_rows: list[dict[str, Any]] = []
        for parsed in parsed_rows:
            try:
                normalized = normalize_record(parsed)
                validate_record(normalized)
                normalized_rows.append(
                    enrich_record(
                        normalized,
                        metadata_by_reservoir=metadata_by_reservoir,
                    )
                )
            except ValueError:
                invalid_rows += 1

        normalized_rows = dedupe(normalized_rows, "record_id")
        _log_event(
            logger,
            logging.INFO,
            job_name=job_name,
            run_id=run_id,
            step="parse_normalize_validate",
            records_in=len(parsed_rows),
            records_out=len(normalized_rows),
            invalid=invalid_rows,
            duration_ms=round((time.perf_counter() - parse_started) * 1000, 2),
        )

        normalized_path = io.write_normalized_json(source=source, rows=normalized_rows)

        inserted = 0
        existing = 0
        refreshed_metadata_rows = 0
        status = "dry_run" if options.dry_run else "success"

        if con is not None:
            load_started = time.perf_counter()
            result = upsert_many(con, normalized_rows)
            inserted = result.inserted
            existing = result.existing
            refreshed_metadata_rows = refresh_measurement_metadata_from_catalog(con)
            _log_event(
                logger,
                logging.INFO,
                job_name=job_name,
                run_id=run_id,
                step="load_upsert",
                records_in=len(normalized_rows),
                records_out=inserted + existing,
                invalid=0,
                inserted=inserted,
                existing=existing,
                duration_ms=round((time.perf_counter() - load_started) * 1000, 2),
            )

            if source == "live" and watermark_key and len(normalized_rows) > 0:
                watermark_value = _max_data_medicao(normalized_rows)
                if watermark_value is not None:
                    io.write_watermark(watermark_key, watermark_value)

        payload = {
            "status": status,
            "processed": len(normalized_rows),
            "inserted": inserted,
            "existing": existing,
            "source": source,
            "run_id": run_id,
        }

        io.write_checkpoint(
            status=status,
            inserted=inserted,
            existing=existing,
            meta={
                "mode": settings.ana_mode,
                "source": source,
                "processed": len(normalized_rows),
                "invalid": invalid_rows,
                "catalog_metadata_entries": len(catalog_metadata),
                "csv_metadata_entries": len(csv_metadata),
                "merged_metadata_entries": len(metadata_by_reservoir),
                "watermark_key": watermark_key,
                "window_source": window_source,
                "data_inicial": data_inicial.isoformat(),
                "data_final": data_final.isoformat(),
                "rows_refreshed_from_catalog": refreshed_metadata_rows,
                "raw_path": str(raw_path),
                "normalized_path": str(normalized_path),
            },
        )

        _log_event(
            logger,
            logging.INFO,
            job_name=job_name,
            run_id=run_id,
            step="finish",
            status=status,
            processed=len(normalized_rows),
            inserted=inserted,
            existing=existing,
            invalid=invalid_rows,
            duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
        )
        return payload
    except Exception as exc:
        _log_event(
            logger,
            logging.ERROR,
            job_name=job_name,
            run_id=run_id,
            step="fail",
            error=str(exc),
            duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
        )
        io.write_checkpoint(
            status="fail",
            error=str(exc),
            meta={"mode": settings.ana_mode, "source": source},
        )
        raise
    finally:
        if con is not None:
            con.close()


def _merge_metadata(
    base: dict[int, dict[str, Any]],
    override: dict[int, dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    output: dict[int, dict[str, Any]] = {rid: dict(meta) for rid, meta in base.items()}
    for rid, extra in override.items():
        target = output.setdefault(rid, {})
        for key, value in extra.items():
            if value in (None, ""):
                continue
            target[key] = value
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run ANA extraction job once")
    parser.add_argument(
        "--dry-run", action="store_true", help="Run extraction without writing to DB."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    parser.add_argument(
        "--since", type=_parse_date_arg, default=None, help="Start date (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--until", type=_parse_date_arg, default=None, help="End date (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore watermark when running in live mode.",
    )
    args = parser.parse_args(argv)

    options = RunOptions(
        dry_run=args.dry_run,
        log_level=args.log_level,
        since=args.since,
        until=args.until,
        force=args.force,
    )
    payload = run_once(options=options)
    # Keep CLI output in JSON for easier scripting/automation.
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
