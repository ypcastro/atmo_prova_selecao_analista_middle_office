from __future__ import annotations

import argparse
import csv
import json
import time
import unicodedata
from pathlib import Path
from typing import Any

import httpx
from bs4 import BeautifulSoup

from app.core.config import load_settings
from app.core.storage import fetch_reservoir_catalog, init_db, upsert_reservoir_catalog
from app.core.subsystems import infer_subsistema_from_uf_text

ANA_MEDICAO_URL = "https://www.ana.gov.br/sar0/MedicaoSin"


_STATE_NAME_TO_UF = {
    "acre": "AC",
    "alagoas": "AL",
    "amapa": "AP",
    "amazonas": "AM",
    "bahia": "BA",
    "ceara": "CE",
    "distrito federal": "DF",
    "espirito santo": "ES",
    "goias": "GO",
    "maranhao": "MA",
    "mato grosso": "MT",
    "mato grosso do sul": "MS",
    "minas gerais": "MG",
    "para": "PA",
    "paraiba": "PB",
    "parana": "PR",
    "pernambuco": "PE",
    "piaui": "PI",
    "rio de janeiro": "RJ",
    "rio grande do norte": "RN",
    "rio grande do sul": "RS",
    "rondonia": "RO",
    "roraima": "RR",
    "santa catarina": "SC",
    "sao paulo": "SP",
    "sergipe": "SE",
    "tocantins": "TO",
}


def _normalize_text(value: str) -> str:
    clean = unicodedata.normalize("NFKD", value)
    clean = clean.encode("ascii", "ignore").decode("ascii")
    return " ".join(clean.lower().split())


def _state_to_uf(state_name: str) -> str | None:
    return _STATE_NAME_TO_UF.get(_normalize_text(state_name))


def _parse_options(html: str, select_id: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    select = soup.find("select", {"id": select_id})
    if select is None:
        return []

    output: list[tuple[str, str]] = []
    for option in select.find_all("option"):
        value = str(option.get("value") or "").strip()
        label = option.get_text(" ", strip=True)
        if not value or not label:
            continue
        output.append((value, " ".join(label.split())))
    return output


def _load_optional_metadata_csv(data_dir: Path) -> dict[int, dict[str, Any]]:
    path = data_dir / "reservatorios_metadata.csv"
    if not path.exists():
        return {}

    output: dict[int, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            rid_raw = str(row.get("reservatorio_id") or "").strip()
            if not rid_raw.isdigit():
                continue
            rid = int(rid_raw)
            output[rid] = {
                "uf": (str(row.get("uf") or "").strip().upper() or None),
                "subsistema": (
                    str(row.get("subsistema") or "").strip().upper() or None
                ),
            }
    return output


def fetch_reservoir_catalog_from_ana(
    *,
    timeout_s: float = 20.0,
    pause_s: float = 0.15,
) -> list[dict[str, Any]]:
    """Fetch reservoir catalog (id, name, state/UF) from ANA MedicaoSin page."""
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        base_html = client.get(ANA_MEDICAO_URL).text
        state_options = _parse_options(base_html, "dropDownListEstados")

        merged: dict[int, dict[str, Any]] = {}
        for state_code_text, state_name in state_options:
            if not state_code_text.isdigit():
                continue

            state_code = int(state_code_text)
            uf = _state_to_uf(state_name)
            html = client.get(
                ANA_MEDICAO_URL,
                params={"dropDownListEstados": state_code},
            ).text
            reservoir_options = _parse_options(html, "dropDownListReservatorios")

            for rid_text, reservoir_name in reservoir_options:
                if not rid_text.isdigit():
                    continue
                rid = int(rid_text)
                entry = merged.get(rid)
                if entry is None:
                    entry = {
                        "reservatorio_id": rid,
                        "reservatorio": reservoir_name,
                        "estado_codigo_ana": state_code,
                        "estado_nome": state_name,
                        "uf": uf,
                        "subsistema": None,
                        "source": "ana_medicao_sin",
                        "_estados": {state_name},
                        "_ufs": {uf} if uf else set(),
                    }
                    merged[rid] = entry
                else:
                    entry["_estados"].add(state_name)
                    if uf:
                        entry["_ufs"].add(uf)

            if pause_s > 0:
                time.sleep(pause_s)

    rows: list[dict[str, Any]] = []
    for rid, entry in merged.items():
        estados = sorted(entry.pop("_estados"))
        ufs = sorted(entry.pop("_ufs"))
        entry["estado_nome"] = ", ".join(estados) if estados else None
        entry["uf"] = ", ".join(ufs) if ufs else None
        entry["subsistema"] = infer_subsistema_from_uf_text(entry.get("uf"))
        rows.append(entry)

    rows.sort(
        key=lambda x: (str(x.get("reservatorio") or ""), int(x["reservatorio_id"]))
    )
    return rows


def sync_catalog_to_db(
    *,
    db_path: Path,
    data_dir: Path | None = None,
    timeout_s: float = 20.0,
    pause_s: float = 0.15,
) -> dict[str, Any]:
    """Fetch ANA reservoir catalog and upsert into ana_reservatorios table."""
    rows = fetch_reservoir_catalog_from_ana(timeout_s=timeout_s, pause_s=pause_s)
    metadata = _load_optional_metadata_csv(data_dir) if data_dir is not None else {}
    for row in rows:
        extra = metadata.get(int(row["reservatorio_id"]))
        if not extra:
            extra = {}
        for field in ("uf", "subsistema"):
            value = extra.get(field)
            if value not in (None, ""):
                row[field] = value
        if row.get("subsistema") in (None, ""):
            row["subsistema"] = infer_subsistema_from_uf_text(row.get("uf"))

    con = init_db(db_path)
    try:
        result = upsert_reservoir_catalog(con, rows)
        return {
            "status": "success",
            "catalog_count": len(rows),
            "inserted": result.inserted,
            "existing": result.existing,
            "metadata_overrides": len(metadata),
            "db_path": str(db_path),
        }
    finally:
        con.close()


def _print_table(rows: list[dict[str, Any]]) -> None:
    print(f"{'ID':<8} {'RESERVATORIO':<34} {'UF':<8} {'ESTADO':<22} {'SUBSISTEMA':<12}")
    print("-" * 95)
    for row in rows:
        rid = str(row.get("reservatorio_id") or "")
        name = str(row.get("reservatorio") or "")
        uf = str(row.get("uf") or "")
        estado = str(row.get("estado_nome") or "")
        subsistema = str(row.get("subsistema") or "")
        print(
            f"{rid:<8} {name[:34]:<34} {uf[:8]:<8} {estado[:22]:<22} {subsistema[:12]:<12}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="ANA reservoir catalog utility")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser(
        "sync", help="Fetch ANA catalog and sync into DB"
    )
    sync_parser.add_argument("--db-path", type=Path, default=None)
    sync_parser.add_argument("--timeout-s", type=float, default=20.0)
    sync_parser.add_argument("--pause-s", type=float, default=0.15)
    sync_parser.add_argument("--json", action="store_true", dest="json_output")

    list_parser = subparsers.add_parser("list", help="List catalog from DB")
    list_parser.add_argument("--db-path", type=Path, default=None)
    list_parser.add_argument("--limit", type=int, default=1000)
    list_parser.add_argument("--uf", type=str, default=None)
    list_parser.add_argument("--json", action="store_true", dest="json_output")

    args = parser.parse_args()
    settings = load_settings()
    db_path = args.db_path or settings.db_path

    if args.command == "sync":
        result = sync_catalog_to_db(
            db_path=db_path,
            data_dir=settings.data_dir,
            timeout_s=args.timeout_s,
            pause_s=args.pause_s,
        )
        if args.json_output:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(result)
        return

    con = init_db(db_path)
    try:
        rows = fetch_reservoir_catalog(con, limit=args.limit, uf=args.uf)
    finally:
        con.close()

    if args.json_output:
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    else:
        _print_table(rows)


if __name__ == "__main__":
    main()
