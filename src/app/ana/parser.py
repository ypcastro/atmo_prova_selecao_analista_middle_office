from __future__ import annotations

import re
import unicodedata
from typing import Any

from bs4 import BeautifulSoup

from app.core.parsing import parse_date_mixed
from app.core.transforms import dedupe


def _normalize_header(text: str) -> str:
    clean = unicodedata.normalize("NFKD", text)
    clean = clean.encode("ascii", "ignore").decode("ascii")
    clean = clean.lower()
    clean = re.sub(r"\(.*?\)", "", clean)
    clean = re.sub(r"[^a-z0-9]+", " ", clean).strip()
    return clean


_HEADER_MAP = {
    "codigo do reservatorio": "reservatorio_id",
    "reservatorio": "reservatorio",
    "cota": "cota_m",
    "afluencia": "afluencia_m3s",
    "defluencia": "defluencia_m3s",
    "vazao vertida": "vazao_vertida_m3s",
    "vazao turbinada": "vazao_turbinada_m3s",
    "vazao natural": "vazao_natural_m3s",
    "volume util": "volume_util_pct",
    "vazao incremental": "vazao_incremental_m3s",
    "data da medicao": "data_medicao",
    "estado": "uf",
    "uf": "uf",
    "subsistema": "subsistema",
}


def _find_table(soup: BeautifulSoup):
    table = soup.find("table", id="registros")
    if table is not None:
        return table

    for candidate in soup.find_all("table"):
        headers = [
            _normalize_header(th.get_text(" ", strip=True))
            for th in candidate.find_all("th")
        ]
        if "codigo do reservatorio" in headers and "data da medicao" in headers:
            return candidate

    return None


def parse_ana_records(html: str) -> list[dict[str, Any]]:
    """Parse ANA HTML into deduplicated record dictionaries."""
    soup = BeautifulSoup(html, "html.parser")
    table = _find_table(soup)
    if table is None:
        return []

    header_cells = table.find_all("th")
    if not header_cells:
        return []

    headers: list[str] = []
    for idx, cell in enumerate(header_cells):
        normalized = _normalize_header(cell.get_text(" ", strip=True))
        headers.append(_HEADER_MAP.get(normalized, f"col_{idx}"))

    rows: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        td_cells = tr.find_all("td")
        if not td_cells:
            continue

        values = [td.get_text(" ", strip=True) for td in td_cells]
        row: dict[str, Any] = {}
        for idx, value in enumerate(values):
            if idx < len(headers):
                row[headers[idx]] = value

        if "reservatorio_id" not in row or "data_medicao" not in row:
            continue

        data_iso = parse_date_mixed(str(row["data_medicao"])).isoformat()
        row["data_medicao"] = data_iso
        row["record_id"] = f"{str(row['reservatorio_id']).strip()}-{data_iso}"
        rows.append(row)

    return dedupe(rows, "record_id")
