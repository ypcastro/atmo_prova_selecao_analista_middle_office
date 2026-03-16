from __future__ import annotations

from typing import Any
from bs4 import BeautifulSoup
from datetime import datetime

def normalize_header(text: str) -> str:
    text = text.lower()
    text = text.replace("ç", "c")
    text = text.replace("á", "a")
    text = text.replace("é", "e")
    text = text.replace("/", "")
    text = text.replace("ã", "a")
    text = text.replace("³", "3")
    return text.strip()

def parse_ana_records(html: str) -> list[dict[str, Any]]:
    """TODO (Q3): parse HTML em registros."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "registros"})
    if not table:
        return []
    
    headers = [
        normalize_header(th.get_text(strip=True)) for th in table.find_all("th")
    ]

    rows = []
    seen = set()

    for tr in table.find_all("tr")[1:]:
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not cols:
            continue

        row = dict(zip(headers, cols))

        reservatorio_id = row.get("código do reservatório")
        data_str = row.get("data da medicao")
        if not reservatorio_id or not data_str:
            continue

        data_iso = datetime.strptime(data_str, "%d/%m/%Y").date().isoformat()
        record_id = f"{reservatorio_id}-{data_iso}"
        if record_id in seen:
            continue
        seen.add(record_id)

        rows.append(
            {
                "record_id": record_id,
                "reservatorio_id": reservatorio_id,
                "reservatorio":row.get("reservatório"),
                "data": data_iso,
                "cota_m":row.get("cota m"),
                "volume_m3":row.get("afluencia m3s"),
                "defluencia_m3s":row.get("defluencia m3s"),
                "volume_util_percent": row.get("volume útil %")
            }
        )

    return rows