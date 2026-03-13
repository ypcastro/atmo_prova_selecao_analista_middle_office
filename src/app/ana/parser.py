"""
Q3 — Parser ANA: HTML → lista de registros normalizados.

parse_ana_records(html, reservatorio_id):
  - Tolera variações de header (case-insensitive, espaços, unidades)
  - Extrai colunas relevantes
  - Constrói record_id = "{reservatorio_id}-{data_iso}"
  - Deduplica por record_id
"""

import logging
import os
import re
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup

from app.core.parsing import parse_date_mixed, safe_float_ptbr

logger = logging.getLogger(__name__)

# Mapeamento de variações de header → nome canônico interno
_HEADER_MAP: Dict[str, str] = {
    # Data
    "data": "data_hora",
    "data/hora": "data_hora",
    "data hora": "data_hora",
    # Cota
    "cota": "cota_m",
    "cota (m)": "cota_m",
    "cota(m)": "cota_m",
    # Afluência
    "afluência": "afluencia_m3s",
    "afluencia": "afluencia_m3s",
    "afluência (m³/s)": "afluencia_m3s",
    "afluencia (m3/s)": "afluencia_m3s",
    # Defluência
    "defluência": "defluencia_m3s",
    "defluencia": "defluencia_m3s",
    "defluência (m³/s)": "defluencia_m3s",
    "defluencia (m3/s)": "defluencia_m3s",
    # Vazão Vertida
    "vazão vertida": "vazao_vertida_m3s",
    "vazao vertida": "vazao_vertida_m3s",
    "vazão vertida (m³/s)": "vazao_vertida_m3s",
    # Vazão Turbinada
    "vazão turbinada": "vazao_turbinada_m3s",
    "vazao turbinada": "vazao_turbinada_m3s",
    "vazão turbinada (m³/s)": "vazao_turbinada_m3s",
    # Nível Montante
    "nível montante": "nivel_montante_m",
    "nivel montante": "nivel_montante_m",
    "nível montante (m)": "nivel_montante_m",
    # Volume Útil
    "volume útil": "volume_util_pct",
    "volume util": "volume_util_pct",
    "volume útil (%)": "volume_util_pct",
    "volume util (%)": "volume_util_pct",
}

_FLOAT_FIELDS = {
    "cota_m",
    "afluencia_m3s",
    "defluencia_m3s",
    "vazao_vertida_m3s",
    "vazao_turbinada_m3s",
    "nivel_montante_m",
    "volume_util_pct",
}


def _normalize_header(raw: str) -> str:
    """Normaliza texto de cabeçalho: minúsculas, strip, colapsa espaços."""
    cleaned = raw.strip().lower()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _map_headers(raw_headers: List[str]) -> List[Optional[str]]:
    """
    Converte lista de headers brutos em nomes canônicos.
    Retorna None para colunas desconhecidas.
    """
    result = []
    for h in raw_headers:
        norm = _normalize_header(h)
        canonical = _HEADER_MAP.get(norm)
        if canonical is None:
            logger.debug("Header desconhecido ignorado: %r", h)
        result.append(canonical)
    return result


def parse_ana_records(
    html: str,
    reservatorio_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Faz parse do HTML da ANA e retorna lista de registros.

    Args:
        html: conteúdo HTML da página de medições.
        reservatorio_id: ID do reservatório (default: env ANA_RESERVATORIO).

    Returns:
        Lista de dicts com campos canônicos + record_id único.
    """
    res_id = reservatorio_id or os.environ.get("ANA_RESERVATORIO", "19091")
    soup = BeautifulSoup(html, "html.parser")

    # Localiza a primeira tabela com dados (tolerante a múltiplas tabelas)
    table = soup.find("table")
    if table is None:
        logger.warning("Nenhuma tabela encontrada no HTML.")
        return []

    rows = table.find_all("tr")
    if not rows:
        return []

    # Extrai headers da primeira linha
    header_cells = rows[0].find_all(["th", "td"])
    raw_headers = [c.get_text(separator=" ", strip=True) for c in header_cells]
    canonical_headers = _map_headers(raw_headers)

    if "data_hora" not in canonical_headers:
        logger.warning("Coluna de data não encontrada. Headers: %s", raw_headers)
        return []

    records: Dict[str, Dict[str, Any]] = {}  # keyed by record_id para dedup

    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue

        raw_values = [c.get_text(separator=" ", strip=True) for c in cells]

        record: Dict[str, Any] = {"reservatorio_id": res_id}

        for idx, canonical in enumerate(canonical_headers):
            if canonical is None or idx >= len(raw_values):
                continue
            raw_val = raw_values[idx]

            if canonical == "data_hora":
                parsed_dt = parse_date_mixed(raw_val)
                if parsed_dt is None:
                    logger.warning("Data inválida ignorada: %r", raw_val)
                    break  # linha sem data válida é descartada
                record["data_hora"] = parsed_dt.isoformat()
                record["data_iso"] = parsed_dt.date().isoformat()
            elif canonical in _FLOAT_FIELDS:
                record[canonical] = safe_float_ptbr(raw_val)
        else:
            # Só adiciona se tiver data válida
            if "data_iso" not in record:
                continue
            record_id = f"{res_id}-{record['data_iso']}"
            record["record_id"] = record_id
            # Deduplicação: mantém primeiro encontrado
            if record_id not in records:
                records[record_id] = record
            else:
                logger.debug("Registro duplicado ignorado: %s", record_id)

    result = list(records.values())
    logger.info("parse_ana_records: %d registros extraídos (res=%s)", len(result), res_id)
    return result
