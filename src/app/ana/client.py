"""
Q5 (Bônus) — Client ANA com retry/backoff e rate limit.

build_ana_url()  → monta URL com parâmetros corretos
fetch_ana_html() → faz requisição com retry em 429/5xx, timeout e User-Agent
"""

import logging
import os
import time
from typing import Optional
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# URL base do SAR ANA
_BASE_URL = "https://www.ana.gov.br/sar0/MedicaoSin"

# User-Agent responsável, identificando o bot
_USER_AGENT = (
    "ANA-Pipeline-Bot/1.0 (pesquisa academica; contato: pipeline@example.com)"
)

# Configurações padrão (sobrepõe por env vars)
_DEFAULT_RESERVATORIO = "19091"
_DEFAULT_DATA_INICIAL = "2025-10-01"
_DEFAULT_DATA_FINAL = "2025-10-07"

# Rate limit: mínimo de segundos entre requisições ao mesmo host
_MIN_REQUEST_INTERVAL = 2.0

_last_request_time: float = 0.0


def build_ana_url(
    reservatorio_id: Optional[str] = None,
    data_inicial: Optional[str] = None,
    data_final: Optional[str] = None,
) -> str:
    """
    Constrói URL completa para a API de medições da ANA.

    Usa variáveis de ambiente como fallback:
      ANA_RESERVATORIO, ANA_DATA_INICIAL, ANA_DATA_FINAL
    """
    res = reservatorio_id or os.environ.get("ANA_RESERVATORIO", _DEFAULT_RESERVATORIO)
    di = data_inicial or os.environ.get("ANA_DATA_INICIAL", _DEFAULT_DATA_INICIAL)
    df = data_final or os.environ.get("ANA_DATA_FINAL", _DEFAULT_DATA_FINAL)

    params = {
        "dropDownListReservatorios": res,
        "DataInicial": di,
        "DataFinal": df,
        "button1": "Consultar",
    }
    return f"{_BASE_URL}?{urlencode(params)}"


def _make_session(
    total_retries: int = 3,
    backoff_factor: float = 1.5,
    status_forcelist: tuple = (429, 500, 502, 503, 504),
) -> requests.Session:
    """
    Cria Session com retry automático via HTTPAdapter.
    O backoff_factor faz o tempo de espera crescer exponencialmente:
    0s, 1.5s, 3s, ... para as tentativas seguintes.
    """
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        read=total_retries,
        connect=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": _USER_AGENT})
    return session


def fetch_ana_html(
    url: Optional[str] = None,
    timeout: float = 15.0,
    reservatorio_id: Optional[str] = None,
    data_inicial: Optional[str] = None,
    data_final: Optional[str] = None,
) -> str:
    """
    Busca HTML da ANA com:
      - Rate limiting (espera mínima entre requisições)
      - Retry com backoff exponencial para 429 e 5xx
      - Timeout configurável
      - User-Agent responsável

    Args:
        url: URL completa (opcional; se None, usa build_ana_url())
        timeout: timeout em segundos
        reservatorio_id, data_inicial, data_final: passados para build_ana_url()

    Returns:
        HTML como string.

    Raises:
        requests.HTTPError: se status inesperado após todas as tentativas.
        requests.Timeout: se timeout excedido.
    """
    global _last_request_time

    target_url = url or build_ana_url(reservatorio_id, data_inicial, data_final)

    # Rate limit: garante intervalo mínimo entre requisições
    elapsed = time.monotonic() - _last_request_time
    if elapsed < _MIN_REQUEST_INTERVAL:
        wait = _MIN_REQUEST_INTERVAL - elapsed
        logger.debug("Rate limit: aguardando %.2fs antes da requisição", wait)
        time.sleep(wait)

    session = _make_session()

    logger.info("Fetching ANA URL: %s", target_url)
    try:
        resp = session.get(target_url, timeout=timeout)
        _last_request_time = time.monotonic()
        resp.raise_for_status()
        logger.info("ANA fetch OK: status=%d, bytes=%d", resp.status_code, len(resp.content))
        return resp.text
    except requests.exceptions.Timeout:
        logger.error("Timeout ao acessar ANA: %s", target_url)
        raise
    except requests.exceptions.HTTPError as exc:
        logger.error("Erro HTTP ao acessar ANA: %s — %s", target_url, exc)
        raise
    except requests.exceptions.RequestException as exc:
        logger.error("Erro de requisição ANA: %s", exc)
        raise
