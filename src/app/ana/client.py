from __future__ import annotations

import time
from datetime import date
from urllib.parse import urlencode

import httpx

ANA_BASE_URL = "https://www.ana.gov.br/sar0/MedicaoSin"


class AnaClientError(RuntimeError):
    pass


def build_ana_url(*, reservatorio: int, data_inicial: date, data_final: date) -> str:
    """Build ANA query URL for one reservoir and date range."""
    reservatorio_id = str(int(reservatorio))
    params = {
        # Keep both parameter names for compatibility with legacy/new handlers.
        "dropDownListReservatorios": reservatorio_id,
        "reservatorio": reservatorio_id,
        # The ANA form generally works with BR date format.
        "dataInicial": data_inicial.strftime("%d/%m/%Y"),
        "dataFinal": data_final.strftime("%d/%m/%Y"),
        # Required by the endpoint to return table rows instead of empty result.
        "button": "Buscar",
    }
    return f"{ANA_BASE_URL}?{urlencode(params)}"


def fetch_ana_html(
    *,
    url: str,
    timeout_s: float = 30.0,
    max_retries: int = 5,
    backoff_s: float = 1.0,
    rate_limit_s: float = 1.5,
    user_agent: str = "ana-pipeline-challenge/1.0",
) -> str:
    """Fetch ANA HTML with timeout, retry/backoff and rate limiting."""
    retries = max(0, int(max_retries))
    with httpx.Client(
        headers={"User-Agent": user_agent},
        timeout=timeout_s,
        follow_redirects=True,
    ) as client:
        for attempt in range(retries + 1):
            if attempt > 0 and rate_limit_s > 0:
                time.sleep(rate_limit_s)

            try:
                response = client.get(url)
            except httpx.TimeoutException as exc:
                if attempt < retries:
                    time.sleep(backoff_s * (2**attempt))
                    continue
                raise AnaClientError(f"timeout fetching ANA HTML: {exc}") from exc
            except httpx.HTTPError as exc:
                if attempt < retries:
                    time.sleep(backoff_s * (2**attempt))
                    continue
                raise AnaClientError(f"http error fetching ANA HTML: {exc}") from exc

            if 200 <= response.status_code < 300:
                return response.text

            is_retryable = (
                response.status_code == 429 or 500 <= response.status_code < 600
            )
            if is_retryable and attempt < retries:
                time.sleep(backoff_s * (2**attempt))
                continue

            raise AnaClientError(
                f"ANA returned status {response.status_code}: {response.text[:200]}"
            )

    raise AnaClientError("unable to fetch ANA HTML after retries")
