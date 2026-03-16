from __future__ import annotations

from datetime import date


ANA_BASE_URL = "https://www.ana.gov.br/sar0/MedicaoSin"


class AnaClientError(RuntimeError):
    pass


def build_ana_url(*, reservatorio: int, data_inicial: date, data_final: date) -> str:
    """TODO (Q5): montar URL correta com parametros necessários."""
    raise NotImplementedError


def fetch_ana_html(
    *,
    url: str,
    timeout_s: float = 10.0,
    max_retries: int = 3,
    backoff_s: float = 0.5,
    rate_limit_s: float = 1.0,
    user_agent: str = "ana-pipeline-challenge/1.0",
) -> str:
    """TODO (Q5): implementar GET com retry/backoff para 429/5xx e rate limit."""
    raise NotImplementedError
