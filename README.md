# Prova Técnica — Pipeline End-to-End (ANA → ETL → Agendamento → API → Análise) [VERSÃO ABERTA]

Este repositório é uma prova em formato **Git**:
1) faça fork
2) implemente as tarefas
3) abra um PR

A prova foi desenhada para avaliar **capacidade de construir e manter** um pipeline real,
incluindo leitura de código legado, organização, testes, idempotência e exposição em API.

Fonte pública (modo live opcional):
- https://www.ana.gov.br/sar0/MedicaoSin

**Importante:** os testes oficiais usam um **snapshot local** (arquivo `data/ana_snapshot.html`) para reprodutibilidade.
O modo `live` é bônus e pode falhar por instabilidade externa.

---

## Objetivo do pipeline

Implementar um pipeline que:
1) Extrai medições da ANA (HTML) — snapshot e/ou live
2) Faz parsing e normalização (tipos, datas, números pt-BR, validações)
3) Persiste em SQLite com **idempotência**
4) Roda automaticamente em frequência definida (estilo “cron”)
5) Expõe dados via FastAPI (consulta + endpoint para disparar extração)
6) Faz uma análise simples (métricas + interpretação)

---

## Como rodar

### Local
```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
pytest -q

uvicorn app.api.main:app --reload --port 8000
```

### Docker
```bash
docker compose up --build
# API: http://localhost:8000/docs

docker compose run --rm tests
```

---

## Variáveis de ambiente

- `APP_DATA_DIR` (default: `data`)
- `ANA_MODE` (`snapshot|live`, default: `snapshot`)
- `PIPELINE_INTERVAL_SECONDS` (default: `60`)
- `ANA_RESERVATORIO` (default: `19091`)
- `ANA_DATA_INICIAL` (default: `2025-10-01`)
- `ANA_DATA_FINAL` (default: `2025-10-07`)

---

## Regras (scraping responsável)
- Não contornar login/CAPTCHA/WAF.
- Use timeout, User-Agent, retry/backoff para 429/5xx e rate limit.
- `live` é opcional (bônus). O essencial é passar no snapshot.

---

# Entregas

- Código implementado (TODOs)
- Testes passando (`pytest -q`)
- `DECISIONS.md` explicando decisões (trade-offs, schema, idempotência, scheduler)
- (Bônus) observabilidade: logs úteis, checkpoint, artefatos (raw/normalized)

---

# Tarefas (7 questões)

## Q1 — Parsing robusto (datas e números pt-BR)
Arquivo: `src/app/core/parsing.py` + `tests/test_parsing.py`

Implemente:
- `parse_date_mixed()` suportando:
  - `DD/MM/YYYY`, `DD/MM/YYYY HH:MM:SS`
  - ISO `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM:SS` (com ou sem timezone/Z)
- `safe_float_ptbr()`:
  - aceitar `1.234,56`, `1234,56`, `1234.56`
  - retornar `None` para tokens ausentes (`""`, `"—"`, `"NaN"`, `"inf"` etc.)


## Q2 — Estruturar I/O do pipeline (artefatos operacionais)
Arquivo: `src/app/core/pipeline_io.py` + `tests/test_pipeline_io.py`

Implementar uma classe (ou funções) para:
- salvar **raw HTML** da extração
- salvar JSON normalizado da rodada
- salvar **checkpoint** de execução (success/fail, inserted/existing, erro, timestamp)
- garantir escrita segura (evitar arquivo corrompido)


## Q3 — Parser ANA (HTML → registros)
Arquivo: `src/app/ana/parser.py` + `tests/test_ana_parser.py`

Implementar `parse_ana_records(html)`:
- tolerar pequenas variações de header
- extrair colunas relevantes e construir `record_id = "{reservatorio_id}-{data_iso}"`
- deduplicar por `record_id`


## Q4 — Normalização e validação (schema)
Arquivo: `src/app/core/transforms.py` + `tests/test_transforms.py`

Implementar:
- `normalize_record()` (tipos e nomes canônicos)
- `validate_record()` (regras mínimas; raise ValueError)
- `deduplicar()` O(n)


## Q5 — Client ANA (modo live) [BÔNUS]
Arquivo: `src/app/ana/client.py` + `tests/test_ana_client.py`

Implementar:
- `build_ana_url()` com params corretos
- `fetch_ana_html()` com retry/backoff (429/5xx), rate limit e timeouts

Observação: os testes não exigem live real; há testes unitários de URL e comportamento de retry (mock).


## Q6 — Persistência idempotente (SQLite)
Arquivo: `src/app/core/storage.py` + `tests/test_storage.py`

Implementar:
- `init_db()` cria schema
- `upsert_many()` idempotente e retorna contagem `{inserted, existing}`
- `fetch_records()` e `fetch_by_id()`


## Q7 — Orquestração (job), Scheduler e API + Análise
- Job: `src/app/jobs/extract_job.py`
- Scheduler: `src/app/jobs/scheduler.py`
- API: `src/app/api/main.py`
- Análise: `src/app/analysis/ana_analysis.py`

Implementar:
- `run_once()` executa extract→parse→normalize→validate→upsert e escreve artefatos/checkpoint
- scheduler que roda a cada `PIPELINE_INTERVAL_SECONDS` sem “drift” grosseiro
- API:
  - `POST /extract/ana` (dispara 1 rodada)
  - `GET /ana/medicoes`
  - `GET /ana/medicoes/{record_id}`
  - `GET /ana/checkpoint`
  - `GET /ana/analysis`

---


Boa Prova e Boa Sorte!