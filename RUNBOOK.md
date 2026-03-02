# RUNBOOK - ANA_Pipeline

Guia operacional direto ao ponto.

## 1) Setup inicial

### Windows

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -r requirements.txt
$env:PYTHONPATH='src'
```

### Linux/macOS

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r requirements.txt
export PYTHONPATH=src
```

## 2) Operacao por objetivo

### Objetivo A: validar rapido se o pipeline funciona

```powershell
$env:ANA_MODE='snapshot'
python -c "from app.jobs.extract_job import run_once; print(run_once())"
```

Esperado:

1. `status=success` ou `status=dry_run`.
2. `checkpoint.json` atualizado.

### Objetivo B: buscar dados reais da ANA

```powershell
$env:ANA_MODE='live'
$env:ANA_RESERVATORIO='19119'
python -m app.jobs.extract_job --since 2025-12-01 --until 2026-03-01 --force --log-level INFO
```

### Objetivo C: rodar sem gravar banco (inspecao)

```powershell
python -m app.jobs.extract_job --dry-run --log-level DEBUG
```

### Objetivo D: ligar API

```powershell
python -m uvicorn app.api.main:app --reload --port 8000
```

### Objetivo E: rodar continuamente

```powershell
python -m app.jobs.scheduler
```

## 3) Backfill historico

### 3.1 Principais reservatorios

```powershell
.\scripts\backfill_principais.ps1 -StartDate '2025-01-01' -EndDate '2025-03-31' -SyncCatalog
```

### 3.2 Sincronizar catalogo de reservatorios

```powershell
python -m app.ana.catalog sync --json
python -m app.ana.catalog list --limit 1000
```

## 4) Artefatos gerados

1. Banco: `data/out/ana.db`
2. Checkpoint: `data/out/checkpoint.json`
3. Watermark: `data/out/watermark.json`
4. Raw HTML: `data/out/raw/`
5. Normalized JSON: `data/out/normalized/`
6. Backfill CSV: `data/out/backfill/`
7. Logs diarios: `logs/ana_pipeline_YYYY-MM-DD.log`

Exemplo de log:

```text
job_name=extract_job | step=finish | run_id=abc123 | status=success | processed=7 | inserted=0 | existing=7 | invalid=0 | duration_ms=1520.6
```

## 5) Como interpretar retorno do job

Exemplo:

```json
{"status":"success","processed":7,"inserted":0,"existing":7,"source":"live","run_id":"..."}
```

Significado:

1. `processed`: registros validos no lote.
2. `inserted`: novos no banco.
3. `existing`: ja existiam (idempotencia).
4. `source`: `snapshot` ou `live`.

## 6) Troubleshooting por sintoma

### Sintoma: `processed=0`

Causas comuns:

1. Periodo sem publicacao.
2. Reservatorio sem dado no intervalo.
3. Resposta sem tabela esperada.

Acoes:

1. Diminuir janela (`--since/--until`).
2. Verificar HTML em `data/out/raw/`.
3. Rodar `--dry-run --log-level DEBUG`.

### Sintoma: timeout da ANA (`ReadTimeout`)

Acoes:

1. Tentar novamente.
2. Diminuir janela.
3. Usar carga em blocos mensais.

### Sintoma: `PYTHONPATH=src` nao funciona no PowerShell

Use:

```powershell
$env:PYTHONPATH='src'
python -m pytest -q
```

### Sintoma: `streamlit` nao reconhecido

Use:

```powershell
python -m pip install -r requirements-streamlit.txt
python -m streamlit run src/app/dashboard/streamlit_app.py
```

### Sintoma: UF/subsistema vazio

Acoes:

1. `python -m app.ana.catalog sync`
2. Rodar extracao novamente para refresh.

## 7) Checklist antes de entrega

1. `python -m pytest -q` verde.
2. `POST /extract/ana` funcional.
3. `GET /ana/medicoes/{record_id}` retorna `404` para inexistente.
4. `GET /ana/checkpoint` e `GET /ana/analysis` funcionando.
5. `README`, `RUNBOOK`, `DECISIONS` coerentes com o codigo.
