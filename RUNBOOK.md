# RUNBOOK - ANA_Pipeline

Guia pratico de operacao local.

## 1) Preparacao rapida

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

## 2) Operacao diaria

### 2.1 Rodar extracao unica

```powershell
$env:ANA_MODE='snapshot'   # ou live
python -c "from app.jobs.extract_job import run_once; print(run_once())"
```

### 2.2 Rodar via CLI oficial

```powershell
python -m app.jobs.extract_job --log-level INFO
python -m app.jobs.extract_job --dry-run
python -m app.jobs.extract_job --since 2025-01-01 --until 2025-01-31
python -m app.jobs.extract_job --force
```

### 2.3 Subir API

```powershell
python -m uvicorn app.api.main:app --reload --port 8000
```

### 2.4 Rodar scheduler

```powershell
python -m app.jobs.scheduler
```

## 3) Backfill historico

### 3.1 Script de principais reservatorios

```powershell
.\scripts\backfill_principais.ps1 -StartDate '2025-01-01' -EndDate '2025-03-31' -SyncCatalog
```

O script:

1. Opcionalmente sincroniza catalogo (`-SyncCatalog`).
2. Roda janelas mensais por reservatorio principal.
3. Salva resumo CSV em `data/out/backfill/`.

### 3.2 Catalogo manual

```powershell
python -m app.ana.catalog sync --json
python -m app.ana.catalog list --limit 1000
python -m app.ana.catalog list --uf SP --limit 1000
```

## 4) Interpretacao de artefatos

### 4.1 `data/out/checkpoint.json`

Campos principais:

1. `status`: `success`, `dry_run` ou `fail`.
2. `inserted`, `existing`.
3. `error` (se `fail`).
4. `meta` com info de janela, paths e contagens.
5. `timestamp_utc`.

### 4.2 `data/out/watermark.json`

Estrutura:

```json
{
  "live:19004": {
    "value": "2025-03-01",
    "updated_at_utc": "2026-03-02T00:00:00+00:00"
  }
}
```

Regras:

1. Sem `--since/--until` em modo live: usa watermark quando existir.
2. `--force`: ignora watermark.

### 4.3 `data/out/raw` e `data/out/normalized`

1. `raw`: HTML original da coleta.
2. `normalized`: payload final antes de persistir, util para debug.

## 5) Troubleshooting

### 5.1 `processed = 0`

Possiveis causas:

1. Janela sem dados publicados.
2. Reservatorio sem medicao no periodo.
3. Resposta live nao contem tabela esperada.

Acoes:

1. Testar janela menor (`--since`/`--until`).
2. Conferir `raw/*.html` da execucao.
3. Rodar `--dry-run` com `--log-level DEBUG`.

### 5.2 Timeout ANA (`ReadTimeout`)

Acoes:

1. Repetir tentativa (instabilidade externa).
2. Reduzir janela de datas.
3. Usar backfill em lotes mensais.

### 5.3 Erro de `PYTHONPATH` no PowerShell

Use:

```powershell
$env:PYTHONPATH='src'
python -m pytest -q
```

No PowerShell, `PYTHONPATH=src ...` (estilo bash) nao funciona.

### 5.4 `streamlit` nao reconhecido

Acoes:

1. Ativar venv correto.
2. Instalar dependencias do dashboard.

```powershell
python -m pip install -r requirements-streamlit.txt
python -m streamlit run src/app/dashboard/streamlit_app.py
```

### 5.5 UF/subsistema faltando

Acoes:

1. Sincronizar catalogo (`python -m app.ana.catalog sync`).
2. Rodar nova extracao para refresh de metadata.
3. Verificar se `reservatorio_id` existe no catalogo.

### 5.6 Cache de dashboard

Se o dashboard nao refletir novos dados:

1. Recarregar pagina.
2. Reiniciar Streamlit.
3. Conferir `data/out/ana.db` atualizado.

## 6) Checklist de release

1. `python -m pytest -q` verde.
2. `POST /extract/ana` responde `success`/`dry_run`.
3. `GET /ana/medicoes` e `GET /ana/medicoes/{record_id}` funcionando (`404` para inexistente).
4. `GET /ana/checkpoint` e `GET /ana/analysis` funcionando.
5. README, RUNBOOK e DECISIONS atualizados e coerentes.
