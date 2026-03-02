# DECISIONS - ANA_Pipeline

## 1) Escopo adotado

1. Escopo principal: requisitos obrigatorios da prova (`Q1`, `Q2`, `Q3`, `Q4`, `Q6`, `Q7`).
2. Escopo adicional: suporte live (`Q5` bonus), catalogo de reservatorios e dashboard Streamlit.
3. Restricao de projeto: manter contrato dos endpoints obrigatorios sem quebra.

## 2) Fonte de dados e reprodutibilidade

1. `snapshot` local (`data/ana_snapshot.html`) como base de execucao reprodutivel.
2. `live` como opcao operacional para carga historica e atualizacao continua.
3. Decisao: separar claramente modo de prova (reprodutivel) e modo operacional (sujeito a timeout/disponibilidade da ANA).

## 3) Schema canonicamente persistido

### 3.1 Tabela `ana_medicoes`

1. PK: `record_id` com formato `{reservatorio_id}-{data_medicao}`.
2. Campos obrigatorios: `record_id`, `reservatorio_id`, `reservatorio`, `data_medicao`.
3. Campos metricos opcionais (nullable): `cota_m`, `afluencia_m3s`, `defluencia_m3s`, `vazao_vertida_m3s`, `vazao_turbinada_m3s`, `vazao_natural_m3s`, `volume_util_pct`, `vazao_incremental_m3s`.
4. Campos de enrich: `uf`, `subsistema`, `balanco_vazao_m3s`, `situacao_hidrologica`.

### 3.2 Tabela `ana_reservatorios`

1. PK: `reservatorio_id`.
2. Campos: `reservatorio`, `estado_codigo_ana`, `estado_nome`, `uf`, `subsistema`, `source`, `updated_at_utc`.
3. Uso: catalogo estruturado para enriquecer medicoes e suportar filtros operacionais.

## 4) Idempotencia e consistencia

1. Estrategia principal: `INSERT OR IGNORE` em `ana_medicoes` por `record_id`.
2. Regra de atualizacao: quando a linha ja existe, executar `UPDATE` para manter campos de enrich/metricas consistentes.
3. Resultado operacional padronizado: `inserted`, `existing`, `processed`, `status`.

Trade-off:

- Mantem simplicidade de SQLite e evita duplicacao sem exigir mecanismo complexo de merge/versionamento.

## 5) Parsing e validacao

1. Datas: parser aceita formatos BR e ISO, incluindo timezone.
2. Numeros: parser de float pt-BR tolera milhares, decimal e tokens ausentes.
3. Parser HTML: tolerante a variacoes leves de header (normalizacao de texto).
4. Validacao: falhas de schema disparam `ValueError`; registros invalidos nao entram no banco.

## 6) Artefatos operacionais e checkpoint

1. `raw/*.html` para rastreabilidade do input.
2. `normalized/*.json` para inspecao do output transformado.
3. `checkpoint.json` para estado da ultima execucao (`success`, `dry_run`, `fail`), com `timestamp_utc` e metadados.
4. Escrita atomica (`.tmp` + `replace`) em checkpoint e watermark para reduzir risco de corrupcao.

## 7) Watermark incremental

1. Arquivo: `data/out/watermark.json`.
2. Chave padrao: `live:{reservatorio_id}`.
3. Uso:
   - CLI live sem `--since/--until` e sem `--force`: usa watermark para definir proxima janela.
   - CLI com `--force`: ignora watermark.
4. Atualizacao: apenas em sucesso live com `processed > 0`, usando maior `data_medicao`.

Trade-off:

- Reduz reprocessamento e custo de coleta, mantendo controle explicito via `--force`.

## 8) CLI e operacionalizacao

1. Job possui CLI oficial com `--dry-run`, `--log-level`, `--since`, `--until`, `--force`.
2. `--dry-run` processa pipeline completo sem gravar no banco, util para diagnostico.
3. Scheduler usa `compute_next_run(last + interval)` para manter cadencia e evitar drift grande.

## 9) Logging e tratamento de erros

1. Logging estruturado em JSON nos pontos principais (`start`, `fetch`, `parse`, `load`, `finish`, `fail`).
2. Campos chave: `job_name`, `run_id`, `step`, `records_in`, `records_out`, `invalid`, `duration_ms`.
3. Erros nao sao silenciados; falha gera checkpoint `fail` e excecao e propagada no job.

## 10) API e contrato

1. Endpoints obrigatorios mantidos:
   - `POST /extract/ana`
   - `GET /ana/medicoes`
   - `GET /ana/medicoes/{record_id}`
   - `GET /ana/checkpoint`
   - `GET /ana/analysis`
2. Filtros de `GET /ana/medicoes` (`uf`, `reservatorio_id`) sao aplicados no SQL antes do `limit` para comportamento previsivel.

## 11) Dashboard

1. Streamlit mantido como camada opcional, isolada do contrato de testes da prova.
2. Leitura direta do SQLite para visualizacao hidrologica.

## 12) Limites conhecidos

1. Modo live depende da disponibilidade e latencia da ANA (timeouts podem ocorrer).
2. Para periodos sem medicao publicada, `processed=0` e esperado.
3. SQLite atende bem ao contexto da prova; para escala maior, migracao para banco servidor seria o proximo passo natural.
