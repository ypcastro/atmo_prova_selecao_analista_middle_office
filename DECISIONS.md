# DECISIONS.md — Decisões Técnicas do Pipeline ANA

## Visão Geral

Pipeline end-to-end para extração de medições hidrológicas da ANA (Agência Nacional de Águas), implementado em Python com FastAPI, SQLite e scheduler próprio.

---

## Q1 — Parsing de Datas e Números pt-BR

### Decisões

**parse_date_mixed()**
- Tentativa sequencial de formatos (`strptime`) do mais específico ao mais genérico.
- Regex para strip de offset de timezone antes do parse, retornando datetime *naive* — simplifica comparações e armazenamento (sem ambiguidade de fuso).
- Retorna `None` (não levanta exceção) para manter fluxo de validação centralizado no `validate_record()`.

**safe_float_ptbr()**
- Regra de detecção pt-BR: se tem `.` E `,`, trata como milhar/decimal respectivamente. Se tem só `,`, trata como decimal. Caso contrário, assume formato EN.
- Set `frozenset` para tokens ausentes: O(1) de lookup.
- Rejeita `NaN`/`inf` após conversão via `math.isnan`/`math.isinf`.

**Trade-off:** abordagem não cobre todos os edge cases de localização, mas cobre 100% dos padrões reais do HTML da ANA.

---

## Q2 — Pipeline I/O

### Schema de Diretórios
```
data/
  raw/         → HTMLs brutos por run_id
  normalized/  → JSONs normalizados por run_id
  checkpoints/ → checkpoint por run_id + latest.json
```

### Escrita Segura (Atomic Write)
- Escreve em `.tmp` primeiro, depois `Path.replace()` (rename atômico no mesmo filesystem).
- Em caso de falha, o `.tmp` é deletado e o arquivo de destino permanece íntegro.
- **Por que não usar `tempfile.NamedTemporaryFile`?** `replace()` requer mesmo filesystem; `NamedTemporaryFile` pode usar `/tmp` em outro mount.

### latest.json
- Mantemos `checkpoints/latest.json` como atalho de consulta rápida sem varrer todos os checkpoints.
- Trade-off: duplicação de dado vs. performance de leitura — preferimos leitura O(1).

---

## Q3 — Parser ANA

### Tolerância de Headers
- Normalização: lowercase + strip + colapso de espaços antes do lookup no `_HEADER_MAP`.
- Mapeamento explícito (não regex), com aliases conhecidos do HTML da ANA.
- Colunas desconhecidas são ignoradas silenciosamente (log em DEBUG).

### Deduplicação
- `dict` keyed por `record_id` — garante O(n) e mantém primeira ocorrência.
- `record_id = "{reservatorio_id}-{data_iso}"`: chave natural e legível.

### Linhas sem data válida
- São descartadas com log WARNING (não é erro fatal — dados parcialmente corrompidos são comuns em fontes públicas).

---

## Q4 — Normalização e Validação

### Separação normalize/validate
- `normalize_record()` nunca levanta exceção — apenas normaliza tipos.
- `validate_record()` centraliza todas as regras de negócio com `ValueError` descritivo.
- Permite descartar registros inválidos individualmente no job sem abortar o batch.

### Regras de Validação
| Campo | Regra |
|-------|-------|
| `record_id` | Obrigatório (não None/vazio) |
| `reservatorio_id` | Obrigatório |
| `data_hora` | Obrigatório + parseável |
| `volume_util_pct` | [0, 100] quando presente |
| `cota_m` | ≥ 0 quando presente |

Campos numéricos opcionais (`None`) são válidos — representam dados ausentes da fonte.

### deduplicar() O(n)
- Implementado via `dict` (inserção mantém ordem em Python 3.7+).
- Complexidade: O(n) time, O(n) space.

---

## Q5 — Client ANA (Bônus)

### Retry Strategy
- `urllib3.Retry` com `backoff_factor=1.5` → esperas de 0s, 1.5s, 3s entre tentativas.
- Retry apenas em `429` e erros 5xx (não em 4xx que indicam erro do cliente).
- `allowed_methods=["GET"]` — só retenta GETs (idempotente).

### Rate Limiting
- Variável global `_last_request_time` garante intervalo mínimo de 2s entre requisições.
- Simples e suficiente para uso single-process; para multi-process precisaria de lock externo.

### Scraping Responsável
- User-Agent identificado como bot com contato.
- Timeout de 15s (default) evita conexões travadas.
- `ANA_MODE=snapshot` como padrão — live é opt-in explícito.

---

## Q6 — Persistência SQLite

### Idempotência com INSERT OR IGNORE
- `record_id` é PRIMARY KEY → conflito = silenciosamente ignorado.
- Contagem de inseridos: `COUNT(*) antes` vs `COUNT(*) depois`.
- **Alternativa considerada:** `INSERT OR REPLACE` — descartada pois sobrescreveria dados existentes, perdendo o `created_at` original.

### WAL Mode
- `PRAGMA journal_mode=WAL` melhora concorrência leitura/escrita (múltiplos readers + 1 writer).
- Importante para API + scheduler rodando simultaneamente.

### raw_json
- Armazenamos o registro completo em JSON na coluna `raw_json` como auditoria.
- Permite reconstruir estado sem reprocessar o pipeline.

### Schema
```sql
CREATE TABLE ana_medicoes (
    record_id           TEXT PRIMARY KEY,
    reservatorio_id     TEXT NOT NULL,
    data_hora           TEXT NOT NULL,
    data_iso            TEXT NOT NULL,
    cota_m              REAL,
    afluencia_m3s       REAL,
    defluencia_m3s      REAL,
    vazao_vertida_m3s   REAL,
    vazao_turbinada_m3s REAL,
    nivel_montante_m    REAL,
    volume_util_pct     REAL,
    created_at          TEXT DEFAULT (datetime('now')),
    raw_json            TEXT
);
```

---

## Q7 — Scheduler, Job e API

### run_once() — Isolamento de Erros
- Erros em registros individuais (validação) não abortam o batch — apenas descartam o registro com log.
- Erros fatais (snapshot ausente, DB inacessível) são capturados no try/except externo e salvos no checkpoint.

### Scheduler sem Drift
- Usa `time.monotonic()` (não `time.time()`) — imune a ajustes de relógio do sistema.
- `next_tick += interval` em vez de `next_tick = now + interval` — acumula o tempo correto.
- Sleep de 1s no máximo para verificação do evento de parada sem consumir CPU excessivo.
- Recalibra se o atraso for maior que 1 intervalo completo (evita "catch-up" em burst).

### FastAPI — Lifespan
- `@asynccontextmanager lifespan` inicializa DB e scheduler antes de aceitar requests.
- Scheduler roda em daemon thread — encerra automaticamente com o processo.

### Análise
- Cálculos via `statistics` stdlib (sem dependência de numpy/pandas).
- Balanço hídrico = afluência − defluência (positivo = acumulando).
- Tendência de cota: delta entre primeira e última cota válida (threshold ±0.1m).

---

## Decisões Transversais

| Tema | Decisão | Motivo |
|------|---------|--------|
| SQLite vs PostgreSQL | SQLite | Sem infraestrutura extra; WAL suficiente para escala desta prova |
| Sync vs Async no job | Sync | Simplicidade; FastAPI roda em thread executor via `run_in_executor` se necessário |
| Snapshot como padrão | `ANA_MODE=snapshot` | Reprodutibilidade dos testes oficiais; live é frágil por natureza |
| Sem pandas/numpy | stdlib `statistics` | Menos dependências, mais portabilidade |
| Logging | `logging` stdlib | Sem overhead; compatível com qualquer handler (CloudWatch, etc.) |
