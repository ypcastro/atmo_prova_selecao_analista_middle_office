## Parsing (Q1)

Para datas, optei por tentativa. Primeiro os formatos brasileiros DD/MM/YYYY, depois ISO 8601. Sempre retorno `date` (não `datetime`) para consistência no restante do pipeline. Tokens ausentes (`"—"`, `"NaN"`, `"inf"`) retornam `None` em vez de lançar exceção isso porque dados de fontes externas são diversos e preferi que fossem resilientes.

Para números pt-BR, a lógica é remover o separador de milhar (`.`) e trocar a vírgula decimal por ponto antes de converter. A verificação de `None` é feita separadamente do `strip()` para não tratar `0` como ausente.

## Schema e normalização (Q4)

O `record_id` foi definido como `"{reservatorio_id}-{data_iso}"`, combinando o código do reservatório com a data em ISO 8601. Essa chave é determinística o que é o que garante a idempotência end-to-end. A normalização converte `reservatorio_id` para inteiro, faz strip em strings e padroniza a data para ISO antes de construir o `record_id`.

## Idempotência (Q6)

Optei por SELECT antes do INSERT em vez de `INSERT OR IGNORE` para ter contagens precisas de `inserted` vs `existing`. O trade-off é 2 queries por linha, mas o volume de dados da ANA não é significativo e não justifica otimização aqui. A chave primária `record_id` no schema SQLite garante que mesmo se o SELECT falhar, um INSERT duplicado seria rejeitado pelo banco.

## Scheduler sem drift (Q7)

O próximo tick é calculado como `last_run + interval`. Então se o `run_once()` demorar 5 segundos num intervalo de 60, o próximo agendamento é em 55 segundos, não em 60. Evita drift acumulado causado por atrasos de execução.

## Pipeline I/O e observabilidade (Q2 e Q7)

A escrita usa `write_text()`  sem escrita atômica. Em produção o correto seria escrever em `.tmp` e fazer `rename()` para evitar arquivos corrompidos em caso de crash

## API (Q7)

Conexão SQLite aberta e fechada por request, sem pool. Simples e correto para volume baixo e single-process. O endpoint `POST /extract/ana` dispara o pipeline de forma síncrona e retorna as contagens.
O scheduler simples em vez de ferramenta dedicada e análise básica apenas para demonstração focando na facilidade de execução local. 
