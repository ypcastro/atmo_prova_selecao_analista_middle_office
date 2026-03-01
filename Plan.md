# Plano de Execução — ANA_Pipeline (1 dia)

```mermaid
flowchart TD
    A[Objetivo: entregar Q1 Q2 Q3 Q4 Q6 Q7 com qualidade] --> B[1. Setup ambiente<br/>venv + requirements + PYTHONPATH=src]
    B --> C[2. Q1 Parsing<br/>parse_date_mixed + safe_float_ptbr]
    C --> D[3. Q4 Transforms<br/>dedupe + normalize_record + validate_record]
    D --> E[4. Q3 Parser ANA<br/>HTML -> registros + dedupe por record_id]
    E --> F[5. Q6 Storage SQLite<br/>init_db + upsert idempotente + fetch]
    F --> G[6. Q2 PipelineIO<br/>raw HTML + normalized JSON + checkpoint atômico]
    G --> H[7. Q7 Core<br/>run_once + scheduler + API + analysis]
    H --> I[8. DECISIONS.md<br/>trade-offs e decisões técnicas]

    I --> J[Testes obrigatórios<br/>pytest por módulo + suite completa]
    J --> K[Qualidade extra<br/>run_once 2x idempotente, checkpoint fail/success, API 404]
    K --> L[Critério final<br/>100% testes verdes + endpoints ok + idempotência comprovada]
