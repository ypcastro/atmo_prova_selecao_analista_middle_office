# 🧠 Mapa Mental do ANA Pipeline

Visão rápida da arquitetura e do fluxo de execução.

## 🌐 Mapa Geral

```mermaid
flowchart TD
    A[🚀 Entradas]

    A --> B[🧩 Job<br/>Extract]
    A --> C[⏱️ Scheduler]
    A --> D[🌍 API]
    A --> E[📊 Dashboard]

    B --> F[⚙️ Config]
    B --> G[🌐 Coleta<br/>HTML]
    B --> H[🧪 Parse]
    B --> I[🧹 Normaliza<br/>Valida]
    B --> J[✨ Enriquece]
    B --> K[🗂️ Dedupe]
    B --> L[💾 Upsert<br/>SQLite]
    B --> M[🧾 Artefatos]
    B --> N[✅ Checkpoint]
    B --> O[💧 Watermark]

    D --> L
    D --> P[📈 Analysis]
    D --> Q[🏷️ Sync<br/>Catálogo]
    Q --> L

    E --> L
```

## 🔄 Fluxo do `run_once`

```mermaid
flowchart LR
    A[⚙️ Load<br/>Settings] --> B[📅 Resolve<br/>Janela]
    B --> C{🌐 Modo}
    C -->|live| D[🔗 Build URL]
    C -->|snapshot| E[📄 Lê HTML]
    D --> F[⬇️ Fetch HTML]
    E --> G[🧪 Parse]
    F --> G
    G --> H[🧹 Normalize]
    H --> I[✅ Validate]
    I --> J[✨ Enrich]
    J --> K[🗂️ Dedupe]
    K --> L[🧾 JSON<br/>Normalizado]
    K --> M{💾 Dry run?}
    M -->|Sim| N[🧪 Sem DB]
    M -->|Não| O[💾 Upsert DB]
    O --> P[🔁 Refresh<br/>Metadata]
    O --> Q[💧 Update<br/>Watermark]
    N --> R[✅ Checkpoint]
    P --> R
    Q --> R
```

## 🧭 Legenda Rápida

- `🧩 Job Extract`: orquestra coleta, parse, tratamento e carga.
- `🗂️ Dedupe`: remove duplicados por `record_id`.
- `💾 Upsert SQLite`: grava de forma idempotente.
- `🧾 Artefatos`: salva HTML bruto e JSON normalizado em `data/out`.
- `✅ Checkpoint`: resume status da última execução.
- `💧 Watermark`: guarda última data processada no modo `live`.
