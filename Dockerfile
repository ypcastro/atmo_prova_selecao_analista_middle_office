FROM python:3.12-slim

WORKDIR /app

# Instala dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements e instala
COPY requirements.txt .
RUN pip install --no-cache-dir -U pip && pip install --no-cache-dir -r requirements.txt

# Copia código-fonte
COPY src/ ./src/
COPY data/ ./data/
COPY conftest.py pytest.ini ./

# Variáveis de ambiente padrão
ENV APP_DATA_DIR=/app/data
ENV ANA_MODE=snapshot
ENV ANA_RESERVATORIO=19091
ENV ANA_DATA_INICIAL=2025-10-01
ENV ANA_DATA_FINAL=2025-10-07
ENV PIPELINE_INTERVAL_SECONDS=60
ENV PYTHONPATH=/app/src

# Porta da API
EXPOSE 8000

CMD ["uvicorn", "app.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
