FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

ENV ANALYTICS_DB_PATH=/app/data/analytics.duckdb
ENV ANALYTICS_PARQUET_DIR=/app/data/parquet

CMD ["uvicorn", "services.analytics_api.main:app", "--host", "0.0.0.0", "--port", "8001"]
