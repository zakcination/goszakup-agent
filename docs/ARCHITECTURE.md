# Architecture

## Overview

Pipeline is built as a layered system:

1. **OWS v3 API** → raw data (REST)
2. **ETL (Python)** → normalized facts in PostgreSQL
3. **Analytics (DuckDB)** → marts (Fair Price, anomalies)
4. **Agent API (FastAPI)** → explainable answers
5. **Demo UI (Streamlit)** → stakeholder exploration

## Data Flow

```
OWS v3 → ETL → Postgres → Parquet → DuckDB marts → Analytics API → Agent API → UI
```

## Trust Boundaries

- Postgres is the single source of truth.
- DuckDB marts are reproducible, derived artifacts.
- Agent API does not compute facts, only formats results.

## Deployment

- Docker Compose on‑prem.
- Postgres/Redis local only.
- Analytics API + Agent API + UI exposed on localhost.
