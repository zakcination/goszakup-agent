# goszakup-agent

AI-агент анализа государственных закупок РК · MVP

---

## Spiral 1 — Living Skeleton

**Goal:** API → PostgreSQL working. Nothing more, nothing less.

```
API (OWS v3) → ETL (Python) → PostgreSQL → ✅ Verified
```

---

## Quickstart (5 minutes)

### 1. Clone / enter project
```bash
cd goszakup-agent
```

### 2. Run setup (checks everything + starts Docker)
```bash
chmod +x setup.sh
./setup.sh
```

This will:
- Check Docker, Python, Git, ports
- Create `.env` from template (you fill secrets)
- Start PostgreSQL + Redis
- Apply database schema
- Seed 27 target BINs

### 3. Fill in secrets
```bash
nano .env   # or your preferred editor
```

Required:
```env
OWS_TOKEN=9e3c82c2e1c542588ef7ae4484e073b4
POSTGRES_PASSWORD=your_strong_password
DATABASE_URL=postgresql://goszakup_user:your_strong_password@postgres:5432/goszakup
AI_API_KEY=sk-d8ed9fead331438a917a8ce8cfb30c1b
```

### 4. Verify connections
```bash
./setup.sh check
# or
python scripts/check_connections.py
```

### 5. Load first data
```bash
source .venv/bin/activate
python etl/load_subjects.py
```

### 6. Check DB via Adminer UI
Open: http://localhost:8080
- Server: `postgres`
- User: `goszakup_user`
- Password: (from .env)
- Database: `goszakup`

---

## Project Structure

```
goszakup-agent/
│
├── .env.example              ← template — copy to .env, fill secrets
├── .env                      ← REAL secrets — never in git ⛔
├── .gitignore                ← .env is excluded
├── docker-compose.yml        ← infrastructure (Spirals 1–4)
├── requirements.txt          ← Python deps
├── setup.sh                  ← master setup script
│
├── infra/
│   └── postgres/
│       └── init.sql          ← full schema + seed data
│
├── etl/                      ← data ingestion layer
│   ├── client.py             ← OWS v3 API client (REST + GraphQL)
│   ├── config.py             ← all config from env vars
│   └── load_subjects.py      ← Spiral 1: load 27 BINs
│
└── scripts/
    └── check_connections.py  ← verify all connections work
```

---

## Spiral Roadmap

| Spiral | What gets built | Done? |
|--------|----------------|-------|
| **1** | Schema · Docker · API client · load_subjects | 🔄 Now |
| **2** | load_plans · load_announcements · load_lots · load_contracts | ⏳ |
| **3** | DuckDB analytics · Fair Price Engine · anomaly detection | ⏳ |
| **4** | Milvus embeddings · semantic search | ⏳ |
| **5** | FastAPI Agent · LLM integration · drill-down responses | ⏳ |
| **6** | Nginx · Airflow scheduler · Grafana · production hardening | ⏳ |

---

## Security

- **Secrets**: `.env` only, never committed, never logged
- **Networks**: PostgreSQL has no exposed ports (internal Docker network only)
- **Access**: Adminer only on `127.0.0.1:8080` (localhost only)
- **Tokens**: OWS token masked in all logs
