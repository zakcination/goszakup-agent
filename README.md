# goszakup-agent

AI-агент анализа государственных закупок РК · MVP

---

## Основной документ (история реализации)

- История (Markdown): [`docs/TECHNICAL_IMPLEMENTATION_STORY_5K.md`](docs/TECHNICAL_IMPLEMENTATION_STORY_5K.md)
- История (DOCX): [`docs/TECHNICAL_IMPLEMENTATION_STORY_5K.docx`](docs/TECHNICAL_IMPLEMENTATION_STORY_5K.docx)

Данный документ является основным источником описания реализации end-to-end: требования → архитектура → схема БД → ingestion → аналитика → agentic workflow → демо → верификация.

---

## Пакет документации (submission)

- Submission (полная версия): [`docs/SUBMISSION_FINAL.md`](docs/SUBMISSION_FINAL.md) / [`docs/SUBMISSION_FINAL.docx`](docs/SUBMISSION_FINAL.docx)
- Submission (версия для защиты): [`docs/SUBMISSION_FINAL_v2.md`](docs/SUBMISSION_FINAL_v2.md) / [`docs/SUBMISSION_FINAL_v2.docx`](docs/SUBMISSION_FINAL_v2.docx)
- Скрипт демо: [`docs/DEMO_SCRIPT_v2.md`](docs/DEMO_SCRIPT_v2.md) / [`docs/DEMO_SCRIPT_v2.docx`](docs/DEMO_SCRIPT_v2.docx)
- Демо-кейсы (3 обязательных класса запросов): [`docs/DEMO_CASES.md`](docs/DEMO_CASES.md)
- Архитектура/схема/аналитика/agentic workflow: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), [`docs/DATA_SCHEMA.md`](docs/DATA_SCHEMA.md), [`docs/ANALYTICS.md`](docs/ANALYTICS.md), [`docs/AGENTIC_WORKFLOW.md`](docs/AGENTIC_WORKFLOW.md)
- Риски и ограничения: [`docs/RISKS.md`](docs/RISKS.md)

---

## Текущий статус

- Spiral 1: infra + schema + connectivity checks — ✅
- Spiral 2: core ingestion (plans → announcements → lots → contracts → contract_items) + lineage + FK integrity — ✅
- Spiral 3: Parquet export + DuckDB marts (fair price + anomalies + volume) — ✅
- Agent API + Analytics API + Streamlit UI (Docker services) — ✅

Детальный подход и принятые решения: [`docs/APPROACH.md`](docs/APPROACH.md).

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
Note:
- `setup.sh` installs a minimal Python set for connectivity/ETL checks. For full analytics tooling on host, install all deps: `pip install -r requirements.txt`.
- For analytics (DuckDB + Parquet), Python 3.12 is recommended to avoid source builds (pyarrow wheels).

This will:
- Check Docker, Python, Git, ports
- Create `.env` from template (you fill secrets)
- Start PostgreSQL + Redis
- Apply database schema
- Target BINs are configured via `TARGET_BINS` (default = 27 from ТЗ)

### 3. Fill in secrets
```bash
nano .env   # or your preferred editor
```

Required:
```env
OWS_TOKEN=<your_ows_bearer_token>
POSTGRES_PASSWORD=<your_strong_password>
DATABASE_URL=postgresql://goszakup_user:<password>@localhost:5432/goszakup
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

### 6. Run Spiral 2 ingestion
```bash
./scripts/run_spiral2.sh
```
This flow now includes `backfill_plans_kato` and `backfill_announcements_from_sources` to reduce KATO and announcement linkage gaps.
It now also runs incremental raw workers for:
- `/v3/plans/kato`
- `/v3/plans/spec`
- `/v3/acts`
- `/v3/treasury-pay`
- `/v3/trd-buy/{id}/cancel` + `/v3/trd-buy/{id}/pause`

Optional resume backstep (reprocess last N IDs per BIN):
```bash
./scripts/run_spiral2.sh --resume-backstep 50
```
Optional BIN concurrency (parallel BINs per loader):
```bash
./scripts/run_spiral2.sh --concurrency 2
```
Strict recovery pass (deep reprocess + integrity checks):
```bash
./scripts/run_recovery_strict.sh --concurrency 5 --rps-start 15 --rps-max 40 --skip-analytics
```

### 7. Check DB via Adminer UI
Open: http://localhost:8080
- Server: `postgres`
- User: `goszakup_user`
- Password: (from .env)
- Database: `goszakup`

### 8. Spiral 3 analytics (macro indices → Parquet → marts)

Option A (host-run, requires full deps):
```bash
source .venv/bin/activate
pip install -r requirements.txt
python etl/load_macro_indices.py
python analytics/export_parquet.py
python analytics/build_marts.py
python scripts/verify_spiral3.py
```

Option B (Docker-run, avoids local pyarrow issues):
```bash
docker compose run --rm analytics_api python etl/load_macro_indices.py
docker compose run --rm analytics_api python analytics/export_parquet.py
docker compose run --rm analytics_api python analytics/build_marts.py
docker compose run --rm analytics_api python scripts/verify_spiral3.py
```

### 9. KATO metadata worker
```bash
KATO_TRD_BUY_IDS=82661999 ./scripts/run_kato_worker.sh
```
Run this to pull `kato`/region metadata (city, parent KATO) straight from a `trd-buy` payload; results land in `trd_buy_kato_metadata` for analytics enrichment. Override `KATO_TRD_BUY_IDS` with multiple IDs separated by spaces and control logging via `KATO_TRD_LOG_LEVEL`.

### 10. Daily incremental update (journal → marts)
```bash
./scripts/run_daily.sh
```

### 11. Demo APIs / UI (optional)
```bash
docker compose up -d analytics_api agent_api ui
```
Open:
- Analytics API: http://localhost:8001/docs
- Agent API: http://localhost:8002/docs
- UI: http://localhost:8501

Primary agent endpoint:
- `POST /query` (strict workflow)
- `POST /ask` (compatibility alias used by current UI)

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
├── docs/                     ← submission documentation bundle
│   ├── TECHNICAL_IMPLEMENTATION_STORY_5K.md  ← main story (primary source)
│   ├── SUBMISSION_FINAL.md
│   ├── SUBMISSION_FINAL_v2.md
│   ├── DEMO_SCRIPT_v2.md
│   └── ...
│
├── infra/
│   └── postgres/
│       └── init.sql          ← full schema (tables + views)
│
├── etl/                      ← data ingestion layer
│   ├── client.py             ← OWS v3 API client (REST + GraphQL)
│   ├── config.py             ← all config from env vars
│   ├── utils.py              ← shared ETL helpers
│   ├── load_subjects.py      ← Spiral 1: load subjects for TARGET_BINS
│   ├── load_refs.py          ← Spiral 2: reference tables
│   ├── load_plans.py         ← Spiral 2: plans
│   ├── load_announcements.py ← Spiral 2: trd_buy
│   ├── load_lots.py          ← Spiral 2: lots + lot_plan_points
│   ├── load_contracts.py     ← Spiral 2: contracts + suppliers
│   ├── load_contract_items.py← Spiral 2: contract units
│   ├── load_plans_kato_incremental.py  ← checkpointed /v3/plans/kato worker
│   ├── load_plans_spec_incremental.py  ← checkpointed /v3/plans/spec worker
│   ├── load_acts_incremental.py        ← checkpointed /v3/acts worker
│   ├── load_treasury_pay_incremental.py← checkpointed /v3/treasury-pay worker
│   ├── load_trd_buy_process_events_incremental.py ← cancel/pause evidence worker
│   ├── backfill_plans_kato.py← compatibility wrapper to plans_kato_incremental
│   └── backfill_announcements_from_sources.py ← Spiral 2: announcement linkage repair
│
├── agent/                    ← strict intent + tool orchestration layer
│   ├── intent.py             ← 8-intent classifier + parameter extraction
│   ├── tools.py              ← whitelist tools (fixed SQL templates)
│   ├── cache.py              ← Redis/in-memory tool cache
│   ├── templates.py          ← deterministic RU/KZ verbalization
│   └── agent.py              ← intent→tool→cache→response workflow
│
├── analytics/                ← Parquet export + DuckDB marts
│   ├── export_parquet.py
│   ├── build_marts.py
│   └── engine.py             ← analytics queries used by APIs/tools
│
├── api/
│   └── app.py                ← FastAPI endpoints (/query, /ask, /tools, /health)
│
├── services/                 ← Docker services (analytics_api, agent_api, ui)
│   ├── analytics_api/
│   ├── agent_api/
│   └── ui/
│
└── scripts/
    ├── check_connections.py  ← verify all connections work
    ├── run_spiral2.sh        ← full Spiral 2 runner
    ├── run_recovery_strict.sh← strict recovery runner
    ├── capture_quality_snapshot.py ← stores quality metrics in DB + artifacts JSON
    └── verify_spiral2.py     ← Spiral 2 verification checks
```

---

## Spiral Roadmap

| Spiral | What gets built | Done? |
|--------|----------------|-------|
| **1** | Schema · Docker · API client · load_subjects | ✅ Done |
| **2** | Ingestion: refs/plans/ann/lots/contracts/units + verify | ✅ Implemented |
| **3** | DuckDB analytics · Fair Price · anomaly + volume detection | ✅ Implemented |
| **4** | Milvus embeddings · semantic search | ⏳ |
| **5** | FastAPI Agent · strict tool workflow · drill-down responses | ✅ Implemented |
| **6** | Nginx · Airflow scheduler · Grafana · production hardening | ⏳ |

---

## Security

- **Secrets**: `.env` only, never committed, never logged
- **Networks**: PostgreSQL/Redis are bound to `127.0.0.1` only (localhost)
- **Access**: Adminer only on `127.0.0.1:8080` (localhost only)
- **Tokens**: OWS token masked in all logs
