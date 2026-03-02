# Current Approach vs Initial Proposal

Status: Spiral 2 (ingestion implemented, analytics pending)

## TL;DR

Shifted from initial technical proposal: YES.

We kept the core philosophy (data-first, on‑prem, reproducible analytics) but intentionally simplified infrastructure for MVP delivery. The current implementation focuses on reliable ingestion into PostgreSQL and audit‑grade lineage. Analytics (DuckDB), vector search (Milvus), orchestration (Airflow), and agent APIs are deferred to later spirals.

This document describes the full approach end‑to‑end (from ingestion to deployment and submission), where we changed direction vs the proposal, and how to upgrade the next steps given the current lower‑level decisions.

## End‑to‑End Approach (Start → Deploy → Submission)

1. **Ingestion**
   - Source: OWS v3 REST endpoints.
   - Scope: `TARGET_BINS`, years `DATA_YEAR_FROM/TO`.
   - ETL scripts: refs → plans → announcements → lots → contracts → contract units.
2. **Storage (Single Source of Truth)**
   - PostgreSQL with normalized schema, FKs, and audit logs.
   - Lineage: plan → lot → contract captured via `lot_plan_points`.
3. **Analytics (Next)**
   - DuckDB over curated extracts from Postgres (Parquet).
   - Fair Price and anomaly scoring computed outside the LLM.
4. **Semantic Layer (Next)**
   - Milvus collections for search and RAG context.
5. **Agent Layer (Next)**
   - FastAPI agent that runs only allow‑listed tools/SQL and assembles explanations.
6. **Deployment (Current MVP)**
   - Docker Compose on‑prem.
   - DB/Redis bound to `127.0.0.1`. No public exposure.
7. **Deployment (Later / Hardening)**
   - DMZ/App/Data network segregation.
   - Airflow scheduling, monitoring, TLS, WAF.
8. **Submission Package**
   - Architecture description + data schema.
   - Analytics & metrics description.
   - Example agent responses and risks/limits.
   - Reproducible scripts + verification checks.

## What We Implemented Now (Current State)

**Data foundation (done):**
- OWS v3 ingestion into PostgreSQL with FK integrity.
- Spiral 2 ETL scripts for:
  - reference tables (`load_refs.py`)
  - plans (`load_plans.py`)
  - announcements (`load_announcements.py`)
  - lots (`load_lots.py`)
  - contracts (`load_contracts.py`)
  - contract units (`load_contract_items.py`)
- Join table for plan→lot linkage: `lot_plan_points`.
- Supplier enrichment (ensure `subjects` has supplier BINs).
- Placeholder ENSTRU rows to avoid FK violations.
- Verification script: `scripts/verify_spiral2.py`.
- Runner: `scripts/run_spiral2.sh`.
- Checkpointed ingestion with `etl_state` (resume from last cursor or reprocess N last IDs).
- National macro indices (`macro_indices`) with annual inflation/GDP constants.

**Security (current):**
- Secrets stored in `.env` (not committed).
- Postgres/Redis ports bound to `127.0.0.1`.
- No external exposure beyond local host.

## End‑to‑End Process (Current)

1. `scripts/run_spiral2.sh` (or run scripts in order):
   - `etl/load_refs.py`
   - `etl/load_plans.py`
   - `etl/load_plans_kato_incremental.py` (checkpointed KATO worker, default incremental)
   - `etl/load_plans_spec_incremental.py` (checkpointed plan-spec worker)
   - `etl/load_announcements.py`
   - `etl/load_lots.py`
   - `etl/load_contracts.py`
   - `etl/backfill_announcements_from_sources.py` (hydrates missing `announcement_id` by `source_trd_buy_id`)
   - `etl/load_contract_items.py`
   - `etl/load_acts_incremental.py`
   - `etl/load_treasury_pay_incremental.py`
   - `scripts/capture_quality_snapshot.py`
   - `scripts/verify_spiral2.py`

2. Results are stored in PostgreSQL as the single source of truth.

## Key Design Decisions (Current)

- **Plan→Lot→Contract lineage preserved**
  - `lot_plan_points` captures full `point_list` from lots.
- **ENSTRU placeholder strategy**
  - Missing codes are inserted into `enstru_ref` with `name_ru='UNKNOWN'`.
- **Supplier enrichment**
  - If `supplier_bin` not in `subjects`, we fetch `/v3/subject/biin/{bin}` and upsert.
- **KATO integrity**
  - `kato_delivery` saved only if the code exists in `kato_ref`.
- **Contract items**
  - `contract_units` are joined via `/v3/plans/view/{pln_point_id}` to fill `enstru_code`, `unit_code`, and names.
- **Resume / backstep**
  - ETL stores `(entity, BIN) → last_id + resume_path` in `etl_state`.
  - Default resume uses stored cursor; optional backstep via `RESUME_BACKSTEP` or `--resume-backstep`.

## Resume Behavior by Ingestion Type (Especially Plans)

**Plans (`etl/load_plans.py`)**
- Cursor: `/v3/plans/{BIN}`.
- Resume: uses `etl_state` `(last_id, resume_path)` per BIN.
- Backstep: `--resume-backstep N` reprocesses last N plan IDs.

**Announcements (`etl/load_announcements.py`)**
- Cursor: `/v3/trd-buy/bin/{BIN}`.
- Resume/backstep identical to plans.

**Lots (`etl/load_lots.py`)**
- Cursor: `/v3/lots/bin/{BIN}`.
- Resume/backstep identical to plans.

**Contracts (`etl/load_contracts.py`)**
- Cursor: `/v3/contract/customer/{BIN}`.
- Resume/backstep identical to plans.

**Contract items (`etl/load_contract_items.py`)**
- Source: local `contracts` table (per BIN).
- Resume: uses last processed `contract_id` from `etl_state`.
- Backstep: `--resume-backstep N` reprocesses last N contract IDs.

## Level‑by‑Level: Proposal vs Current

The original proposal defined levels (0→5). Current status:

| Level | Proposal | Current | Change |
|-------|----------|---------|--------|
| 0 Real World | Contracts/Prices are facts | Same | No change |
| 1 Raw OWS | OWS v3 JSON | Same (REST focus) | GraphQL de‑emphasized |
| 2 Storage | PostgreSQL with FKs | Same, implemented | Added `lot_plan_points` |
| 3 Analytics | DuckDB + Parquet | Deferred | Data foundation prioritized |
| 4 Semantic | Milvus | Deferred | Depends on analytics |
| 5 Agent | LLM w/ tool calling | Deferred | Requires analytics + security gates |

## Where We Shifted from the Initial Proposal (Why)

**Shift: YES — intentional simplification.**

| Area | Initial Proposal | Current Implementation | Reason |
|------|------------------|------------------------|--------|
| Orchestration | Airflow | Manual scripts + runner | Reduce ops until data is stable |
| Analytics | DuckDB + Parquet | Deferred | Prioritize ingestion integrity |
| Vector Search | Milvus | Deferred | Not needed until analytics ready |
| API Layer | FastAPI services | Deferred | No agent endpoints yet |
| Network Topology | DMZ/App/Data zones | Single host + localhost‑bound ports | Faster MVP setup |
| Prompt Injection | Regex guard | Deferred | Agent layer not active; will use allow‑listed tools |

**What stayed aligned:**
- Data‑first philosophy (facts before AI).
- PostgreSQL as the single source of truth.
- On‑prem / self‑hosted posture.

## Upgrade Next Steps (Based on Current Lower‑Level Choices)

Because we committed to REST‑based ingestion, placeholder ENSTRU, and explicit lineage tables, the next steps must align with those decisions:

1. **DuckDB analytics must use Postgres as truth**
   - Export from Postgres tables (not raw OWS JSON).
   - Include `lot_plan_points` for lineage and `contract_items` for fair price baselines.
2. **Fair Price must accept “insufficient data”**
   - Use `verdict_insufficient_data(n, min_n)` stub in `etl/fair_price.py`.
   - Baselines should be built from `contract_items` joined to plan KATO and ENSTRU.
3. **ENSTRU placeholders require backfill**
   - Add a ref ingestion step once a reliable ENSTRU source is available.
   - Replace `UNKNOWN` names during backfill, keep codes stable.
4. **Supplier enrichment is mandatory for any agent output**
   - Agents must rely on `subjects` as the canonical supplier registry.
5. **Security controls must be attached at the Agent layer**
   - Enforce allow‑listed SQL/templates only (no arbitrary SQL).
   - Log each tool invocation (who/what/when, not raw data).

## Request Optimization (Time‑Efficient Ingestion)

**Immediate optimizations (implemented)**
- **Plan data reuse in contract items:** `contract_items` now reads ENSTRU/unit/name from local `plan_points` first and only falls back to `/v3/plans/view/{id}` when missing. This removes a large class of redundant API calls.
- **Announcement prefetch for lots/contracts:** `announcements` for each BIN are prefetched into memory to avoid per‑row DB lookups and reduce API fetches on missing links.
- **Concurrent request de-duplication:** repeated `GET /v3/trd-buy/{id}` calls are now coalesced in `OWSClient` so parallel workers await one in-flight request per ID.
- **Resume/backstep:** `etl_state` continues from the last cursor and can reprocess only the last N IDs per BIN.
- **Adaptive RPS:** rate limiter auto‑tunes between `OWS_RPS_MIN` and `OWS_RPS_MAX`, stepping up after `OWS_RPS_SUCCESS_WINDOW` successful requests and backing off on 429/5xx.
- **BIN concurrency:** loaders accept `--concurrency` (default `ETL_CONCURRENCY`) to process multiple BINs in parallel while sharing the global rate limiter.
- **Checkpointed incremental workers for high-volume feeds:** `/v3/plans/kato`, `/v3/plans/spec`, `/v3/acts`, `/v3/treasury-pay` now use `etl_state` resume cursors and stop scanning once they reach known IDs (incremental mode).

**Next‑level optimization (recommended)**
- **Incremental ingestion via Journal:** Use `/v3/journal` daily to fetch only changed IDs, then pull only those entities (plans/lots/contracts/trd_buy). This removes full‑scan REST pagination on every run.
- **Batch DB writes:** Buffer inserts (e.g., 100–500 rows) and use `executemany` or `COPY` to reduce DB overhead.

## OWS Endpoint Leverage Plan (Data Quality)

Used now:
- `/v3/plans/{bin}` for core plan points.
- `/v3/plans/kato` for reliable delivery KATO enrichment.
- `/v3/trd-buy/bin/{bin}` and `/v3/trd-buy/{id}` for announcements and linkage repair.
- `/v3/lots/bin/{bin}` with `point_list` to preserve plan↔lot lineage.
- `/v3/contract/customer/{bin}` and `/v3/contract/{id}/units` for contracts and line items.
- `/v3/refs/*` including `ref_kato` and `ref_units` for FK quality.
- `/v3/acts` for execution evidence against contracts.
- `/v3/treasury-pay` for payment-level validation and post-award analytics.
- `/v3/trd-buy/{id}/cancel` and `/v3/trd-buy/{id}/pause` for legal/process anomaly context.

## Gaps to Close Next (Planned)

1. **Analytics layer**
   - DuckDB marts + Parquet export (Fair Price, anomaly detection).
2. **Agent layer**
   - FastAPI endpoints + tool‑calling policy.
3. **Vector search**
   - Milvus embeddings for semantic search and RAG.
4. **Orchestration**
   - Airflow scheduling once ingestion stabilizes.

## Deployment Path (MVP → Hardened)

- **Now:** Docker Compose on single host, localhost‑bound DB/Redis, no public APIs.
- **Next:** Add FastAPI services, Airflow scheduler, DuckDB analytics service.
- **Later:** DMZ/App/Data network split, TLS via Nginx, monitoring (Prometheus/Grafana).

## Submission Checklist (Final Deliverables)

1. Architecture description (text + diagram).
2. Data schema (Postgres tables + lineage).
3. Analytics & metrics definition (Fair Price, anomalies).
4. Example agent answers (explainable + reproducible).
5. Risks and limitations.
6. Scripts to reproduce ingestion and verification.

Execution order and quality gates are documented in:
- `docs/EXECUTION_CHECKLIST.md`

## Decision Log (Short)

- Use REST endpoints for Spiral 2 core entities (more stable than GraphQL).
- Add `lot_plan_points` to preserve audit trail.
- Keep infra minimal until analytics layer is validated.
- Add stubs: `etl/fair_price.py` with `K_VOL` placeholder and `verdict_insufficient_data`.
