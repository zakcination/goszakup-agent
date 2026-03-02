# Spiral 3 Architecture Review (Doc vs Code)

Reviewed sources:
- Architecture doc (extracted): `artifacts/goszakup_spiral3_architecture_extracted.txt`
- Current codebase under `etl/`, `analytics/`, `services/`, `scripts/`
- Latest generated metrics artifacts:
  - `artifacts/quality_after_continue_20260301_221224.txt`
  - `artifacts/analytics_check_2026-03-01_194846.txt`
  - `artifacts/db_snapshot_20260301_182152/SUMMARY.md`

Review date: 2026-03-02

## Executive Verdict

The architecture document is directionally aligned, but **not yet satisfied by current implementation**.

Main blockers:
1. Data quality prerequisites in the doc are still unmet.
2. Analytics methods implemented are a smaller subset of what the doc specifies.
3. Agentic layer is rule-based, without LLM/tool orchestration described in the doc.
4. Security/ops controls (auth/rate-limit/cache orchestration/tests) are not at document level yet.

## Gap Matrix

| Area | Doc Expectation | Actual Implementation | Status | Evidence |
|---|---|---|---|---|
| Lots ENSTRU coverage | `lots.enstru_code` must be materially recovered before analytics | Coverage remains `1017 / 97920` (`1.04%`) | Missing | `artifacts/quality_after_continue_20260301_221224.txt` |
| Contract items coverage | `contract_items` should be `>8000` before proceeding | `713` rows in analytics checks | Missing | `artifacts/analytics_check_2026-03-01_194846.txt` |
| Contract linkage | `contracts.lot_id` + `trade_method_id` should be mapped | Loader writes `None` for both fields | Missing | `etl/load_contracts.py:245`, `etl/load_contracts.py:254` |
| Regional completeness | KATO needed for regional fair price | `plan_points_with_kato=866/31670` (`2.73%`) | Missing | `artifacts/quality_after_continue_20260301_221224.txt` |
| ENSTRU hierarchy fields | section/division/group should be derived | `ensure_enstru_ref` inserts all hierarchy fields as NULL | Missing | `etl/utils.py:117`, `etl/utils.py:118` |
| Layer L1/L2 pipeline | Postgres → DuckDB marts exists | Implemented | Done | `analytics/export_parquet.py`, `analytics/build_marts.py:340` |
| Layer L3 writeback | Write stats/flags back to Postgres (`anomaly_flags`, `market_price_stats`) | Marts remain in DuckDB only; no Postgres writeback stage | Missing | `analytics/build_marts.py:340`, `infra/postgres/init.sql:394` |
| Fair Price method | city→oblast→national fallback, MAD/winsorization, K_region | Fallback ladder exists; MAD/winsorization/K_region ratio not implemented | Partial | `analytics/build_marts.py:209`, `analytics/build_marts.py:248`, `docs/ANALYTICS.md:13` |
| Time factor | CPI adjustment | Implemented using annual manual constants (not quarterly official feed) | Partial | `analytics/build_marts.py:70`, `etl/load_macro_indices.py:30` |
| Anomaly methods | rule + IQR + MAD + Isolation Forest + YoY + supplier concentration | Implemented: price deviation threshold and quantity YoY ratio only | Missing | `analytics/build_marts.py:279`, `analytics/build_marts.py:293` |
| Agent architecture | 8 intents + strict tool whitelist orchestration | Rule-based regex routing; 5 intents (`fair_price`, `anomaly`, `volume`, `compare`, `search`) | Partial | `services/agent_api/main.py:31`, `services/agent_api/main.py:353` |
| LLM layer | NITEC LLM verbalization over structured tool outputs | No LLM call in agent service | Missing | `services/agent_api/main.py:1`, `etl/config.py:51` |
| Cache layer | Redis cache in request flow | No Redis use in analytics/agent APIs | Missing | `services/agent_api/main.py`, `services/analytics_api/main.py` |
| API contract | `POST /query` with auth/rate limiting | `POST /ask`; no auth/rate limiting in API code | Missing | `services/agent_api/main.py:221` |
| Drill-down L0→L4 | layered response including full audit trail layer | Current output is `verdict/parameters/analytics/top_k` | Partial | `services/agent_api/main.py:156` |
| Airflow orchestration | DAG `spiral3_refresh` | Not present | Missing | repo file scan; no Airflow DAG files |
| Canonical tests | `tests/test_canonical.py` for no-hallucination/latency | Not present | Missing | repo file scan; no `tests/` directory |

## Acceptance Criteria Check (from architecture doc §7)

1. `contract_items > 8,000`: **Fail** (`713`).
2. `lots.enstru_code non-null > 50%`: **Fail** (`1.04%`).
3. Fair Price for >=20 distinct KTRU: **Not demonstrably met under reliable N>=20 coverage**.
   - `lot_fair_price_eval` has 265 ENSTRU codes, but only 72 non-insufficient rows and only 2 ENSTRU in non-insufficient set.
4. `anomaly_flags > 0`: **Not implemented in pipeline writeback**.
5. Cached response <=3s: **Not testable (no API cache layer implemented)**.
6. No-hallucination SQL parity tests: **Not implemented (no canonical tests)**.
7. Insufficient data when N<20: **Implemented**.
8. Audit trail chain >=80%: **Not implemented as verified acceptance test**.

## What Is Already Strong

1. Incremental ETL and checkpointing are mature:
   - `etl_state`-driven resume, backstep controls, high-volume workers.
2. Core service chain exists and runs:
   - Postgres + DuckDB marts + analytics API + agent API + UI.
3. Explainability fields in outputs improved:
   - IDs, links, sample sizes, fallback level, confidence, method.
4. Robust repair tooling exists:
   - `repair_missing_source_links.py`, `backfill_announcements_from_sources.py`.

## Most Urgent Corrections

1. Fix source mappings that keep critical columns null:
   - `lots.enstru_code`, `lots.unit_code`, `contracts.lot_id`, `contracts.trade_method_id`.
2. Raise contract item coverage from 713 to target band:
   - Full contract units pass + targeted retries on failed contracts.
3. Implement ENSTRU hierarchy derivation:
   - Populate `section/division/group_code` from `code`.
4. Add Postgres writeback stage for analytics outputs:
   - Populate `anomaly_flags` from marts.
5. Add minimal API hardening:
   - Auth header + rate limit + Redis cache for deterministic endpoints.

