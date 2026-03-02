# Spiral 3 Recovery Execution Checklist

This is the strict execution order to close the architecture gaps and reach submission-grade analytics quality.

## Baseline (from latest stable artifact)

Source: `artifacts/quality_after_continue_20260301_221224.txt`

- `lots_total = 97,920`
- `lots_with_enstru = 1,017 (1.04%)`
- `lots_with_unit = 1,017 (1.04%)`
- `lots_with_kato = 18,406 (18.80%)`
- `lots_with_announcement = 37,524 (38.32%)`
- `contracts_total = 11,674`
- `contracts_with_announcement = 9,861 (84.47%)`
- `contract_items_total = 713`
- `contract_items_with_pln_point_id = 713 (100%)`
- `plan_points_total = 31,670`
- `plan_points_with_kato = 866 (2.73%)`

## Global Rule

Do not move to the next phase if the current phase gate fails.

## Phase 0 — Instrumentation First

1. Capture fresh baseline before changes.
   - `python scripts/check_quality_gaps.py`
   - `python scripts/capture_quality_snapshot.py --run-id quality_gate_p0`
2. Save DB profile snapshot.
   - `python scripts/profile_database_snapshot.py`

### Gate P0

- Snapshot files created under `artifacts/`.
- Metrics are visible for:
  - `contracts_with_lot_id`
  - `contracts_with_trade_method_id`
  - `enstru_ref_with_section/division/group_code`

## Phase 1 — ETL Mapping Corrections (Hard Blockers)

### 1.1 Contracts mapping

Patch `etl/load_contracts.py`:
- Map `lot_id` from contract payload (optional fallback `--resolve-contract-view`).
- Map `trade_method_id` from payload.
- If available, map `start_date`, `end_date`.

### 1.2 Lots mapping

Patch `etl/load_lots.py`:
- Prefer direct API fields:
  - `ref_enstru_code -> enstru_code`
  - `ref_units_code -> unit_code`
  - `deliveryPlaces[0].refKatoCode -> kato_delivery`
- Keep current plan-point fallback as backup only.

### 1.3 ENSTRU hierarchy

Backfill `enstru_ref` hierarchy:
- `section = SUBSTRING(code, 1, 2)`
- `division = SUBSTRING(code, 1, 4)`
- `group_code = SUBSTRING(code, 1, 6)`
- Command: `python etl/backfill_enstru_hierarchy.py`

### Gate P1

After rerun:
- `contracts_with_lot_id_pct >= 80%`
- `contracts_with_trade_method_id_pct >= 80%`
- `lots_with_enstru_pct >= 50%`
- `lots_with_unit_pct >= 50%`
- `enstru_ref_with_section_pct >= 95%`

## Phase 2 — Contract Items Coverage (Primary Analytics Fuel)

1. Run full contract-items pass for all 27 BINs with resume and concurrency.
   - `python etl/load_contract_items.py --concurrency 10 --resume-backstep 20000 --repair-missing`
2. Retry failed subset:
   - run again with same params (idempotent upsert).

### Gate P2

- `contract_items_total >= 8,000` (minimum gate from architecture doc)
- `contract_items_with_enstru_code_pct >= 95%`
- `contract_items_with_unit_code_pct >= 95%`
- `contract_items_price_valid_pct >= 70%` (operational target)

## Phase 3 — KATO & Lineage Enrichment

1. Continue checkpointed workers:
   - `python etl/load_plans_kato_incremental.py`
   - `python etl/load_plans_spec_incremental.py`
2. Repair source links and announcements:
   - `python etl/repair_missing_source_links.py --concurrency 10`
   - `python etl/backfill_announcements_from_sources.py --concurrency 10`

### Gate P3

- `plan_points_with_kato_pct >= 20%` (minimum), stretch `>= 40%`
- `lots_with_announcement_pct >= 60%`
- `lots_linked_to_plan_points_pct >= 30%` (minimum)

## Phase 4 — Analytics Refresh and Gate

1. Rebuild analytics from corrected data:
   - `python analytics/export_parquet.py`
   - `python analytics/build_marts.py`
2. Verify:
   - `python scripts/verify_spiral3.py`

### Gate P4

- `market_price_stats > 0`
- `lot_fair_price_eval > 0`
- `lot_anomalies > 0`
- Non-insufficient fair-price coverage:
  - distinct ENSTRU with verdict != `insufficient_data` should grow materially vs baseline.

## Phase 5 — Agent/API Quality Gate

1. Smoke tests for mandatory classes:
   - Fairness query by lot
   - Price anomaly by ENSTRU/year
   - Volume anomaly by ENSTRU/year
2. Save snapshots:
   - `artifacts/demo_cases/*.json`

### Gate P5

- All three mandatory demo classes return structured output:
  - `verdict`
  - `parameters`
  - `analytics`
  - `confidence`
  - `top_k` with evidence IDs/links

## Fast Runbook (Single Command Path)

After code patches in Phase 1:

1. `./scripts/run_recovery_strict.sh --concurrency 10 --rps-start 20 --rps-max 120`
2. `python scripts/check_quality_gaps.py`
3. `python scripts/capture_quality_snapshot.py --run-id quality_gate_after_recovery`
4. `python analytics/export_parquet.py && python analytics/build_marts.py`
5. `python scripts/verify_spiral3.py`

## Stop Conditions (Do Not Proceed)

- If `lots_with_enstru_pct < 50%`: stop analytics/agent polishing.
- If `contract_items_total < 8000`: stop fair-price claims in demo.
- If `contracts_with_lot_id_pct < 80%`: stop audit-trail claims.

## Deliverables Per Phase

- Phase 0: baseline artifacts
- Phase 1: ETL mapping patches + snapshot
- Phase 2: contract-items recovery evidence + snapshot
- Phase 3: lineage/KATO improvement evidence + snapshot
- Phase 4: analytics verification output
- Phase 5: demo case JSONs + final presentation script
