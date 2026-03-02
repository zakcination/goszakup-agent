# Data Schema (Core)

## Reference
- `units_ref`
- `kato_ref`
- `enstru_ref`
- `trade_methods_ref`
- `lot_statuses_ref`
- `contract_statuses_ref`
- `macro_indices`
- `trd_buy_kato_metadata`

## Core
- `subjects`
- `plan_points`
- `announcements`
- `lots`
- `lot_plan_points`
- `contracts`
- `contract_items`

## Operational
- `etl_runs`
- `etl_state`
- `journal_entries`
- `analytics_export_state`
- `anomaly_flags`
- `quality_snapshots`

## Raw Landing (Incremental High-Volume)
- `raw_plans_kato` (`/v3/plans/kato`)
- `raw_plans_spec` (`/v3/plans/spec`)
- `raw_acts` (`/v3/acts`)
- `raw_treasury_pay` (`/v3/treasury-pay`)
- `raw_trd_buy_events` (`/v3/trd-buy/{id}/cancel`, `/v3/trd-buy/{id}/pause`)

See `infra/postgres/init.sql` for full DDL.
