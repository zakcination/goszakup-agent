# Database Snapshot Summary

- Generated at: `2026-03-01T18:21:54.317789+00:00`
- Output dir: `artifacts/db_snapshot_20260301_182152`
- Tables: `25`
- Total rows across all tables: `218804`
- Numeric describe rows: `87`
- Datetime describe rows: `46`

## Largest Tables (by total bytes)

| table | rows | total size | table size | indexes size |
|---|---:|---:|---:|---:|
| lots | 97920 | 59.56 MB | 34.48 MB | 25.03 MB |
| plan_points | 31670 | 12.68 MB | 8.95 MB | 3.69 MB |
| announcements | 36317 | 11.35 MB | 6.95 MB | 4.36 MB |
| contracts | 11674 | 4.12 MB | 1.84 MB | 2.25 MB |
| kato_ref | 16764 | 2.80 MB | 1.98 MB | 800.00 KB |
| subjects | 7885 | 2.35 MB | 1.96 MB | 360.00 KB |
| enstru_ref | 9009 | 1.20 MB | 608.00 KB | 584.00 KB |
| raw_acts | 749 | 904.00 KB | 752.00 KB | 120.00 KB |
| contract_items | 713 | 800.00 KB | 288.00 KB | 472.00 KB |
| raw_trd_buy_events | 2850 | 712.00 KB | 400.00 KB | 272.00 KB |
| etl_runs | 1116 | 416.00 KB | 256.00 KB | 128.00 KB |
| raw_plans_spec | 168 | 368.00 KB | 272.00 KB | 64.00 KB |
| raw_treasury_pay | 100 | 312.00 KB | 200.00 KB | 80.00 KB |
| raw_plans_kato | 255 | 296.00 KB | 200.00 KB | 64.00 KB |
| lot_plan_points | 942 | 184.00 KB | 64.00 KB | 96.00 KB |

## Highest Null Ratios (column-level)

| table | column | row_count | null_count | null_pct |
|---|---|---:|---:|---:|
| contracts | lot_id | 11674 | 11674 | 100.00% |
| contracts | start_date | 11674 | 11674 | 100.00% |
| contracts | end_date | 11674 | 11674 | 100.00% |
| contracts | trade_method_id | 11674 | 11674 | 100.00% |
| subjects | kato_code | 7885 | 7885 | 100.00% |
| enstru_ref | name_kz | 9009 | 9009 | 100.00% |
| enstru_ref | section | 9009 | 9009 | 100.00% |
| enstru_ref | division | 9009 | 9009 | 100.00% |
| enstru_ref | group_code | 9009 | 9009 | 100.00% |
| raw_acts | approve_date | 749 | 749 | 100.00% |
| raw_acts | revoke_date | 749 | 749 | 100.00% |
| etl_runs | fin_year | 1116 | 1116 | 100.00% |
| etl_runs | error_detail | 1116 | 1116 | 100.00% |
| macro_indices | basket_price_kzt | 3 | 3 | 100.00% |
| subjects | rnu_checked_at | 7885 | 7858 | 99.66% |
| lots | enstru_code | 97920 | 96903 | 98.96% |
| lots | unit_code | 97920 | 96903 | 98.96% |
| etl_state | resume_path | 142 | 140 | 98.59% |
| plan_points | kato_delivery | 31670 | 30804 | 97.27% |
| plan_points | delivery_address_ru | 31670 | 30633 | 96.73% |
| lots | kato_delivery | 97920 | 79514 | 81.20% |
| lots | source_trd_buy_id | 97920 | 63269 | 64.61% |
| lots | announcement_id | 97920 | 60396 | 61.68% |
| units_ref | name_kz | 81 | 31 | 38.27% |
| etl_runs | customer_bin | 1116 | 191 | 17.11% |
| contracts | announcement_id | 11674 | 1813 | 15.53% |
| announcements | last_updated | 36317 | 1317 | 3.63% |
| subjects | name_kz | 7885 | 267 | 3.39% |
| subjects | full_name_ru | 7885 | 267 | 3.39% |
| subjects | ref_kopf_code | 7885 | 267 | 3.39% |

## Files

- `table_overview.csv`
- `columns.csv`
- `null_profile.csv`
- `numeric_describe.csv`
- `datetime_describe.csv`
- `snapshot.json`
