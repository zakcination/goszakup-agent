# Analytics

## Fair Price

**Base price:** median unit price from `contract_items` grouped by:
`enstru_code + unit_code + region + year`.

**Region key:** `region_id` from `kato_ref` if available, otherwise fallback to `kato_delivery`
(to keep locality matching even when the region mapping is missing).

**Adjustments:**
- `K_time`: CPI index ratio (derived from `macro_indices`)
- `K_region`: 1.0 (national constants only)
- `K_vol`: 1.0 (reserved)

**CPI index:**
- Base year 2024 = 1.000
- 2025–2026 are chained by inflation %

**Verdicts:**
- `insufficient_data` if N < 20
- `anomaly` if deviation > 30%
- `ok` otherwise

**Fallback ladder (Fair Price):**
1) same region + same year  
2) national + same year  
3) same region ±1 year  
4) national ±1 year

**Lot-level evaluation:**
If a query references a specific lot (`лот № ...`), the system:
- uses the lot's `enstru_code`, `unit_code`, `kato_delivery`
- computes actual unit price from `unit_price` or `lot_amount/quantity`
- compares against the Fair Price stats using the fallback ladder

## Anomalies

- Price anomalies via Fair Price deviation > 30%.
- Volume anomalies via YoY quantity spikes (>= 1.5x).

## Explainability

Every response must include:
- Evidence IDs
- Sample size (N)
- Method
- Confidence
- Limitations
