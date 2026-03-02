# Agentic Workflow (Strict, Tool-Driven)

## What Changed

The agent API was rebuilt around a strict flow:

1. Intent classification (`agent/intent.py`)
2. Tool call selection from whitelist (`agent/tools.py`)
3. Parameter validation via Pydantic schemas (`agent/schemas.py`)
4. Cache lookup (`agent/cache.py`)
5. Tool execution (parameterized SQL only, no generated SQL)
6. Deterministic verbalization (`agent/templates.py`)
7. Layered response assembly (`agent/agent.py`)

Entrypoint: `api/app.py` with:
- `POST /query`
- `POST /ask` (compatibility alias for UI)
- `GET /tools`
- `GET /health`

## Intent Types (8)

- `PRICE_CHECK`
- `ORG_SUMMARY`
- `ANOMALY_SCAN`
- `SUPPLIER_CHECK`
- `AUDIT_TRAIL`
- `PLAN_VS_FACT`
- `TOP_K`
- `COVERAGE_GAPS`

## Tool Whitelist

- `get_fair_price`
- `get_org_summary`
- `get_anomalies`
- `get_supplier_profile`
- `get_audit_trail`
- `get_plan_execution`
- `get_top_k`
- `get_uncontracted_plans`

Every tool has:
- fixed SQL templates
- strict input schema
- structured output: verdict/data/evidence/top_k/confidence

`get_fair_price` supports two modes:
- direct: `enstru_code + period (+ optional region)`
- reference-based: `lot_id` or `trd_buy_id` with internal ENSTRU resolution from `lots` / `contracts+contract_items`

If ENSTRU cannot be resolved, tool returns structured `insufficient_data` (no fabricated numbers).

## Response Layers

- `l0`: short verdict
- `l1`: breakdown table
- `l2`: analytics + method
- `l3`: examples/top evidence
- `l4`: audit readiness metadata
- `meta`: freshness, cache status, tool name, limitations

## Security Posture

- LLM is not used for SQL generation.
- Agent cannot execute arbitrary queries.
- Only whitelisted tools are callable.
- Parameters are schema-validated before execution.
