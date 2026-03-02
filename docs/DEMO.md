# Demo

## Build analytics

```bash
python etl/load_macro_indices.py
python analytics/export_parquet.py
python analytics/build_marts.py
python scripts/verify_spiral3.py
```

## Run services

```bash
docker compose up -d analytics_api agent_api ui
```

Open:
- Analytics API: http://localhost:8001/docs
- Agent API: http://localhost:8002/docs
- UI: http://localhost:8501

Canonical first-check demo scenarios are documented in:
- `docs/DEMO_CASES.md`
- JSON evidence snapshots in `artifacts/demo_cases/`

## Example questions (RU)

- «Найди закупки с отклонением цены > 30% по ЕНСТРУ 85.10.11.000»
- «Оцени справедливость цены по ЕНСТРУ 85.10.11.000 за 2025»
- «Оцени адекватность цены лота № 32899983 относительно аналогичных контрактов»
- «Выяви нетипичное завышение количества ТРУ по сравнению с предыдущими годами по ЕНСТРУ 172213.000.000002 за 2025»
