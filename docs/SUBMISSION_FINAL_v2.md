# AI-Агент Анализа Госзакупок РК
## Submission v2 (Defense Edition)

Версия: 2.0  
Дата: 2026-03-02  
Проект: `goszakup-agent`

---

## Executive Summary

Проект реализует production-like прототип AI-агента для госзакупок РК на данных OWS v3 с архитектурой `API -> ETL -> PostgreSQL -> DuckDB marts -> Agent API -> UI`.

Система уже умеет:
- принимать RU/KZ запросы;
- автоматически определять тип аналитического запроса;
- вычислять показатели только через SQL/Python (без «выдумывания» чисел LLM);
- выдавать explainable ответы с evidence, выборкой `N`, методом, ограничениями и Top-K ссылками.

Обязательные классы ТЗ покрыты:
- аномалии цен;
- справедливость цены по лоту;
- аномалии объемов (YoY).

---

## 1) Business Goal vs Delivered Value

Цель ТЗ: проверяемые ответы, контроль аномалий и оценка эффективности закупок.

Что реализовано:
- end-to-end контур загрузки и инкрементального обновления;
- формализованная метрика Fair Price (с fallback-лестницей и недостаточностью данных);
- anomaly scoring по цене и объему;
- строгий агентный слой с whitelist инструментов и схемной валидацией входов;
- demo UI, показывающий факты, не «общие слова».

---

## 2) Scope and Data Sources

Источники:
- `https://goszakup.gov.kz/`
- `https://goszakup.gov.kz/ru/developer/ows_v3`

Целевая глубина:
- 2024, 2025, 2026.

Целевой набор организаций:
- 27 BIN (зафиксированы в конфиге/документации проекта).

---

## 3) Final Architecture

```text
OWS v3
 -> ETL workers (async, checkpointed)
 -> PostgreSQL (source of truth)
 -> Parquet export
 -> DuckDB marts
 -> Analytics API
 -> Agent API (strict tools)
 -> Streamlit UI
```

Компоненты:
- PostgreSQL: нормализованное хранилище с FK и lineage.
- DuckDB: быстрые аналитические витрины.
- Redis: response cache / runtime cache.
- FastAPI:
  - analytics endpoints;
  - agent endpoints (`/query`, `/ask`, `/tools`).
- Streamlit UI (`:8501`) для экспертной проверки.

---

## 4) Data Engineering Delivery

## 4.1 Core schema
Минимальный охват ТЗ выполнен:
- объявления;
- план;
- договоры;
- лоты;
- заказчики/поставщики;
- статусы;
- справочники.

Ключевые таблицы:
- `subjects`, `plan_points`, `announcements`, `lots`, `contracts`, `contract_items`, `lot_plan_points`.
- refs: `enstru_ref`, `units_ref`, `kato_ref`, статусы/методы.
- operational: `etl_state`, `etl_runs`, `journal_entries`, `quality_snapshots`.

## 4.2 ETL quality model
- idempotent upsert;
- resume-by-checkpoint в `etl_state`;
- finite backstep для безопасного восстановления;
- incremental workers для high-volume endpoint’ов;
- strict verification scripts (`verify_spiral2`, `verify_spiral3`).

## 4.3 Incremental <24h
- `load_journal_incremental.py` + daily runner;
- загружаются только изменившиеся сущности;
- обновляются аналитические витрины.

---

## 5) Analytics Methodology

## 5.1 Fair Price
Метод:
- baseline: median unit price по сопоставимым объектам;
- time factor: `K_time` (CPI/macro indices);
- region factor: применяется при наличии региональных данных;
- volume factor: placeholder `K_vol=1.0` (осознанно отключен в MVP).

Fallback ladder:
1. region + period
2. national + period
3. region ±1 year
4. national ±1 year

Вердикт:
- `anomaly`, если отклонение `>30%`;
- `insufficient_data`, если `N < 20` на всех уровнях.

## 5.2 Anomaly detection
- price anomalies: deviation threshold + confidence layers;
- volume anomalies: YoY ratio на `volume_anomalies`.

## 5.3 Explainability
Каждый результат содержит:
- параметры;
- статистику (`N`, median/percentiles/ratio...);
- метод;
- уверенность;
- ограничения;
- Top-K с ID и ссылками.

---

## 6) Agentic AI Design (Risk-Controlled)

Принцип:
- LLM не имеет raw DB доступа;
- LLM не генерирует SQL;
- только whitelist функций с параметрической валидацией.

8 intent types:
- `PRICE_CHECK`, `ORG_SUMMARY`, `ANOMALY_SCAN`, `SUPPLIER_CHECK`, `AUDIT_TRAIL`, `PLAN_VS_FACT`, `TOP_K`, `COVERAGE_GAPS`.

Ответы формируются в структуре L0-L4:
- L0 verdict;
- L1 параметры;
- L2 аналитика;
- L3 примеры;
- L4 evidence/ограничения.

---

## 7) Current Technical Status (as-is)

Интеграция:
- `verify_spiral2.py`: FK и duplicate checks проходят.
- `verify_spiral3.py`: analytics marts построены и не пусты.

Ключевой факт для комиссии:
- платформа архитектурно готова и воспроизводима;
- основной remaining gap связан не с логикой агента, а с полнотой отдельных данных ingestion.

---

## 8) Compliance Matrix (Technical Task)

| Требование | Статус | Комментарий |
|---|---|---|
| Полный цикл DE + AI | DONE | ETL + schema + marts + agent + UI |
| Проверяемые ответы | DONE | Structured output + evidence links |
| Обязательные классы вопросов | DONE | Fair/Anomaly/Volume покрыты |
| Explainability | DONE | N, method, confidence, limits, Top-K |
| Инкрементальное обновление | DONE | Journal-based incremental |
| Защита от домысливания | DONE | Tool whitelist, no free SQL |
| Accuracy >=85% facts | IN PROGRESS | Требуется формальный benchmark-report |

---

## 9) Risks and Mitigations

Основные риски:
- неполное покрытие некоторых полей (KATO/ENSTRU linkage по части сущностей);
- ограниченный объем `contract_items` для статистической мощности;
- зависимость от стабильности OWS schema.

Митигирующие меры уже внедрены:
- checkpointed ETL;
- strict recovery flows;
- quality snapshots;
- deterministic analytics without LLM math.

---

## 10) Final Demo Narrative (for reviewers)

1. Показать архитектуру и boundaries (2 минуты).
2. Показать data integrity checks (1 минута).
3. Запустить 3 обязательных кейса (4 минуты):
   - fairness by lot;
   - anomaly by ENSTRU/year;
   - volume anomaly by ENSTRU/year.
4. Показать audit trail и evidence links (1 минута).
5. Показать incremental update path (1 минута).

Итог: демонстрация не «чат-ответов», а проверяемого аналитического инструмента для закупочного контроля.

---

## 11) Submission Package

Рекомендуемый набор:
- `docs/SUBMISSION_FINAL_v2.docx` (этот документ)
- `docs/SUBMISSION_FINAL.docx` (полная расширенная версия)
- `docs/DEMO_SCRIPT_v2.docx` (сценарий защиты)
- `docs/ARCHITECTURE.md`
- `docs/DATA_SCHEMA.md`
- `docs/ANALYTICS.md`
- `docs/DEMO_CASES.md`
- `artifacts/demo_cases/*.json`

---

## 12) Conclusion

Проект реализован как контролируемый аналитический AI-agent с прозрачной вычислительной цепочкой, воспроизводимыми результатами и on-prem готовностью.

Финальный инженерный фокус перед официальной сдачей:
- повысить полноту ingestion для роста статистической силы;
- зафиксировать benchmark точности извлечения фактов (>=85%) в отдельном отчете.
