# AI-Агент Анализа Госзакупок РК

## Финальная документация проекта (от старта до submission)

Версия: 1.0  
Дата: 2026-03-02  
Проект: `goszakup-agent`

---

## 1. Резюме

Этот проект реализует полный цикл Data Engineering + AI Development для анализа закупок РК на базе OWS v3:
- ingestion данных из OWS v3 в PostgreSQL;
- нормализация и контроль качества;
- аналитические витрины в DuckDB;
- agentic API с tool whitelist (без free-form SQL);
- демо-интерфейс на Streamlit с explainable ответами и Top-K evidence.

Система поддерживает обязательные классы запросов ТЗ:
- аномалии цен;
- справедливость цены;
- аномалии объемов.

---

## 2. Исходное ТЗ и охват

### 2.1 Цель
Разработать аналитического AI-агента, который на данных OWS v3 формирует проверяемые ответы, выявляет аномалии и оценивает эффективность закупок.

Источники:
- `https://goszakup.gov.kz/`
- `https://goszakup.gov.kz/ru/developer/ows_v3`

### 2.2 Целевые организации (27 BIN)
```text
000740001307, 020240002363, 020440003656, 030440003698,
050740004819, 051040005150, 100140011059, 120940001946,
140340016539, 150540000186, 171041003124, 210240019348,
210240033968, 210941010761, 230740013340, 231040023028,
780140000023, 900640000128, 940740000911, 940940000384,
960440000220, 970940001378, 971040001050, 980440001034,
981140001551, 990340005977, 990740002243
```

### 2.3 Период
- 2024, 2025, 2026.

---

## 3. Эволюция реализации (Spiral 1 -> Spiral 5)

### Spiral 1: Infra + базовая связность
Сделано:
- Docker-инфраструктура: Postgres, Redis, Adminer.
- Первичная схема БД.
- Скрипт проверок окружения и connectivity.

Ключевые файлы:
- `docker-compose.yml`
- `infra/postgres/init.sql`
- `scripts/check_connections.py`
- `setup.sh`

### Spiral 2: Основной ingestion + lineage
Сделано:
- ETL-пайплайн:
  - `etl/load_refs.py`
  - `etl/load_plans.py`
  - `etl/load_announcements.py`
  - `etl/load_lots.py`
  - `etl/load_contracts.py`
  - `etl/load_contract_items.py`
- Линковка `lot <-> plan_points` через `lot_plan_points`.
- FK-safe helpers (`ensure_subject`, `ensure_enstru_ref`, `ensure_unit_ref`).
- Multi-BIN и restart-safe ingestion через `etl_state`.
- Верификация: `scripts/verify_spiral2.py`.

### Spiral 3: Quality-first redesign + incremental workers
Сделано:
- Высоконагруженные endpoint workers (incremental/checkpointed):
  - `etl/load_plans_kato_incremental.py`
  - `etl/load_plans_spec_incremental.py`
  - `etl/load_acts_incremental.py`
  - `etl/load_treasury_pay_incremental.py`
  - `etl/load_trd_buy_process_events_incremental.py`
- Raw landing tables для качества и последующего enrich.
- Recovery scripts и strict repair flows.
- Quality snapshots и gap checks.

### Spiral 4: Analytics marts + APIs
Сделано:
- Экспорт в Parquet + сборка DuckDB marts:
  - `analytics/export_parquet.py`
  - `analytics/build_marts.py`
- Marts:
  - `market_price_stats`
  - `lot_fair_price_eval`
  - `lot_anomalies`
  - `volume_anomalies`
  - `coverage_stats`
- Аналитический API:
  - `services/analytics_api/main.py`

### Spiral 5: Strict agentic workflow + submission UI
Сделано:
- Strict agent workflow (intent -> whitelisted tool -> cache -> response).
- 8 intent types и Pydantic validation.
- Запрет free-form SQL от LLM.
- Explainable structured output L0-L4.
- Submission-grade UI.

Ключевые файлы:
- `agent/intent.py`
- `agent/tools.py`
- `agent/agent.py`
- `agent/schemas.py`
- `api/app.py`
- `services/ui/app.py`

---

## 4. Архитектура решения

## 4.1 High-level
```text
OWS v3 API
   -> ETL (async, checkpointed, idempotent)
   -> PostgreSQL (source of truth)
   -> Parquet export
   -> DuckDB marts (analytics layer)
   -> Analytics API + Agent API
   -> Streamlit UI
```

## 4.2 Компоненты
- Хранилище истины: PostgreSQL.
- Кэш и быстрый response cache: Redis.
- OLAP слой: DuckDB.
- Agent API: FastAPI (`/query`, `/ask`, `/tools`, `/health`).
- Analytics API: FastAPI (`/fair-price`, `/anomaly`, `/search`, `/compare`, `/volume-anomaly`, ...).
- UI: Streamlit на `8501`.

## 4.3 On-prem готовность
- Docker Compose уже используется как базовый on-prem runtime.
- Секреты вынесены в `.env`.
- Порты сервисов ограничены loopback (`127.0.0.1`).

---

## 5. Схема данных и связи

## 5.1 Основные сущности (ядро ТЗ)
- `subjects` (заказчики/поставщики)
- `plan_points` (план)
- `announcements` (объявления/trd-buy)
- `lots` (лоты)
- `contracts` (договоры)
- `contract_items` (позиции договора)
- `lot_plan_points` (lineage связь lot->plan)

## 5.2 Справочники
- `enstru_ref`, `units_ref`, `kato_ref`
- `trade_methods_ref`, `lot_statuses_ref`, `contract_statuses_ref`

## 5.3 Операционные таблицы
- `etl_state`, `etl_runs`, `journal_entries`
- `quality_snapshots`, `analytics_export_state`, `anomaly_flags`

## 5.4 Raw landing для high-volume feed
- `raw_plans_kato`
- `raw_plans_spec`
- `raw_acts`
- `raw_treasury_pay`
- `raw_trd_buy_events`

## 5.5 Критические связи
- `lots.announcement_id -> announcements.id`
- `contracts.supplier_bin -> subjects.bin`
- `lot_plan_points.plan_point_id -> plan_points.id`
- `contract_items.contract_id -> contracts.id`

---

## 6. Endpoint coverage OWS v3

Используемые endpoint-группы:
- `/v3/subject/biin/{bin}`
- `/v3/plans/{bin}`
- `/v3/plans/view/{id}`
- `/v3/plans/kato`
- `/v3/plans/spec`
- `/v3/trd-buy/bin/{bin}`
- `/v3/trd-buy/{id}`
- `/v3/trd-buy/{id}/cancel`
- `/v3/trd-buy/{id}/pause`
- `/v3/lots/bin/{bin}`
- `/v3/contract/customer/{bin}`
- `/v3/contract/{id}/units`
- `/v3/acts`
- `/v3/treasury-pay`
- `/v3/journal`
- `/v3/refs/*`

---

## 7. ETL и контроль качества

## 7.1 Идемпотентность и резюмирование
- Каждый ingestion worker пишет checkpoint в `etl_state`.
- Повторный запуск продолжает с checkpoint (+ finite backstep).
- Дубликаты предотвращаются UPSERT-логикой.

## 7.2 Устойчивость
- Повторы запросов с retry/backoff.
- Обработка сетевых ошибок и частичных прогонов.
- Скрипты strict recovery для восстановления lineage и gaps.

## 7.3 Инкрементальность (<24h)
- Daily update через `etl/load_journal_incremental.py`.
- Обновляются только затронутые сущности.
- Далее обновляется аналитический слой.

---

## 8. Аналитический слой

## 8.1 Fair Price (формализовано)
База:
- медиана `unit_price` по сопоставимым `ENSTRU + unit + period + region`.

Коэффициенты:
- `K_time`: CPI adjustment по `macro_indices`.
- `K_region`: пока 1.0 (ограничение текущего датасета).
- `K_vol`: 1.0 (MVP placeholder).

Fallback ladder:
1. region + period
2. national + period
3. region ±1 year
4. national ±1 year

Порог аномалии:
- `deviation_pct > 30%`.

Insufficient data:
- при `N < 20` на всех уровнях -> structured `insufficient_data`.

## 8.2 Аномалии
- Price anomalies: rule-based по `lot_anomalies`.
- Volume anomalies: YoY ratio по `volume_anomalies`.

## 8.3 Макро-параметры (согласованные константы)
- 2024: inflation 8.6%, GDP growth 5.0%
- 2025: inflation 12.3%, GDP growth 6.5%
- 2026: inflation 12.2%, GDP growth 4.7%

---

## 9. Agentic workflow

## 9.1 Принцип
LLM не выполняет SQL и не получает сырые выгрузки.

Пайплайн ответа:
1. Intent classification.
2. Валидация параметров (Pydantic schema).
3. Tool selection из whitelist.
4. Cache check.
5. Tool execution (fixed SQL templates).
6. Structured response assembly + verbalization.

## 9.2 8 intent types
- `PRICE_CHECK`
- `ORG_SUMMARY`
- `ANOMALY_SCAN`
- `SUPPLIER_CHECK`
- `AUDIT_TRAIL`
- `PLAN_VS_FACT`
- `TOP_K`
- `COVERAGE_GAPS`

## 9.3 Tool whitelist
- `get_fair_price`
- `get_org_summary`
- `get_anomalies`
- `get_supplier_profile`
- `get_audit_trail`
- `get_plan_execution`
- `get_top_k`
- `get_uncontracted_plans`

## 9.4 Explainability
Каждый ответ содержит:
- verdict;
- параметры;
- аналитические показатели (N, method, threshold/fallback);
- ограничения/уверенность;
- Top-K с ID и ссылками.

---

## 10. UI и представление результатов

Реализовано в `services/ui/app.py`:
- submission presets по обязательным классам ТЗ;
- L0-L4 вкладки;
- Top-K evidence cards;
- отдельное anomaly list представление с join-enrichment полями:
  - customer/supplier BIN и names,
  - supplier RNU flag,
  - ENSTRU name,
  - delivery place/KATO,
  - contract/announcement metadata,
  - прямые ссылки.

---

## 11. Актуальный статус качества данных (на 2026-03-02)

Источник: `scripts/verify_spiral2.py`, `scripts/verify_spiral3.py`, `scripts/check_quality_gaps.py`.

## 11.1 Интеграция и целостность
```text
plan_points:        31670
announcements:      37178
lots:               98156
lot_plan_points:      967
contracts:          14762
contract_items:       713

FK violations:
- lots.announcement_id -> announcements.id: 0
- contracts.supplier_bin -> subjects.bin: 0
- lot_plan_points.plan_point_id -> plan_points.id: 0

Duplicates: 0 по всем ключевым сущностям
```

## 11.2 Analytics coverage
```text
macro_indices:      3
market_price_stats: 348
lot_fair_price_eval:712
lot_anomalies:      24
```

## 11.3 Gaps snapshot (ключевые)
```text
lots_with_enstru:      1896 / 98156 (1.93%)
lots_with_kato:       18536 / 98156 (18.88%)
plan_points_with_kato:  866 / 31670 (2.73%)
contract_items_total:    713
```

Вывод:
- Слой целостности и воспроизводимости уже стабильный.
- Основной лимит качества аналитики сейчас связан с неполной заполняемостью ENSTRU/KATO в лотах и малым `contract_items` покрытием.

---

## 12. Матрица соответствия ТЗ

| Требование ТЗ | Статус | Что реализовано | Доказательство |
|---|---|---|---|
| Полный цикл DE + AI | PARTIAL | ETL + analytics + agent + UI реализованы | `README.md`, `docs/*`, codebase |
| Реляционная схема | DONE | Нормализованная PostgreSQL схема + FK | `infra/postgres/init.sql` |
| ETL очистка/нормализация | DONE | Normalization helpers, refs, quality workers | `etl/utils.py`, `etl/load_*.py` |
| Аналитический слой для LLM | DONE | DuckDB marts + analytics API | `analytics/*`, `services/analytics_api/main.py` |
| RU/KZ natural language | PARTIAL | RU полно, KZ поддерживается базово | `agent/intent.py`, UI demo |
| Авто-тип вопроса | DONE | 8 intent classifier | `agent/intent.py` |
| Фактический анализ данных | DONE | Tools используют SQL по marts | `agent/tools.py` |
| Explainable ответ | DONE | L0-L4, N/method/limitations/evidence | `agent/agent.py`, UI |
| Аномалии >30% | DONE | `ANOMALY_SCAN`, `lot_anomalies` | `agent/tools.py`, marts |
| Справедливость по лоту | DONE | lot reference resolution + fair price | `get_fair_price` |
| Объемные аномалии | DONE | volume route + `volume_anomalies` | `intent/tools/ui` |
| Инкрементальность <24h | DONE | journal incremental + daily runner | `etl/load_journal_incremental.py`, `scripts/run_daily.sh` |
| Accuracy >=85% facts | IN_PROGRESS | Архитектура не допускает SQL hallucination; formal benchmark pending | требуется labeled set + automated score |
| Top-K с прямыми ссылками | DONE | API URLs + portal links | tool outputs, UI |
| Защита токенов API | DONE | `.env` + no hardcoded secrets | `.env.example`, scripts |
| On-prem readiness | DONE | Docker Compose deployment | `docker-compose.yml` |

---

## 13. Риски и ограничения

1. Низкое покрытие `contract_items` (713) ограничивает статистическую силу Fair Price.
2. Неполная KATO заполняемость снижает региональную точность и повышает долю national fallback.
3. Для части ENSTRU отсутствует описание в `enstru_ref` (`UNKNOWN`) из-за источника.
4. KPI "accuracy >=85%" требует отдельного labeled benchmark набора (20-50 запросов) и формальной оценки.
5. Поддержка сложных KZ-формулировок пока слабее RU (нужен усиленный NLU слой).

---

## 14. План финального hardening перед отправкой

1. Довести `contract_items_total` до целевого уровня (`>= 8000`) через расширенный replay + targeted repair.
2. Повысить `plan_points_with_kato` и `lots_with_enstru` через endpoint-specific backfills.
3. Добавить benchmark script фактической точности (digits/dates/BIN exact match).
4. Зафиксировать 3 demo-кейса с проверяемыми JSON snapshots и скриншотами UI.
5. Обновить `docs/DEMO_CASES.md` final numbers после последнего refresh.

---

## 15. Операционный runbook

### 15.1 Полный запуск
```bash
./setup.sh
python scripts/check_connections.py
python scripts/run_spiral2.sh
python etl/load_macro_indices.py
python analytics/export_parquet.py
python analytics/build_marts.py
python scripts/verify_spiral2.py
python scripts/verify_spiral3.py
```

### 15.2 Daily incremental
```bash
./scripts/run_daily.sh
```

### 15.3 Demo services
```bash
docker compose up -d analytics_api agent_api ui
# UI: http://127.0.0.1:8501
# Agent API: http://127.0.0.1:8002/tools
```

---

## 16. Примеры обязательных demo-запросов

1. Справедливость:
`Оцени адекватность цены лота № 32899983 относительно аналогичных контрактов других ведомств в идентичном городе поставки`

2. Аномалии цены:
`Найди закупки с отклонением цены >30% по ЕНСТРУ 331312.100.000000 за 2024 год`

3. Объемы:
`Выяви нетипичное завышение количества ТРУ по сравнению с предыдущими годами по ЕНСТРУ 172213.000.000002 за 2025 год`

4. Audit trail:
`История договора №24764966`

---

## 17. Состав submission пакета

Рекомендуемый комплект файлов:
- `docs/SUBMISSION_FINAL.md` (этот документ)
- `docs/ARCHITECTURE.md`
- `docs/DATA_SCHEMA.md`
- `docs/ANALYTICS.md`
- `docs/DEMO_CASES.md`
- `docs/RISKS.md`
- `artifacts/demo_cases/*.json`
- `artifacts/quality_baseline_*.txt`
- `artifacts/analytics_check_*.txt`

---

## 18. Заключение

Проект доведен до рабочего end-to-end прототипа с воспроизводимым агентным контуром, explainable аналитикой и on-prem deployability.

Текущий технический приоритет перед финальной подачей: нарастить полноту данных (`contract_items`, KATO/ENSTRU coverage), после чего зафиксировать формальный accuracy benchmark >=85% и финальные демо-кейсы в неизменяемых артефактах.

