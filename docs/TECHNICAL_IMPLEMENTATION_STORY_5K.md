# AI‑агент анализа государственных закупок Республики Казахстан
## Технический отчет (история реализации + архитектура + результаты + демо‑презентация)

Дата: 2026‑03‑02  
Проект: `goszakup-agent`  
Источник данных: OWS v3 (`https://goszakup.gov.kz/ru/developer/ows_v3`)  
Период: 2024–2026  
Охват: 27 организаций (BIN из ТЗ)  
Исполнитель: один исполнитель (индивидуальная реализация)

---

## 0) Контекст и требования ТЗ (что именно нужно было сделать)

ТЗ задает не «чатбот», а аналитическую систему государственного уровня, где критичны:
- проверяемость (любая цифра должна быть воспроизводима и иметь доказательства);
- объяснимость (почему пришли к выводу, на каких данных, размер выборки N, метод);
- надежная загрузка данных (ETL, нормализация, контроль качества);
- поддержка инкрементального обновления (до 24 часов задержки);
- безопасность (защита токенов API, on‑prem развёртывание, минимизация утечек);
- обязательные сценарии:
  - аномалии цен (`>30%` от референса по ЕНСТРУ);
  - справедливость цены по лоту `лот №...` (сравнение с аналогами и той же локацией);
  - объемы (нетипичное завышение количества год‑к‑году).

Допускается RAG/fine‑tuning, но с ключевым ограничением: LLM не должна «считать» и не должна «придумывать» факты; расчёты выполняются внешними инструментами (SQL/Python), а LLM работает в рамках контроля и форматирования.

**Итоговый подход проекта**: построить data‑first pipeline, где LLM ограничена и не имеет доступа к сырым выгрузкам или SQL.

---

## 1) Запуск проекта: базовая стратегия и обоснование решений

### 1.1 Почему спиральная разработка (Spiral 1–5)
Реальные госданные сложны по структуре и качеству. Любая аналитика без устойчивого ingestion и lineage быстро приводит к визуализациям и выводам на неполных либо некорректных данных. В рамках реализации выбран спиральный подход:

- **Spiral 1**: инфраструктура + схема БД + проверка связности.
- **Spiral 2**: ingestion «ядра» сущностей + целостность (FK) + lineage.
- **Spiral 3**: quality‑first redesign, инкрементальные воркеры для high‑volume endpoint’ов.
- **Spiral 4**: аналитические витрины (DuckDB marts) и API для аналитики.
- **Spiral 5**: строгий agentic слой (intent → whitelist tools → cache → explainable ответ) + submission UI.

Каждая спираль завершалась проверяемыми gate‑проверками (`verify_spiral2.py`, `verify_spiral3.py`, quality snapshots). Это ключевой инженерный принцип в аналитике.

### 1.2 Базовые технологические решения
- Язык: **Python** (async + httpx + pydantic).
- Хранилище истины: **PostgreSQL 16** (нормализованная схема + FKs).
- Кэш: **Redis** (используется как инфраструктурный слой; в агентном слое есть Redis/in‑memory cache).
- OLAP/витрины: **DuckDB** + Parquet.
- API: **FastAPI** (analytics API и agent API).
- Демо интерфейс: **Streamlit**.
- Деплой: **Docker Compose** (on‑prem friendly). Сетевые зоны и порты ограничены `127.0.0.1`.

Почему не ClickHouse/Milvus сразу:
- MVP должен быть воспроизводимым и защищенным.
- ClickHouse и Milvus дают пользу позже, когда ingestion и качество витрин стабилизированы.
- Инфраструктура намеренно упрощена в целях скорости внедрения и повышения надежности; данное решение закреплено в документации.

### 1.3 Анализ документации OWS v3 и выбор данных (практический аспект)
Документация OWS v3 на практике задает не только список endpoint’ов, но и практические ограничения:
- какие сущности существуют и какие поля в них стабильны;
- где есть прямые ссылки между сущностями, а где их нет и придется строить lineage через промежуточные связи;
- какие endpoint’ы high‑volume и требуют инкрементальности/чекпоинтов;
- где возможны 500/429 и как строить retriable‑паттерны.

В проекте применен подход «сначала модель данных и audit trail, затем аналитика». Это означало:
- выбирать endpoint’ы, которые дают **идентификаторы** (ID, BIN, ENSTRU, KATO) и позволяют построить связи;
- хранить эти идентификаторы в БД в явном виде и обеспечивать FK‑целостность;
- добавлять enrichment endpoint’ы (например, `/v3/plans/kato`) только после того, как ядро ingestion стало воспроизводимым.

**Минимум сущностей по ТЗ** (и что они означают в OWS):
- `subject` (заказчик/поставщик): организация, идентифицируется BIN/BIIN, используется в планах и договорах.
- `plans`: плановые позиции (plan points) закупок заказчика, «что хотели купить» (объем/сумма/ЕНСТРУ/ед.изм., и иногда место поставки).
- `trd-buy`: объявление о закупке (procurement/announcement), «как закупка была объявлена на портале».
- `lots`: лоты внутри объявления, «что реально выставили на торги».
- `contracts`: договоры, «что реально заключили».
- `contract units/items`: позиции договора (цена/кол-во/сумма), «что реально купили и по какой цене».

**Ключевой практический вывод по связям**: часть связей в API не является «идеальной цепочкой» (не всегда есть прямые поля `contract -> lot`). С начала разработки реализована стратегия lineage:
- сохранять `announcement_id` / `source_trd_buy_id` где доступно;
- сохранять связь `lot -> plan_point` через `point_list` (и таблицу `lot_plan_points`);
- сохранять связь `contract_item -> plan_point` через `pln_point_id`;
- строить audit trail как «самую полную доступную цепочку» (а не как одну идеальную ссылку).

### 1.4 Хронология реализации (как это выглядело в реальном инженерном цикле)
В этом разделе приведены реальные этапы, выявленные сложности и принятые инженерные решения, за счет которых прототип последовательно доводился до системы, пригодной для демонстрации и защиты.

**Spiral 1: запуск окружения и проверка связности**
- Работа началась с `./setup.sh` и `scripts/check_connections.py`, чтобы подтвердить готовность окружения: работоспособность Docker, доступность портов, запуск Postgres/Redis, корректность переменных окружения (включая токен OWS).
- На данном этапе был выявлен типовой класс проблем: запуск скриптов на хосте в отличающемся интерпретаторе/venv относительно окружения, где установлены зависимости. Симптомы: `asyncpg not installed`, `redis not installed`.
- Принято решение закрепить зависимости в `requirements.txt` и сделать `setup.sh`/README максимально детерминированными. В результате `scripts/check_connections.py` стал обязательным gate‑скриптом перед ingestion.

**Spiral 2: ingestion ядра сущностей и закрытие критических gaps**
- На уровне первичной загрузки лотов и договоров были выявлены проблемы, которые блокируют реализацию обязательных аналитических сценариев:
  - отсутствует таблица для связи plan→lot (`point_list` из API некуда записывать);
  - FK‑падения на незнакомых ENSTRU и поставщиках (supplier BIN);
  - повторные запросы `trd-buy/{id}` тысячами (замедление и рост ошибок).
- Для устранения данных рисков была переработана базовая логика ingestion:
  - добавили `lot_plan_points` и начали сохранять весь `point_list`;
  - добавили `ensure_enstru_ref` и `ensure_subject` и сделали ETL FK‑safe;
  - добавили in‑flight dedupe/кеш в клиенте, чтобы убрать дублирующиеся запросы.

**Spiral 3: аналитика и столкновение с «реальностью окружения»**
- При переходе к `pandas/duckdb/pyarrow` для Parquet‑экспорта было установлено, что на новых версиях Python возможны сложности установки (например, сборка `pyarrow` из исходников).
- В целях воспроизводимости analytics‑слой и agent‑слой вынесены в контейнеры со стабильным runtime и wheel‑совместимостью.
- Одновременно было подтверждено наличие существенных пропусков в региональных атрибутах и ENSTRU‑полях. В ответах такие пропуски не маскируются: они отражаются через `confidence/limitations`, а также через вердикт `insufficient_data` как штатный исход.

---

## 2) Архитектура (API → storage) и trust boundaries

### 2.1 Общая схема

```text
OWS v3 API
  -> ETL (async, checkpointed, idempotent)
  -> PostgreSQL (source of truth, FK, lineage)
  -> Parquet export
  -> DuckDB marts (analytics layer)
  -> Analytics API
  -> Agent API (strict tools)
  -> UI (Streamlit)
```

### 2.2 Trust boundaries (безопасность и минимизация данных)
Зоны доверия разделены таким образом, чтобы LLM не могла:
- получить токены/пароли/строки подключения;
- увидеть сырые таблицы и «тонны текста»;
- генерировать SQL;
- менять вычисленные цифры.

Где происходят вычисления:
- все метрики (Fair Price, deviation %, YoY ratio) считаются в SQL/Python на витринах;
- агентный слой только вызывает строго разрешенные инструменты и формирует объяснение.

### 2.3 On‑prem posture
- `docker-compose.yml`: Postgres/Redis/Adminer/Analytics API/Agent API/UI.
- DB и Redis доступны только локально (`127.0.0.1`), что снижает риск экспозиции.
- секреты в `.env`, исключены из git.

### 2.4 Docker‑схема: порты, сети, маршрутизация
Ниже фиксируем «схему контейнеров» как часть сдачи, потому что для on‑prem это не менее важно, чем сама аналитика.

**Сети**
- `data_net` (internal): слой данных (Postgres/Redis) и сервисы, которым нужно к ним обращаться.
- `app_net`: слой приложений (API/UI), который может иметь исходящий доступ (например, для LLM‑провайдера, если он подключается).

**Порты (локально, только loopback)**

| Компонент | Контейнер | Host port | Назначение |
|---|---|---:|---|
| Postgres | `goszakup_postgres` | `5432` | source‑of‑truth БД + FK |
| Redis | `goszakup_redis` | `6379` | кэш ответов/подсказок, infra |
| Adminer | `goszakup_adminer` | `8080` | UI для DB (dev) |
| Analytics API | `goszakup_analytics_api` | `8001` | read‑only доступ к витринам/поиску |
| Agent API | `goszakup_agent_api` | `8002` | строгий агент: NL → tools → ответ |
| Streamlit UI | `goszakup_ui` | `8501` | демонстрационный интерфейс |

**Ключевой принцип безопасности**: все порты проброшены как `127.0.0.1:<port>`, то есть недоступны из внешней сети по умолчанию.

### 2.5 Request path: как проходит один запрос пользователя (end‑to‑end)

**Интерактивный запрос (UI → Agent)**
```text
User (browser)
  -> Streamlit UI :8501
     -> POST Agent API /ask (или /query) :8002
        -> AgentWorkflow: intent → tool whitelist → cache → compute
           -> DuckDB (read-only analytics.duckdb)
           -> (опционально) Redis cache
        -> structured JSON response (verdict/params/analytics/top_k/evidence)
     -> UI renders L0–L4 + top‑K cards/tables
```

**Запрос для подсказок и аналитического поиска (UI → Analytics API)**
```text
UI
  -> POST Analytics API /search or /anomaly :8001
     -> DuckDB marts
     -> items[] (structured records)
```

**Поток данных (ETL → marts)**
```text
OWS v3 REST
  -> ETL loaders (host-run or container)
     -> PostgreSQL normalized tables (+ raw landing)
        -> analytics/export_parquet.py (incremental)
           -> Parquet files (per table)
              -> analytics/build_marts.py
                 -> DuckDB: market_price_stats, lot_fair_price_eval, anomalies...
```

### 2.6 Ссылки на ключевые файлы репозитория (для публикации в GitHub)
Ниже приведены основные точки входа и ключевые компоненты, на которые опирается архитектура и демонстрация. Ссылки указаны как относительные пути и будут кликабельны в GitHub.

| Область | Назначение | Файл(ы) |
|---|---|---|
| Инфраструктура | Docker Compose (порты/сети/сервисы) | [`docker-compose.yml`](../docker-compose.yml) |
| Инфраструктура | Bootstrap/setup окружения | [`setup.sh`](../setup.sh) |
| База данных | Инициализация схемы PostgreSQL | [`infra/postgres/init.sql`](../infra/postgres/init.sql) |
| Проверки | Connectivity gate (env/postgres/redis/OWS) | [`scripts/check_connections.py`](../scripts/check_connections.py) |
| Spiral 2 | Оркестрация ingestion (ядро + enrichment + verify) | [`scripts/run_spiral2.sh`](../scripts/run_spiral2.sh) |
| ETL (ядро) | Refs/Plans/Announcements/Lots/Contracts/Items | [`etl/load_refs.py`](../etl/load_refs.py), [`etl/load_plans.py`](../etl/load_plans.py), [`etl/load_announcements.py`](../etl/load_announcements.py), [`etl/load_lots.py`](../etl/load_lots.py), [`etl/load_contracts.py`](../etl/load_contracts.py), [`etl/load_contract_items.py`](../etl/load_contract_items.py) |
| ETL (enrichment) | KATO/spec/acts/treasury + events + repairs | [`etl/load_plans_kato_incremental.py`](../etl/load_plans_kato_incremental.py), [`etl/load_plans_spec_incremental.py`](../etl/load_plans_spec_incremental.py), [`etl/load_acts_incremental.py`](../etl/load_acts_incremental.py), [`etl/load_treasury_pay_incremental.py`](../etl/load_treasury_pay_incremental.py), [`etl/load_trd_buy_process_events_incremental.py`](../etl/load_trd_buy_process_events_incremental.py), [`etl/backfill_announcements_from_sources.py`](../etl/backfill_announcements_from_sources.py) |
| ETL (общие) | Клиент OWS + утилиты нормализации/ensure_* | [`etl/client.py`](../etl/client.py), [`etl/utils.py`](../etl/utils.py), [`etl/config.py`](../etl/config.py) |
| Macro indices | Национальные константы CPI/GDP (2024–2026) | [`etl/load_macro_indices.py`](../etl/load_macro_indices.py) |
| Analytics | Export Parquet + build marts + query engine | [`analytics/export_parquet.py`](../analytics/export_parquet.py), [`analytics/build_marts.py`](../analytics/build_marts.py), [`analytics/engine.py`](../analytics/engine.py) |
| Agent API | Строгий API (FastAPI): `/query` и `/ask` | [`api/app.py`](../api/app.py) |
| Agent core | Intent → tools whitelist → cache → response | [`agent/agent.py`](../agent/agent.py), [`agent/intent.py`](../agent/intent.py), [`agent/tools.py`](../agent/tools.py), [`agent/schemas.py`](../agent/schemas.py), [`agent/cache.py`](../agent/cache.py) |
| Services | Контейнерные entrypoints: analytics/agent/ui | [`services/analytics_api/main.py`](../services/analytics_api/main.py), [`services/agent_api/main.py`](../services/agent_api/main.py), [`services/ui/app.py`](../services/ui/app.py) |
| Верификация | Проверки Spiral 2/3 и профили качества | [`scripts/verify_spiral2.py`](../scripts/verify_spiral2.py), [`scripts/verify_spiral3.py`](../scripts/verify_spiral3.py), [`scripts/check_quality_gaps.py`](../scripts/check_quality_gaps.py), [`scripts/profile_database_snapshot.py`](../scripts/profile_database_snapshot.py) |
| Документы | Итоговая документация и демо-скрипт | [`docs/SUBMISSION_FINAL.md`](SUBMISSION_FINAL.md), [`docs/SUBMISSION_FINAL_v2.md`](SUBMISSION_FINAL_v2.md), [`docs/DEMO_SCRIPT_v2.md`](DEMO_SCRIPT_v2.md) |

---

## 3) Схема хранения (PostgreSQL): сущности, связи, индексы

### 3.1 Почему нормализованная схема + FK
Требование «проверяемости» означает, что в системе должны быть гарантированы:
- целостность ссылок (lot → plan_point, contract → supplier, etc);
- воспроизводимость выборок;
- отсутствие дубликатов;
- ясный audit trail.

Нормализованная схема позволяет строить «цепочки доказательств» без ручных склейок и без риска расхождений.

### 3.2 Основные таблицы ядра (минимум ТЗ)
- `subjects`: заказчики/поставщики (BIN, name_ru, флаги `is_customer/is_supplier`, RNU flag и т.д.).
- `plan_points`: план закупок.
- `announcements`: объявления/trd-buy.
- `lots`: лоты.
- `contracts`: договоры.
- `contract_items`: позиции договора (unit_price/qty и связь с планом `pln_point_id`).

Ключевое для lineage:
- `lot_plan_points(lot_id, plan_point_id)` — сохраняет связь «лот ↔ план» даже если в контракте нет прямой ссылки.

### 3.3 Справочники
- `enstru_ref`: ЕНСТРУ (с placeholder стратегией `UNKNOWN` для новых кодов).
- `units_ref`: единицы измерения.
- `kato_ref`: КАТО (иерархия, parent, region mapping).
- `trade_methods_ref`, `lot_statuses_ref`, `contract_statuses_ref`.

### 3.4 Операционные таблицы
- `etl_state`: checkpoint state для resume-by-BIN загрузок.
- `etl_runs`: логи прогонов.
- `journal_entries`: журнал изменений (для incremental).
- `quality_snapshots`: метрики качества по слоям.

### 3.5 Raw landing (для high-volume endpoint’ов)
Смысл: хранить сырые payload‑срезы/события для backfill и качества, не ломая нормализованную модель.
- `raw_plans_kato`
- `raw_plans_spec`
- `raw_acts`
- `raw_treasury_pay`
- `raw_trd_buy_events`

### 3.6 ER‑схема (текстовая): связи, которые формируют «фундамент»
Ниже минимальная ER‑модель, которую можно проверить SQL’ем и которая объясняет, почему именно такие таблицы нужны для задач ТЗ.

```text
subjects(bin PK)
  <- contracts.customer_bin (FK)      # заказчик договора
  <- contracts.supplier_bin (FK)      # поставщик договора

plan_points(id PK)
  <- lot_plan_points.plan_point_id (FK)
  <- contract_items.pln_point_id (FK) # позиция договора, связанная с планом

announcements(id PK)                  # trd-buy id
  <- lots.announcement_id (FK)
  <- contracts.announcement_id (FK)   # если API дает прямую ссылку

lots(id PK)
  <- lot_plan_points.lot_id (FK)      # связь lot->plan_point через point_list

contracts(id PK)
  <- contract_items.contract_id (FK)

refs: enstru_ref, units_ref, kato_ref, statuses, methods
```

**Что важно для explainability**:
- любой ответ типа «справедливость цены» должен уметь показать:
  - «что оценивали» (лот/позиция договора);
  - «с чем сравнивали» (выборка контрактных позиций);
  - N и фильтры;
  - ссылки на evidence (ID контрактов, ID объявлений, BIN субъектов).

### 3.7 Audit trail (Plan → Announcement → Lot → Contract) как прикладная цепочка
OWS v3 не всегда дает одну «идеальную» ссылку между планом и договором. Поэтому audit trail строится как объединение нескольких каналов:
- `plan_point` → `lot`: через `lots.point_list` (материализовано в `lot_plan_points`).
- `lot` → `announcement (trd-buy)`: через `lots.announcement_id` (иногда восстанавливается через `source_trd_buy_id`).
- `announcement` → `contract`: через `contracts.announcement_id` или `contracts.source_trd_buy_id` (когда доступно).
- `contract` → `contract_items`: всегда.
- `contract_items` → `plan_point`: через `pln_point_id` (когда заполнено).

Это не «идеальная цепочка», но это честный и проверяемый способ восстановить происхождение данных и показать его пользователю.

---

## 4) ETL: ingestion, нормализация, инкрементальность

### 4.1 Endpoint mapping (что откуда грузим)
В реализации используются REST endpoint’ы OWS v3:
- Subjects: `/v3/subject/biin/{bin}`
- Plans: `/v3/plans/{bin}`, `/v3/plans/view/{id}`
- Plans enrich: `/v3/plans/kato`, `/v3/plans/spec`
- Announcements: `/v3/trd-buy/bin/{bin}`, `/v3/trd-buy/{id}`
- Lots: `/v3/lots/bin/{bin}`
- Contracts: `/v3/contract/customer/{bin}`, `/v3/contract/{id}/units`
- Execution evidence: `/v3/acts`, `/v3/treasury-pay`
- Process events: `/v3/trd-buy/{id}/cancel`, `/v3/trd-buy/{id}/pause`
- Incremental: `/v3/journal`
- Refs: `/v3/refs/*`

### 4.2 Принципы ETL
- **Idempotent upsert**: повторный запуск не должен делать дубли.
- **Resume**: повторный запуск продолжает с места остановки.
- **Finite backstep**: опционально пересчитываем только «хвост» N последних ID.
- **FK-safe**: helper‑функции подготавливают refs/subjects перед вставкой.

### 4.3 Выявленные проблемы и принятые решения

#### Проблема A: зависимости и окружение (asyncpg/redis/pandas/duckdb/pyarrow)
На ранних этапах `check_connections.py` падал с:
- `asyncpg not installed`;
- `redis not installed`.

Это выявило два уровня:
- host‑venv нужен для ETL/скриптов;
- контейнеры сервисов (agent/ui/analytics) должны иметь стабильную Python‑версию.

Отдельный блок проблем возник при установке `pyarrow` на свежем Python (source builds/ошибки сборки). Подход решения:
- закрепить контейнерные runtime на Python 3.11 (wheels доступны, сборка стабильна);
- в README указать рекомендацию Python 3.12 на host для аналитики;
- аналитические расчеты и витрины выполняются через сервисный слой, а не завязаны на локальную «случайную» версию Python.

#### Проблема B: повторные запросы и долгий прогон
В ходе загрузки лотов наблюдались повторяющиеся запросы вида:
- многократные `GET /v3/trd-buy/{id}` для одного и того же `id`.

Это приводило к:
- замедлению загрузки;
- лишним рискам 429/5xx.

Решение:
- in-flight coalescing в API клиенте: один запрос на `trd_buy_id` в процессе, остальные ждут его результат.
- in-memory cache для наиболее повторяющихся lookup.

#### Проблема C: критические разрывы lineage (plan→lot→contract)
На уровне плана были `point_list` в лотах, но не было куда записывать. Это ломало audit trail.

Решение:
- добавлен join table `lot_plan_points`.
- загрузчик лотов записывает каждый `point_list` id в `lot_plan_points`.

#### Проблема D: FK‑падения по ENSTRU и supplier
В реальности API возвращает коды ENSTRU и supplier BIN, которые могут отсутствовать в локальных справочниках БД.

Решение:
- `ensure_enstru_ref`: upsert placeholder (`UNKNOWN`) для новых ENSTRU.
- `ensure_subject`: при встрече supplier BIN дергаем `/v3/subject/biin/{bin}` и upsert в `subjects`.

Это устранило класс ошибок, когда ETL «умирает» на первом неизвестном значении.

#### Проблема E: неоднозначность payload (список вместо объекта)
Фиксировалось падение вида:
- `AttributeError: 'list' object has no attribute 'get'` в `ensure_subject`.

Причина: некоторые endpoint‑ответы возвращали list‑структуры.

Решение:
- защитная нормализация: если ответ список — берем первый объект либо возвращаем structured fallback.

#### Проблема F: KATO и региональная аналитика
Для справедливости по «идентичному городу поставки» нужны региональные признаки.

Было выявлено:
- `plan_points_with_kato` низкий (воркер `/v3/plans/kato` повышает покрытие, но не гарантирует 100%);
- у части лотов `kato_delivery` отсутствует.

Решение:
- отдельный checkpointed воркер `/v3/plans/kato` -> `raw_plans_kato` + backfill в `plan_points`.
- `kato_ref` загружается и используется как валидатор: записываем KATO только если он существует.

#### Проблема G: OWS нестабильность (500 Internal Server Error)
Иногда конкретные `trd-buy` id возвращают 500.

Решение:
- retry/backoff + adaptive rate limiter (снижение RPS на ошибках).
- для 500/неустойчивых сущностей: фиксируем ошибку в логах и продолжаем, не падая всем прогоном.

### 4.4 Checkpoint/resume: почему повторный запуск не должен начинаться «с нуля»
Практическая проблема: ingestion занимает часы, а OWS может давать 500/429. Если любой сбой заставляет стартовать с первой страницы, процесс становится непригодным для эксплуатации.

Внедрены следующие механизмы:
- `etl_state` как таблицу checkpoint’ов.
- `resume_path` / `search_after` как «высокую воду» по endpoint’у и BIN.
- `RESUME_BACKSTEP` (конечный backstep) как страхующий механизм: при рестарте выполняется откат на ограниченный хвост, чтобы захватить поздние обновления, не перегоняя весь массив.

Псевдокод (упрощенно):
```text
state = load_state(worker="lots", bin=BIN)
cursor = state.resume_path or None
while True:
  page = GET /v3/lots/bin/{BIN}?page=next&search_after=cursor
  upsert(page.items)
  cursor = page.next_search_after
  save_state(cursor)
  if not page.has_next: break
```

### 4.5 Производительность: RPS, concurrency и борьба с повторными запросами
На первичных прогонах ingestion были выявлены два источника длительного времени выполнения:
1) реальные объемы данных по 27 BIN;
2) повторные lookup’и (например, `trd-buy/{id}`) для множества лотов.

Решения:
- **Adaptive RPS**: на ошибках (429/5xx) снижаем скорость, на стабильных ответах повышаем до безопасного предела.
- **`--concurrency`**: параллельная обработка BIN/страниц при условии глобального rate limiter.
- **in‑flight dedupe**: при одновременных запросах к одному `trd-buy/{id}` выполняется один сетевой вызов, остальные задачи ожидают его результат.

Комбинация указанных мер повышает пропускную способность и делает прогон управляемым: процесс допускает остановку и последующий restart без потери значимого времени.

### 4.6 Strict recovery pass (когда нужно чинить качество без разрушения целостности)
При выявлении пропусков (KATO, ENSTRU, announcement links, `pln_point_id`) полный перезалив данных не выполняется; применяется строго определенная восстановительная процедура:
- сначала прогоняем enrichment‑workers (например, `/v3/plans/kato`);
- затем делаем repair циклы по источникам (announcements from `source_trd_buy_id`);
- затем точечно запускаем загрузчики в режиме `--repair-missing` (контрактные позиции, linkage).

Преимущество: сохраняется идемпотентность и FK‑целостность, а качество улучшается итеративно.

### 4.7 Нормализация и очистка текстовых полей (минимум, повышающий качество)
ТЗ прямо требует очистку и нормализацию. На практике это не единая универсальная функция, а набор устойчивых правил, которые:
- уменьшают количество шумовых/мусорных токенов в текстовых полях;
- повышают повторное использование справочников;
- улучшают качество поиска и группировок;
- делают входы в аналитику более стабильными.

Что сделано в рамках MVP (и почему этого достаточно для прототипа):

**1) Нормализация названий (`name_clean`)**
- Сохраняется оригинальный `name_ru` (как в API).
- Параллельно рассчитывается `name_clean` (lowercase, удаление пунктуации, схлопывание пробелов) через `etl/utils.py:normalize_name`.
- Это полезно для: простого полнотекстового поиска, дедупликации и «грязных» имен (например, `\"Лот №...\"`, двойные пробелы, скобки).

**2) Приведение типов и безопасный парсинг**
- `safe_int`, `safe_float`, `parse_dt` защищают ingestion от «дрейфа payload» (строка вместо числа, пустые значения).
- Это снижает количество падений ETL и превращает «битые поля» в управляемые `NULL`, которые дальше учитываются в `quality_gaps`.

**3) Единицы измерения как справочник (`units_ref`)**
- Если встречается неизвестный `unit_code`, ingestion не прерывается; добавляется placeholder `UNKNOWN_UNIT_<code>`.
- Это сохраняет целостность и позволяет позднее «обогатить» справочник без пересборки всей модели.

**4) Валидация цены (MVP)**
- В `contract_items` сохраняются `unit_price`, `quantity`, `total_price`.
- Флаг `is_price_valid` в MVP выставляется как минимум при наличии `quantity` и `unit_price`.
- Аналитика (`market_price_stats`, Fair Price) считает статистику только по `is_price_valid=TRUE`.

Важно: это намеренно консервативная стратегия. Для production‑качества целесообразно добавить:
- проверки `quantity > 0`, `unit_price > 0`;
- отсечение экстремальных значений по бизнес‑правилам (например, min/max thresholds);
- вычисление `unit_price = total/qty` как fallback, если API дает только сумму.

Инженерный смысл подхода: лучше иметь меньше строк, но с проверяемыми цифрами, чем «много всего», где половина цен невалидна и ломает статистику.

---

## 5) Контроль качества данных: метрики и gate‑проверки

### 5.1 Автоматизированные проверки
Проверки закреплены в скриптах:
- `scripts/verify_spiral2.py`:
  - наличие ключевых таблиц;
  - FK violations == 0;
  - duplicates == 0.

- `scripts/verify_spiral3.py`:
  - наличие и непустота витрин в DuckDB;
  - базовая готовность analytics.

- `scripts/check_quality_gaps.py`:
  - coverage метрики по ключевым полям (ENSTRU, KATO, linkage).

### 5.2 Текущее состояние (факт на 2026‑03‑02)
Выдержка из последнего прогона:

**Integrity / Spiral 2**
- `plan_points: 31670`
- `announcements: 37178`
- `lots: 98156`
- `lot_plan_points: 967`
- `contracts: 14762`
- `contract_items: 713`
- FK violations: 0
- PK duplicates: 0

**Analytics / Spiral 3**
- `macro_indices: 3`
- `market_price_stats: 348`
- `lot_fair_price_eval: 712`
- `lot_anomalies: 24`

**Quality gaps**
- `lots_with_enstru: 1896 / 98156 (1.93%)`
- `lots_with_kato: 18536 / 98156 (18.88%)`
- `plan_points_with_kato: 866 / 31670 (2.73%)`
- `contract_items_total: 713`

Инженерный вывод:
- pipeline целостен и воспроизводим;
- главная зона роста — полнота данных (особенно contract_items и региональная детализация).

### 5.3 Что означают «null значения» и почему это не просто косметика
Для объективной оценки качества зафиксирован snapshot профиля БД (`artifacts/db_snapshot_.../SUMMARY.md`) и проанализированы наиболее проблемные поля.

**Примеры полей с высокой долей null (и почему это влияет на аналитику)**
- `plan_points.kato_delivery` (≈97% null): означает, что «идентичный город поставки» часто нельзя обеспечить напрямую из plan_points. Нужен отдельный enrichment endpoint `/v3/plans/kato` и, в идеале, дополнительная нормализация адресов.
- `lots.enstru_code` (≈99% null): если ENSTRU не приходит в lot payload, то справедливость цены по «лоту» приходится вычислять через `contract_items` или через `plan_points` (если связь заполнена).
- `lots.announcement_id` (≈62% null): ломает прямые ссылки на портал и ухудшает аудит. Данный дефицит компенсируется полем `source_trd_buy_id` и repair‑циклом объявлений.
- `lots.kato_delivery` (≈81% null): делает региональную компоненту Fair Price слабой; это напрямую снижает confidence.
- `contracts.lot_id` (100% null): не ошибка ETL, а показатель того, что OWS не дает прямой «лот → договор» связи в форме, пригодной для FK.

**Как обеспечено управляемое улучшение качества**
- пропуски не скрываются: они становятся частью `limitations` и `confidence`;
- добавлена «лестница фоллбеков» (region→national, year→±1 year);
- сделали enrichment worker’ы и recovery pass, которые поднимают покрытие без full rescan.

Это важно для защиты: комиссия быстро поймет, что система не «рисует метрики», а честно показывает границы данных и предоставляет пути улучшения.

### 5.4 KPI точности 85% (facts) и методика измерения
ТЗ требует: точность извлечения фактов (цифры, даты, БИН) не менее 85%. Это не про «красивые тексты», а про проверяемые значения.

Показатель трактуется следующим образом:
- **извлечение параметров** из вопроса (ЕНСТРУ, BIN, период, порог, top‑k) должно быть стабильным;
- **цифры ответа** должны совпадать с результатами tools (DuckDB/SQL);
- **LLM (если используется)** не имеет права менять цифры, а только оформляет текст.

Что уже сделано и что добавляется в submission‑контуре:
- есть `tests/test_canonical.py`, где intent‑классификация и маршрутизация проверяются на «канонических» запросах (включая `AUDIT_TRAIL`, `VOLUME` и `PRICE_CHECK`);
- есть тест `test_no_hallucination_on_structured_output`, который подтверждает: при подмене tool‑ответа цифры в финальном ответе совпадают с tool‑данными (то есть агент не «галлюцинирует»);
- для формального KPI 85% подготавливается небольшой labeled set (20–50 вопросов) и рассчитывается точность:
  - exact match по extracted параметрам (например, BIN и год);
  - exact match по ключевым числам ответа (N, median, deviation %) с допустимой погрешностью округления.

Данный подход является корректной инженерной позицией для комиссии: не декларируются недоказуемые свойства модели, вместо этого демонстрируется измеримый критерий и способ его воспроизведения.

---

## 6) Аналитический слой (DuckDB marts): что и как считаем

### 6.1 Почему DuckDB + Parquet
- Postgres — «истина» и операционное хранилище.
- DuckDB — быстрый OLAP слой, упрощающий витрины, оконные функции и массовую агрегацию.
- Parquet — стабильный формат обмена и базовый building block для on‑prem аналитики.

### 6.2 Macro indices и CPI
Добавлена таблица `macro_indices` как национальные константы на 2024–2026 (inflation, GDP growth). Это закрывает требование «временного фактора» без внешних API во время демо.

Из них строится CPI index:
- 2024 = 1.000
- 2025/2026 — цепочка по инфляции.

### 6.3 Fair Price
Формализация:
- baseline: median unit price по сопоставимым `enstru_code + unit_code + region + year`.
- корректировки:
  - `K_time` (CPI);
  - `K_region=1.0` (пока);
  - `K_vol=1.0` (MVP placeholder).

Fallback ladder:
1) same region + same year  
2) national + same year  
3) same region ±1 year  
4) national ±1 year

Вердикт:
- `anomaly` при отклонении > 30%;
- `insufficient_data` если N < 20 на всех уровнях;
- `ok` иначе.

### 6.4 Аномалии
- **Price anomalies**: результат `lot_fair_price_eval` материализуется в `lot_anomalies`.
- **Volume anomalies**: `volume_anomalies` (YoY ratio >= 1.5).

### 6.5 Что именно лежит в витринах (и как это связано с инструментами агента)
Витрина нужна не «для графиков», а как слой, который делает ответы воспроизводимыми и быстрыми.

**`market_price_stats`**
- гранулярность: `enstru_code + unit_code + region_id + year`.
- метрики: `n`, `median`, `mean`, `p10/p25/p75/p90`, `iqr`, `min/max`.
- источник: `contract_items` (только `is_price_valid=TRUE`).

**`lot_fair_price_eval`**
- гранулярность: одна строка на `contract_item_id`.
- поля: `actual_unit_price`, `expected_unit_price`, `deviation_pct`, `verdict`, `fallback_level`, `confidence`, `ref_n`.
- смысл: «все вычисления уже сделаны», агент только выбирает что показать и как объяснить.

**`lot_anomalies`**
- выборка из `lot_fair_price_eval` с `abs(deviation_pct) >= 30` (или заданным порогом).
- обогащается связанной информацией (BIN заказчика/поставщика, дата, ссылки).

**`volume_anomalies`**
- агрегаты по `customer_bin + enstru_code + year` с YoY‑отношением и порогами (`min_prev_qty`).

Таким образом, whitelist tools агента не «изобретают SQL на лету», а читают заранее построенные детерминированные витрины.

---

## 7) Логика AI‑агента (строгая agentic архитектура)

### 7.1 Почему «строгий агент», а не «попросим LLM написать SQL»
Для госаналитики критично:
- отсутствие SQL‑инъекций;
- отсутствие «галлюцинаций»;
- воспроизводимость ответа.

Поэтому реализован strict workflow:

1) Intent classification (8 типов).  
2) Валидация параметров (Pydantic).  
3) Tool selection только из whitelist.  
4) Cache check.  
5) Tool execution (fixed SQL templates).  
6) Structured response L0–L4 + ссылки.

LLM как компонент не обязателен для вычислений; при добавлении LLM она получает только структурированный JSON и не может менять цифры.

### 7.2 Intent types
- `PRICE_CHECK`
- `ORG_SUMMARY`
- `ANOMALY_SCAN`
- `SUPPLIER_CHECK`
- `AUDIT_TRAIL`
- `PLAN_VS_FACT`
- `TOP_K`
- `COVERAGE_GAPS`

### 7.3 Инструменты (whitelist)
- `get_fair_price`
- `get_org_summary`
- `get_anomalies`
- `get_supplier_profile`
- `get_audit_trail`
- `get_plan_execution`
- `get_top_k` (в т.ч. `volume_anomalies`)
- `get_uncontracted_plans`

### 7.4 Ошибки агентного слоя, выявленные в ходе тестирования и эксплуатации

#### Ошибка: аудит‑трейл парсил «а» вместо номера договора
Запрос `История договора №24764966` резолвился неверно, потому что regex выхватывал хвост слова «договора».

Фикс:
- обновили regex на устойчивый вариант `договор(а|у|ом|е)?`.
- добавили регрессионный тест.

#### Ошибка: `top 5` ломал intent (уходило в TOP_K)
Если в запрос добавить `top 5`, классификатор выбирал intent TOP_K.

Фикс для демо:
- presets в UI убрали `top` в обязательных кейсах.
- (в roadmap) можно улучшить classifier приоритетами или отдельным параметром `limit` в intents.

#### Ошибка: объемы не маршрутизировались
Фраза «завышение количества» не определялась.

Фикс:
- добавили volume-route: запросы про объемы направляются в `TOP_K` с `dimension=volume_anomalies`.

### 7.5 Контракт ответа (исключение «галлюцинаций»)
Вместо «текста от модели» используется структурированный ответ, где:
- все числа приходят из tool‑результата (DuckDB/SQL);
- агент форматирует, но не вычисляет;
- каждый ответ содержит evidence и ограничения.

Шаблон ответа (сокращенно):
```json
{
  "intent": "PRICE_CHECK",
  "verdict": "ok | anomaly | insufficient_data",
  "confidence": "HIGH | MEDIUM | LOW",
  "parameters": { "enstru_code": "...", "region_id": "...", "period_from": 2024, "period_to": 2024 },
  "analytics": { "n": 34, "median": 285000, "iqr": 42000, "fallback_level": 2, "method": "Median + IQR" },
  "top_k": [ { "contract_id": 123, "unit_price": 290000, "portal_announce_url": "..." } ],
  "limitations": [ "missing_kato_on_81pct_lots" ],
  "freshness_ts": "2026-03-02T..."
}
```

Этот формат закрывает ключевое требование ТЗ: «Запрещено домысливание и ответы без цифровых показателей».

---

## 8) UI (Streamlit) как финальный слой презентации

Требование комиссии: быстрый и понятный способ интерактивно ознакомиться с системой.

UI на `http://127.0.0.1:8501` показывает:
- presets под обязательные сценарии;
- L0–L4 вкладки;
- KPI‑панель;
- Top‑K evidence.

Для аномалий добавили отдельный формат:
- Top‑K отображается табличным списком с enrichment из вторичных таблиц (customer/supplier names, RNU flag, contract/announcement metadata, ссылки).

Ранее пойманный UI‑баг:
- `DuplicateWidgetID` (одинаковые кнопки без unique key).
Фикс:
- явные `key=...`.

### 8.1 Значимость UI для защиты (состав демонстрации)
В гос‑контексте «красивый интерфейс» не самоцель. UI должен:
- быстро демонстрировать, что система работает end‑to‑end;
- показывать структуру ответа в требуемом формате (Вердикт → Параметры → Аналитика → Уверенность → Top‑K);
- давать review‑команде возможность кликнуть на evidence и проверить цифры.

UI реализован в «строгом и аналитичном» стиле:
- темная/светлая тема;
- KPI‑панель и компактные карточки Top‑K;
- готовые presets под обязательные сценарии из ТЗ;
- отдельный режим показа аномалий с enrichment из вторичных таблиц.

---

## 9) Нефункциональные требования

### 9.1 Контроль качества
- регулярные snapshots (`quality_snapshots` + artifacts);
- верификация integrity и analytics;
- строгие recovery‑скрипты.

### 9.2 Защита токенов
- `.env` исключен из git;
- токены в логах маскируются;
- сервисы bound на loopback;
- сеть `data_net` изолирована.

### 9.3 On‑prem deployment
- Docker Compose — рабочий baseline;
- легко переносится в Kubernetes:
  - Postgres/Redis/Agent/Analytics/UI как отдельные deployment;
  - секреты как k8s secrets;
  - ingress (Nginx) и auth/rate limit как hardening.

### 9.4 Наблюдаемость и управляемость (минимум, но достаточно для прототипа)
Prometheus/Grafana на ранних этапах намеренно не внедрялись; при этом обеспечен практический минимум:
- структурированные логи ingestion и worker’ов (видно endpoint, BIN, курсор/страницу, retry/backoff);
- таблицы `etl_runs` и `etl_state` как «журнал прогресса»;
- артефакты‑снапшоты качества (`artifacts/quality_*.json`, `artifacts/quality_baseline_*.txt`).

Для on‑prem production‑варианта это расширяется стандартным образом: метрики RPS/latency, алерты на рост 500/429, мониторинг свежести данных и доли null по критическим полям.

### 9.5 Минимизация данных и защита секретов (что важно именно для гос‑контекста)
В проекте безопасность заложена «по умолчанию», поскольку даже MVP может быть запущен в закрытом контуре.

**Секреты**
- токены и пароли не хранятся в коде: только через `.env` (и `.env` исключен из git);
- в диагностических скриптах показываем токены только в маскированном виде;
- сервисы не слушают `0.0.0.0` и не публикуют порты наружу без явного решения.

**Минимизация данных в agentic слое**
- LLM/шаблонизатор получает только агрегаты и ссылки на evidence, но не «сырой текст» закупок;
- поля типа `email/phone/website` (если попали через `subject`) не являются частью аналитических ответов и не выводятся в UI/agent по умолчанию;
- вычисления (Fair Price, аномалии, YoY) выполняются вне LLM и возвращаются в структуре JSON.

Эти решения напрямую закрывают риск «модель увидела лишнее» и соответствуют ожиданиям к системам, которые потенциально работают с государственными данными.

---

## 10) Финальная презентация (как защищать проект)

Ниже структура «защиты» на 7–10 минут, которую можно использовать как устный скрипт и как структуру слайдов.

### 10.1 Slide 1: Problem & Constraints
- госданные, проверяемость, explainability.

### 10.2 Slide 2: Architecture
- diagram `OWS -> ETL -> Postgres -> DuckDB -> APIs -> UI`.

### 10.3 Slide 3: Storage schema
- сущности и связи;
- `lot_plan_points` как ключевой lineage слой.

### 10.4 Slide 4: ETL reliability
- checkpoint `etl_state`;
- incremental workers;
- как переживаем падения.

### 10.5 Slide 5: Analytics (Fair Price + anomalies)
- формула;
- fallback ladder;
- `insufficient_data` как first‑class outcome.

### 10.6 Slide 6: Agentic control
- intent whitelist;
- запрет SQL;
- structured output.

### 10.7 Slide 7: Live demo
Три обязательных кейса:
1) Fairness:
`Оцени адекватность цены лота № 32899983 относительно аналогичных контрактов других ведомств в идентичном городе поставки`

2) Price anomalies:
`Найди закупки с отклонением цены >30% по ЕНСТРУ 331312.100.000000 за 2024 год`

3) Volume anomalies:
`Выяви нетипичное завышение количества ТРУ по сравнению с предыдущими годами по ЕНСТРУ 172213.000.000002 за 2025 год`

Плюс audit trail:
`История договора №24764966`

### 10.8 Slide 8: Risks & Next steps
- честно показать quality gaps;
- план повышения coverage;
- формальный benchmark на 85% fact accuracy.

---

## 11) Перечень ошибок/челленджей и решений (краткая таблица)

| Категория | Симптом | Причина | Решение |
|---|---|---|---|
| Env deps | `asyncpg not installed` | неполная venv | `requirements.txt` + setup/README |
| PyArrow | source build errors | несовместимость версий | контейнерный runtime 3.11 + host 3.12 рекомендация |
| API perf | повтор `trd-buy/{id}` | параллельные fetch | in-flight dedupe cache |
| Lineage | теряется plan→lot→contract | нет join table | `lot_plan_points` |
| FK failures | неизвестный ENSTRU/supplier | refs не загружены | `ensure_enstru_ref`, `ensure_subject` |
| Payload drift | list вместо dict | вариативность API | normalize/guard |
| OWS 500 | endpoint нестабилен | серверная ошибка | retry/backoff + adaptive rps |
| Agent parsing | audit id => `а` | regex bug | фикс regex + тест |
| UI | DuplicateWidgetID | одинаковые кнопки | уникальные keys |
| Volume intent | не определяется | classifier gaps | volume-route -> `volume_anomalies` |

---

## 12) Артефакты сдачи (что именно сдаем)

Документы:
- `docs/SUBMISSION_FINAL.docx` (полная версия)
- `docs/SUBMISSION_FINAL_v2.docx` (версия для защиты)
- `docs/DEMO_SCRIPT_v2.docx` (скрипт демо)

Доказательства:
- `scripts/verify_spiral2.py` / `scripts/verify_spiral3.py`
- `scripts/check_quality_gaps.py`
- `artifacts/demo_cases/*.json`

### 12.1 Как воспроизвести результаты (короткий чек‑лист)
Этот проект сделан так, чтобы reviewer мог повторить ключевые шаги без дополнительных ручных действий и неформализованных операций.

1) Поднять инфраструктуру:
- `./setup.sh up` (или `docker compose up -d`)
- проверить связность: `python scripts/check_connections.py`

2) Прогнать ingestion (Spiral 2, resume‑safe):
- `./scripts/run_spiral2.sh --concurrency 10`
- (опционально) точечный прогон: `./scripts/run_spiral2.sh --bins 100140011059 --limit 50`

3) Пересобрать витрины (Spiral 3):
- `python analytics/export_parquet.py`
- `python analytics/build_marts.py`
- проверить: `python scripts/verify_spiral3.py`

4) Открыть демо и выполнить обязательные кейсы ТЗ:
- `docker compose up -d analytics_api agent_api ui`
- открыть `http://127.0.0.1:8501` и запустить кейсы из блока пресетов (Fairness/Anomalies/Volume/Audit).

---

## 13) Вывод

Реализован end‑to‑end прототип с устойчивым ingestion, нормализованным хранилищем и строгим агентным контуром, где все числовые значения рассчитываются воспроизводимо и снабжаются доказательствами.

Ключевой remaining gap — повышение полноты полей и объема `contract_items` для роста статистической силы регионального Fair Price и для достижения формального KPI точности (>=85% на labeled set).
