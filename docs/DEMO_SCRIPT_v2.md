# Demo Script v2 (7-10 minutes)

Дата: 2026-03-02  
Цель: защитить решение по ТЗ как проверяемую аналитическую систему, а не только как чат.

---

## 0) Pre-flight (30 сек)

Открыть:
- UI: `http://127.0.0.1:8501`
- Agent tools: `http://127.0.0.1:8002/tools`

Коротко проговорить:
- LLM не делает SQL;
- все цифры считаются в tool layer и marts;
- ответы имеют evidence links.

---

## 1) Data integrity gate (1 мин)

В терминале:
```bash
python scripts/verify_spiral2.py
python scripts/verify_spiral3.py
```

Что сказать:
- FK violations = 0;
- duplicates = 0;
- analytics tables не пусты.

---

## 2) Case A — Fairness (2 мин)

Запрос:
`Оцени адекватность цены лота № 32899983 относительно аналогичных контрактов других ведомств в идентичном городе поставки`

Что показать:
- Verdict + confidence;
- `N`, method, fallback/scope;
- Top-K comparables с ссылками.

Что подчеркнуть:
- если данных мало, система не придумывает цену, а возвращает `insufficient_data`.

---

## 3) Case B — Price Anomaly (2 мин)

Запрос:
`Найди закупки с отклонением цены >30% по ЕНСТРУ 331312.100.000000 за 2024 год`

Что показать:
- Top-K anomaly list в UI (обогащенный joins);
- customer/supplier BIN + name;
- deviation, expected vs actual;
- announcement/contract links.

---

## 4) Case C — Volume Anomaly (1.5 мин)

Запрос:
`Выяви нетипичное завышение количества ТРУ по сравнению с предыдущими годами по ЕНСТРУ 172213.000.000002 за 2025 год`

Что показать:
- YoY ratio и change %;
- затронутые BIN;
- evidence на plan points / related IDs.

---

## 5) Audit Traceability (1 мин)

Запрос:
`История договора №24764966`

Что показать:
- цепочку plan -> announcement -> lot -> contract -> acts (где доступно);
- coverage ratio + ограничения;
- прямые API/portal ссылки.

---

## 6) Incremental Freshness (1 мин)

Показать команду:
```bash
./scripts/run_daily.sh
```

Что сказать:
- используются checkpointed workers и journal incremental;
- SLA по актуализации проектно поддерживается архитектурой (<24h).

---

## 7) Closing (30 сек)

Финальный месседж:
- это не «болтающий LLM», а контролируемый аналитический pipeline;
- каждое число воспроизводимо;
- архитектура готова к on-prem и расширению.
