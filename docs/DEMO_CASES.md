# Demo Cases (First Check)

This file contains 3 concrete demo storylines aligned to mandatory classes in the technical task:
- Fairness (`Справедливость`)
- Price anomalies (`Аномалии`)
- Volume anomalies (`Объемы`)

All outputs are generated from live data in local marts via `POST /ask`.
Raw JSON snapshots are stored in:
- `artifacts/demo_cases/case1_fair_lot_32899983.json`
- `artifacts/demo_cases/case2_anomaly_enstru_331312_2024.json`
- `artifacts/demo_cases/case3_volume_enstru_172213_2025.json`

## Unified Response Format

Each case below is presented in submission-friendly format:
1. `Краткий вывод`
2. `Использованные данные`
3. `Сравнение`
4. `Метрика оценки`
5. `Ограничения и уверенность`
6. `Top-K примеры (ID + ссылки)`

---

## Case 1 — Fairness by Lot

**Prompt**
`Оцени адекватность цены лота № 32899983 относительно аналогичных контрактов других ведомств в идентичном городе поставки. top 5`

### Краткий вывод
Цена лота `32899983` классифицирована как `anomaly`: отклонение `+936.22%` от ожидаемой медианной цены по референсной выборке.  
Фактическая цена значительно выше верхнего дециля (`above_p90`).

### Использованные данные
- Период: `2025` (лот), референсный год: `2024`
- Сущность: `lots.id = 32899983`
- ЕНСТРУ: `331312.100.000000`
- Код ед.: `980`
- Фильтр справедливости: `MIN_N=20`, `fallback_level=4` (национальный fallback)
- Объем выборки: `N=21`

### Сравнение
- Фактическая цена за ед.: `269,973,303.57`
- Ожидаемая цена за ед.: `26,053,600.00`
- Медиана референса: `23,200,000.00`
- Диапазон P10–P90: `4,965,000.00` – `143,200,000.00`
- Отклонение: `+936.22%`

### Метрика оценки
- Метод: `median + CPI time adjustment`
- `K_time = 1.123` (на основе `macro_indices`)
- Вердикт: `anomaly` при пороге `>30%`

### Ограничения и уверенность
- `region_id = NULL` (идентичный город не восстановлен для этого лота), поэтому использован fallback на национальный рынок.
- Уверенность: `MEDIUM` (`N=21`).

### Top-K примеры (ID + ссылки)
1. `contract_id=21296035`, `contract_item_id=48904426`
   - `https://ows.goszakup.gov.kz/v3/contract/21296035`
   - `https://ows.goszakup.gov.kz/v3/trd-buy/13167919`
   - `https://goszakup.gov.kz/ru/announce/index/13167919`
2. `contract_id=21296043`, `contract_item_id=48904434`
   - `https://ows.goszakup.gov.kz/v3/contract/21296043`
   - `https://ows.goszakup.gov.kz/v3/trd-buy/13165915`
   - `https://goszakup.gov.kz/ru/announce/index/13165915`
3. `contract_id=21296061`, `contract_item_id=48904460`
   - `https://ows.goszakup.gov.kz/v3/contract/21296061`
   - `https://ows.goszakup.gov.kz/v3/trd-buy/13164669`
   - `https://goszakup.gov.kz/ru/announce/index/13164669`

---

## Case 2 — Price Anomalies by ENSTRU

**Prompt**
`Найди закупки с отклонением цены >30% от средневзвешенной по ЕНСТРУ 331312.100.000000 за 2024 год. top 5`

### Краткий вывод
Найдено `9` аномальных позиций по ЕНСТРУ `331312.100.000000` за `2024` год.  
Максимальное отклонение составило `+1063.45%`.

### Использованные данные
- Период: `2024`
- ЕНСТРУ: `331312.100.000000`
- Порог: `deviation > 30%`
- Выборка референса: `ref_n = 21`
- Таблица анализа: `lot_anomalies` (из `lot_fair_price_eval`)

### Сравнение
- `count = 9`
- `max_deviation_pct = 1063.45`
- Пример top-1:
  - `actual_unit_price = 269,920,000.00`
  - `expected_unit_price = 23,200,000.00`
  - `deviation_pct = +1063.45%`

### Метрика оценки
- Метод: `Median baseline + threshold rule (>30%)`
- Дополнительно: `fallback_level` присутствует в каждом результате.

### Ограничения и уверенность
- Для части записей `region_id = NULL`, анализ строится по доступной региональности/национальному fallback.
- Уверенность: `MEDIUM`.

### Top-K примеры (ID + ссылки)
1. `contract_id=21296035`, `contract_item_id=48904426`, `deviation=1063.45%`
   - `https://ows.goszakup.gov.kz/v3/contract/21296035`
   - `https://ows.goszakup.gov.kz/v3/trd-buy/13167919`
2. `contract_id=21296043`, `contract_item_id=48904434`, `deviation=668.75%`
   - `https://ows.goszakup.gov.kz/v3/contract/21296043`
   - `https://ows.goszakup.gov.kz/v3/trd-buy/13165915`
3. `contract_id=21296061`, `contract_item_id=48904460`, `deviation=517.24%`
   - `https://ows.goszakup.gov.kz/v3/contract/21296061`
   - `https://ows.goszakup.gov.kz/v3/trd-buy/13164669`

---

## Case 3 — Volume Anomalies (YoY)

**Prompt**
`Выяви нетипичное завышение количества ТРУ по сравнению с предыдущими годами по ЕНСТРУ 172213.000.000002 за 2025 год. top 5`

### Краткий вывод
Обнаружены `2` выраженные аномалии роста объема по ЕНСТРУ `172213.000.000002` в `2025`.  
Максимальная кратность роста: `157.31x` (`+15631.43%`).

### Использованные данные
- Период: `2025` против `2024` (YoY)
- ЕНСТРУ: `172213.000.000002`
- Порог: `ratio >= 1.5`
- Фильтр устойчивости базы: `prev_qty >= 50`
- Таблица анализа: `volume_anomalies`

### Сравнение
- БИН `020440003656`: `22,024` vs `140` (пред. год), `157.31x`
- БИН `030440003698`: `8,000` vs `2,000`, `4.00x`

### Метрика оценки
- Метод: `YoY ratio = total_qty(year) / total_qty(previous_year)`
- Вердикт по строке: аномалия при `ratio >= 1.5`

### Ограничения и уверенность
- По ряду plan points `kato_delivery = NULL`, региональная детализация ограничена.
- Уверенность: `MEDIUM`; рекомендован ручной контроль extreme outliers.

### Top-K примеры (ID + ссылки)
1. `plan_point_id=77716802` (БИН `020440003656`, qty `22000`)
   - `https://ows.goszakup.gov.kz/v3/plans/view/77716802`
2. `plan_point_id=79602840` (БИН `030440003698`, qty `4000`)
   - `https://ows.goszakup.gov.kz/v3/plans/view/79602840`
3. `plan_point_id=79027952` (БИН `030440003698`, qty `2000`)
   - `https://ows.goszakup.gov.kz/v3/plans/view/79027952`

---

## Issues Found During First Demo Check (Fixed)

1. Volume questions were misrouted to compare due phrase `по сравнению`.
   - Fix: dedicated `volume` intent and endpoint path.
2. Lot fairness failed when user entered external procurement ID instead of internal `lots.id`.
   - Fix: fallback resolver now checks `lots.id`, `source_trd_buy_id`, `announcement_id`.
3. Top-K anomaly/fair outputs lacked direct links.
   - Fix: contract, trd-buy, and portal links are now included in results.
4. Streamlit duplicate button key (`DuplicateWidgetID`) in suggestions.
   - Fix: explicit unique keys for dynamic and preset buttons.
