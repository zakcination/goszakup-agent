"""
analytics/engine.py
------------------
DuckDB-backed analytics query helpers for API services.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


DB_PATH = os.environ.get("ANALYTICS_DB_PATH", "data/analytics.duckdb")
MIN_N = int(os.environ.get("FAIR_PRICE_MIN_N", "20"))


def _df_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    # Replace NaN/NaT/Inf with None so FastAPI can JSON-encode safely.
    clean = df.replace([float("inf"), float("-inf")], pd.NA)
    clean = clean.astype(object).where(pd.notnull(clean), None)
    return clean.to_dict(orient="records")


def _confidence(n: int) -> str:
    if n >= 50:
        return "HIGH"
    if n >= MIN_N:
        return "MEDIUM"
    return "LOW"


def _fetch_one(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> dict[str, Any] | None:
    df = con.execute(sql, params).fetchdf()
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    # Normalize NaN/Inf to None for JSON safety
    norm: dict[str, Any] = {}
    for k, v in row.items():
        if pd.isna(v) or v in (float("inf"), float("-inf")):
            norm[k] = None
        else:
            norm[k] = v
    return norm


def _get_cpi(con: duckdb.DuckDBPyConnection, year: int | None) -> float:
    if year is None:
        return 1.0
    try:
        row = con.execute("SELECT cpi_index FROM cpi_index WHERE year = ?", [year]).fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        return 1.0
    return 1.0


def _infer_year(con: duckdb.DuckDBPyConnection, enstru_code: str, unit_code: int | None) -> int | None:
    if unit_code is None:
        row = con.execute(
            "SELECT MAX(year) FROM market_price_stats WHERE enstru_code = ?",
            [enstru_code],
        ).fetchone()
    else:
        row = con.execute(
            "SELECT MAX(year) FROM market_price_stats WHERE enstru_code = ? AND unit_code = ?",
            [enstru_code, unit_code],
        ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _connect() -> duckdb.DuckDBPyConnection:
    db_path = Path(DB_PATH)
    if not db_path.exists():
        raise FileNotFoundError(f"Analytics DB not found at {db_path}")
    return duckdb.connect(str(db_path), read_only=True)


def query_fair_price(
    enstru_code: str,
    unit_code: int | None = None,
    year: int | None = None,
    region_id: str | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    con = _connect()
    filters = ["enstru_code = ?"]
    params: list[Any] = [enstru_code]
    if unit_code is not None:
        filters.append("unit_code = ?")
        params.append(unit_code)
    if year is not None:
        filters.append("item_year = ?")
        params.append(year)
    if region_id is not None:
        filters.append("region_id = ?")
        params.append(region_id)
    where = " AND ".join(filters)
    sql = f"""
        SELECT
            lfp.*,
            c.supplier_bin,
            c.sign_date,
            CAST(c.announcement_id AS BIGINT) AS announcement_id,
            CAST(c.source_trd_buy_id AS BIGINT) AS source_trd_buy_id,
            ('https://ows.goszakup.gov.kz/v3/contract/' || CAST(lfp.contract_id AS VARCHAR)) AS contract_api_url,
            CASE
                WHEN c.source_trd_buy_id IS NOT NULL AND c.source_trd_buy_id > 0
                THEN ('https://ows.goszakup.gov.kz/v3/trd-buy/' || CAST(CAST(c.source_trd_buy_id AS BIGINT) AS VARCHAR))
                ELSE NULL
            END AS trd_buy_api_url,
            CASE
                WHEN c.announcement_id IS NOT NULL AND c.announcement_id > 0
                THEN ('https://goszakup.gov.kz/ru/announce/index/' || CAST(CAST(c.announcement_id AS BIGINT) AS VARCHAR))
                ELSE NULL
            END AS portal_announce_url
        FROM lot_fair_price_eval lfp
        LEFT JOIN contracts c ON c.id = lfp.contract_id
        WHERE {where}
        ORDER BY lfp.deviation_pct DESC NULLS LAST
        LIMIT ?
    """
    params.append(limit)
    rows = con.execute(sql, params).fetchdf()
    con.close()
    return _df_to_records(rows)


def query_anomalies(
    enstru_code: str | None = None,
    year: int | None = None,
    min_deviation: float = 30.0,
    limit: int = 50,
) -> list[dict[str, Any]]:
    con = _connect()
    filters = ["deviation_pct >= ?"]
    params: list[Any] = [min_deviation]
    if enstru_code:
        filters.append("enstru_code = ?")
        params.append(enstru_code)
    if year is not None:
        filters.append("item_year = ?")
        params.append(year)
    where = " AND ".join(filters)
    sql = f"""
        SELECT
            la.*,
            c.supplier_bin,
            c.sign_date,
            CAST(c.announcement_id AS BIGINT) AS announcement_id,
            CAST(c.source_trd_buy_id AS BIGINT) AS source_trd_buy_id,
            ('https://ows.goszakup.gov.kz/v3/contract/' || CAST(la.contract_id AS VARCHAR)) AS contract_api_url,
            CASE
                WHEN c.source_trd_buy_id IS NOT NULL AND c.source_trd_buy_id > 0
                THEN ('https://ows.goszakup.gov.kz/v3/trd-buy/' || CAST(CAST(c.source_trd_buy_id AS BIGINT) AS VARCHAR))
                ELSE NULL
            END AS trd_buy_api_url,
            CASE
                WHEN c.announcement_id IS NOT NULL AND c.announcement_id > 0
                THEN ('https://goszakup.gov.kz/ru/announce/index/' || CAST(CAST(c.announcement_id AS BIGINT) AS VARCHAR))
                ELSE NULL
            END AS portal_announce_url
        FROM lot_anomalies la
        LEFT JOIN contracts c ON c.id = la.contract_id
        WHERE {where}
        ORDER BY la.deviation_pct DESC
        LIMIT ?
    """
    params.append(limit)
    rows = con.execute(sql, params).fetchdf()
    con.close()
    return _df_to_records(rows)


def query_compare(
    bin_a: str,
    bin_b: str,
    enstru_code: str,
    year: int | None = None,
) -> dict[str, Any]:
    con = _connect()
    params: list[Any] = [bin_a, bin_b, enstru_code]
    year_filter = ""
    if year is not None:
        year_filter = "AND item_year = ?"
        params.append(year)
    sql = f"""
        SELECT customer_bin, COUNT(*) AS n, AVG(deviation_pct) AS avg_deviation
        FROM lot_fair_price_eval
        WHERE customer_bin IN (?, ?) AND enstru_code = ? {year_filter}
        GROUP BY customer_bin
    """
    rows = con.execute(sql, params).fetchdf()
    con.close()
    return {"results": _df_to_records(rows)}


def query_search(
    mode: str = "market",
    enstru_code: str | None = None,
    unit_code: int | None = None,
    customer_bin: str | None = None,
    region_id: str | None = None,
    year: int | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    verdict: str | None = None,
    fallback_level: int | None = None,
    min_n: int | None = None,
    min_deviation: float | None = None,
    max_deviation: float | None = None,
    sort_by: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    con = _connect()
    mode_norm = (mode or "market").strip().lower()
    if mode_norm not in {"market", "fair_price", "anomaly", "volume"}:
        mode_norm = "market"

    filters: list[str] = []
    params: list[Any] = []

    if mode_norm == "market":
        table = "market_price_stats"
        year_col = "year"
        deviation_col = None
        sort_map = {
            "n": "n DESC",
            "median": "median DESC",
            "mean": "mean DESC",
            "year": "year DESC",
        }
        if enstru_code:
            filters.append("enstru_code = ?")
            params.append(enstru_code)
        if unit_code is not None:
            filters.append("unit_code = ?")
            params.append(unit_code)
        if region_id:
            filters.append("region_id = ?")
            params.append(region_id)
        if year is not None:
            filters.append("year = ?")
            params.append(year)
        if year_from is not None:
            filters.append("year >= ?")
            params.append(year_from)
        if year_to is not None:
            filters.append("year <= ?")
            params.append(year_to)
        if min_n is not None:
            filters.append("n >= ?")
            params.append(min_n)
    elif mode_norm == "fair_price":
        table = "lot_fair_price_eval"
        year_col = "item_year"
        deviation_col = "deviation_pct"
        sort_map = {
            "deviation": "deviation_pct DESC NULLS LAST",
            "abs_deviation": "ABS(deviation_pct) DESC NULLS LAST",
            "year": "item_year DESC",
            "n": "ref_n DESC",
            "price": "actual_unit_price DESC",
        }
        if enstru_code:
            filters.append("enstru_code = ?")
            params.append(enstru_code)
        if unit_code is not None:
            filters.append("unit_code = ?")
            params.append(unit_code)
        if customer_bin:
            filters.append("customer_bin = ?")
            params.append(customer_bin)
        if region_id:
            filters.append("region_id = ?")
            params.append(region_id)
        if year is not None:
            filters.append("item_year = ?")
            params.append(year)
        if year_from is not None:
            filters.append("item_year >= ?")
            params.append(year_from)
        if year_to is not None:
            filters.append("item_year <= ?")
            params.append(year_to)
        if verdict:
            filters.append("verdict = ?")
            params.append(verdict)
        if fallback_level is not None:
            filters.append("fallback_level = ?")
            params.append(fallback_level)
        if min_n is not None:
            filters.append("ref_n >= ?")
            params.append(min_n)
        if min_deviation is not None:
            filters.append("deviation_pct >= ?")
            params.append(min_deviation)
        if max_deviation is not None:
            filters.append("deviation_pct <= ?")
            params.append(max_deviation)
    elif mode_norm == "anomaly":
        table = "lot_anomalies"
        year_col = "item_year"
        deviation_col = "deviation_pct"
        sort_map = {
            "deviation": "deviation_pct DESC NULLS LAST",
            "abs_deviation": "ABS(deviation_pct) DESC NULLS LAST",
            "year": "item_year DESC",
            "n": "ref_n DESC",
            "price": "actual_unit_price DESC",
        }
        if enstru_code:
            filters.append("enstru_code = ?")
            params.append(enstru_code)
        if unit_code is not None:
            filters.append("unit_code = ?")
            params.append(unit_code)
        if customer_bin:
            filters.append("customer_bin = ?")
            params.append(customer_bin)
        if region_id:
            filters.append("region_id = ?")
            params.append(region_id)
        if year is not None:
            filters.append("item_year = ?")
            params.append(year)
        if year_from is not None:
            filters.append("item_year >= ?")
            params.append(year_from)
        if year_to is not None:
            filters.append("item_year <= ?")
            params.append(year_to)
        if fallback_level is not None:
            filters.append("fallback_level = ?")
            params.append(fallback_level)
        if min_n is not None:
            filters.append("ref_n >= ?")
            params.append(min_n)
        # anomaly table is already thresholded, but support explicit bounds.
        if min_deviation is not None:
            filters.append("deviation_pct >= ?")
            params.append(min_deviation)
        if max_deviation is not None:
            filters.append("deviation_pct <= ?")
            params.append(max_deviation)
    else:
        table = "volume_anomalies"
        year_col = "year"
        deviation_col = "ratio"
        sort_map = {
            "ratio": "ratio DESC NULLS LAST",
            "year": "year DESC",
            "qty": "total_qty DESC NULLS LAST",
            "prev_qty": "prev_qty DESC NULLS LAST",
        }
        if enstru_code:
            filters.append("enstru_code = ?")
            params.append(enstru_code)
        if customer_bin:
            filters.append("customer_bin = ?")
            params.append(customer_bin)
        if year is not None:
            filters.append("year = ?")
            params.append(year)
        if year_from is not None:
            filters.append("year >= ?")
            params.append(year_from)
        if year_to is not None:
            filters.append("year <= ?")
            params.append(year_to)
        # For volume mode, min_n is interpreted as minimum prior-year baseline.
        if min_n is not None:
            filters.append("prev_qty >= ?")
            params.append(float(min_n))
        # For volume mode, min/max deviation are interpreted as ratio bounds.
        if min_deviation is not None:
            filters.append("ratio >= ?")
            params.append(float(min_deviation))
        if max_deviation is not None:
            filters.append("ratio <= ?")
            params.append(float(max_deviation))

    # If no exact year and no explicit range, keep latest years first.
    order = sort_map.get((sort_by or "").strip().lower())
    if not order:
        if mode_norm == "market":
            order = "n DESC, year DESC"
        elif deviation_col:
            order = f"{deviation_col} DESC NULLS LAST, {year_col} DESC"
        else:
            order = f"{year_col} DESC"

    where = " AND ".join(filters) if filters else "1=1"
    safe_limit = max(1, min(int(limit or 20), 200))
    sql = f"""
        SELECT *
        FROM {table}
        WHERE {where}
        ORDER BY {order}
        LIMIT ?
    """
    params.append(safe_limit)
    rows = con.execute(sql, params).fetchdf()
    con.close()
    return _df_to_records(rows)


def query_volume_anomalies(
    enstru_code: str | None = None,
    customer_bin: str | None = None,
    year: int | None = None,
    min_ratio: float = 1.5,
    min_prev_qty: float = 50.0,
    limit: int = 20,
    evidence_limit: int = 5,
) -> list[dict[str, Any]]:
    con = _connect()
    filters = ["ratio >= ?"]
    params: list[Any] = [float(min_ratio)]
    if enstru_code:
        filters.append("enstru_code = ?")
        params.append(enstru_code)
    if customer_bin:
        filters.append("customer_bin = ?")
        params.append(customer_bin)
    if year is not None:
        filters.append("year = ?")
        params.append(year)
    if min_prev_qty is not None:
        filters.append("prev_qty >= ?")
        params.append(float(min_prev_qty))

    where = " AND ".join(filters)
    sql = f"""
        SELECT customer_bin, enstru_code, year, total_qty, prev_qty, ratio
        FROM volume_anomalies
        WHERE {where}
        ORDER BY ratio DESC, year DESC
        LIMIT ?
    """
    params.append(max(1, min(int(limit), 200)))
    df = con.execute(sql, params).fetchdf()
    rows = _df_to_records(df)

    for row in rows:
        try:
            ratio = float(row.get("ratio") or 0.0)
            row["change_pct"] = (ratio - 1.0) * 100.0
        except Exception:
            row["change_pct"] = None
        e_df = con.execute(
            """
            SELECT
                id AS plan_point_id,
                customer_bin,
                enstru_code,
                quantity,
                date_approved,
                kato_delivery,
                ('https://ows.goszakup.gov.kz/v3/plans/view/' || CAST(id AS VARCHAR)) AS plan_point_api_url
            FROM plan_points
            WHERE customer_bin = ?
              AND enstru_code = ?
              AND EXTRACT(year FROM date_approved) = ?
            ORDER BY quantity DESC NULLS LAST, id DESC
            LIMIT ?
            """,
            [
                row.get("customer_bin"),
                row.get("enstru_code"),
                row.get("year"),
                max(1, min(int(evidence_limit), 20)),
            ],
        ).fetchdf()
        row["evidence_plan_points"] = _df_to_records(e_df)

    con.close()
    return rows


def _resolve_lot_record(con: duckdb.DuckDBPyConnection, lot_id: int) -> tuple[dict[str, Any] | None, str]:
    lot = _fetch_one(
        con,
        """
        SELECT
            l.id AS lot_id,
            l.enstru_code,
            l.unit_code,
            l.quantity,
            l.unit_price,
            l.lot_amount,
            l.kato_delivery,
            kr.region_id,
            a.publish_date
        FROM lots l
        LEFT JOIN kato_ref kr ON kr.code = l.kato_delivery
        LEFT JOIN announcements a ON a.id = l.announcement_id
        WHERE l.id = ?
        """,
        [lot_id],
    )
    if lot:
        return lot, "lots.id"

    lot = _fetch_one(
        con,
        """
        SELECT
            l.id AS lot_id,
            l.enstru_code,
            l.unit_code,
            l.quantity,
            l.unit_price,
            l.lot_amount,
            l.kato_delivery,
            kr.region_id,
            a.publish_date
        FROM lots l
        LEFT JOIN kato_ref kr ON kr.code = l.kato_delivery
        LEFT JOIN announcements a ON a.id = l.announcement_id
        WHERE l.source_trd_buy_id = ?
           OR l.announcement_id = ?
        ORDER BY
            (l.enstru_code IS NOT NULL) DESC,
            (l.unit_code IS NOT NULL) DESC,
            (l.unit_price IS NOT NULL) DESC,
            (l.quantity IS NOT NULL) DESC,
            l.id DESC
        LIMIT 1
        """,
        [lot_id, lot_id],
    )
    if lot:
        return lot, "source_trd_buy_id_or_announcement_id"
    return None, "not_found"


def query_lot_fair_price(lot_id: int, limit: int = 10) -> dict[str, Any]:
    con = _connect()
    lot, resolved_by = _resolve_lot_record(con, lot_id)
    if not lot:
        con.close()
        raise FileNotFoundError(
            f"Lot {lot_id} not found in analytics DB (checked lots.id, source_trd_buy_id, announcement_id)"
        )

    enstru_code = lot.get("enstru_code")
    unit_code = lot.get("unit_code")
    region_id = lot.get("region_id") or lot.get("kato_delivery")
    year = None
    if lot.get("publish_date") is not None:
        year = int(pd.to_datetime(lot["publish_date"]).year)
    if year is None and enstru_code:
        year = _infer_year(con, str(enstru_code), int(unit_code) if unit_code is not None else None)

    actual_unit_price = None
    if lot.get("unit_price") not in (None, 0):
        actual_unit_price = float(lot["unit_price"])
    elif lot.get("lot_amount") not in (None, 0) and lot.get("quantity") not in (None, 0):
        try:
            actual_unit_price = float(lot["lot_amount"]) / float(lot["quantity"])
        except Exception:
            actual_unit_price = None

    # Resolve reference stats with fallbacks
    fallback_level = 1
    stats = None
    if enstru_code and unit_code is not None and year is not None and region_id is not None:
        stats = _fetch_one(
            con,
            """
            SELECT * FROM market_price_stats
            WHERE enstru_code = ? AND unit_code = ? AND region_id = ? AND year = ?
            """,
            [enstru_code, unit_code, region_id, year],
        )
    if not stats or (stats.get("n") or 0) < MIN_N:
        fallback_level = 2
        if enstru_code and unit_code is not None and year is not None:
            stats = _fetch_one(
                con,
                """
                SELECT * FROM market_price_stats_national
                WHERE enstru_code = ? AND unit_code = ? AND year = ?
                """,
                [enstru_code, unit_code, year],
            )
    if (not stats or (stats.get("n") or 0) < MIN_N) and enstru_code and unit_code is not None and year is not None and region_id is not None:
        fallback_level = 3
        stats = _fetch_one(
            con,
            """
            SELECT * FROM market_price_stats
            WHERE enstru_code = ? AND unit_code = ? AND region_id = ? AND year IN (?, ?)
            ORDER BY ABS(year - ?) ASC
            LIMIT 1
            """,
            [enstru_code, unit_code, region_id, year - 1, year + 1, year],
        )
    if not stats or (stats.get("n") or 0) < MIN_N:
        fallback_level = 4
        if enstru_code and unit_code is not None and year is not None:
            stats = _fetch_one(
                con,
                """
                SELECT * FROM market_price_stats_national
                WHERE enstru_code = ? AND unit_code = ? AND year IN (?, ?)
                ORDER BY ABS(year - ?) ASC
                LIMIT 1
                """,
                [enstru_code, unit_code, year - 1, year + 1, year],
            )

    ref_n = int(stats.get("n")) if stats and stats.get("n") is not None else 0
    ref_median = float(stats.get("median")) if stats and stats.get("median") is not None else None
    ref_mean = float(stats.get("mean")) if stats and stats.get("mean") is not None else None
    ref_p10 = float(stats.get("p10")) if stats and stats.get("p10") is not None else None
    ref_p90 = float(stats.get("p90")) if stats and stats.get("p90") is not None else None
    ref_iqr = float(stats.get("iqr")) if stats and stats.get("iqr") is not None else None
    ref_year = int(stats.get("year")) if stats and stats.get("year") is not None else None
    ref_region_id = stats.get("region_id") if stats else None

    k_time = None
    expected = None
    deviation = None
    verdict = "insufficient_data"
    confidence = _confidence(ref_n) if ref_n else "LOW"
    if ref_n >= MIN_N and actual_unit_price is not None and ref_median is not None:
        cpi_item = _get_cpi(con, year)
        cpi_ref = _get_cpi(con, ref_year)
        k_time = cpi_item / cpi_ref if cpi_ref else 1.0
        expected = ref_median * k_time
        deviation = ((actual_unit_price - expected) / expected) * 100.0 if expected else None
        verdict = "anomaly" if deviation is not None and deviation > 30.0 else "ok"

    position_band = None
    if actual_unit_price is not None and ref_p10 is not None and ref_p90 is not None:
        if actual_unit_price < ref_p10:
            position_band = "below_p10"
        elif actual_unit_price > ref_p90:
            position_band = "above_p90"
        else:
            position_band = "within_p10_p90"

    # Evidence query (contracts) aligned with chosen fallback
    evidence_filters = []
    evidence_params: list[Any] = []
    if enstru_code:
        evidence_filters.append("ci.enstru_code = ?")
        evidence_params.append(enstru_code)
    if unit_code is not None:
        evidence_filters.append("ci.unit_code = ?")
        evidence_params.append(unit_code)
    if fallback_level in (1, 3) and (ref_region_id or region_id):
        evidence_filters.append("COALESCE(kr.region_id, pp.kato_delivery::VARCHAR) = ?")
        evidence_params.append(ref_region_id or region_id)
    if ref_year is not None:
        evidence_filters.append("EXTRACT(year FROM c.sign_date) = ?")
        evidence_params.append(ref_year)

    where = " AND ".join(evidence_filters) if evidence_filters else "1=1"
    order_by = "ABS(ci.unit_price - ?) ASC" if actual_unit_price is not None else "c.sign_date DESC"
    if actual_unit_price is not None:
        evidence_params.append(actual_unit_price)

    evidence_sql = f"""
        SELECT
            ci.id AS contract_item_id,
            ci.contract_id,
            c.customer_bin,
            c.supplier_bin,
            c.sign_date,
            CAST(c.announcement_id AS BIGINT) AS announcement_id,
            CAST(c.source_trd_buy_id AS BIGINT) AS source_trd_buy_id,
            ci.unit_price,
            ci.quantity,
            ci.total_price,
            ci.enstru_code,
            ci.unit_code,
            COALESCE(kr.region_id, pp.kato_delivery::VARCHAR) AS region_id,
            ('https://ows.goszakup.gov.kz/v3/contract/' || CAST(ci.contract_id AS VARCHAR)) AS contract_api_url,
            CASE
                WHEN c.source_trd_buy_id IS NOT NULL AND c.source_trd_buy_id > 0
                THEN ('https://ows.goszakup.gov.kz/v3/trd-buy/' || CAST(CAST(c.source_trd_buy_id AS BIGINT) AS VARCHAR))
                ELSE NULL
            END AS trd_buy_api_url,
            CASE
                WHEN c.announcement_id IS NOT NULL AND c.announcement_id > 0
                THEN ('https://goszakup.gov.kz/ru/announce/index/' || CAST(CAST(c.announcement_id AS BIGINT) AS VARCHAR))
                ELSE NULL
            END AS portal_announce_url
        FROM contract_items ci
        JOIN contracts c ON c.id = ci.contract_id
        LEFT JOIN plan_points pp ON pp.id = ci.pln_point_id
        LEFT JOIN kato_ref kr ON kr.code = pp.kato_delivery
        WHERE {where}
        ORDER BY {order_by}
        LIMIT ?
    """
    evidence_params.append(limit)
    evidence = con.execute(evidence_sql, evidence_params).fetchdf()
    con.close()

    return {
        "verdict": verdict,
        "parameters": {
            "lot_id": lot_id,
            "resolved_lot_id": lot.get("lot_id"),
            "resolved_by": resolved_by,
            "enstru_code": enstru_code,
            "unit_code": unit_code,
            "year": year,
            "region_id": region_id,
            "fallback_level": fallback_level,
        },
        "analytics": {
            "actual_unit_price": actual_unit_price,
            "expected_unit_price": expected,
            "deviation_pct": deviation,
            "ref_n": ref_n,
            "ref_median": ref_median,
            "ref_mean": ref_mean,
            "ref_p10": ref_p10,
            "ref_p90": ref_p90,
            "ref_iqr": ref_iqr,
            "ref_year": ref_year,
            "k_time": k_time,
            "position_band": position_band,
            "method": "median + CPI time adjustment",
        },
        "confidence": confidence,
        "top_k": _df_to_records(evidence),
    }
