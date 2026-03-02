"""
analytics/build_marts.py
------------------------
Build DuckDB marts for fair price + anomalies from Parquet exports.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


PARQUET_DIR = Path(os.environ.get("ANALYTICS_PARQUET_DIR", "data/parquet"))
DB_PATH = os.environ.get("ANALYTICS_DB_PATH", "data/analytics.duckdb")
MIN_N = int(os.environ.get("FAIR_PRICE_MIN_N", "20"))


def _require_parquet(table: str) -> str:
    table_dir = PARQUET_DIR / table
    if not table_dir.exists():
        raise FileNotFoundError(f"Parquet dir not found: {table_dir}")
    files = list(table_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files for {table_dir}")
    return str(table_dir / "*.parquet")


def _read_parquet_expr(table: str) -> str:
    path = _require_parquet(table)
    return f"read_parquet('{path}', union_by_name=true)"


def _create_latest_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    source_expr: str,
    pk_col: str,
    order_col: str,
) -> None:
    # Previous builds may have created either view or table with this name.
    try:
        con.execute(f"DROP VIEW {table_name}")
    except Exception:
        pass
    try:
        con.execute(f"DROP TABLE {table_name}")
    except Exception:
        pass
    con.execute(
        f"""
        CREATE TABLE {table_name} AS
        SELECT * EXCLUDE (_rn)
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY {pk_col}
                       ORDER BY COALESCE({order_col}, TIMESTAMP '1970-01-01') DESC, {pk_col} DESC
                   ) AS _rn
            FROM {source_expr}
        ) t
        WHERE _rn = 1
        """
    )


def _create_distinct_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    source_expr: str,
) -> None:
    try:
        con.execute(f"DROP VIEW {table_name}")
    except Exception:
        pass
    try:
        con.execute(f"DROP TABLE {table_name}")
    except Exception:
        pass
    con.execute(f"CREATE TABLE {table_name} AS SELECT DISTINCT * FROM {source_expr}")


def _build_cpi_index(con: duckdb.DuckDBPyConnection) -> None:
    df = con.execute("SELECT year, inflation_pct FROM macro_indices ORDER BY year").fetchdf()
    if df.empty:
        raise RuntimeError("macro_indices is empty; run etl/load_macro_indices.py")
    years = df["year"].tolist()
    infl = df["inflation_pct"].tolist()
    idx_values = []
    idx = 1.0
    base_year = years[0]
    for year, inf in zip(years, infl):
        if year == base_year:
            idx = 1.0
        else:
            idx *= (1.0 + (inf or 0) / 100.0)
        idx_values.append({"year": year, "cpi_index": idx})
    cpi_df = pd.DataFrame(idx_values)
    con.register("cpi_df", cpi_df)
    con.execute("CREATE OR REPLACE TABLE cpi_index AS SELECT * FROM cpi_df")


def _build_market_price_stats(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE market_price_stats AS
        SELECT
            ci.enstru_code,
            ci.unit_code,
            COALESCE(kr.region_id, pp.kato_delivery::VARCHAR) AS region_id,
            EXTRACT(year FROM c.sign_date) AS year,
            COUNT(*) AS n,
            quantile_cont(ci.unit_price, 0.5) AS median,
            AVG(ci.unit_price) AS mean,
            quantile_cont(ci.unit_price, 0.25) AS p25,
            quantile_cont(ci.unit_price, 0.75) AS p75,
            (quantile_cont(ci.unit_price, 0.75) - quantile_cont(ci.unit_price, 0.25)) AS iqr,
            quantile_cont(ci.unit_price, 0.10) AS p10,
            quantile_cont(ci.unit_price, 0.90) AS p90,
            MIN(ci.unit_price) AS min_price,
            MAX(ci.unit_price) AS max_price
        FROM contract_items ci
        JOIN contracts c ON c.id = ci.contract_id
        LEFT JOIN plan_points pp ON pp.id = ci.pln_point_id
        LEFT JOIN kato_ref kr ON kr.code = pp.kato_delivery
        WHERE ci.is_price_valid = TRUE
          AND ci.unit_price IS NOT NULL
          AND ci.enstru_code IS NOT NULL
        GROUP BY 1,2,3,4
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE market_price_stats_national AS
        SELECT
            enstru_code,
            unit_code,
            year,
            COUNT(*) AS n,
            quantile_cont(unit_price, 0.5) AS median,
            AVG(unit_price) AS mean,
            quantile_cont(unit_price, 0.25) AS p25,
            quantile_cont(unit_price, 0.75) AS p75,
            (quantile_cont(unit_price, 0.75) - quantile_cont(unit_price, 0.25)) AS iqr,
            quantile_cont(unit_price, 0.10) AS p10,
            quantile_cont(unit_price, 0.90) AS p90,
            MIN(unit_price) AS min_price,
            MAX(unit_price) AS max_price
        FROM (
            SELECT
                ci.enstru_code,
                ci.unit_code,
                EXTRACT(year FROM c.sign_date) AS year,
                ci.unit_price
            FROM contract_items ci
            JOIN contracts c ON c.id = ci.contract_id
            WHERE ci.is_price_valid = TRUE
              AND ci.unit_price IS NOT NULL
              AND ci.enstru_code IS NOT NULL
        ) t
        GROUP BY 1,2,3
        """
    )


def _confidence(n: int) -> str:
    if n >= 50:
        return "HIGH"
    if n >= MIN_N:
        return "MEDIUM"
    return "LOW"


def _build_fair_price_eval(con: duckdb.DuckDBPyConnection) -> None:
    stats = con.execute("SELECT * FROM market_price_stats").fetchdf()
    stats_nat = con.execute("SELECT * FROM market_price_stats_national").fetchdf()
    cpi = con.execute("SELECT year, cpi_index FROM cpi_index").fetchdf()
    if cpi.empty:
        raise RuntimeError("cpi_index is empty")
    cpi_map = {int(r.year): float(r.cpi_index) for r in cpi.itertuples()}

    stats_by_key: dict[tuple, Any] = {}
    for r in stats.itertuples():
        stats_by_key[(r.enstru_code, r.unit_code, r.region_id, int(r.year))] = r

    stats_nat_key: dict[tuple, Any] = {}
    for r in stats_nat.itertuples():
        stats_nat_key[(r.enstru_code, r.unit_code, int(r.year))] = r

    items = con.execute(
        """
        SELECT
            ci.id AS contract_item_id,
            ci.contract_id,
            ci.enstru_code,
            ci.unit_code,
            ci.unit_price AS actual_unit_price,
            ci.quantity,
            ci.total_price,
            ci.pln_point_id,
            c.customer_bin,
            EXTRACT(year FROM c.sign_date) AS item_year,
            COALESCE(kr.region_id, pp.kato_delivery::VARCHAR) AS region_id
        FROM contract_items ci
        JOIN contracts c ON c.id = ci.contract_id
        LEFT JOIN plan_points pp ON pp.id = ci.pln_point_id
        LEFT JOIN kato_ref kr ON kr.code = pp.kato_delivery
        WHERE ci.is_price_valid = TRUE
          AND ci.unit_price IS NOT NULL
          AND ci.enstru_code IS NOT NULL
        """
    ).fetchdf()

    rows = []
    for row in items.itertuples():
        key = (row.enstru_code, row.unit_code, row.region_id, int(row.item_year))
        ref = stats_by_key.get(key)
        fallback_level = 1

        # Fallbacks
        if not ref or ref.n < MIN_N:
            ref = stats_nat_key.get((row.enstru_code, row.unit_code, int(row.item_year)))
            fallback_level = 2
        if (not ref or ref.n < MIN_N) and row.region_id is not None:
            ref = stats_by_key.get((row.enstru_code, row.unit_code, row.region_id, int(row.item_year) - 1)) or \
                  stats_by_key.get((row.enstru_code, row.unit_code, row.region_id, int(row.item_year) + 1))
            fallback_level = 3
        if not ref or ref.n < MIN_N:
            ref = stats_nat_key.get((row.enstru_code, row.unit_code, int(row.item_year) - 1)) or \
                  stats_nat_key.get((row.enstru_code, row.unit_code, int(row.item_year) + 1))
            fallback_level = 4

        if not ref or ref.n < MIN_N:
            rows.append({
                "contract_item_id": row.contract_item_id,
                "contract_id": row.contract_id,
                "customer_bin": row.customer_bin,
                "enstru_code": row.enstru_code,
                "unit_code": row.unit_code,
                "region_id": row.region_id,
                "item_year": int(row.item_year),
                "actual_unit_price": float(row.actual_unit_price),
                "ref_year": None,
                "ref_region_id": None,
                "ref_n": int(ref.n) if ref else 0,
                "ref_median": None,
                "k_time": None,
                "expected_unit_price": None,
                "deviation_pct": None,
                "verdict": "insufficient_data",
                "confidence": "LOW",
                "fallback_level": fallback_level,
            })
            continue

        ref_year = int(getattr(ref, "year"))
        ref_region_id = getattr(ref, "region_id", None)
        cpi_item = cpi_map.get(int(row.item_year), 1.0)
        cpi_ref = cpi_map.get(ref_year, 1.0)
        k_time = cpi_item / cpi_ref if cpi_ref else 1.0
        expected = float(ref.median) * k_time
        deviation = ((float(row.actual_unit_price) - expected) / expected) * 100.0 if expected else None

        verdict = "anomaly" if deviation is not None and deviation > 30.0 else "ok"
        rows.append({
            "contract_item_id": row.contract_item_id,
            "contract_id": row.contract_id,
            "customer_bin": row.customer_bin,
            "enstru_code": row.enstru_code,
            "unit_code": row.unit_code,
            "region_id": row.region_id,
            "item_year": int(row.item_year),
            "actual_unit_price": float(row.actual_unit_price),
            "ref_year": ref_year,
            "ref_region_id": ref_region_id,
            "ref_n": int(ref.n),
            "ref_median": float(ref.median),
            "k_time": float(k_time),
            "expected_unit_price": float(expected),
            "deviation_pct": float(deviation) if deviation is not None else None,
            "verdict": verdict,
            "confidence": _confidence(int(ref.n)),
            "fallback_level": fallback_level,
        })

    df_eval = pd.DataFrame(rows)
    con.register("df_eval", df_eval)
    con.execute("CREATE OR REPLACE TABLE lot_fair_price_eval AS SELECT * FROM df_eval")


def _build_anomalies(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE lot_anomalies AS
        SELECT *
        FROM lot_fair_price_eval
        WHERE verdict = 'anomaly'
          AND deviation_pct IS NOT NULL
          AND deviation_pct > 30
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE volume_anomalies AS
        WITH qty AS (
            SELECT
                customer_bin,
                enstru_code,
                EXTRACT(year FROM date_approved) AS year,
                SUM(quantity) AS total_qty
            FROM plan_points
            WHERE quantity IS NOT NULL
              AND enstru_code IS NOT NULL
            GROUP BY 1,2,3
        ),
        yoy AS (
            SELECT
                a.*,
                b.total_qty AS prev_qty,
                CASE
                    WHEN b.total_qty IS NOT NULL AND b.total_qty > 0
                    THEN a.total_qty / b.total_qty
                    ELSE NULL
                END AS ratio
            FROM qty a
            LEFT JOIN qty b
              ON a.customer_bin = b.customer_bin
             AND a.enstru_code = b.enstru_code
             AND a.year = b.year + 1
        )
        SELECT * FROM yoy
        WHERE ratio IS NOT NULL AND ratio >= 1.5
        """
    )


def _build_coverage(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE coverage_stats AS
        SELECT
            (SELECT COUNT(*) FROM plan_points) AS plan_points,
            (SELECT COUNT(*) FROM announcements) AS announcements,
            (SELECT COUNT(*) FROM lots) AS lots,
            (SELECT COUNT(*) FROM contracts) AS contracts,
            (SELECT COUNT(*) FROM contract_items) AS contract_items,
            (SELECT COUNT(*) FROM lot_plan_points) AS lot_plan_points,
            (SELECT COUNT(*) FROM contract_acts) AS contract_acts
        """
    )


def build_marts() -> None:
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DB_PATH)

    # Base deduplicated tables from incremental parquet history.
    _create_latest_table(con, "subjects", _read_parquet_expr("subjects"), "bin", "updated_at")
    _create_latest_table(con, "plan_points", _read_parquet_expr("plan_points"), "id", "synced_at")
    _create_latest_table(con, "announcements", _read_parquet_expr("announcements"), "id", "synced_at")
    _create_latest_table(con, "lots", _read_parquet_expr("lots"), "id", "synced_at")
    try:
        _create_distinct_table(con, "lot_plan_points", _read_parquet_expr("lot_plan_points"))
    except Exception:
        con.execute("CREATE OR REPLACE TABLE lot_plan_points AS SELECT NULL::BIGINT AS lot_id, NULL::BIGINT AS plan_point_id LIMIT 0")
    _create_latest_table(con, "contracts", _read_parquet_expr("contracts"), "id", "synced_at")
    _create_latest_table(con, "contract_items", _read_parquet_expr("contract_items"), "id", "synced_at")
    try:
        _create_latest_table(con, "contract_acts", _read_parquet_expr("contract_acts"), "id", "synced_at")
    except Exception:
        con.execute("CREATE OR REPLACE TABLE contract_acts AS SELECT NULL::BIGINT AS id, NULL::BIGINT AS contract_id LIMIT 0")
    _create_latest_table(con, "macro_indices", _read_parquet_expr("macro_indices"), "year", "updated_at")

    # optional refs
    try:
        _create_latest_table(con, "kato_ref", _read_parquet_expr("kato_ref"), "code", "created_at")
    except Exception:
        con.execute("CREATE OR REPLACE TABLE kato_ref AS SELECT NULL::VARCHAR AS code, NULL::VARCHAR AS region_id LIMIT 0")
    try:
        _create_latest_table(con, "enstru_ref", _read_parquet_expr("enstru_ref"), "code", "created_at")
    except Exception:
        con.execute("CREATE OR REPLACE TABLE enstru_ref AS SELECT NULL::VARCHAR AS code LIMIT 0")

    _build_cpi_index(con)
    _build_market_price_stats(con)
    _build_fair_price_eval(con)
    _build_anomalies(con)
    _build_coverage(con)

    con.close()


if __name__ == "__main__":
    build_marts()
