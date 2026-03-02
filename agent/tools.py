"""
agent/tools.py
--------------
Whitelist tools with fixed SQL templates and validated input schemas.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import duckdb
import pandas as pd

from agent.schemas import (
    AnomalyScanParams,
    AuditTrailParams,
    CoverageGapsParams,
    FairPriceParams,
    IntentType,
    OrgSummaryParams,
    PlanVsFactParams,
    SupplierCheckParams,
    ToolResult,
    TopKParams,
)

DB_PATH = os.environ.get("ANALYTICS_DB_PATH", "data/analytics.duckdb")
MIN_N = int(os.environ.get("FAIR_PRICE_MIN_N", "20"))


def _connect() -> duckdb.DuckDBPyConnection:
    db = Path(DB_PATH)
    if not db.exists():
        raise FileNotFoundError(f"Analytics DB not found: {db}")
    return duckdb.connect(str(db), read_only=True)


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    clean = df.replace([float("inf"), float("-inf")], pd.NA)
    clean = clean.astype(object).where(pd.notnull(clean), None)
    return clean.to_dict(orient="records")


def _confidence_from_n(n: int | None) -> str:
    if not n:
        return "LOW"
    if n >= 50:
        return "HIGH"
    if n >= MIN_N:
        return "MEDIUM"
    return "LOW"


def _freshness_ts(con: duckdb.DuckDBPyConnection) -> datetime | None:
    q = """
    SELECT MAX(ts) AS freshness
    FROM (
      SELECT MAX(synced_at) AS ts FROM contracts
      UNION ALL
      SELECT MAX(synced_at) AS ts FROM contract_items
      UNION ALL
      SELECT MAX(synced_at) AS ts FROM lots
      UNION ALL
      SELECT MAX(synced_at) AS ts FROM plan_points
      UNION ALL
      SELECT MAX(synced_at) AS ts FROM announcements
    ) t
    """
    try:
        row = con.execute(q).fetchone()
        if row and row[0] is not None:
            return pd.to_datetime(row[0]).to_pydatetime()
    except Exception:
        return None
    return None


def _has_table(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    try:
        row = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE lower(table_name) = lower(?)",
            [table_name],
        ).fetchone()
        return bool(row and int(row[0]) > 0)
    except Exception:
        return False


def _table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        return {str(r[1]).lower() for r in rows}
    except Exception:
        return set()


def _level_filters(region_id: str | None, year_from: int, year_to: int) -> list[tuple[str, str]]:
    plus_minus = (year_from - 1, year_to + 1)
    base_year = f"EXTRACT(year FROM c.sign_date) BETWEEN {year_from} AND {year_to}"
    pm_year = f"EXTRACT(year FROM c.sign_date) BETWEEN {plus_minus[0]} AND {plus_minus[1]}"
    region_pred = "COALESCE(kr.region_id, pp.kato_delivery::VARCHAR) = ?"
    return [
        ("region_period", f"{base_year} AND {region_pred}" if region_id else base_year),
        ("national_period", base_year),
        ("region_pm1", f"{pm_year} AND {region_pred}" if region_id else pm_year),
        ("national_pm1", pm_year),
    ]


def get_fair_price(params: FairPriceParams) -> ToolResult:
    con = _connect()
    freshness = _freshness_ts(con)
    try:
        chosen_scope = "national_period"
        chosen_stats: dict[str, Any] | None = None
        region_value = params.region_id
        enstru_code = params.enstru_code
        comp_year_from = params.period_from
        comp_year_to = params.period_to
        comp_region: str | None = params.region_id
        resolved_from: dict[str, Any] = {}

        if not enstru_code:
            ref_id = params.lot_id or params.trd_buy_id
            if ref_id is not None:
                ref_df = con.execute(
                    """
                    SELECT
                      id AS lot_id,
                      announcement_id,
                      source_trd_buy_id,
                      enstru_code,
                      CAST(kato_delivery AS VARCHAR) AS kato
                    FROM lots
                    WHERE (id = ? OR announcement_id = ? OR source_trd_buy_id = ?)
                      AND enstru_code IS NOT NULL
                    ORDER BY
                      CASE
                        WHEN id = ? THEN 0
                        WHEN source_trd_buy_id = ? THEN 1
                        WHEN announcement_id = ? THEN 2
                        ELSE 3
                      END
                    LIMIT 1
                    """,
                    [ref_id, ref_id, ref_id, ref_id, ref_id, ref_id],
                ).fetchdf()
                if not ref_df.empty:
                    ref = _records(ref_df)[0]
                    enstru_code = ref.get("enstru_code")
                    resolved_from = {
                        "reference_id": ref_id,
                        "lot_id": ref.get("lot_id"),
                        "announcement_id": ref.get("announcement_id"),
                        "source_trd_buy_id": ref.get("source_trd_buy_id"),
                    }
                    if region_value is None and ref.get("kato"):
                        region_value = str(ref.get("kato"))
                elif _has_table(con, "contracts") and _has_table(con, "contract_items"):
                    ci_ref_df = con.execute(
                        """
                        SELECT
                          ci.enstru_code,
                          MAX(CAST(pp.kato_delivery AS VARCHAR)) AS kato,
                          MAX(CAST(c.announcement_id AS BIGINT)) AS announcement_id,
                          MAX(CAST(c.source_trd_buy_id AS BIGINT)) AS source_trd_buy_id,
                          COUNT(*) AS freq
                        FROM contract_items ci
                        JOIN contracts c ON c.id = ci.contract_id
                        LEFT JOIN plan_points pp ON pp.id = ci.pln_point_id
                        WHERE ci.enstru_code IS NOT NULL
                          AND (c.source_trd_buy_id = ? OR c.announcement_id = ?)
                        GROUP BY ci.enstru_code
                        ORDER BY freq DESC
                        LIMIT 1
                        """,
                        [ref_id, ref_id],
                    ).fetchdf()
                    if not ci_ref_df.empty:
                        ref = _records(ci_ref_df)[0]
                        enstru_code = ref.get("enstru_code")
                        resolved_from = {
                            "reference_id": ref_id,
                            "announcement_id": ref.get("announcement_id"),
                            "source_trd_buy_id": ref.get("source_trd_buy_id"),
                            "strategy": "contracts_contract_items",
                        }
                        if region_value is None and ref.get("kato"):
                            region_value = str(ref.get("kato"))

        if not enstru_code:
            return ToolResult(
                verdict="insufficient_data",
                confidence="LOW",
                method="Median + IQR",
                freshness_ts=freshness,
                scope_level=chosen_scope,
                data={
                    "reason": "Не удалось определить ЕНСТРУ по входным параметрам.",
                    "recommendation": "Укажите enstru_code или корректный lot/trd_buy reference.",
                    "period_from": params.period_from,
                    "period_to": params.period_to,
                    "enstru_code": None,
                    "region_id": region_value,
                    "resolved_from": resolved_from,
                },
                limitations=["missing_enstru_and_unresolved_reference"],
            )

        for scope, where_expr in _level_filters(region_value, params.period_from, params.period_to):
            sql = f"""
            SELECT
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
              AND ci.enstru_code = ?
              AND {where_expr}
            """
            sql_params: list[Any] = [enstru_code]
            if "?" in where_expr and region_value is not None:
                sql_params.append(region_value)
            row = con.execute(sql, sql_params).fetchone()
            if not row:
                continue
            n = int(row[0] or 0)
            chosen_stats = {
                "n": n,
                "median": float(row[1]) if row[1] is not None else None,
                "mean": float(row[2]) if row[2] is not None else None,
                "p25": float(row[3]) if row[3] is not None else None,
                "p75": float(row[4]) if row[4] is not None else None,
                "iqr": float(row[5]) if row[5] is not None else None,
                "p10": float(row[6]) if row[6] is not None else None,
                "p90": float(row[7]) if row[7] is not None else None,
                "min_price": float(row[8]) if row[8] is not None else None,
                "max_price": float(row[9]) if row[9] is not None else None,
            }
            chosen_scope = scope
            if scope.endswith("_pm1"):
                comp_year_from = params.period_from - 1
                comp_year_to = params.period_to + 1
            else:
                comp_year_from = params.period_from
                comp_year_to = params.period_to
            comp_region = region_value if scope.startswith("region_") else None
            if n >= MIN_N:
                break

        if not chosen_stats or int(chosen_stats.get("n") or 0) < MIN_N:
            n = int((chosen_stats or {}).get("n") or 0)
            return ToolResult(
                verdict="insufficient_data",
                confidence="LOW",
                n=n,
                method="Median + IQR",
                freshness_ts=freshness,
                scope_level=chosen_scope,
                data={
                    "reason": f"N={n} < {MIN_N}",
                    "recommendation": "Расширьте период или регион сравнения.",
                    "period_from": params.period_from,
                    "period_to": params.period_to,
                    "enstru_code": enstru_code,
                    "region_id": region_value,
                    "resolved_from": resolved_from,
                },
                limitations=["insufficient_sample_size"],
            )

        median = float(chosen_stats["median"])
        comp_sql = """
        SELECT
          ci.contract_id,
          ci.id AS contract_item_id,
          c.customer_bin,
          c.supplier_bin,
          c.sign_date,
          ci.unit_price,
          ci.quantity,
          ci.total_price,
          CAST(c.announcement_id AS BIGINT) AS announcement_id,
          CAST(c.source_trd_buy_id AS BIGINT) AS source_trd_buy_id,
          ('https://ows.goszakup.gov.kz/v3/contract/' || CAST(ci.contract_id AS VARCHAR)) AS contract_api_url,
          CASE
            WHEN c.announcement_id IS NOT NULL AND c.announcement_id > 0
            THEN ('https://goszakup.gov.kz/ru/announce/index/' || CAST(CAST(c.announcement_id AS BIGINT) AS VARCHAR))
            ELSE NULL
          END AS portal_announce_url
        FROM contract_items ci
        JOIN contracts c ON c.id = ci.contract_id
        LEFT JOIN plan_points pp ON pp.id = ci.pln_point_id
        LEFT JOIN kato_ref kr ON kr.code = pp.kato_delivery
        WHERE ci.is_price_valid = TRUE
          AND ci.unit_price IS NOT NULL
          AND ci.enstru_code = ?
          AND EXTRACT(year FROM c.sign_date) BETWEEN ? AND ?
          AND (? IS NULL OR COALESCE(kr.region_id, pp.kato_delivery::VARCHAR) = ?)
        ORDER BY ABS(ci.unit_price - ?) ASC, c.sign_date DESC
        LIMIT ?
        """
        comp_df = con.execute(
            comp_sql,
            [
                enstru_code,
                comp_year_from,
                comp_year_to,
                comp_region,
                comp_region,
                median,
                params.top_k,
            ],
        ).fetchdf()

        n = int(chosen_stats["n"])
        return ToolResult(
            verdict="ok",
            confidence=_confidence_from_n(n),
            n=n,
            method="Median + IQR with fallback scopes",
            freshness_ts=freshness,
            scope_level=chosen_scope,
            data={
                "fair_price": median,
                "stats": chosen_stats,
                "period_from": params.period_from,
                "period_to": params.period_to,
                "enstru_code": enstru_code,
                "region_id": region_value,
                "resolved_from": resolved_from,
            },
            evidence={"comparables_n": len(comp_df)},
            top_k=_records(comp_df),
        )
    finally:
        con.close()


def get_org_summary(params: OrgSummaryParams) -> ToolResult:
    con = _connect()
    freshness = _freshness_ts(con)
    try:
        planned_sum = con.execute(
            """
            SELECT COALESCE(SUM(total_amount), 0)
            FROM plan_points
            WHERE customer_bin = ?
              AND fin_year BETWEEN ? AND ?
            """,
            [params.customer_bin, params.period_from, params.period_to],
        ).fetchone()[0]

        contracted_sum = con.execute(
            """
            SELECT COALESCE(SUM(contract_sum), 0)
            FROM contracts
            WHERE customer_bin = ?
              AND EXTRACT(year FROM sign_date) BETWEEN ? AND ?
            """,
            [params.customer_bin, params.period_from, params.period_to],
        ).fetchone()[0]

        top_enstru_df = con.execute(
            """
            SELECT
              ci.enstru_code,
              COUNT(*) AS contracts_count,
              COALESCE(SUM(ci.total_price), 0) AS total_sum
            FROM contract_items ci
            JOIN contracts c ON c.id = ci.contract_id
            WHERE c.customer_bin = ?
              AND EXTRACT(year FROM c.sign_date) BETWEEN ? AND ?
              AND ci.enstru_code IS NOT NULL
            GROUP BY 1
            ORDER BY total_sum DESC
            LIMIT 5
            """,
            [params.customer_bin, params.period_from, params.period_to],
        ).fetchdf()

        planned = float(planned_sum or 0.0)
        contracted = float(contracted_sum or 0.0)
        exec_rate = (contracted / planned) if planned > 0 else None

        verdict = "ok" if planned > 0 else "insufficient_data"
        confidence = "HIGH" if planned > 0 and contracted > 0 else "MEDIUM"
        return ToolResult(
            verdict=verdict,
            confidence=confidence,
            method="Plan vs Contract aggregate",
            freshness_ts=freshness,
            data={
                "customer_bin": params.customer_bin,
                "period_from": params.period_from,
                "period_to": params.period_to,
                "total_planned": planned,
                "total_contracted": contracted,
                "execution_rate": exec_rate,
            },
            evidence={"top_5_enstru": _records(top_enstru_df)},
            top_k=_records(top_enstru_df),
            limitations=[] if planned > 0 else ["no_plans_in_period"],
        )
    finally:
        con.close()


def get_anomalies(params: AnomalyScanParams) -> ToolResult:
    con = _connect()
    freshness = _freshness_ts(con)
    try:
        conf_order = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}
        min_rank = conf_order.get(params.confidence_min, 1)
        sql = """
        SELECT
          la.contract_item_id,
          la.contract_id,
          la.customer_bin,
          cust.name_ru AS customer_name_ru,
          c.supplier_bin,
          sup.name_ru AS supplier_name_ru,
          sup.is_rnu AS supplier_is_rnu,
          la.enstru_code,
          er.name_ru AS enstru_name_ru,
          la.unit_code,
          la.region_id,
          la.item_year,
          la.actual_unit_price,
          la.expected_unit_price,
          la.ref_n,
          la.ref_median,
          la.k_time,
          la.deviation_pct,
          la.verdict,
          la.confidence,
          la.fallback_level,
          c.contract_number,
          c.contract_sum,
          c.sign_date,
          CAST(c.announcement_id AS BIGINT) AS announcement_id,
          a.number_anno,
          a.publish_date,
          CAST(c.source_trd_buy_id AS BIGINT) AS source_trd_buy_id,
          ci.pln_point_id,
          ci.quantity AS item_quantity,
          ci.total_price AS item_total_price,
          pp.kato_delivery,
          kr.region_id AS kato_region_id,
          COALESCE(kr.name_ru, pp.delivery_address_ru) AS delivery_place_name,
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
        LEFT JOIN contract_items ci ON ci.id = la.contract_item_id
        LEFT JOIN plan_points pp ON pp.id = ci.pln_point_id
        LEFT JOIN kato_ref kr ON kr.code = pp.kato_delivery
        LEFT JOIN subjects cust ON cust.bin = la.customer_bin
        LEFT JOIN subjects sup ON sup.bin = c.supplier_bin
        LEFT JOIN enstru_ref er ON er.code = la.enstru_code
        LEFT JOIN announcements a ON a.id = c.announcement_id
        WHERE la.item_year BETWEEN ? AND ?
          AND la.deviation_pct >= ?
          AND (? IS NULL OR la.customer_bin = ?)
          AND (
            CASE la.confidence WHEN 'HIGH' THEN 3 WHEN 'MEDIUM' THEN 2 ELSE 1 END
          ) >= ?
        ORDER BY la.deviation_pct DESC
        LIMIT ?
        """
        df = con.execute(
            sql,
            [
                params.period_from,
                params.period_to,
                params.threshold_pct,
                params.customer_bin,
                params.customer_bin,
                min_rank,
                params.limit,
            ],
        ).fetchdf()
        rows = _records(df)
        if not rows:
            return ToolResult(
                verdict="ok",
                confidence="MEDIUM",
                method="Rule anomaly scan (> threshold)",
                freshness_ts=freshness,
                data={
                    "count": 0,
                    "period_from": params.period_from,
                    "period_to": params.period_to,
                    "threshold_pct": params.threshold_pct,
                    "customer_bin": params.customer_bin,
                },
                top_k=[],
            )
        max_dev = max(float(r.get("deviation_pct") or 0.0) for r in rows)
        avg_dev = sum(float(r.get("deviation_pct") or 0.0) for r in rows) / len(rows)
        unique_customers = len({r.get("customer_bin") for r in rows if r.get("customer_bin")})
        unique_suppliers = len({r.get("supplier_bin") for r in rows if r.get("supplier_bin")})
        rnu_hits = sum(1 for r in rows if r.get("supplier_is_rnu") is True)
        return ToolResult(
            verdict="anomaly",
            confidence="HIGH" if len(rows) >= 10 else "MEDIUM",
            method="Rule anomaly scan (> threshold)",
            freshness_ts=freshness,
            n=len(rows),
            data={
                "count": len(rows),
                "max_deviation_pct": max_dev,
                "avg_deviation_pct": avg_dev,
                "unique_customers": unique_customers,
                "unique_suppliers": unique_suppliers,
                "rnu_supplier_hits": rnu_hits,
                "period_from": params.period_from,
                "period_to": params.period_to,
                "threshold_pct": params.threshold_pct,
                "customer_bin": params.customer_bin,
            },
            top_k=rows,
        )
    finally:
        con.close()


def get_supplier_profile(params: SupplierCheckParams) -> ToolResult:
    con = _connect()
    freshness = _freshness_ts(con)
    try:
        bins = params.customer_bins or []
        customer_filter = ""
        sql_params: list[Any] = [params.enstru_code, params.period_from, params.period_to]
        if bins:
            placeholders = ", ".join(["?"] * len(bins))
            customer_filter = f" AND c.customer_bin IN ({placeholders})"
            sql_params.extend(bins)
        sql = f"""
        SELECT
          c.supplier_bin,
          COUNT(DISTINCT c.id) AS contracts_count,
          COALESCE(SUM(COALESCE(ci.total_price, c.contract_sum, 0)), 0) AS total_sum,
          AVG(ci.unit_price) AS avg_unit_price,
          MAX(CASE WHEN s.is_rnu THEN 1 ELSE 0 END) AS rnu_flag
        FROM contracts c
        JOIN contract_items ci ON ci.contract_id = c.id
        LEFT JOIN subjects s ON s.bin = c.supplier_bin
        WHERE ci.enstru_code = ?
          AND EXTRACT(year FROM c.sign_date) BETWEEN ? AND ?
          {customer_filter}
        GROUP BY 1
        ORDER BY total_sum DESC
        LIMIT ?
        """
        sql_params.append(params.limit)
        df = con.execute(sql, sql_params).fetchdf()
        rows = _records(df)
        if not rows:
            return ToolResult(
                verdict="insufficient_data",
                confidence="LOW",
                method="Supplier profile aggregate",
                freshness_ts=freshness,
                limitations=["no_supplier_data_for_filters"],
                data={
                    "enstru_code": params.enstru_code,
                    "period_from": params.period_from,
                    "period_to": params.period_to,
                    "customer_bins": params.customer_bins,
                },
            )

        total = sum(float(r.get("total_sum") or 0.0) for r in rows)
        hhi = 0.0
        for r in rows:
            share = (float(r.get("total_sum") or 0.0) / total) if total > 0 else 0.0
            r["share_pct"] = share * 100.0
            hhi += share * share

        return ToolResult(
            verdict="ok",
            confidence="HIGH" if len(rows) >= 5 else "MEDIUM",
            method="Supplier concentration (HHI)",
            freshness_ts=freshness,
            n=len(rows),
            data={
                "enstru_code": params.enstru_code,
                "period_from": params.period_from,
                "period_to": params.period_to,
                "supplier_count": len(rows),
                "concentration_score": hhi,
            },
            top_k=rows,
        )
    finally:
        con.close()


def get_audit_trail(params: AuditTrailParams) -> ToolResult:
    con = _connect()
    freshness = _freshness_ts(con)
    try:
        if params.contract_id is not None:
            c_sql = "SELECT * FROM contracts WHERE id = ? LIMIT 1"
            c_params = [params.contract_id]
        else:
            c_sql = "SELECT * FROM contracts WHERE contract_number = ? LIMIT 1"
            c_params = [params.contract_number]

        c_df = con.execute(c_sql, c_params).fetchdf()
        if c_df.empty:
            return ToolResult(
                verdict="insufficient_data",
                confidence="LOW",
                freshness_ts=freshness,
                method="Audit trail chain lookup",
                limitations=["contract_not_found"],
            )

        contract = _records(c_df)[0]
        contract_id = int(contract["id"])
        items_df = con.execute(
            """
            SELECT id AS contract_item_id, pln_point_id, enstru_code, unit_code, total_price
            FROM contract_items
            WHERE contract_id = ?
            ORDER BY id
            """,
            [contract_id],
        ).fetchdf()
        item_rows = _records(items_df)
        plan_point_ids = sorted({int(r["pln_point_id"]) for r in item_rows if r.get("pln_point_id") is not None})

        if plan_point_ids:
            pp_placeholders = ", ".join(["?"] * len(plan_point_ids))
            pp_df = con.execute(
                f"""
                SELECT id AS plan_id, customer_bin, fin_year, enstru_code, total_amount, date_approved
                FROM plan_points
                WHERE id IN ({pp_placeholders})
                ORDER BY id
                """,
                plan_point_ids,
            ).fetchdf()
        else:
            pp_df = pd.DataFrame()
        plan_rows = _records(pp_df)

        has_lpp = _has_table(con, "lot_plan_points")
        if plan_point_ids and has_lpp:
            lpp_placeholders = ", ".join(["?"] * len(plan_point_ids))
            lot_df = con.execute(
                f"""
                SELECT DISTINCT l.id AS lot_id, l.announcement_id, l.source_trd_buy_id, l.customer_bin, l.enstru_code
                FROM lots l
                LEFT JOIN lot_plan_points lpp ON lpp.lot_id = l.id
                WHERE l.id = ?
                   OR lpp.plan_point_id IN ({lpp_placeholders})
                ORDER BY lot_id
                """,
                [contract.get("lot_id"), *plan_point_ids],
            ).fetchdf()
        elif plan_point_ids:
            lot_df = con.execute(
                """
                SELECT DISTINCT l.id AS lot_id, l.announcement_id, l.source_trd_buy_id, l.customer_bin, l.enstru_code
                FROM lots l
                WHERE l.id = ?
                   OR l.id IN (
                     SELECT lot_id FROM contracts WHERE id = ?
                   )
                ORDER BY lot_id
                """,
                [contract.get("lot_id"), contract_id],
            ).fetchdf()
        else:
            lot_df = con.execute(
                """
                SELECT DISTINCT l.id AS lot_id, l.announcement_id, l.source_trd_buy_id, l.customer_bin, l.enstru_code
                FROM lots l
                WHERE l.id = ?
                ORDER BY lot_id
                """,
                [contract.get("lot_id")],
            ).fetchdf()
        lot_rows = _records(lot_df)

        lot_ids = [int(l["lot_id"]) for l in lot_rows] if lot_rows else []
        if lot_ids:
            lot_placeholders = ", ".join(["?"] * len(lot_ids))
            ann_df = con.execute(
                f"""
                SELECT DISTINCT a.id AS announcement_id, a.number_anno, a.publish_date, a.customer_bin
                FROM announcements a
                WHERE a.id = ?
                   OR a.id IN (
                     SELECT announcement_id FROM lots WHERE id IN ({lot_placeholders})
                   )
                ORDER BY announcement_id
                """,
                [contract.get("announcement_id"), *lot_ids],
            ).fetchdf()
        else:
            ann_df = con.execute(
                """
                SELECT DISTINCT a.id AS announcement_id, a.number_anno, a.publish_date, a.customer_bin
                FROM announcements a
                WHERE a.id = ?
                ORDER BY announcement_id
                """,
                [contract.get("announcement_id")],
            ).fetchdf()
        ann_rows = _records(ann_df)

        act_cols = _table_columns(con, "contract_acts")
        if {"approve_date", "revoke_date", "total_sum", "is_revoked"}.issubset(act_cols):
            acts_df = con.execute(
                """
                SELECT id AS act_id, contract_id, approve_date, revoke_date, total_sum, is_revoked
                FROM contract_acts
                WHERE contract_id = ?
                ORDER BY act_id
                """,
                [contract_id],
            ).fetchdf()
        else:
            acts_df = con.execute(
                """
                SELECT id AS act_id, contract_id
                FROM contract_acts
                WHERE contract_id = ?
                ORDER BY act_id
                """,
                [contract_id],
            ).fetchdf()
        act_rows = _records(acts_df)

        completeness = {
            "has_plan": bool(plan_rows),
            "has_announcement": bool(ann_rows),
            "has_lot": bool(lot_rows),
            "has_contract": True,
            "has_acts": bool(act_rows),
        }
        coverage = sum(1 for v in completeness.values() if v) / len(completeness)

        return ToolResult(
            verdict="ok",
            confidence="HIGH" if coverage >= 0.8 else "MEDIUM",
            method="Audit trail chain lookup",
            freshness_ts=freshness,
            data={
                "contract": contract,
                "completeness": completeness,
                "coverage_ratio": coverage,
            },
            evidence={
                "plan_ids": [r["plan_id"] for r in plan_rows],
                "announcement_ids": [r["announcement_id"] for r in ann_rows],
                "lot_ids": [r["lot_id"] for r in lot_rows],
                "act_ids": [r["act_id"] for r in act_rows],
            },
            top_k=[
                {
                    "plan_points": plan_rows[:5],
                    "announcements": ann_rows[:5],
                    "lots": lot_rows[:5],
                    "acts": act_rows[:5],
                }
            ],
        )
    finally:
        con.close()


def get_plan_execution(params: PlanVsFactParams) -> ToolResult:
    con = _connect()
    freshness = _freshness_ts(con)
    try:
        planned = con.execute(
            """
            SELECT COALESCE(SUM(total_amount), 0)
            FROM plan_points
            WHERE customer_bin = ? AND fin_year = ?
            """,
            [params.customer_bin, params.fin_year],
        ).fetchone()[0]
        contracted = con.execute(
            """
            SELECT COALESCE(SUM(contract_sum), 0)
            FROM contracts
            WHERE customer_bin = ? AND EXTRACT(year FROM sign_date) = ?
            """,
            [params.customer_bin, params.fin_year],
        ).fetchone()[0]
        df = con.execute(
            """
            WITH plan AS (
              SELECT enstru_code, COALESCE(SUM(total_amount), 0) AS planned_sum
              FROM plan_points
              WHERE customer_bin = ? AND fin_year = ? AND enstru_code IS NOT NULL
              GROUP BY 1
            ),
            fact AS (
              SELECT ci.enstru_code, COALESCE(SUM(ci.total_price), 0) AS contracted_sum
              FROM contract_items ci
              JOIN contracts c ON c.id = ci.contract_id
              WHERE c.customer_bin = ? AND EXTRACT(year FROM c.sign_date) = ? AND ci.enstru_code IS NOT NULL
              GROUP BY 1
            )
            SELECT
              COALESCE(plan.enstru_code, fact.enstru_code) AS enstru_code,
              COALESCE(plan.planned_sum, 0) AS planned_sum,
              COALESCE(fact.contracted_sum, 0) AS contracted_sum,
              COALESCE(fact.contracted_sum, 0) - COALESCE(plan.planned_sum, 0) AS gap
            FROM plan
            FULL OUTER JOIN fact ON fact.enstru_code = plan.enstru_code
            ORDER BY ABS(COALESCE(fact.contracted_sum, 0) - COALESCE(plan.planned_sum, 0)) DESC
            LIMIT 20
            """,
            [params.customer_bin, params.fin_year, params.customer_bin, params.fin_year],
        ).fetchdf()
        planned_val = float(planned or 0.0)
        contracted_val = float(contracted or 0.0)
        gap = contracted_val - planned_val
        exec_rate = (contracted_val / planned_val) if planned_val > 0 else None
        verdict = "ok" if planned_val > 0 else "insufficient_data"
        return ToolResult(
            verdict=verdict,
            confidence="HIGH" if planned_val > 0 else "LOW",
            method="Plan vs fact by customer/year",
            freshness_ts=freshness,
            data={
                "customer_bin": params.customer_bin,
                "fin_year": params.fin_year,
                "planned_sum": planned_val,
                "contracted_sum": contracted_val,
                "gap": gap,
                "execution_rate": exec_rate,
            },
            top_k=_records(df),
            limitations=[] if planned_val > 0 else ["no_plan_amount_for_period"],
        )
    finally:
        con.close()


def get_top_k(params: TopKParams) -> ToolResult:
    con = _connect()
    freshness = _freshness_ts(con)
    try:
        customer_bin = params.filters.get("customer_bin")
        enstru_code = params.filters.get("enstru_code")
        if params.dimension == "expensive_lots":
            sql = """
            SELECT
              l.id AS lot_id,
              l.customer_bin,
              l.enstru_code,
              l.lot_amount,
              l.quantity,
              l.unit_price,
              a.publish_date,
              CAST(l.announcement_id AS BIGINT) AS announcement_id,
              CASE
                WHEN l.announcement_id IS NOT NULL AND l.announcement_id > 0
                THEN ('https://goszakup.gov.kz/ru/announce/index/' || CAST(CAST(l.announcement_id AS BIGINT) AS VARCHAR))
                ELSE NULL
              END AS portal_announce_url
            FROM lots l
            LEFT JOIN announcements a ON a.id = l.announcement_id
            WHERE EXTRACT(year FROM a.publish_date) BETWEEN ? AND ?
              AND (? IS NULL OR l.customer_bin = ?)
              AND (? IS NULL OR l.enstru_code = ?)
            ORDER BY l.lot_amount DESC NULLS LAST
            LIMIT ?
            """
            df = con.execute(
                sql,
                [
                    params.period_from,
                    params.period_to,
                    customer_bin,
                    customer_bin,
                    enstru_code,
                    enstru_code,
                    params.k,
                ],
            ).fetchdf()
        elif params.dimension == "expensive_contracts":
            sql = """
            SELECT
              c.id AS contract_id,
              c.contract_number,
              c.customer_bin,
              c.supplier_bin,
              c.contract_sum,
              c.sign_date,
              ('https://ows.goszakup.gov.kz/v3/contract/' || CAST(c.id AS VARCHAR)) AS contract_api_url
            FROM contracts c
            WHERE EXTRACT(year FROM c.sign_date) BETWEEN ? AND ?
              AND (? IS NULL OR c.customer_bin = ?)
            ORDER BY c.contract_sum DESC NULLS LAST
            LIMIT ?
            """
            df = con.execute(
                sql,
                [params.period_from, params.period_to, customer_bin, customer_bin, params.k],
            ).fetchdf()
        elif params.dimension == "anomalies":
            sql = """
            SELECT
              la.contract_id,
              la.contract_item_id,
              la.customer_bin,
              la.enstru_code,
              la.item_year,
              la.deviation_pct,
              la.actual_unit_price,
              la.expected_unit_price,
              ('https://ows.goszakup.gov.kz/v3/contract/' || CAST(la.contract_id AS VARCHAR)) AS contract_api_url
            FROM lot_anomalies la
            WHERE la.item_year BETWEEN ? AND ?
              AND (? IS NULL OR la.customer_bin = ?)
              AND (? IS NULL OR la.enstru_code = ?)
            ORDER BY la.deviation_pct DESC
            LIMIT ?
            """
            df = con.execute(
                sql,
                [
                    params.period_from,
                    params.period_to,
                    customer_bin,
                    customer_bin,
                    enstru_code,
                    enstru_code,
                    params.k,
                ],
            ).fetchdf()
        elif params.dimension == "volume_anomalies":
            sql = """
            SELECT
              va.customer_bin,
              va.enstru_code,
              va.year,
              va.total_qty,
              va.prev_qty,
              va.ratio,
              CASE
                WHEN va.prev_qty IS NULL OR va.prev_qty = 0 THEN NULL
                ELSE ((va.total_qty - va.prev_qty) / va.prev_qty) * 100
              END AS change_pct
            FROM volume_anomalies va
            WHERE va.year BETWEEN ? AND ?
              AND (? IS NULL OR va.customer_bin = ?)
              AND (? IS NULL OR va.enstru_code = ?)
            ORDER BY va.ratio DESC NULLS LAST
            LIMIT ?
            """
            df = con.execute(
                sql,
                [
                    params.period_from,
                    params.period_to,
                    customer_bin,
                    customer_bin,
                    enstru_code,
                    enstru_code,
                    params.k,
                ],
            ).fetchdf()
        else:
            sql = """
            SELECT
              c.supplier_bin,
              COUNT(DISTINCT c.id) AS contracts_count,
              COALESCE(SUM(c.contract_sum), 0) AS total_sum
            FROM contracts c
            JOIN contract_items ci ON ci.contract_id = c.id
            WHERE EXTRACT(year FROM c.sign_date) BETWEEN ? AND ?
              AND (? IS NULL OR c.customer_bin = ?)
              AND (? IS NULL OR ci.enstru_code = ?)
            GROUP BY 1
            ORDER BY total_sum DESC
            LIMIT ?
            """
            df = con.execute(
                sql,
                [
                    params.period_from,
                    params.period_to,
                    customer_bin,
                    customer_bin,
                    enstru_code,
                    enstru_code,
                    params.k,
                ],
            ).fetchdf()

        rows = _records(df)
        verdict = "ok" if rows else "insufficient_data"
        return ToolResult(
            verdict=verdict,
            confidence="HIGH" if len(rows) >= min(10, params.k) else "MEDIUM",
            method=f"Top-K: {params.dimension}",
            freshness_ts=freshness,
            n=len(rows),
            data={
                "dimension": params.dimension,
                "period_from": params.period_from,
                "period_to": params.period_to,
                "k": params.k,
                "filters": params.filters,
            },
            top_k=rows,
        )
    finally:
        con.close()


def get_uncontracted_plans(params: CoverageGapsParams) -> ToolResult:
    con = _connect()
    freshness = _freshness_ts(con)
    try:
        has_lpp = _has_table(con, "lot_plan_points")
        if has_lpp:
            sql = """
            SELECT
              pp.id AS plan_point_id,
              pp.customer_bin,
              pp.fin_year,
              pp.enstru_code,
              pp.name_ru,
              pp.total_amount,
              pp.quantity,
              pp.kato_delivery,
              ('https://ows.goszakup.gov.kz/v3/plans/view/' || CAST(pp.id AS VARCHAR)) AS plan_point_api_url
            FROM plan_points pp
            LEFT JOIN contract_items ci ON ci.pln_point_id = pp.id
            LEFT JOIN lot_plan_points lpp ON lpp.plan_point_id = pp.id
            LEFT JOIN lots l ON l.id = lpp.lot_id
            LEFT JOIN contracts c ON c.lot_id = l.id
            WHERE pp.customer_bin = ?
              AND pp.fin_year = ?
              AND ci.id IS NULL
              AND c.id IS NULL
            ORDER BY pp.total_amount DESC NULLS LAST, pp.id DESC
            LIMIT ?
            """
        else:
            sql = """
            SELECT
              pp.id AS plan_point_id,
              pp.customer_bin,
              pp.fin_year,
              pp.enstru_code,
              pp.name_ru,
              pp.total_amount,
              pp.quantity,
              pp.kato_delivery,
              ('https://ows.goszakup.gov.kz/v3/plans/view/' || CAST(pp.id AS VARCHAR)) AS plan_point_api_url
            FROM plan_points pp
            LEFT JOIN contract_items ci ON ci.pln_point_id = pp.id
            WHERE pp.customer_bin = ?
              AND pp.fin_year = ?
              AND ci.id IS NULL
            ORDER BY pp.total_amount DESC NULLS LAST, pp.id DESC
            LIMIT ?
            """
        df = con.execute(sql, [params.customer_bin, params.fin_year, params.limit]).fetchdf()
        rows = _records(df)
        if not rows:
            return ToolResult(
                verdict="ok",
                confidence="HIGH",
                method="Coverage gap check",
                freshness_ts=freshness,
                data={"count": 0, "customer_bin": params.customer_bin, "fin_year": params.fin_year},
                top_k=[],
            )
        return ToolResult(
            verdict="anomaly",
            confidence="MEDIUM",
            method="Coverage gap check",
            freshness_ts=freshness,
            n=len(rows),
            data={"count": len(rows), "customer_bin": params.customer_bin, "fin_year": params.fin_year},
            top_k=rows,
        )
    finally:
        con.close()


@dataclass(frozen=True)
class ToolSpec:
    name: str
    param_model: Any
    handler: Callable[[Any], ToolResult]


TOOL_REGISTRY: dict[IntentType, ToolSpec] = {
    IntentType.PRICE_CHECK: ToolSpec("get_fair_price", FairPriceParams, get_fair_price),
    IntentType.ORG_SUMMARY: ToolSpec("get_org_summary", OrgSummaryParams, get_org_summary),
    IntentType.ANOMALY_SCAN: ToolSpec("get_anomalies", AnomalyScanParams, get_anomalies),
    IntentType.SUPPLIER_CHECK: ToolSpec("get_supplier_profile", SupplierCheckParams, get_supplier_profile),
    IntentType.AUDIT_TRAIL: ToolSpec("get_audit_trail", AuditTrailParams, get_audit_trail),
    IntentType.PLAN_VS_FACT: ToolSpec("get_plan_execution", PlanVsFactParams, get_plan_execution),
    IntentType.TOP_K: ToolSpec("get_top_k", TopKParams, get_top_k),
    IntentType.COVERAGE_GAPS: ToolSpec("get_uncontracted_plans", CoverageGapsParams, get_uncontracted_plans),
}


def tools_manifest() -> dict[str, Any]:
    result: dict[str, Any] = {}
    for intent, spec in TOOL_REGISTRY.items():
        result[intent.value] = {
            "tool_name": spec.name,
            "input_schema": spec.param_model.model_json_schema(),
        }
    return result
