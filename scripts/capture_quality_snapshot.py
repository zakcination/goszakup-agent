"""
scripts/capture_quality_snapshot.py
-----------------------------------
Capture key data-quality metrics into quality_snapshots.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


def _maybe_reexec_in_venv() -> None:
    if os.environ.get("VIRTUAL_ENV"):
        return
    venv_python = Path(__file__).resolve().parent.parent / ".venv" / "bin" / "python"
    if not venv_python.exists():
        return
    if Path(sys.executable).resolve() == venv_python.resolve():
        return
    os.execv(str(venv_python), [str(venv_python), *sys.argv])


def _load_env() -> None:
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())


_maybe_reexec_in_venv()
_load_env()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", type=str, default="", help="Custom run identifier")
    p.add_argument("--output", type=str, default="", help="Optional JSON output path")
    return p.parse_args()


async def main() -> None:
    import asyncpg

    args = parse_args()
    run_id = args.run_id or datetime.now(timezone.utc).strftime("quality_%Y%m%d_%H%M%S")

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("DATABASE_URL not set")
        sys.exit(1)
    if not Path("/.dockerenv").exists():
        db_url = db_url.replace("@postgres:", "@localhost:")

    conn = await asyncpg.connect(db_url, timeout=10)
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quality_snapshots (
            id BIGSERIAL PRIMARY KEY,
            run_id VARCHAR(80) NOT NULL,
            metric_name VARCHAR(120) NOT NULL,
            metric_value NUMERIC(20,6) NOT NULL,
            captured_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_quality_run ON quality_snapshots(run_id)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_quality_metric ON quality_snapshots(metric_name, captured_at DESC)")

    async def has_table(table: str) -> bool:
        return bool(
            await conn.fetchval(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema='public' AND table_name=$1
                """,
                table,
            )
        )

    async def has_column(table: str, column: str) -> bool:
        return bool(
            await conn.fetchval(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=$1 AND column_name=$2
                """,
                table,
                column,
            )
        )

    async def count(sql: str, *params) -> float:
        v = await conn.fetchval(sql, *params)
        return float(v or 0)

    metrics: dict[str, float] = {}
    metrics["lots_total"] = await count("SELECT COUNT(*) FROM lots")
    metrics["lots_with_announcement"] = await count("SELECT COUNT(*) FROM lots WHERE announcement_id IS NOT NULL")
    metrics["lots_with_source_trd_buy_id"] = (
        await count("SELECT COUNT(*) FROM lots WHERE source_trd_buy_id IS NOT NULL")
        if await has_column("lots", "source_trd_buy_id")
        else 0.0
    )
    metrics["lots_with_enstru"] = await count("SELECT COUNT(*) FROM lots WHERE enstru_code IS NOT NULL")
    metrics["lots_with_unit"] = await count("SELECT COUNT(*) FROM lots WHERE unit_code IS NOT NULL")
    metrics["lots_with_kato"] = await count("SELECT COUNT(*) FROM lots WHERE kato_delivery IS NOT NULL")
    metrics["lots_linked_to_plan_points"] = (
        await count("SELECT COUNT(DISTINCT lot_id) FROM lot_plan_points")
        if await has_table("lot_plan_points")
        else 0.0
    )

    metrics["contracts_total"] = await count("SELECT COUNT(*) FROM contracts")
    metrics["contracts_with_announcement"] = await count("SELECT COUNT(*) FROM contracts WHERE announcement_id IS NOT NULL")
    metrics["contracts_with_source_trd_buy_id"] = (
        await count("SELECT COUNT(*) FROM contracts WHERE source_trd_buy_id IS NOT NULL")
        if await has_column("contracts", "source_trd_buy_id")
        else 0.0
    )
    metrics["contracts_with_lot_id"] = (
        await count("SELECT COUNT(*) FROM contracts WHERE lot_id IS NOT NULL")
        if await has_column("contracts", "lot_id")
        else 0.0
    )
    metrics["contracts_with_trade_method_id"] = (
        await count("SELECT COUNT(*) FROM contracts WHERE trade_method_id IS NOT NULL")
        if await has_column("contracts", "trade_method_id")
        else 0.0
    )

    metrics["contract_items_total"] = await count("SELECT COUNT(*) FROM contract_items")
    metrics["contract_items_with_pln_point_id"] = await count(
        "SELECT COUNT(*) FROM contract_items WHERE pln_point_id IS NOT NULL"
    )
    metrics["contract_items_with_enstru_code"] = await count(
        "SELECT COUNT(*) FROM contract_items WHERE enstru_code IS NOT NULL"
    )
    metrics["contract_items_with_unit_code"] = await count(
        "SELECT COUNT(*) FROM contract_items WHERE unit_code IS NOT NULL"
    )
    metrics["contract_items_price_valid"] = (
        await count("SELECT COUNT(*) FROM contract_items WHERE is_price_valid IS TRUE")
        if await has_column("contract_items", "is_price_valid")
        else 0.0
    )

    metrics["plan_points_total"] = await count("SELECT COUNT(*) FROM plan_points")
    metrics["plan_points_with_kato"] = await count("SELECT COUNT(*) FROM plan_points WHERE kato_delivery IS NOT NULL")
    metrics["plan_points_with_enstru"] = (
        await count("SELECT COUNT(*) FROM plan_points WHERE enstru_code IS NOT NULL")
        if await has_column("plan_points", "enstru_code")
        else 0.0
    )
    metrics["lot_plan_points_rows"] = await count("SELECT COUNT(*) FROM lot_plan_points")

    metrics["enstru_ref_rows"] = await count("SELECT COUNT(*) FROM enstru_ref")
    metrics["enstru_ref_with_section"] = await count("SELECT COUNT(*) FROM enstru_ref WHERE section IS NOT NULL")
    metrics["enstru_ref_with_division"] = await count("SELECT COUNT(*) FROM enstru_ref WHERE division IS NOT NULL")
    metrics["enstru_ref_with_group_code"] = await count("SELECT COUNT(*) FROM enstru_ref WHERE group_code IS NOT NULL")

    metrics["raw_plans_kato_rows"] = await count("SELECT COUNT(*) FROM raw_plans_kato") if await has_table("raw_plans_kato") else 0.0
    metrics["raw_plans_spec_rows"] = await count("SELECT COUNT(*) FROM raw_plans_spec") if await has_table("raw_plans_spec") else 0.0
    metrics["raw_acts_rows"] = await count("SELECT COUNT(*) FROM raw_acts") if await has_table("raw_acts") else 0.0
    metrics["raw_treasury_pay_rows"] = await count("SELECT COUNT(*) FROM raw_treasury_pay") if await has_table("raw_treasury_pay") else 0.0
    metrics["raw_trd_buy_events_rows"] = await count("SELECT COUNT(*) FROM raw_trd_buy_events") if await has_table("raw_trd_buy_events") else 0.0

    def ratio(num_key: str, den_key: str, out_key: str) -> None:
        den = metrics.get(den_key, 0.0)
        num = metrics.get(num_key, 0.0)
        metrics[out_key] = (num / den * 100.0) if den > 0 else 0.0

    ratio("lots_with_announcement", "lots_total", "lots_with_announcement_pct")
    ratio("lots_with_source_trd_buy_id", "lots_total", "lots_with_source_trd_buy_id_pct")
    ratio("lots_with_enstru", "lots_total", "lots_with_enstru_pct")
    ratio("lots_with_unit", "lots_total", "lots_with_unit_pct")
    ratio("lots_with_kato", "lots_total", "lots_with_kato_pct")
    ratio("lots_linked_to_plan_points", "lots_total", "lots_linked_to_plan_points_pct")
    ratio("contracts_with_announcement", "contracts_total", "contracts_with_announcement_pct")
    ratio("contracts_with_source_trd_buy_id", "contracts_total", "contracts_with_source_trd_buy_id_pct")
    ratio("contracts_with_lot_id", "contracts_total", "contracts_with_lot_id_pct")
    ratio("contracts_with_trade_method_id", "contracts_total", "contracts_with_trade_method_id_pct")
    ratio("contract_items_with_pln_point_id", "contract_items_total", "contract_items_with_pln_point_id_pct")
    ratio("contract_items_with_enstru_code", "contract_items_total", "contract_items_with_enstru_code_pct")
    ratio("contract_items_with_unit_code", "contract_items_total", "contract_items_with_unit_code_pct")
    ratio("contract_items_price_valid", "contract_items_total", "contract_items_price_valid_pct")
    ratio("plan_points_with_kato", "plan_points_total", "plan_points_with_kato_pct")
    ratio("plan_points_with_enstru", "plan_points_total", "plan_points_with_enstru_pct")
    ratio("enstru_ref_with_section", "enstru_ref_rows", "enstru_ref_with_section_pct")
    ratio("enstru_ref_with_division", "enstru_ref_rows", "enstru_ref_with_division_pct")
    ratio("enstru_ref_with_group_code", "enstru_ref_rows", "enstru_ref_with_group_code_pct")

    inserts = [(run_id, k, v) for k, v in metrics.items()]
    await conn.executemany(
        """
        INSERT INTO quality_snapshots (run_id, metric_name, metric_value, captured_at)
        VALUES ($1, $2, $3, NOW())
        """,
        inserts,
    )
    await conn.close()

    out_path = args.output.strip()
    if not out_path:
        out_dir = Path(__file__).resolve().parent.parent / "artifacts"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = str(out_dir / f"{run_id}.json")
    with Path(out_path).open("w", encoding="utf-8") as f:
        json.dump({"run_id": run_id, "metrics": metrics}, f, ensure_ascii=False, indent=2)

    print(f"run_id={run_id}")
    print(out_path)


if __name__ == "__main__":
    asyncio.run(main())
