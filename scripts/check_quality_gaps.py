"""
scripts/check_quality_gaps.py
-----------------------------
Quick quality profile for lineage and null-gap indicators.
"""

from __future__ import annotations

import asyncio
import os
import sys
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


async def main() -> None:
    import asyncpg

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("DATABASE_URL not set")
        sys.exit(1)
    if not Path("/.dockerenv").exists():
        db_url = db_url.replace("@postgres:", "@localhost:")

    conn = await asyncpg.connect(db_url, timeout=10)

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

    def p(name: str, n: int, d: int | None = None) -> None:
        if d:
            pct = (n / d * 100) if d else 0
            print(f"{name:<40} {n:>10} / {d:<10} ({pct:6.2f}%)")
        else:
            print(f"{name:<40} {n:>10}")

    lots_total = await conn.fetchval("SELECT COUNT(*) FROM lots")
    p("lots_total", lots_total)
    p("lots_with_announcement", await conn.fetchval("SELECT COUNT(*) FROM lots WHERE announcement_id IS NOT NULL"), lots_total)
    if await has_column("lots", "source_trd_buy_id"):
        p("lots_with_source_trd_buy_id", await conn.fetchval("SELECT COUNT(*) FROM lots WHERE source_trd_buy_id IS NOT NULL"), lots_total)
    p("lots_with_enstru", await conn.fetchval("SELECT COUNT(*) FROM lots WHERE enstru_code IS NOT NULL"), lots_total)
    p("lots_with_unit", await conn.fetchval("SELECT COUNT(*) FROM lots WHERE unit_code IS NOT NULL"), lots_total)
    p("lots_with_kato", await conn.fetchval("SELECT COUNT(*) FROM lots WHERE kato_delivery IS NOT NULL"), lots_total)
    if await has_table("lot_plan_points"):
        linked_lots = await conn.fetchval(
            """
            SELECT COUNT(DISTINCT lot_id)
            FROM lot_plan_points
            """
        )
        p("lots_linked_to_plan_points", linked_lots, lots_total)

    contracts_total = await conn.fetchval("SELECT COUNT(*) FROM contracts")
    p("contracts_total", contracts_total)
    p("contracts_with_announcement", await conn.fetchval("SELECT COUNT(*) FROM contracts WHERE announcement_id IS NOT NULL"), contracts_total)
    if await has_column("contracts", "source_trd_buy_id"):
        p("contracts_with_source_trd_buy_id", await conn.fetchval("SELECT COUNT(*) FROM contracts WHERE source_trd_buy_id IS NOT NULL"), contracts_total)
    if await has_column("contracts", "lot_id"):
        p("contracts_with_lot_id", await conn.fetchval("SELECT COUNT(*) FROM contracts WHERE lot_id IS NOT NULL"), contracts_total)
    if await has_column("contracts", "trade_method_id"):
        p(
            "contracts_with_trade_method_id",
            await conn.fetchval("SELECT COUNT(*) FROM contracts WHERE trade_method_id IS NOT NULL"),
            contracts_total,
        )

    items_total = await conn.fetchval("SELECT COUNT(*) FROM contract_items")
    p("contract_items_total", items_total)
    p("contract_items_with_pln_point_id", await conn.fetchval("SELECT COUNT(*) FROM contract_items WHERE pln_point_id IS NOT NULL"), items_total)
    p("contract_items_with_enstru_code", await conn.fetchval("SELECT COUNT(*) FROM contract_items WHERE enstru_code IS NOT NULL"), items_total)
    p("contract_items_with_unit_code", await conn.fetchval("SELECT COUNT(*) FROM contract_items WHERE unit_code IS NOT NULL"), items_total)
    if await has_column("contract_items", "is_price_valid"):
        p(
            "contract_items_price_valid",
            await conn.fetchval("SELECT COUNT(*) FROM contract_items WHERE is_price_valid IS TRUE"),
            items_total,
        )

    plans_total = await conn.fetchval("SELECT COUNT(*) FROM plan_points")
    p("plan_points_total", plans_total)
    p("plan_points_with_kato", await conn.fetchval("SELECT COUNT(*) FROM plan_points WHERE kato_delivery IS NOT NULL"), plans_total)
    if await has_column("plan_points", "enstru_code"):
        p("plan_points_with_enstru", await conn.fetchval("SELECT COUNT(*) FROM plan_points WHERE enstru_code IS NOT NULL"), plans_total)

    p("kato_ref_rows", await conn.fetchval("SELECT COUNT(*) FROM kato_ref"))
    if await has_table("enstru_ref"):
        enstru_total = await conn.fetchval("SELECT COUNT(*) FROM enstru_ref")
        p("enstru_ref_rows", enstru_total)
        if enstru_total:
            p("enstru_ref_with_section", await conn.fetchval("SELECT COUNT(*) FROM enstru_ref WHERE section IS NOT NULL"), enstru_total)
            p("enstru_ref_with_division", await conn.fetchval("SELECT COUNT(*) FROM enstru_ref WHERE division IS NOT NULL"), enstru_total)
            p(
                "enstru_ref_with_group_code",
                await conn.fetchval("SELECT COUNT(*) FROM enstru_ref WHERE group_code IS NOT NULL"),
                enstru_total,
            )
    p("lot_plan_points_rows", await conn.fetchval("SELECT COUNT(*) FROM lot_plan_points"))
    if await has_table("raw_plans_kato"):
        p("raw_plans_kato_rows", await conn.fetchval("SELECT COUNT(*) FROM raw_plans_kato"))
    if await has_table("raw_plans_spec"):
        p("raw_plans_spec_rows", await conn.fetchval("SELECT COUNT(*) FROM raw_plans_spec"))
    if await has_table("raw_acts"):
        p("raw_acts_rows", await conn.fetchval("SELECT COUNT(*) FROM raw_acts"))
    if await has_table("raw_treasury_pay"):
        p("raw_treasury_pay_rows", await conn.fetchval("SELECT COUNT(*) FROM raw_treasury_pay"))
    if await has_table("raw_trd_buy_events"):
        p("raw_trd_buy_events_rows", await conn.fetchval("SELECT COUNT(*) FROM raw_trd_buy_events"))

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
