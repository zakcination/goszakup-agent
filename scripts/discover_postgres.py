"""
scripts/discover_postgres.py
----------------------------
Quick, reviewer-friendly exploration of the Postgres (source-of-truth) database:
- connection info (masked)
- key table row counts + freshness (max synced/updated)
- completeness for lineage-critical columns
- small samples for core entities

This is intentionally read-only and safe to run before demos.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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


def _resolve_db_url(db_url: str) -> str:
    if not Path("/.dockerenv").exists():
        return db_url.replace("@postgres:", "@localhost:")
    return db_url


def _section(title: str) -> None:
    print("")
    print("=" * 78)
    print(title)
    print("=" * 78)


def _json_lines(rows: list[dict[str, Any]], limit: int = 5) -> None:
    for r in rows[:limit]:
        print(json.dumps(r, ensure_ascii=False, default=str))


async def main() -> None:
    import asyncpg

    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=int, default=5, help="Sample rows per section (default: 5)")
    parser.add_argument("--top-enstru", type=int, default=20, help="Top ENSTRU codes by contract_items count")
    parser.add_argument("--top-enstru-samples", type=int, default=3, help="How many ENSTRU codes to sample")
    args = parser.parse_args()

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("DATABASE_URL not set")
        sys.exit(1)
    db_url = _resolve_db_url(db_url)

    conn = await asyncpg.connect(db_url, timeout=15)
    try:
        _section("PostgreSQL: Connection Info")
        info = await conn.fetchrow(
            """
            SELECT
              current_database() AS db,
              current_user AS user,
              inet_server_addr()::TEXT AS server_addr,
              inet_server_port() AS server_port,
              version() AS version
            """
        )
        if info:
            # Avoid printing full DSN; show only resolved host/port.
            print(f"timestamp_utc: {datetime.now(timezone.utc).isoformat()}")
            print(f"db: {info['db']}")
            print(f"user: {info['user']}")
            print(f"server: {info['server_addr']}:{info['server_port']}")
            print(f"version: {str(info['version']).splitlines()[0]}")

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

        async def table_columns(table: str) -> set[str]:
            rows = await conn.fetch(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=$1
                """,
                table,
            )
            return {str(r["column_name"]).lower() for r in rows}

        async def count_rows(table: str) -> int:
            return int(await conn.fetchval(f'SELECT COUNT(*) FROM "{table}"'))

        async def max_ts(table: str, col: str) -> str | None:
            try:
                ts = await conn.fetchval(f'SELECT MAX("{col}") FROM "{table}"')
                return ts.isoformat() if ts else None
            except Exception:
                return None

        key_tables = [
            "subjects",
            "plan_points",
            "announcements",
            "lots",
            "lot_plan_points",
            "contracts",
            "contract_items",
            "enstru_ref",
            "units_ref",
            "kato_ref",
            "macro_indices",
            "etl_state",
            "etl_runs",
        ]

        _section("Key Tables: Row Counts + Freshness")
        for t in key_tables:
            if not await has_table(t):
                continue
            n = await count_rows(t)
            cols = await table_columns(t)
            freshness = None
            for candidate in ("synced_at", "updated_at", "last_synced_at", "created_at"):
                if candidate in cols:
                    freshness = await max_ts(t, candidate)
                    if freshness:
                        break
            print(f"{t:<24} rows={n:<10} freshness={freshness or '—'}")

        _section("Completeness: Lineage-Critical Columns (null gaps)")
        checks: list[tuple[str, str]] = [
            ("plan_points", "kato_delivery"),
            ("lots", "enstru_code"),
            ("lots", "unit_code"),
            ("lots", "kato_delivery"),
            ("lots", "announcement_id"),
            ("lots", "source_trd_buy_id"),
            ("contracts", "announcement_id"),
            ("contracts", "source_trd_buy_id"),
            ("contracts", "lot_id"),
            ("contract_items", "pln_point_id"),
            ("contract_items", "enstru_code"),
            ("contract_items", "unit_code"),
            ("contract_items", "unit_price"),
            ("contract_items", "quantity"),
        ]
        for table, col in checks:
            if not await has_table(table):
                continue
            cols = await table_columns(table)
            if col.lower() not in cols:
                continue
            total = int(await conn.fetchval(f'SELECT COUNT(*) FROM "{table}"'))
            filled = int(await conn.fetchval(f'SELECT COUNT("{col}") FROM "{table}"'))
            pct = (filled / total * 100.0) if total else 0.0
            print(f"{table}.{col:<18} filled={filled:<10} total={total:<10} pct={pct:6.2f}%")

        _section("Samples: Core Entities")
        sample_specs: dict[str, list[str]] = {
            "subjects": ["bin", "name_ru", "is_customer", "is_supplier", "is_rnu", "updated_at"],
            "plan_points": ["id", "customer_bin", "fin_year", "enstru_code", "unit_code", "kato_delivery", "amount", "synced_at"],
            "announcements": ["id", "customer_bin", "name_ru", "status_id", "last_updated", "synced_at"],
            "lots": ["id", "announcement_id", "source_trd_buy_id", "customer_bin", "enstru_code", "unit_code", "kato_delivery", "name_ru", "price", "amount", "synced_at"],
            "contracts": ["id", "customer_bin", "supplier_bin", "announcement_id", "source_trd_buy_id", "lot_id", "sign_date", "total_sum", "synced_at"],
            "contract_items": ["id", "contract_id", "pln_point_id", "enstru_code", "unit_code", "quantity", "unit_price", "total_price", "is_price_valid", "synced_at"],
            "lot_plan_points": ["lot_id", "plan_point_id"],
        }

        for table, prefer_cols in sample_specs.items():
            if not await has_table(table):
                continue
            cols = await table_columns(table)
            use_cols = [c for c in prefer_cols if c.lower() in cols]
            if not use_cols:
                continue
            order_by = None
            for candidate in ("synced_at", "updated_at", "id"):
                if candidate in cols:
                    order_by = candidate
                    break
            q_cols = ", ".join(f'"{c}"' for c in use_cols)
            q = f'SELECT {q_cols} FROM "{table}"'
            if order_by:
                q += f' ORDER BY "{order_by}" DESC NULLS LAST'
            q += f" LIMIT {int(args.samples)}"
            rows = [dict(r) for r in await conn.fetch(q)]
            print("")
            print(f"[{table}] columns={use_cols}")
            _json_lines(rows, limit=args.samples)

        _section("Discovery: Lots With KATO / ENSTRU (examples)")
        if await has_table("lots"):
            cols = await table_columns("lots")
            base_cols = ["id", "announcement_id", "source_trd_buy_id", "enstru_code", "unit_code", "kato_delivery", "name_ru"]
            use_cols = [c for c in base_cols if c.lower() in cols]
            if use_cols:
                q_cols = ", ".join(f'"{c}"' for c in use_cols)
                q = f"""
                SELECT {q_cols}
                FROM "lots"
                WHERE kato_delivery IS NOT NULL
                ORDER BY synced_at DESC NULLS LAST, id DESC
                LIMIT {int(args.samples)}
                """
                rows = [dict(r) for r in await conn.fetch(q)]
                print("lots with kato_delivery IS NOT NULL:")
                _json_lines(rows, limit=args.samples)

                q2 = f"""
                SELECT {q_cols}
                FROM "lots"
                WHERE enstru_code IS NOT NULL
                ORDER BY synced_at DESC NULLS LAST, id DESC
                LIMIT {int(args.samples)}
                """
                rows2 = [dict(r) for r in await conn.fetch(q2)]
                print("")
                print("lots with enstru_code IS NOT NULL:")
                _json_lines(rows2, limit=args.samples)

        _section("Discovery: Top ENSTRU Codes (contract_items)")
        if await has_table("contract_items"):
            top = await conn.fetch(
                f"""
                SELECT enstru_code, COUNT(*) AS n
                FROM contract_items
                WHERE enstru_code IS NOT NULL
                GROUP BY 1
                ORDER BY n DESC
                LIMIT {int(args.top_enstru)}
                """
            )
            top_rows = [dict(r) for r in top]
            for i, r in enumerate(top_rows, start=1):
                print(f"{i:>2}. {r.get('enstru_code')}  n={r.get('n')}")

            sample_k = min(int(args.top_enstru_samples), len(top_rows))
            if sample_k:
                for r in top_rows[:sample_k]:
                    code = r.get("enstru_code")
                    if not code:
                        continue
                    rows = await conn.fetch(
                        f"""
                        SELECT id, contract_id, pln_point_id, unit_code, quantity, unit_price, total_price
                        FROM contract_items
                        WHERE enstru_code = $1
                        ORDER BY synced_at DESC NULLS LAST, id DESC
                        LIMIT {int(args.samples)}
                        """,
                        code,
                    )
                    print("")
                    print(f"ENSTRU {code} (sample contract_items):")
                    _json_lines([dict(x) for x in rows], limit=args.samples)

    finally:
        await conn.close()


if __name__ == "__main__":
    _maybe_reexec_in_venv()
    _load_env()
    asyncio.run(main())

