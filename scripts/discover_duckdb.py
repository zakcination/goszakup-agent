"""
scripts/discover_duckdb.py
--------------------------
Quick, reviewer-friendly exploration of the DuckDB analytics database:
- table list + row counts
- schemas for key marts
- sample rows for market stats / fair price eval / anomalies

Read-only by design. Intended for demos and debugging.
"""

from __future__ import annotations

import argparse
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


def _section(title: str) -> None:
    print("")
    print("=" * 78)
    print(title)
    print("=" * 78)


def _json_lines(rows: list[dict[str, Any]], limit: int = 5) -> None:
    for r in rows[:limit]:
        print(json.dumps(r, ensure_ascii=False, default=str))


def _has_table(con, table_name: str) -> bool:
    try:
        row = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE lower(table_name)=lower(?)",
            [table_name],
        ).fetchone()
        return bool(row and int(row[0]) > 0)
    except Exception:
        return False


def main() -> None:
    try:
        import duckdb
    except Exception as e:
        print(f"duckdb not available in current Python environment: {e}")
        print("Tip: run from Docker to avoid local dependency issues:")
        print("  docker compose run --rm analytics_api python scripts/discover_duckdb.py")
        sys.exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=int, default=5, help="Sample rows per table (default: 5)")
    args = parser.parse_args()

    db_path = os.environ.get("ANALYTICS_DB_PATH", "data/analytics.duckdb")
    db = Path(db_path)
    if not db.exists():
        print(f"Analytics DB not found: {db}")
        sys.exit(1)

    _section("DuckDB: Connection Info")
    print(f"timestamp_utc: {datetime.now(timezone.utc).isoformat()}")
    print(f"path: {db}")
    print(f"size_bytes: {db.stat().st_size}")

    con = duckdb.connect(str(db), read_only=True)
    try:
        _section("DuckDB: Tables")
        tables = con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_name
            """
        ).fetchall()
        table_names = [str(t[0]) for t in tables]
        for t in table_names:
            try:
                n = int(con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])
            except Exception:
                n = -1
            print(f"{t:<28} rows={n}")

        _section("DuckDB: Key Marts (schema + samples)")
        key = [
            "market_price_stats",
            "market_price_stats_national",
            "lot_fair_price_eval",
            "lot_anomalies",
            "volume_anomalies",
            "coverage_stats",
        ]
        for t in key:
            if not _has_table(con, t):
                continue
            print("")
            print(f"[{t}]")
            try:
                cols = con.execute(f"PRAGMA table_info('{t}')").fetchall()
                col_names = [str(r[1]) for r in cols]
                print("columns:", ", ".join(col_names))
            except Exception as e:
                print(f"schema error: {e}")
                continue

            sample_sql = f"SELECT * FROM {t} LIMIT {int(args.samples)}"
            if t == "market_price_stats":
                sample_sql = f"SELECT * FROM {t} ORDER BY n DESC, year DESC LIMIT {int(args.samples)}"
            elif t == "lot_anomalies":
                sample_sql = f"SELECT * FROM {t} ORDER BY ABS(deviation_pct) DESC LIMIT {int(args.samples)}"
            elif t == "lot_fair_price_eval":
                sample_sql = f"SELECT * FROM {t} ORDER BY ABS(deviation_pct) DESC NULLS LAST LIMIT {int(args.samples)}"
            elif t == "volume_anomalies":
                sample_sql = f"SELECT * FROM {t} ORDER BY ratio DESC NULLS LAST LIMIT {int(args.samples)}"

            try:
                df = con.execute(sample_sql).fetchdf()
                rows = df.astype(object).where(df.notnull(), None).to_dict(orient="records")
                _json_lines(rows, limit=int(args.samples))
            except Exception as e:
                print(f"sample error: {e}")

    finally:
        con.close()


if __name__ == "__main__":
    _maybe_reexec_in_venv()
    _load_env()
    main()

