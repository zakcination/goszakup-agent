"""
scripts/profile_database_snapshot.py
------------------------------------
Create a reproducible DB profiling snapshot with:
- table sizes and exact row counts
- column metadata
- null/non-null stats by column
- pandas-like describe for numeric/date columns
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


NUMERIC_UDT = {
    "int2",
    "int4",
    "int8",
    "float4",
    "float8",
    "numeric",
    "decimal",
    "money",
}
DATETIME_UDT = {"date", "timestamp", "timestamptz", "time", "timetz"}


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


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _human_size(num_bytes: int | None) -> str:
    if num_bytes is None:
        return "0 B"
    size = float(num_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size >= 1024 and i < len(units) - 1:
        size /= 1024.0
        i += 1
    return f"{size:.2f} {units[i]}"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_summary_md(
    out_dir: Path,
    table_rows: list[dict[str, Any]],
    null_rows: list[dict[str, Any]],
    numeric_rows: list[dict[str, Any]],
    datetime_rows: list[dict[str, Any]],
) -> str:
    total_rows = sum(int(r["row_count"]) for r in table_rows)
    top_tables = sorted(table_rows, key=lambda r: int(r["total_bytes"]), reverse=True)[:15]
    high_nulls = [
        r
        for r in sorted(null_rows, key=lambda x: float(x["null_pct"]), reverse=True)
        if int(r["row_count"]) > 0
    ][:30]

    lines: list[str] = []
    lines.append("# Database Snapshot Summary")
    lines.append("")
    lines.append(f"- Generated at: `{datetime.now(timezone.utc).isoformat()}`")
    lines.append(f"- Output dir: `{out_dir}`")
    lines.append(f"- Tables: `{len(table_rows)}`")
    lines.append(f"- Total rows across all tables: `{total_rows}`")
    lines.append(f"- Numeric describe rows: `{len(numeric_rows)}`")
    lines.append(f"- Datetime describe rows: `{len(datetime_rows)}`")
    lines.append("")
    lines.append("## Largest Tables (by total bytes)")
    lines.append("")
    lines.append("| table | rows | total size | table size | indexes size |")
    lines.append("|---|---:|---:|---:|---:|")
    for r in top_tables:
        lines.append(
            "| {table_name} | {row_count} | {total_human} | {table_human} | {index_human} |".format(
                **r
            )
        )
    lines.append("")
    lines.append("## Highest Null Ratios (column-level)")
    lines.append("")
    lines.append("| table | column | row_count | null_count | null_pct |")
    lines.append("|---|---|---:|---:|---:|")
    for r in high_nulls:
        lines.append(
            f"| {r['table_name']} | {r['column_name']} | {r['row_count']} | {r['null_count']} | {float(r['null_pct']):.2f}% |"
        )
    lines.append("")
    lines.append("## Files")
    lines.append("")
    lines.append("- `table_overview.csv`")
    lines.append("- `columns.csv`")
    lines.append("- `null_profile.csv`")
    lines.append("- `numeric_describe.csv`")
    lines.append("- `datetime_describe.csv`")
    lines.append("- `snapshot.json`")
    return "\n".join(lines) + "\n"


async def _profile_database(output_dir: Path) -> dict[str, Any]:
    import asyncpg

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    if not Path("/.dockerenv").exists():
        db_url = db_url.replace("@postgres:", "@localhost:")

    conn = await asyncpg.connect(db_url, timeout=30)
    try:
        table_rows_raw = await conn.fetch(
            """
            SELECT
                c.relname AS table_name,
                COALESCE(c.reltuples, 0)::BIGINT AS est_rows,
                pg_total_relation_size(c.oid) AS total_bytes,
                pg_relation_size(c.oid) AS table_bytes,
                pg_indexes_size(c.oid) AS indexes_bytes
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relkind = 'r'
            ORDER BY pg_total_relation_size(c.oid) DESC, c.relname
            """
        )
        table_names = [r["table_name"] for r in table_rows_raw]

        row_count_map: dict[str, int] = {}
        for table in table_names:
            q_table = _quote_ident(table)
            row_count_map[table] = int(await conn.fetchval(f"SELECT COUNT(*) FROM {q_table}"))

        table_rows: list[dict[str, Any]] = []
        for r in table_rows_raw:
            table = r["table_name"]
            total_bytes = int(r["total_bytes"] or 0)
            table_bytes = int(r["table_bytes"] or 0)
            idx_bytes = int(r["indexes_bytes"] or 0)
            table_rows.append(
                {
                    "table_name": table,
                    "row_count": row_count_map.get(table, 0),
                    "est_rows": int(r["est_rows"] or 0),
                    "total_bytes": total_bytes,
                    "table_bytes": table_bytes,
                    "indexes_bytes": idx_bytes,
                    "total_human": _human_size(total_bytes),
                    "table_human": _human_size(table_bytes),
                    "index_human": _human_size(idx_bytes),
                }
            )

        cols_raw = await conn.fetch(
            """
            SELECT
                table_name,
                column_name,
                ordinal_position,
                data_type,
                udt_name,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
            """
        )
        columns: list[dict[str, Any]] = [
            {
                "table_name": r["table_name"],
                "column_name": r["column_name"],
                "ordinal_position": int(r["ordinal_position"]),
                "data_type": r["data_type"],
                "udt_name": r["udt_name"],
                "is_nullable": r["is_nullable"],
                "column_default": r["column_default"] or "",
            }
            for r in cols_raw
        ]
        cols_by_table: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for c in columns:
            cols_by_table[c["table_name"]].append(c)

        null_profile: list[dict[str, Any]] = []
        numeric_describe: list[dict[str, Any]] = []
        datetime_describe: list[dict[str, Any]] = []

        for table in table_names:
            t_cols = cols_by_table.get(table, [])
            total_rows = row_count_map.get(table, 0)
            if not t_cols:
                continue
            q_table = _quote_ident(table)

            if total_rows == 0:
                for c in t_cols:
                    null_profile.append(
                        {
                            "table_name": table,
                            "column_name": c["column_name"],
                            "data_type": c["data_type"],
                            "row_count": 0,
                            "null_count": 0,
                            "non_null_count": 0,
                            "null_pct": 0.0,
                        }
                    )
            else:
                agg_parts: list[str] = []
                for i, c in enumerate(t_cols):
                    q_col = _quote_ident(c["column_name"])
                    agg_parts.append(
                        f"SUM(CASE WHEN {q_col} IS NULL THEN 1 ELSE 0 END) AS n_{i}"
                    )
                    agg_parts.append(f"COUNT({q_col}) AS nn_{i}")
                sql = f"SELECT {', '.join(agg_parts)} FROM {q_table}"
                r = await conn.fetchrow(sql)
                for i, c in enumerate(t_cols):
                    null_count = int(r[f"n_{i}"] or 0)
                    non_null_count = int(r[f"nn_{i}"] or 0)
                    null_pct = (null_count / total_rows * 100.0) if total_rows else 0.0
                    null_profile.append(
                        {
                            "table_name": table,
                            "column_name": c["column_name"],
                            "data_type": c["data_type"],
                            "row_count": total_rows,
                            "null_count": null_count,
                            "non_null_count": non_null_count,
                            "null_pct": round(null_pct, 4),
                        }
                    )

            for c in t_cols:
                col = c["column_name"]
                q_col = _quote_ident(col)
                udt_name = (c["udt_name"] or "").lower()
                if udt_name in NUMERIC_UDT:
                    drow = await conn.fetchrow(
                        f"""
                        SELECT
                            COUNT({q_col}) AS count_non_null,
                            AVG(({q_col})::DOUBLE PRECISION) AS mean,
                            STDDEV_POP(({q_col})::DOUBLE PRECISION) AS std,
                            MIN({q_col})::TEXT AS min,
                            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ({q_col})::DOUBLE PRECISION) AS p25,
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ({q_col})::DOUBLE PRECISION) AS p50,
                            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ({q_col})::DOUBLE PRECISION) AS p75,
                            MAX({q_col})::TEXT AS max
                        FROM {q_table}
                        WHERE {q_col} IS NOT NULL
                        """
                    )
                    numeric_describe.append(
                        {
                            "table_name": table,
                            "column_name": col,
                            "count_non_null": int(drow["count_non_null"] or 0),
                            "mean": float(drow["mean"]) if drow["mean"] is not None else None,
                            "std": float(drow["std"]) if drow["std"] is not None else None,
                            "min": drow["min"],
                            "p25": float(drow["p25"]) if drow["p25"] is not None else None,
                            "p50": float(drow["p50"]) if drow["p50"] is not None else None,
                            "p75": float(drow["p75"]) if drow["p75"] is not None else None,
                            "max": drow["max"],
                        }
                    )
                elif udt_name in DATETIME_UDT:
                    drow = await conn.fetchrow(
                        f"""
                        SELECT
                            COUNT({q_col}) AS count_non_null,
                            MIN({q_col})::TEXT AS min,
                            MAX({q_col})::TEXT AS max
                        FROM {q_table}
                        WHERE {q_col} IS NOT NULL
                        """
                    )
                    datetime_describe.append(
                        {
                            "table_name": table,
                            "column_name": col,
                            "count_non_null": int(drow["count_non_null"] or 0),
                            "min": drow["min"],
                            "max": drow["max"],
                        }
                    )

        _write_csv(output_dir / "table_overview.csv", table_rows)
        _write_csv(output_dir / "columns.csv", columns)
        _write_csv(output_dir / "null_profile.csv", null_profile)
        _write_csv(output_dir / "numeric_describe.csv", numeric_describe)
        _write_csv(output_dir / "datetime_describe.csv", datetime_describe)

        snapshot_payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "table_overview": table_rows,
            "columns_count": len(columns),
            "null_profile_count": len(null_profile),
            "numeric_describe_count": len(numeric_describe),
            "datetime_describe_count": len(datetime_describe),
        }
        _write_json(output_dir / "snapshot.json", snapshot_payload)

        summary_md = _build_summary_md(
            out_dir=output_dir,
            table_rows=table_rows,
            null_rows=null_profile,
            numeric_rows=numeric_describe,
            datetime_rows=datetime_describe,
        )
        (output_dir / "SUMMARY.md").write_text(summary_md, encoding="utf-8")

        return snapshot_payload
    finally:
        await conn.close()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create full Postgres profiling snapshot for analytics planning."
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="Optional output directory. Default: artifacts/db_snapshot_<UTC timestamp>",
    )
    args = parser.parse_args()

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    if args.output_dir:
        out_dir = Path(args.output_dir)
    else:
        out_dir = Path("artifacts") / f"db_snapshot_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"▶ Profiling database into {out_dir}")
    payload = await _profile_database(out_dir)
    print(
        "✅ Done: tables={tables}, columns={cols}, null_stats={nulls}, numeric={num}, datetime={dt}".format(
            tables=len(payload["table_overview"]),
            cols=payload["columns_count"],
            nulls=payload["null_profile_count"],
            num=payload["numeric_describe_count"],
            dt=payload["datetime_describe_count"],
        )
    )
    print(f"📄 Summary: {out_dir / 'SUMMARY.md'}")


if __name__ == "__main__":
    _maybe_reexec_in_venv()
    _load_env()
    asyncio.run(main())
