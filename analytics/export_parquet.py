"""
analytics/export_parquet.py
---------------------------
Export core tables from Postgres to Parquet (incremental by updated/synced columns).
"""

import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.config import get_config
from etl.utils import load_env, maybe_reexec_in_venv, resolve_db_url

maybe_reexec_in_venv()
load_env()


EXPORT_TABLES = {
    "subjects": "updated_at",
    "plan_points": "synced_at",
    "announcements": "synced_at",
    "lots": "synced_at",
    "contracts": "synced_at",
    "contract_items": "synced_at",
    "contract_acts": "synced_at",
    "lot_plan_points": None,
    "macro_indices": "updated_at",
    "kato_ref": "created_at",
    "enstru_ref": "created_at",
    "units_ref": "created_at",
}


async def _ensure_state_table(conn) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS analytics_export_state (
            table_name      VARCHAR(60) PRIMARY KEY,
            last_exported_at TIMESTAMPTZ,
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )


async def _get_last_export(conn, table: str):
    return await conn.fetchval(
        "SELECT last_exported_at FROM analytics_export_state WHERE table_name=$1",
        table,
    )


async def _set_last_export(conn, table: str, ts):
    await conn.execute(
        """
        INSERT INTO analytics_export_state (table_name, last_exported_at, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (table_name) DO UPDATE SET
            last_exported_at = EXCLUDED.last_exported_at,
            updated_at = NOW()
        """,
        table,
        ts,
    )


async def export_parquet():
    import asyncpg

    cfg = get_config()
    db_url = resolve_db_url(cfg.db_url)
    out_dir = Path(os.environ.get("ANALYTICS_PARQUET_DIR", "data/parquet"))
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = await asyncpg.connect(db_url, timeout=30)
    await _ensure_state_table(conn)

    for table, updated_col in EXPORT_TABLES.items():
        last_exported = await _get_last_export(conn, table)
        query = f"SELECT * FROM {table}"
        params = []
        if updated_col and last_exported:
            query += f" WHERE {updated_col} > $1"
            params.append(last_exported)

        rows = await conn.fetch(query, *params)
        if not rows:
            continue

        df = pd.DataFrame([dict(r) for r in rows])
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        table_dir = out_dir / table
        table_dir.mkdir(parents=True, exist_ok=True)
        file_path = table_dir / f"export_{ts}.parquet"
        pq.write_table(pa.Table.from_pandas(df), file_path)

        max_ts = None
        if updated_col in df.columns:
            try:
                max_ts = pd.to_datetime(df[updated_col]).max().to_pydatetime()
            except Exception:
                max_ts = datetime.now(timezone.utc)
        else:
            max_ts = datetime.now(timezone.utc)
        await _set_last_export(conn, table, max_ts)

    await conn.close()


if __name__ == "__main__":
    asyncio.run(export_parquet())
