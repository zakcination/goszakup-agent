"""
scripts/verify_spiral3.py
-------------------------
Spiral 3 verification checks for analytics layer.
"""

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


async def main():
    import asyncpg
    import duckdb

    def ok(msg): print(f"  OK  {msg}")
    def fail(msg): print(f"  FAIL {msg}")

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("DATABASE_URL not set")
        sys.exit(1)
    if not Path("/.dockerenv").exists():
        db_url = db_url.replace("@postgres:", "@localhost:")

    conn = await asyncpg.connect(db_url, timeout=10)

    n_macro = await conn.fetchval("SELECT COUNT(*) FROM macro_indices")
    if n_macro >= 3:
        ok(f"macro_indices: {n_macro}")
    else:
        fail(f"macro_indices: {n_macro}")

    n_items = await conn.fetchval("SELECT COUNT(*) FROM contract_items")
    if n_items > 0:
        ok(f"contract_items: {n_items}")
    else:
        fail(f"contract_items: {n_items}")

    await conn.close()

    db_path = os.environ.get("ANALYTICS_DB_PATH", "data/analytics.duckdb")
    if not Path(db_path).exists():
        fail(f"Analytics DB not found: {db_path}")
        return

    con = duckdb.connect(db_path, read_only=True)
    for table in ("market_price_stats", "lot_fair_price_eval", "lot_anomalies"):
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if n > 0:
                ok(f"{table}: {n}")
            else:
                fail(f"{table}: {n}")
        except Exception as e:
            fail(f"{table}: {e}")
    con.close()


if __name__ == "__main__":
    asyncio.run(main())
