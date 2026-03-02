"""
scripts/verify_spiral2.py
-------------------------
Spiral 2 verification checks for counts and FK integrity.
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

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("DATABASE_URL not set")
        sys.exit(1)
    if not Path("/.dockerenv").exists():
        db_url = db_url.replace("@postgres:", "@localhost:")

    conn = await asyncpg.connect(db_url, timeout=10)

    def ok(msg): print(f"  OK  {msg}")
    def fail(msg): print(f"  FAIL {msg}")

    # Check table existence
    lot_plan_exists = await conn.fetchval("SELECT to_regclass('public.lot_plan_points')")
    if lot_plan_exists:
        ok("lot_plan_points table exists")
    else:
        fail("lot_plan_points table missing (run migration)")

    checks = [
        ("plan_points", "SELECT COUNT(*) FROM plan_points", lambda n: n > 0),
        ("announcements", "SELECT COUNT(*) FROM announcements", lambda n: n > 0),
        ("lots", "SELECT COUNT(*) FROM lots", lambda n: n > 0),
        ("lot_plan_points", "SELECT COUNT(*) FROM lot_plan_points", lambda n: n > 0),
        ("contracts", "SELECT COUNT(*) FROM contracts", lambda n: n > 0),
        ("contract_items", "SELECT COUNT(*) FROM contract_items", lambda n: n > 0),
    ]

    for name, sql, predicate in checks:
        try:
            n = await conn.fetchval(sql)
            if predicate(n):
                ok(f"{name}: {n}")
            else:
                fail(f"{name}: {n}")
        except Exception as e:
            fail(f"{name}: {e}")

    fk_checks = [
        (
            "lots.announcement_id -> announcements.id",
            """
            SELECT COUNT(*) FROM lots l
            WHERE l.announcement_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM announcements a WHERE a.id = l.announcement_id)
            """,
        ),
        (
            "contracts.supplier_bin -> subjects.bin",
            """
            SELECT COUNT(*) FROM contracts c
            WHERE c.supplier_bin IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM subjects s WHERE s.bin = c.supplier_bin)
            """,
        ),
        (
            "lot_plan_points.plan_point_id -> plan_points.id",
            """
            SELECT COUNT(*) FROM lot_plan_points lpp
            WHERE NOT EXISTS (SELECT 1 FROM plan_points p WHERE p.id = lpp.plan_point_id)
            """,
        ),
    ]

    for name, sql in fk_checks:
        try:
            n = await conn.fetchval(sql)
            if n == 0:
                ok(f"FK {name}: 0")
            else:
                fail(f"FK {name}: {n}")
        except Exception as e:
            fail(f"FK {name}: {e}")

    dupe_checks = [
        (
            "plan_points id duplicates",
            """
            SELECT COUNT(*) FROM (
                SELECT id, COUNT(*) AS c
                FROM plan_points
                GROUP BY id
                HAVING COUNT(*) > 1
            ) t
            """,
        ),
        (
            "announcements id duplicates",
            """
            SELECT COUNT(*) FROM (
                SELECT id, COUNT(*) AS c
                FROM announcements
                GROUP BY id
                HAVING COUNT(*) > 1
            ) t
            """,
        ),
        (
            "lots id duplicates",
            """
            SELECT COUNT(*) FROM (
                SELECT id, COUNT(*) AS c
                FROM lots
                GROUP BY id
                HAVING COUNT(*) > 1
            ) t
            """,
        ),
        (
            "contracts id duplicates",
            """
            SELECT COUNT(*) FROM (
                SELECT id, COUNT(*) AS c
                FROM contracts
                GROUP BY id
                HAVING COUNT(*) > 1
            ) t
            """,
        ),
        (
            "contract_items id duplicates",
            """
            SELECT COUNT(*) FROM (
                SELECT id, COUNT(*) AS c
                FROM contract_items
                GROUP BY id
                HAVING COUNT(*) > 1
            ) t
            """,
        ),
        (
            "lot_plan_points duplicates",
            """
            SELECT COUNT(*) FROM (
                SELECT lot_id, plan_point_id, COUNT(*) AS c
                FROM lot_plan_points
                GROUP BY lot_id, plan_point_id
                HAVING COUNT(*) > 1
            ) t
            """,
        ),
    ]

    for name, sql in dupe_checks:
        try:
            n = await conn.fetchval(sql)
            if n == 0:
                ok(f"Duplicates {name}: 0")
            else:
                fail(f"Duplicates {name}: {n}")
        except Exception as e:
            fail(f"Duplicates {name}: {e}")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
