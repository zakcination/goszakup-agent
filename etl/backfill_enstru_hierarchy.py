"""
etl/backfill_enstru_hierarchy.py
--------------------------------
Fill ENSTRU hierarchy columns (section/division/group_code) from enstru_ref.code.
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.config import get_config
from etl.utils import load_env, maybe_reexec_in_venv, resolve_db_url

maybe_reexec_in_venv()
load_env()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="Show affected rows without updating")
    return p.parse_args()


SQL_COUNT_MISSING = """
SELECT COUNT(*)
FROM enstru_ref
WHERE section IS NULL OR division IS NULL OR group_code IS NULL
"""


SQL_PREVIEW = """
SELECT
    code,
    section,
    division,
    group_code,
    SUBSTRING(REGEXP_REPLACE(code, '[^0-9]', '', 'g') FROM 1 FOR 2) AS new_section,
    SUBSTRING(REGEXP_REPLACE(code, '[^0-9]', '', 'g') FROM 1 FOR 4) AS new_division,
    SUBSTRING(REGEXP_REPLACE(code, '[^0-9]', '', 'g') FROM 1 FOR 6) AS new_group_code
FROM enstru_ref
WHERE section IS NULL OR division IS NULL OR group_code IS NULL
ORDER BY code
LIMIT 20
"""


SQL_UPDATE = """
WITH norm AS (
    SELECT
        code,
        REGEXP_REPLACE(code, '[^0-9]', '', 'g') AS digits
    FROM enstru_ref
)
UPDATE enstru_ref AS e
SET
    section = CASE WHEN LENGTH(n.digits) >= 2 THEN SUBSTRING(n.digits FROM 1 FOR 2) ELSE section END,
    division = CASE WHEN LENGTH(n.digits) >= 4 THEN SUBSTRING(n.digits FROM 1 FOR 4) ELSE division END,
    group_code = CASE WHEN LENGTH(n.digits) >= 6 THEN SUBSTRING(n.digits FROM 1 FOR 6) ELSE group_code END
FROM norm AS n
WHERE e.code = n.code
  AND (e.section IS NULL OR e.division IS NULL OR e.group_code IS NULL)
"""


async def main() -> None:
    import asyncpg

    args = parse_args()
    cfg = get_config()
    db_url = resolve_db_url(cfg.db_url)

    conn = await asyncpg.connect(db_url, timeout=20)
    try:
        missing_before = await conn.fetchval(SQL_COUNT_MISSING)
        print(f"Missing hierarchy rows before: {missing_before}")

        if missing_before and args.dry_run:
            rows = await conn.fetch(SQL_PREVIEW)
            print("Preview (first 20):")
            for r in rows:
                print(
                    f"{r['code']} | old=({r['section']},{r['division']},{r['group_code']}) "
                    f"-> new=({r['new_section']},{r['new_division']},{r['new_group_code']})"
                )
            return

        if not args.dry_run:
            await conn.execute(SQL_UPDATE)

        missing_after = await conn.fetchval(SQL_COUNT_MISSING)
        updated = (missing_before or 0) - (missing_after or 0)
        print(f"Updated rows: {updated}")
        print(f"Missing hierarchy rows after: {missing_after}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
