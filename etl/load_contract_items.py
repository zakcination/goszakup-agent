"""
etl/load_contract_items.py
-------------------------
Spiral 2 — Load contract units into contract_items.
"""

import argparse
import asyncio
import logging
import os
import sys
import time
from pathlib import Path

# Ensure project root is in path
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.client import OWSClient
from etl.config import get_config
from etl.utils import (
    ensure_enstru_ref,
    ensure_unit_ref,
    ensure_etl_state_table,
    get_etl_state,
    load_env,
    maybe_reexec_in_venv,
    normalize_name,
    resolve_db_url,
    safe_int,
    update_etl_state,
)

maybe_reexec_in_venv()
load_env()

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("etl.contract_items")


UPSERT_ITEM_SQL = """
INSERT INTO contract_items (
    id, contract_id, pln_point_id, enstru_code, name_ru, name_clean, unit_code,
    quantity, unit_price, total_price, is_price_valid, synced_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10, $11, NOW()
)
ON CONFLICT (id) DO UPDATE SET
    contract_id    = EXCLUDED.contract_id,
    pln_point_id   = EXCLUDED.pln_point_id,
    enstru_code    = EXCLUDED.enstru_code,
    name_ru        = EXCLUDED.name_ru,
    name_clean     = EXCLUDED.name_clean,
    unit_code      = EXCLUDED.unit_code,
    quantity       = EXCLUDED.quantity,
    unit_price     = EXCLUDED.unit_price,
    total_price    = EXCLUDED.total_price,
    is_price_valid = EXCLUDED.is_price_valid,
    synced_at      = NOW()
"""

LOG_ETL_SQL = """
INSERT INTO etl_runs (entity, customer_bin, records_fetched, records_inserted,
                      records_updated, records_skipped, errors, status, duration_sec, completed_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
"""


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bins", type=str, default="", help="Comma-separated BINs (default: TARGET_BINS)")
    p.add_argument("--year-from", type=int, default=None)
    p.add_argument("--year-to", type=int, default=None)
    p.add_argument("--limit", type=int, default=0, help="Limit contracts per BIN (0 = no limit)")
    p.add_argument("--dry-run", action="store_true", help="Fetch only, no DB writes")
    p.add_argument("--resume-backstep", type=int, default=None, help="Reprocess N last contract IDs per BIN (default: RESUME_BACKSTEP)")
    p.add_argument("--concurrency", type=int, default=None, help="Parallel BINs per loader (default: ETL_CONCURRENCY)")
    p.add_argument(
        "--repair-missing",
        action="store_true",
        default=True,
        help="Also revisit contracts that already have items with missing pln_point/enstru/unit",
    )
    return p.parse_args()


def parse_bins(arg: str, fallback: tuple[str, ...]) -> list[str]:
    if arg:
        return [b.strip() for b in arg.split(",") if b.strip()]
    return list(fallback)


async def get_plan_point_info(
    conn,
    plan_point_id: int,
    cache: dict[int, tuple[str | None, int | None, str | None] | None],
) -> tuple[str | None, int | None, str | None] | None:
    if plan_point_id in cache:
        return cache[plan_point_id]
    row = await conn.fetchrow(
        "SELECT enstru_code, unit_code, name_ru FROM plan_points WHERE id=$1",
        plan_point_id,
    )
    if not row:
        cache[plan_point_id] = None
        return None
    cache[plan_point_id] = (row["enstru_code"], row["unit_code"], row["name_ru"])
    return cache[plan_point_id]


async def load_contract_items():
    import asyncpg

    args = parse_args()
    cfg = get_config()

    bins = parse_bins(args.bins, cfg.target_bins)
    year_from = args.year_from or cfg.data_year_from
    year_to = args.year_to or cfg.data_year_to

    db_url = resolve_db_url(cfg.db_url)
    concurrency = args.concurrency if args.concurrency is not None else cfg.etl_concurrency
    if bins:
        concurrency = max(1, min(concurrency, len(bins)))
    else:
        concurrency = 1

    pool = await asyncpg.create_pool(db_url, min_size=1, max_size=max(4, concurrency * 2), timeout=15)
    async with pool.acquire() as conn:
        await ensure_etl_state_table(conn)
        await conn.execute("ALTER TABLE contract_items ADD COLUMN IF NOT EXISTS pln_point_id BIGINT")

    async with OWSClient(token=cfg.ows_token, base_url=cfg.ows_base_url) as client:
        sem = asyncio.Semaphore(concurrency)

        async def process_bin(bin_code: str) -> None:
            async with sem:
                async with pool.acquire() as conn:
                    enstru_cache: set[str] = set()
                    unit_cache: set[int] = set()
                    pln_point_cache: dict[int, tuple[str | None, int | None, str | None] | None] = {}

                    fetched = 0
                    upserted = 0
                    skipped = 0
                    errors = 0
                    t_start = time.monotonic()

                    try:
                        state_last_id, _ = await get_etl_state(conn, "contract_items", bin_code)
                        backstep = args.resume_backstep if args.resume_backstep is not None else cfg.resume_backstep
                        resume_from_id = None
                        use_gte = False
                        if state_last_id:
                            if backstep > 0:
                                resume_from_id = max(state_last_id - backstep, 0)
                                use_gte = True
                            else:
                                resume_from_id = state_last_id

                        if resume_from_id is None:
                            contract_rows = await conn.fetch(
                                """
                                SELECT id, sign_date FROM contracts
                                WHERE customer_bin=$1
                                ORDER BY id
                                """,
                                bin_code,
                            )
                        elif use_gte:
                            contract_rows = await conn.fetch(
                                """
                                SELECT id, sign_date FROM contracts
                                WHERE customer_bin=$1 AND id >= $2
                                ORDER BY id
                                """,
                                bin_code,
                                resume_from_id,
                            )
                        else:
                            contract_rows = await conn.fetch(
                                """
                                SELECT id, sign_date FROM contracts
                                WHERE customer_bin=$1 AND id > $2
                                ORDER BY id
                                """,
                                bin_code,
                                resume_from_id,
                            )

                        if args.repair_missing:
                            repair_rows = await conn.fetch(
                                """
                                SELECT DISTINCT c.id, c.sign_date
                                FROM contracts c
                                JOIN contract_items ci ON ci.contract_id = c.id
                                WHERE c.customer_bin=$1
                                  AND (ci.pln_point_id IS NULL OR ci.enstru_code IS NULL OR ci.unit_code IS NULL)
                                ORDER BY c.id
                                """,
                                bin_code,
                            )
                            by_id: dict[int, object] = {row["id"]: row for row in contract_rows}
                            for row in repair_rows:
                                by_id[row["id"]] = row
                            contract_rows = [by_id[k] for k in sorted(by_id.keys())]

                        for row in contract_rows:
                            if args.limit and fetched >= args.limit:
                                break
                            sign_date = row["sign_date"]
                            if sign_date and (sign_date.year < year_from or sign_date.year > year_to):
                                skipped += 1
                                continue
                            contract_id = row["id"]
                            fetched += 1

                            try:
                                units = await client.get_contract_units(contract_id)
                            except Exception as e:
                                errors += 1
                                logger.debug("BIN %s contract %s: units fetch failed: %s", bin_code, contract_id, e)
                                continue
                            for u in units:
                                item_id = safe_int(u.get("id"))
                                if item_id is None:
                                    skipped += 1
                                    continue

                                pln_point_id = safe_int(u.get("pln_point_id"))
                                enstru_code = None
                                unit_code = None
                                name_ru = None

                                if pln_point_id:
                                    local_info = await get_plan_point_info(conn, pln_point_id, pln_point_cache)
                                    if local_info:
                                        enstru_code, unit_code, name_ru = local_info
                                    else:
                                        try:
                                            view = await client.get_plan_point_view(pln_point_id)
                                        except Exception:
                                            view = {}
                                        if view:
                                            enstru_code = view.get("ref_enstru_code")
                                            unit_code = safe_int(view.get("ref_units_code"))
                                            name_ru = view.get("name_ru") or view.get("spec")
                                            pln_point_cache[pln_point_id] = (enstru_code, unit_code, name_ru)

                                if enstru_code:
                                    await ensure_enstru_ref(conn, enstru_code, enstru_cache)
                                await ensure_unit_ref(conn, unit_code, unit_cache)

                                quantity = u.get("quantity")
                                unit_price = u.get("item_price")
                                total_price = u.get("total_sum")
                                is_price_valid = bool(quantity) and bool(unit_price)

                                if args.dry_run:
                                    upserted += 1
                                    continue

                                try:
                                    await conn.execute(
                                        UPSERT_ITEM_SQL,
                                        item_id,
                                        contract_id,
                                        pln_point_id,
                                        enstru_code,
                                        name_ru,
                                        normalize_name(name_ru),
                                        unit_code,
                                        quantity,
                                        unit_price,
                                        total_price,
                                        is_price_valid,
                                    )
                                    upserted += 1
                                except Exception:
                                    errors += 1

                            if not args.dry_run:
                                await update_etl_state(conn, "contract_items", bin_code, contract_id, None)
                    except Exception as e:
                        errors += 1
                        logger.exception("BIN %s: fatal contract_items ingestion error: %s", bin_code, e)

                    status = "ok" if errors == 0 else ("partial" if upserted > 0 else "failed")
                    await conn.execute(
                        LOG_ETL_SQL,
                        "contract_items",
                        bin_code,
                        fetched,
                        upserted,
                        0,
                        skipped,
                        errors,
                        status,
                        round(time.monotonic() - t_start, 2),
                    )
                    logger.info("BIN %s: contract_items upserted=%d contracts=%d skipped=%d errors=%d", bin_code, upserted, fetched, skipped, errors)

        await asyncio.gather(*(process_bin(b) for b in bins))

    await pool.close()


if __name__ == "__main__":
    asyncio.run(load_contract_items())
