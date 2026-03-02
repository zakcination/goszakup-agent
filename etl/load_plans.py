"""
etl/load_plans.py
----------------
Spiral 2 — Load plan points for TARGET_BINS and year range.
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
    kato_exists,
    ensure_etl_state_table,
    get_etl_state,
    load_env,
    maybe_reexec_in_venv,
    parse_dt,
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
logger = logging.getLogger("etl.plans")


UPSERT_PLAN_SQL = """
INSERT INTO plan_points (
    id, customer_bin, enstru_code, fin_year, plan_act_number,
    name_ru, name_kz, unit_code, quantity, unit_price, total_amount,
    kato_delivery, delivery_address_ru, trade_method_id, status_id,
    ref_finsource_id, desc_ru, is_qvazi, rootrecord_id, date_approved, synced_at
)
VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9, $10, $11,
    $12, $13, $14, $15,
    $16, $17, $18, $19, $20, NOW()
)
ON CONFLICT (id) DO UPDATE SET
    customer_bin        = EXCLUDED.customer_bin,
    enstru_code         = EXCLUDED.enstru_code,
    fin_year            = EXCLUDED.fin_year,
    plan_act_number     = EXCLUDED.plan_act_number,
    name_ru             = EXCLUDED.name_ru,
    name_kz             = EXCLUDED.name_kz,
    unit_code           = EXCLUDED.unit_code,
    quantity            = EXCLUDED.quantity,
    unit_price          = EXCLUDED.unit_price,
    total_amount        = EXCLUDED.total_amount,
    kato_delivery       = EXCLUDED.kato_delivery,
    delivery_address_ru = EXCLUDED.delivery_address_ru,
    trade_method_id     = EXCLUDED.trade_method_id,
    status_id           = EXCLUDED.status_id,
    ref_finsource_id    = EXCLUDED.ref_finsource_id,
    desc_ru             = EXCLUDED.desc_ru,
    is_qvazi            = EXCLUDED.is_qvazi,
    rootrecord_id       = EXCLUDED.rootrecord_id,
    date_approved       = EXCLUDED.date_approved,
    synced_at           = NOW()
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
    p.add_argument("--limit", type=int, default=0, help="Limit items per BIN (0 = no limit)")
    p.add_argument("--dry-run", action="store_true", help="Fetch only, no DB writes")
    p.add_argument("--resume-backstep", type=int, default=None, help="Reprocess N last IDs per BIN (default: RESUME_BACKSTEP)")
    p.add_argument("--concurrency", type=int, default=None, help="Parallel BINs per loader (default: ETL_CONCURRENCY)")
    p.add_argument(
        "--resolve-kato-view",
        action="store_true",
        default=os.environ.get("PLANS_RESOLVE_KATO_VIEW", "0").lower() in ("1", "true", "yes", "on"),
        help="When /v3/plans payload lacks KATO, call /v3/plans/view/{id} fallback",
    )
    return p.parse_args()


def parse_bins(arg: str, fallback: tuple[str, ...]) -> list[str]:
    if arg:
        return [b.strip() for b in arg.split(",") if b.strip()]
    return list(fallback)


def build_start_path(base_path: str, resume_path: str | None, last_id: int | None, backstep: int) -> str:
    if last_id and backstep > 0:
        # Avoid dropping to 0, which triggers an expensive near-full rescan.
        back_id = (last_id - backstep) if last_id > backstep else last_id
        return f"{base_path}?page=next&search_after={back_id}"
    if resume_path:
        return resume_path
    if last_id:
        return f"{base_path}?page=next&search_after={last_id}"
    return base_path


async def load_plans():
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

    async with OWSClient(token=cfg.ows_token, base_url=cfg.ows_base_url) as client:
        sem = asyncio.Semaphore(concurrency)

        async def process_bin(bin_code: str) -> None:
            async with sem:
                async with pool.acquire() as conn:
                    enstru_cache: set[str] = set()
                    kato_cache: set[str] = set()
                    unit_cache: set[int] = set()
                    view_kato_cache: dict[int, tuple[str | None, str | None] | None] = {}

                    fetched = 0
                    upserted = 0
                    skipped = 0
                    errors = 0
                    t_start = time.monotonic()

                    try:
                        state_last_id, state_resume_path = await get_etl_state(conn, "plans", bin_code)
                        backstep = args.resume_backstep if args.resume_backstep is not None else cfg.resume_backstep
                        base_path = f"/v3/plans/{bin_code}"
                        start_path = build_start_path(base_path, state_resume_path, state_last_id, backstep)

                        limit_reached = False
                        async for items, next_page in client.iter_rest_pages(base_path, start_path=start_path):
                            page_last_id: int | None = None
                            for plan in items:
                                if args.limit and fetched >= args.limit:
                                    limit_reached = True
                                    break
                                fetched += 1

                                plan_id = safe_int(plan.get("id"))
                                if plan_id is not None:
                                    page_last_id = plan_id

                                fin_year = safe_int(plan.get("plan_fin_year")) or safe_int(plan.get("pln_point_year"))
                                if fin_year is None or fin_year < year_from or fin_year > year_to:
                                    skipped += 1
                                    continue

                                if plan_id is None:
                                    skipped += 1
                                    continue

                                enstru_code = plan.get("ref_enstru_code")
                                await ensure_enstru_ref(conn, enstru_code, enstru_cache)

                                unit_code = safe_int(plan.get("ref_units_code"))
                                await ensure_unit_ref(conn, unit_code, unit_cache)
                                quantity = plan.get("count")
                                unit_price = plan.get("price")
                                total_amount = plan.get("amount")

                                kato_code = None
                                delivery_address_ru = None
                                kato_list = plan.get("kato") or []
                                if kato_list:
                                    first = kato_list[0] or {}
                                    kato_code = first.get("ref_kato_code")
                                    delivery_address_ru = first.get("full_delivery_place_name_ru")
                                    if kato_code and not await kato_exists(conn, kato_code, kato_cache):
                                        kato_code = None
                                if not kato_code and args.resolve_kato_view and plan_id:
                                    if plan_id in view_kato_cache:
                                        view_kato = view_kato_cache[plan_id]
                                    else:
                                        try:
                                            view = await client.get_plan_point_view(plan_id)
                                        except Exception:
                                            view = {}
                                        view_kato = None
                                        if view:
                                            view_kato_list = view.get("kato") or []
                                            if isinstance(view_kato_list, list) and view_kato_list:
                                                first = view_kato_list[0] or {}
                                                view_kato = (
                                                    first.get("ref_kato_code"),
                                                    first.get("full_delivery_place_name_ru"),
                                                )
                                        view_kato_cache[plan_id] = view_kato
                                    if view_kato and view_kato[0] and await kato_exists(conn, view_kato[0], kato_cache):
                                        kato_code = view_kato[0]
                                        delivery_address_ru = delivery_address_ru or view_kato[1]

                                if args.dry_run:
                                    upserted += 1
                                    continue

                                try:
                                    await conn.execute(
                                        UPSERT_PLAN_SQL,
                                        plan_id,
                                        plan.get("subject_biin") or bin_code,
                                        enstru_code,
                                        fin_year,
                                        plan.get("plan_act_number"),
                                        plan.get("name_ru"),
                                        plan.get("name_kz"),
                                        unit_code,
                                        quantity,
                                        unit_price,
                                        total_amount,
                                        kato_code,
                                        delivery_address_ru,
                                        safe_int(plan.get("ref_trade_methods_id")),
                                        safe_int(plan.get("ref_pln_point_status_id")),
                                        safe_int(plan.get("ref_finsource_id")),
                                        plan.get("desc_ru"),
                                        bool(plan.get("is_qvazi", 0)),
                                        safe_int(plan.get("rootrecord_id")),
                                        parse_dt(plan.get("date_approved")),
                                    )
                                    upserted += 1
                                except Exception:
                                    errors += 1

                            if not args.dry_run:
                                new_last_id = page_last_id or state_last_id
                                resume_path = next_page
                                if limit_reached and page_last_id:
                                    resume_path = f"{base_path}?page=next&search_after={page_last_id}"
                                await update_etl_state(conn, "plans", bin_code, new_last_id, resume_path)
                                state_last_id = new_last_id

                            if limit_reached:
                                break
                    except Exception as e:
                        errors += 1
                        logger.exception("BIN %s: fatal plans ingestion error: %s", bin_code, e)

                    status = "ok" if errors == 0 else ("partial" if upserted > 0 else "failed")
                    await conn.execute(
                        LOG_ETL_SQL,
                        "plans",
                        bin_code,
                        fetched,
                        upserted,
                        0,
                        skipped,
                        errors,
                        status,
                        round(time.monotonic() - t_start, 2),
                    )
                    logger.info("BIN %s: plans upserted=%d fetched=%d skipped=%d errors=%d", bin_code, upserted, fetched, skipped, errors)

        await asyncio.gather(*(process_bin(b) for b in bins))

    await pool.close()


if __name__ == "__main__":
    asyncio.run(load_plans())
