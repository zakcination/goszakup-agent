"""
etl/load_lots.py
---------------
Spiral 2 — Load lots and lot↔plan_point linkage.
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
    kato_exists,
    load_env,
    maybe_reexec_in_venv,
    normalize_name,
    parse_dt,
    resolve_db_url,
    safe_int,
    safe_float,
    ensure_subject,
    get_etl_state,
    update_etl_state,
)

maybe_reexec_in_venv()
load_env()

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("etl.lots")


UPSERT_LOT_SQL = """
INSERT INTO lots (
    id, announcement_id, source_trd_buy_id, customer_bin, enstru_code,
    name_ru, name_kz, name_clean, description_ru,
    unit_code, quantity, unit_price, lot_amount,
    kato_delivery, status_id, trade_method_id, is_price_valid,
    system_id, synced_at
)
VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9,
    $10, $11, $12, $13,
    $14, $15, $16, $17,
    $18, NOW()
)
ON CONFLICT (id) DO UPDATE SET
    announcement_id = EXCLUDED.announcement_id,
    source_trd_buy_id = EXCLUDED.source_trd_buy_id,
    customer_bin    = EXCLUDED.customer_bin,
    enstru_code     = EXCLUDED.enstru_code,
    name_ru         = EXCLUDED.name_ru,
    name_kz         = EXCLUDED.name_kz,
    name_clean      = EXCLUDED.name_clean,
    description_ru  = EXCLUDED.description_ru,
    unit_code       = EXCLUDED.unit_code,
    quantity        = EXCLUDED.quantity,
    unit_price      = EXCLUDED.unit_price,
    lot_amount      = EXCLUDED.lot_amount,
    kato_delivery   = EXCLUDED.kato_delivery,
    status_id       = EXCLUDED.status_id,
    trade_method_id = EXCLUDED.trade_method_id,
    is_price_valid  = EXCLUDED.is_price_valid,
    system_id       = EXCLUDED.system_id,
    synced_at       = NOW()
"""

UPSERT_ANN_SQL = """
INSERT INTO announcements (
    id, number_anno, customer_bin, organizer_bin, name_ru, total_sum,
    publish_date, end_date, status_id, trade_method_id, system_id, last_updated, synced_at
)
VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11, $12, NOW()
)
ON CONFLICT (id) DO UPDATE SET
    number_anno     = EXCLUDED.number_anno,
    customer_bin    = EXCLUDED.customer_bin,
    organizer_bin   = EXCLUDED.organizer_bin,
    name_ru         = EXCLUDED.name_ru,
    total_sum       = EXCLUDED.total_sum,
    publish_date    = EXCLUDED.publish_date,
    end_date        = EXCLUDED.end_date,
    status_id       = EXCLUDED.status_id,
    trade_method_id = EXCLUDED.trade_method_id,
    system_id       = EXCLUDED.system_id,
    last_updated    = EXCLUDED.last_updated,
    synced_at       = NOW()
"""

INSERT_LPP_SQL = """
INSERT INTO lot_plan_points (lot_id, plan_point_id)
VALUES ($1, $2)
ON CONFLICT DO NOTHING
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
        "--resolve-plan-view",
        action="store_true",
        default=os.environ.get("LOTS_RESOLVE_PLAN_VIEW", "0").lower() in ("1", "true", "yes", "on"),
        help="When local plan_points miss, call /v3/plans/view/{id} to resolve ENSTRU/unit/KATO",
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


def _first_non_empty(data: dict, keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str):
            value = value.strip()
        if value not in (None, ""):
            return str(value)
    return None


def _first_int(data: dict, keys: tuple[str, ...]) -> int | None:
    for key in keys:
        value = safe_int(data.get(key))
        if value is not None:
            return value
    return None


def _extract_kato_from_delivery_places(lot: dict) -> str | None:
    delivery = lot.get("delivery_places") or lot.get("delivery_place") or lot.get("deliveryPlaces")
    if isinstance(delivery, dict):
        delivery = [delivery]
    if isinstance(delivery, list):
        for item in delivery:
            if not isinstance(item, dict):
                continue
            candidate = (
                item.get("ref_kato_code")
                or item.get("refKatoCode")
                or item.get("kato")
                or item.get("kato_code")
            )
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    return None


async def get_plan_point_info(conn, plan_point_id: int, cache: dict[int, tuple[str | None, int | None] | None]) -> tuple[str | None, int | None] | None:
    if plan_point_id in cache:
        return cache[plan_point_id]
    row = await conn.fetchrow("SELECT enstru_code, unit_code FROM plan_points WHERE id=$1", plan_point_id)
    if not row:
        cache[plan_point_id] = None
        return None
    cache[plan_point_id] = (row["enstru_code"], row["unit_code"])
    return cache[plan_point_id]


async def ensure_announcement(conn, client, ann_id: int, subject_cache: set[str]) -> None:
    exists = await conn.fetchval("SELECT 1 FROM announcements WHERE id=$1", ann_id)
    if exists:
        return
    ann = await client.get_trd_buy(ann_id)
    if not ann:
        return
    organizer_bin = ann.get("org_bin")
    customer_bin = ann.get("customer_bin") or organizer_bin
    if organizer_bin:
        await ensure_subject(conn, client, organizer_bin, is_supplier=False, cache=subject_cache)
    if customer_bin and customer_bin != organizer_bin:
        await ensure_subject(conn, client, customer_bin, is_supplier=False, cache=subject_cache)

    publish_dt = parse_dt(ann.get("publish_date"))
    end_dt = parse_dt(ann.get("end_date"))
    await conn.execute(
        UPSERT_ANN_SQL,
        safe_int(ann.get("id")),
        ann.get("number_anno"),
        customer_bin,
        organizer_bin,
        ann.get("name_ru"),
        ann.get("total_sum"),
        publish_dt.date() if publish_dt else None,
        end_dt.date() if end_dt else None,
        safe_int(ann.get("ref_buy_status_id")),
        safe_int(ann.get("ref_trade_methods_id")),
        safe_int(ann.get("system_id")),
        parse_dt(ann.get("last_update_date")),
    )


async def load_lots():
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
        await conn.execute("ALTER TABLE lots ADD COLUMN IF NOT EXISTS source_trd_buy_id BIGINT")
        # Ensure join table exists (for existing DBs)
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lot_plan_points (
                lot_id        BIGINT NOT NULL REFERENCES lots(id),
                plan_point_id BIGINT NOT NULL REFERENCES plan_points(id),
                PRIMARY KEY (lot_id, plan_point_id)
            )
            """
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lpp_plan_point ON lot_plan_points(plan_point_id)"
        )

    async with OWSClient(token=cfg.ows_token, base_url=cfg.ows_base_url) as client:
        sem = asyncio.Semaphore(concurrency)

        async def process_bin(bin_code: str) -> None:
            async with sem:
                async with pool.acquire() as conn:
                    enstru_cache: set[str] = set()
                    kato_cache: set[str] = set()
                    unit_cache: set[int] = set()
                    subject_cache: set[str] = set()
                    plan_point_cache: dict[int, tuple[str | None, int | None] | None] = {}
                    plan_point_view_cache: dict[int, tuple[str | None, int | None, str | None] | None] = {}
                    plan_point_exists_cache: dict[int, bool] = {}

                    fetched = 0
                    upserted = 0
                    skipped = 0
                    errors = 0
                    t_start = time.monotonic()

                    try:
                        ann_rows = await conn.fetch(
                            """
                            SELECT id, publish_date
                            FROM announcements
                            WHERE organizer_bin=$1 OR customer_bin=$1
                            """,
                            bin_code,
                        )
                        ann_publish: dict[int, object] = {row["id"]: row["publish_date"] for row in ann_rows}
                        ann_ids: set[int] = set(ann_publish.keys())
                        ann_checked: set[int] = set(ann_ids)

                        state_last_id, state_resume_path = await get_etl_state(conn, "lots", bin_code)
                        backstep = args.resume_backstep if args.resume_backstep is not None else cfg.resume_backstep
                        base_path = f"/v3/lots/bin/{bin_code}"
                        start_path = build_start_path(base_path, state_resume_path, state_last_id, backstep)

                        limit_reached = False
                        async for items, next_page in client.iter_rest_pages(base_path, start_path=start_path):
                            page_last_id: int | None = None
                            for lot in items:
                                if args.limit and fetched >= args.limit:
                                    limit_reached = True
                                    break
                                fetched += 1

                                lot_id = safe_int(lot.get("id"))
                                if lot_id is not None:
                                    page_last_id = lot_id

                                if lot_id is None:
                                    skipped += 1
                                    continue

                                # Date filter via related announcement publish date if available.
                                source_trd_buy_id = safe_int(lot.get("trd_buy_id"))
                                ann_id = source_trd_buy_id
                                if source_trd_buy_id and source_trd_buy_id not in ann_checked:
                                    ann_checked.add(source_trd_buy_id)
                                    if source_trd_buy_id not in ann_ids:
                                        try:
                                            await ensure_announcement(conn, client, source_trd_buy_id, subject_cache)
                                        except Exception as exc:
                                            logger.debug("Failed to ensure announcement %s: %s", source_trd_buy_id, exc)
                                        row = await conn.fetchrow("SELECT publish_date FROM announcements WHERE id=$1", source_trd_buy_id)
                                        if row:
                                            ann_publish[source_trd_buy_id] = row["publish_date"]
                                            ann_ids.add(source_trd_buy_id)
                                        else:
                                            ann_id = None
                                # Filter by announcement publish_date if available
                                pub_date = ann_publish.get(source_trd_buy_id) if source_trd_buy_id else None
                                if pub_date and (pub_date.year < year_from or pub_date.year > year_to):
                                    skipped += 1
                                    continue

                                amount = safe_float(lot.get("amount"))
                                quantity = safe_float(lot.get("count"))
                                if quantity is None:
                                    quantity = safe_float(lot.get("quantity"))
                                unit_price = safe_float(lot.get("unit_price"))
                                if unit_price is None:
                                    unit_price = safe_float(lot.get("price"))
                                if unit_price is None and amount and quantity:
                                    unit_price = (amount / quantity) if quantity != 0 else None

                                # Prefer direct lot fields, then fallback to plan-point linkage.
                                enstru_code = _first_non_empty(
                                    lot,
                                    ("ref_enstru_code", "enstru_code", "tru_code"),
                                )
                                unit_code = _first_int(
                                    lot,
                                    ("ref_units_code", "ref_unit_code", "unit_code", "unit_id"),
                                )
                                point_list = lot.get("point_list") or []
                                if not enstru_code or not unit_code:
                                    for pid in point_list:
                                        pid_int = safe_int(pid)
                                        if not pid_int:
                                            continue
                                        info = await get_plan_point_info(conn, pid_int, plan_point_cache)
                                        if info:
                                            if not enstru_code:
                                                enstru_code = info[0]
                                            if not unit_code:
                                                unit_code = info[1]
                                            if enstru_code and unit_code:
                                                break
                                        if not args.resolve_plan_view:
                                            continue
                                        if pid_int in plan_point_view_cache:
                                            view_info = plan_point_view_cache[pid_int]
                                        else:
                                            try:
                                                view = await client.get_plan_point_view(pid_int)
                                            except Exception:
                                                view = {}
                                            if view:
                                                view_info = (
                                                    view.get("ref_enstru_code"),
                                                    safe_int(view.get("ref_units_code")),
                                                    None,
                                                )
                                                kato_view = view.get("kato") or []
                                                if isinstance(kato_view, list) and kato_view:
                                                    first_kato = kato_view[0] or {}
                                                    view_info = (view_info[0], view_info[1], first_kato.get("ref_kato_code"))
                                            else:
                                                view_info = None
                                            plan_point_view_cache[pid_int] = view_info
                                        if view_info:
                                            if not enstru_code:
                                                enstru_code = view_info[0]
                                            if not unit_code:
                                                unit_code = view_info[1]
                                            if enstru_code and unit_code:
                                                break

                                if enstru_code:
                                    await ensure_enstru_ref(conn, enstru_code, enstru_cache)
                                await ensure_unit_ref(conn, unit_code, unit_cache)

                                # Prefer KATO from lot payload, then fallback to plan-point KATO sources.
                                kato_code = (
                                    _first_non_empty(lot, ("ref_kato_code", "kato_delivery", "kato_code"))
                                    or _extract_kato_from_delivery_places(lot)
                                )
                                if kato_code and not await kato_exists(conn, kato_code, kato_cache):
                                    kato_code = None
                                if not kato_code:
                                    kato_list = lot.get("pln_point_kato_list") or []
                                    if kato_list:
                                        kato_candidate = kato_list[0]
                                        if await kato_exists(conn, kato_candidate, kato_cache):
                                            kato_code = kato_candidate
                                if not kato_code and args.resolve_plan_view:
                                    for pid in point_list:
                                        pid_int = safe_int(pid)
                                        if not pid_int:
                                            continue
                                        view_info = plan_point_view_cache.get(pid_int)
                                        if not view_info:
                                            continue
                                        kato_candidate = view_info[2]
                                        if kato_candidate and await kato_exists(conn, kato_candidate, kato_cache):
                                            kato_code = kato_candidate
                                            break

                                if args.dry_run:
                                    upserted += 1
                                    continue

                                try:
                                    await conn.execute(
                                        UPSERT_LOT_SQL,
                                        lot_id,
                                        ann_id if ann_id and ann_id > 0 else None,
                                        source_trd_buy_id,
                                        lot.get("customer_bin") or bin_code,
                                        enstru_code,
                                        lot.get("name_ru"),
                                        lot.get("name_kz"),
                                        normalize_name(lot.get("name_ru")),
                                        lot.get("description_ru"),
                                        unit_code,
                                        quantity,
                                        unit_price,
                                        amount,
                                        kato_code,
                                        safe_int(lot.get("ref_lot_status_id")),
                                        safe_int(lot.get("ref_trade_methods_id")),
                                        bool(quantity) and bool(unit_price),
                                        safe_int(lot.get("system_id")),
                                    )
                                    upserted += 1

                                    # lot↔plan_point linkage
                                    for pid in point_list:
                                        pid_int = safe_int(pid)
                                        if not pid_int:
                                            continue
                                        exists = plan_point_exists_cache.get(pid_int)
                                        if exists is None:
                                            exists = bool(await conn.fetchval("SELECT 1 FROM plan_points WHERE id=$1", pid_int))
                                            plan_point_exists_cache[pid_int] = exists
                                        if exists:
                                            await conn.execute(INSERT_LPP_SQL, lot_id, pid_int)
                                except Exception:
                                    errors += 1

                            if not args.dry_run:
                                new_last_id = page_last_id or state_last_id
                                resume_path = next_page
                                if limit_reached and page_last_id:
                                    resume_path = f"{base_path}?page=next&search_after={page_last_id}"
                                await update_etl_state(conn, "lots", bin_code, new_last_id, resume_path)
                                state_last_id = new_last_id

                            if limit_reached:
                                break
                    except Exception as e:
                        errors += 1
                        logger.exception("BIN %s: fatal lots ingestion error: %s", bin_code, e)

                    status = "ok" if errors == 0 else ("partial" if upserted > 0 else "failed")
                    await conn.execute(
                        LOG_ETL_SQL,
                        "lots",
                        bin_code,
                        fetched,
                        upserted,
                        0,
                        skipped,
                        errors,
                        status,
                        round(time.monotonic() - t_start, 2),
                    )
                    logger.info("BIN %s: lots upserted=%d fetched=%d skipped=%d errors=%d", bin_code, upserted, fetched, skipped, errors)

        await asyncio.gather(*(process_bin(b) for b in bins))

    await pool.close()


if __name__ == "__main__":
    asyncio.run(load_lots())
