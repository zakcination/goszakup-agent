"""
etl/repair_missing_source_links.py
---------------------------------
Checkpointed repair worker for missing source linkage fields:
- lots.source_trd_buy_id / lots.announcement_id
- contracts.source_trd_buy_id / contracts.announcement_id

Uses detail endpoints to recover missing IDs:
- /v3/lots/{id}
- /v3/contract/{id}

Also attempts lightweight lot enrichment from detail payload:
- lot_plan_points linkage
- lots.enstru_code / unit_code / kato_delivery (when derivable)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.client import OWSClient
from etl.config import get_config
from etl.utils import (
    ensure_enstru_ref,
    ensure_etl_state_table,
    ensure_subject,
    ensure_unit_ref,
    get_etl_state,
    kato_exists,
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
logger = logging.getLogger("etl.repair_missing_source_links")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

STATE_LOTS = "repair_lot_sources_worker"
STATE_CONTRACTS = "repair_contract_sources_worker"
STATE_SCOPE = "__global__"

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

UPSERT_LOT_STATUS_REF_SQL = """
INSERT INTO lot_statuses_ref (id, name_ru, name_kz)
VALUES ($1, 'UNKNOWN', NULL)
ON CONFLICT (id) DO NOTHING
"""

UPSERT_TRADE_METHOD_REF_SQL = """
INSERT INTO trade_methods_ref (id, name_ru, name_kz, code)
VALUES ($1, 'UNKNOWN', NULL, NULL)
ON CONFLICT (id) DO NOTHING
"""

INSERT_LPP_SQL = """
INSERT INTO lot_plan_points (lot_id, plan_point_id)
VALUES ($1, $2)
ON CONFLICT DO NOTHING
"""

UPDATE_LOT_SQL = """
UPDATE lots
SET
    source_trd_buy_id = COALESCE(source_trd_buy_id, $2),
    announcement_id = COALESCE(announcement_id, $3),
    enstru_code = COALESCE(enstru_code, $4),
    unit_code = COALESCE(unit_code, $5),
    kato_delivery = COALESCE(kato_delivery, $6),
    synced_at = NOW()
WHERE id = $1
"""

UPDATE_CONTRACT_SQL = """
UPDATE contracts
SET
    source_trd_buy_id = COALESCE(source_trd_buy_id, $2),
    announcement_id = COALESCE(announcement_id, $3),
    synced_at = NOW()
WHERE id = $1
"""

LOG_ETL_SQL = """
INSERT INTO etl_runs (entity, customer_bin, records_fetched, records_inserted,
                      records_updated, records_skipped, errors, status, duration_sec, completed_at)
VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8, NOW())
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--mode",
        choices=("lots", "contracts", "both"),
        default="both",
        help="Repair scope",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Global cap per entity (0 = no limit)",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=int(os.environ.get("RAW_WORKER_BATCH_SIZE", "500")),
        help="Rows fetched from DB per chunk",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=int(os.environ.get("ETL_CONCURRENCY", "6")),
        help="Parallel detail requests",
    )
    p.add_argument(
        "--checkpoint-every",
        type=int,
        default=int(os.environ.get("RAW_WORKER_CHECKPOINT_EVERY", "100")),
        help="Persist etl_state every N processed rows",
    )
    p.add_argument(
        "--resume-backstep",
        type=int,
        default=int(os.environ.get("RESUME_BACKSTEP", "20000")),
        help="Reprocess N IDs behind stored checkpoint",
    )
    p.add_argument(
        "--resolve-plan-view",
        action="store_true",
        default=False,
        help="Use /v3/plans/view fallback when local plan_point not present",
    )
    p.add_argument(
        "--skip-announcements",
        action="store_true",
        default=os.environ.get("REPAIR_SKIP_ANNOUNCEMENTS", "0").lower() in ("1", "true", "yes", "on"),
        help="Skip per-lot /v3/trd-buy hydration; fill source_trd_buy_id now and backfill announcements separately.",
    )
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


async def _ensure_announcement_refs(conn, status_id: int | None, trade_method_id: int | None) -> None:
    if status_id:
        await conn.execute(UPSERT_LOT_STATUS_REF_SQL, status_id)
    if trade_method_id:
        await conn.execute(UPSERT_TRADE_METHOD_REF_SQL, trade_method_id)


async def _ensure_announcement(
    conn,
    client: OWSClient,
    trd_buy_id: int | None,
    subject_cache: set[str],
    ann_cache: dict[int, bool],
) -> bool:
    if not trd_buy_id:
        return False
    if trd_buy_id in ann_cache:
        return ann_cache[trd_buy_id]

    exists = bool(await conn.fetchval("SELECT 1 FROM announcements WHERE id=$1", trd_buy_id))
    if exists:
        ann_cache[trd_buy_id] = True
        return True

    try:
        ann = await client.get_trd_buy(trd_buy_id)
    except Exception:
        ann_cache[trd_buy_id] = False
        return False

    if not ann:
        ann_cache[trd_buy_id] = False
        return False

    organizer_bin = ann.get("org_bin")
    customer_bin = ann.get("customer_bin") or organizer_bin
    if organizer_bin:
        await ensure_subject(conn, client, organizer_bin, is_supplier=False, cache=subject_cache)
    if customer_bin and customer_bin != organizer_bin:
        await ensure_subject(conn, client, customer_bin, is_supplier=False, cache=subject_cache)

    publish_dt = parse_dt(ann.get("publish_date"))
    end_dt = parse_dt(ann.get("end_date"))
    status_id = safe_int(ann.get("ref_buy_status_id"))
    trade_method_id = safe_int(ann.get("ref_trade_methods_id"))
    await _ensure_announcement_refs(conn, status_id, trade_method_id)

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
        status_id,
        trade_method_id,
        safe_int(ann.get("system_id")),
        parse_dt(ann.get("last_update_date")),
    )
    ann_cache[trd_buy_id] = True
    return True


def _eta(seconds_done: float, done: int, total: int) -> str:
    if done <= 0 or total <= done:
        return "0m"
    rate = done / max(seconds_done, 1e-9)
    rem = (total - done) / max(rate, 1e-9)
    mins = int(rem // 60)
    secs = int(rem % 60)
    if mins >= 60:
        return f"{mins // 60}h {mins % 60}m"
    return f"{mins}m {secs}s"


async def _resolve_lot_enrichment(
    conn,
    client: OWSClient,
    lot_id: int,
    detail: dict,
    args: argparse.Namespace,
    enstru_cache: set[str],
    unit_cache: set[int],
    kato_cache: set[str],
    plan_point_cache: dict[int, tuple[str | None, int | None] | None],
    plan_view_cache: dict[int, tuple[str | None, int | None, str | None] | None],
) -> tuple[str | None, int | None, str | None]:
    point_list = detail.get("point_list") or []
    enstru_code = None
    unit_code = None
    kato_code = None

    for pid in point_list:
        pid_int = safe_int(pid)
        if not pid_int:
            continue

        row = plan_point_cache.get(pid_int)
        if row is None and pid_int not in plan_point_cache:
            db_row = await conn.fetchrow(
                "SELECT enstru_code, unit_code FROM plan_points WHERE id=$1",
                pid_int,
            )
            if db_row:
                row = (db_row["enstru_code"], db_row["unit_code"])
            plan_point_cache[pid_int] = row

        if row:
            if row[0] and not enstru_code:
                enstru_code = row[0]
            if row[1] and not unit_code:
                unit_code = row[1]

            # Only link if plan_point exists in local table.
            await conn.execute(INSERT_LPP_SQL, lot_id, pid_int)

        if args.resolve_plan_view and (not enstru_code or not unit_code or not kato_code):
            if pid_int in plan_view_cache:
                v = plan_view_cache[pid_int]
            else:
                try:
                    view = await client.get_plan_point_view(pid_int)
                except Exception:
                    view = {}
                if view:
                    kato_list = view.get("kato") or []
                    kato_view = None
                    if isinstance(kato_list, list) and kato_list:
                        first = kato_list[0] or {}
                        if isinstance(first, dict):
                            kato_view = first.get("ref_kato_code")
                    v = (view.get("ref_enstru_code"), safe_int(view.get("ref_units_code")), kato_view)
                else:
                    v = None
                plan_view_cache[pid_int] = v

            if v:
                if not enstru_code:
                    enstru_code = v[0]
                if not unit_code:
                    unit_code = v[1]
                if not kato_code and v[2] and await kato_exists(conn, v[2], kato_cache):
                    kato_code = v[2]

    if not kato_code:
        for raw_kato in (detail.get("pln_point_kato_list") or []):
            if raw_kato and await kato_exists(conn, raw_kato, kato_cache):
                kato_code = raw_kato
                break

    if enstru_code:
        await ensure_enstru_ref(conn, enstru_code, enstru_cache)
    await ensure_unit_ref(conn, unit_code, unit_cache)
    return enstru_code, unit_code, kato_code


async def repair_lots(
    pool,
    client: OWSClient,
    args: argparse.Namespace,
) -> tuple[int, int, int, int, int]:
    fetched = 0
    updated = 0
    skipped = 0
    errors = 0

    async with pool.acquire() as conn:
        last_id, _ = await get_etl_state(conn, STATE_LOTS, STATE_SCOPE)
        cursor = max((last_id or 0) - max(0, args.resume_backstep), 0)
        total_pending = await conn.fetchval("SELECT COUNT(*) FROM lots WHERE source_trd_buy_id IS NULL AND id > $1", cursor)
    logger.info("repair_lots: pending=%s start_cursor=%s", total_pending, cursor)

    ann_cache: dict[int, bool] = {}
    subject_cache: set[str] = set()
    enstru_cache: set[str] = set()
    unit_cache: set[int] = set()
    kato_cache: set[str] = set()
    plan_point_cache: dict[int, tuple[str | None, int | None] | None] = {}
    plan_view_cache: dict[int, tuple[str | None, int | None, str | None] | None] = {}
    sem = asyncio.Semaphore(max(1, args.concurrency))
    stats_lock = asyncio.Lock()
    t0 = time.monotonic()
    checkpoint_every = max(1, args.checkpoint_every)
    batch_size = max(1, args.batch_size)

    processed = 0
    while True:
        if args.limit and processed >= args.limit:
            break
        left = (args.limit - processed) if args.limit else batch_size
        chunk_size = min(batch_size, left) if left > 0 else batch_size

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id
                FROM lots
                WHERE source_trd_buy_id IS NULL
                  AND id > $1
                ORDER BY id
                LIMIT $2
                """,
                cursor,
                chunk_size,
            )

        if not rows:
            break

        ids = [r["id"] for r in rows]
        chunk_last = ids[-1]

        async def process_lot(lot_id: int) -> None:
            nonlocal fetched, updated, skipped, errors
            async with sem:
                try:
                    detail = await client.get_lot(lot_id)
                except Exception:
                    async with stats_lock:
                        fetched += 1
                        errors += 1
                    return

                trd_buy_id = safe_int(detail.get("trd_buy_id"))
                async with pool.acquire() as conn:
                    try:
                        enstru_code, unit_code, kato_code = await _resolve_lot_enrichment(
                            conn,
                            client,
                            lot_id,
                            detail,
                            args,
                            enstru_cache,
                            unit_cache,
                            kato_cache,
                            plan_point_cache,
                            plan_view_cache,
                        )
                        announcement_id = None
                        if not args.skip_announcements:
                            ann_ok = await _ensure_announcement(conn, client, trd_buy_id, subject_cache, ann_cache)
                            announcement_id = trd_buy_id if ann_ok else None
                        if not args.dry_run:
                            await conn.execute(
                                UPDATE_LOT_SQL,
                                lot_id,
                                trd_buy_id,
                                announcement_id,
                                enstru_code,
                                unit_code,
                                kato_code,
                            )
                    except Exception:
                        async with stats_lock:
                            fetched += 1
                            errors += 1
                        return

                async with stats_lock:
                    fetched += 1
                    if trd_buy_id:
                        updated += 1
                    else:
                        skipped += 1

        await asyncio.gather(*(process_lot(i) for i in ids))
        processed += len(ids)
        cursor = chunk_last

        if not args.dry_run:
            async with pool.acquire() as conn:
                await update_etl_state(conn, STATE_LOTS, STATE_SCOPE, cursor, None)

        if processed % checkpoint_every == 0 or processed == len(ids):
            eta = _eta(time.monotonic() - t0, processed, total_pending)
            logger.info(
                "repair_lots progress: %d/%d processed, updated=%d skipped=%d errors=%d eta=%s",
                processed,
                total_pending,
                updated,
                skipped,
                errors,
                eta,
            )

    duration = round(time.monotonic() - t0, 2)
    status = "ok" if errors == 0 else ("partial" if updated > 0 else "failed")
    if not args.dry_run:
        async with pool.acquire() as conn:
            await conn.execute(
                LOG_ETL_SQL,
                "repair_lot_sources",
                fetched,
                updated,
                0,
                skipped,
                errors,
                status,
                duration,
            )
    logger.info(
        "repair_lots done: fetched=%d updated=%d skipped=%d errors=%d duration=%.2fs",
        fetched,
        updated,
        skipped,
        errors,
        duration,
    )
    return fetched, updated, skipped, errors, duration


async def repair_contracts(
    pool,
    client: OWSClient,
    args: argparse.Namespace,
) -> tuple[int, int, int, int, int]:
    fetched = 0
    updated = 0
    skipped = 0
    errors = 0

    async with pool.acquire() as conn:
        last_id, _ = await get_etl_state(conn, STATE_CONTRACTS, STATE_SCOPE)
        cursor = max((last_id or 0) - max(0, args.resume_backstep), 0)
        total_pending = await conn.fetchval("SELECT COUNT(*) FROM contracts WHERE source_trd_buy_id IS NULL AND id > $1", cursor)
    logger.info("repair_contracts: pending=%s start_cursor=%s", total_pending, cursor)

    ann_cache: dict[int, bool] = {}
    subject_cache: set[str] = set()
    sem = asyncio.Semaphore(max(1, args.concurrency))
    stats_lock = asyncio.Lock()
    t0 = time.monotonic()
    checkpoint_every = max(1, args.checkpoint_every)
    batch_size = max(1, args.batch_size)

    processed = 0
    while True:
        if args.limit and processed >= args.limit:
            break
        left = (args.limit - processed) if args.limit else batch_size
        chunk_size = min(batch_size, left) if left > 0 else batch_size

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id
                FROM contracts
                WHERE source_trd_buy_id IS NULL
                  AND id > $1
                ORDER BY id
                LIMIT $2
                """,
                cursor,
                chunk_size,
            )

        if not rows:
            break

        ids = [r["id"] for r in rows]
        chunk_last = ids[-1]

        async def process_contract(contract_id: int) -> None:
            nonlocal fetched, updated, skipped, errors
            async with sem:
                try:
                    detail = await client.get_contract(contract_id)
                except Exception:
                    async with stats_lock:
                        fetched += 1
                        errors += 1
                    return

                trd_buy_id = safe_int(detail.get("trd_buy_id"))
                async with pool.acquire() as conn:
                    try:
                        ann_ok = await _ensure_announcement(conn, client, trd_buy_id, subject_cache, ann_cache)
                        announcement_id = trd_buy_id if ann_ok else None
                        if not args.dry_run:
                            await conn.execute(
                                UPDATE_CONTRACT_SQL,
                                contract_id,
                                trd_buy_id,
                                announcement_id,
                            )
                    except Exception:
                        async with stats_lock:
                            fetched += 1
                            errors += 1
                        return

                async with stats_lock:
                    fetched += 1
                    if trd_buy_id:
                        updated += 1
                    else:
                        skipped += 1

        await asyncio.gather(*(process_contract(i) for i in ids))
        processed += len(ids)
        cursor = chunk_last

        if not args.dry_run:
            async with pool.acquire() as conn:
                await update_etl_state(conn, STATE_CONTRACTS, STATE_SCOPE, cursor, None)

        if processed % checkpoint_every == 0 or processed == len(ids):
            eta = _eta(time.monotonic() - t0, processed, total_pending)
            logger.info(
                "repair_contracts progress: %d/%d processed, updated=%d skipped=%d errors=%d eta=%s",
                processed,
                total_pending,
                updated,
                skipped,
                errors,
                eta,
            )

    duration = round(time.monotonic() - t0, 2)
    status = "ok" if errors == 0 else ("partial" if updated > 0 else "failed")
    if not args.dry_run:
        async with pool.acquire() as conn:
            await conn.execute(
                LOG_ETL_SQL,
                "repair_contract_sources",
                fetched,
                updated,
                0,
                skipped,
                errors,
                status,
                duration,
            )
    logger.info(
        "repair_contracts done: fetched=%d updated=%d skipped=%d errors=%d duration=%.2fs",
        fetched,
        updated,
        skipped,
        errors,
        duration,
    )
    return fetched, updated, skipped, errors, duration


async def main() -> None:
    import asyncpg

    args = parse_args()
    cfg = get_config()
    db_url = resolve_db_url(cfg.db_url)
    pool = await asyncpg.create_pool(
        db_url,
        min_size=1,
        max_size=max(4, args.concurrency * 2),
        timeout=20,
    )
    async with pool.acquire() as conn:
        await ensure_etl_state_table(conn)
        await conn.execute("ALTER TABLE lots ADD COLUMN IF NOT EXISTS source_trd_buy_id BIGINT")
        await conn.execute("ALTER TABLE contracts ADD COLUMN IF NOT EXISTS source_trd_buy_id BIGINT")
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lot_plan_points (
                lot_id        BIGINT NOT NULL REFERENCES lots(id),
                plan_point_id BIGINT NOT NULL REFERENCES plan_points(id),
                PRIMARY KEY (lot_id, plan_point_id)
            )
            """
        )
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_lpp_plan_point ON lot_plan_points(plan_point_id)")

    async with OWSClient(token=cfg.ows_token, base_url=cfg.ows_base_url) as client:
        if args.mode in ("lots", "both"):
            await repair_lots(pool, client, args)
        if args.mode in ("contracts", "both"):
            await repair_contracts(pool, client, args)

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
