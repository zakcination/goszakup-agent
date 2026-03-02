"""
etl/backfill_announcements_from_sources.py
-----------------------------------------
Backfill announcements by source_trd_buy_id referenced by lots/contracts.

Purpose:
- fill missing announcements rows
- backfill lots.announcement_id and contracts.announcement_id
"""

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
    ensure_subject,
    load_env,
    maybe_reexec_in_venv,
    parse_dt,
    resolve_db_url,
    safe_int,
)

maybe_reexec_in_venv()
load_env()

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("etl.backfill_announcements")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


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

LOG_ETL_SQL = """
INSERT INTO etl_runs (entity, customer_bin, records_fetched, records_inserted,
                      records_updated, records_skipped, errors, status, duration_sec, completed_at)
VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8, NOW())
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


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=0, help="Limit source_trd_buy IDs (0 = no limit)")
    p.add_argument("--concurrency", type=int, default=6, help="Parallel source IDs")
    p.add_argument("--dry-run", action="store_true", help="Inspect only, no DB writes")
    return p.parse_args()


def _rows_affected(command_tag: str) -> int:
    try:
        return int(command_tag.rsplit(" ", 1)[-1])
    except Exception:
        return 0


async def ensure_announcement_refs(conn, status_id: int | None, trade_method_id: int | None) -> None:
    # Backfill must not fail on unseen reference IDs from historical trd-buy payloads.
    if status_id:
        await conn.execute(UPSERT_LOT_STATUS_REF_SQL, status_id)
    if trade_method_id:
        await conn.execute(UPSERT_TRADE_METHOD_REF_SQL, trade_method_id)


async def main():
    import asyncpg

    args = parse_args()
    cfg = get_config()
    db_url = resolve_db_url(cfg.db_url)
    t_start = time.monotonic()

    pool = await asyncpg.create_pool(db_url, min_size=1, max_size=max(4, args.concurrency * 2), timeout=15)
    async with pool.acquire() as conn:
        await conn.execute("ALTER TABLE lots ADD COLUMN IF NOT EXISTS source_trd_buy_id BIGINT")
        await conn.execute("ALTER TABLE contracts ADD COLUMN IF NOT EXISTS source_trd_buy_id BIGINT")
        source_ids = await conn.fetch(
            """
            WITH src AS (
                SELECT DISTINCT source_trd_buy_id AS sid
                FROM lots
                WHERE source_trd_buy_id IS NOT NULL AND source_trd_buy_id > 0
                UNION
                SELECT DISTINCT source_trd_buy_id AS sid
                FROM contracts
                WHERE source_trd_buy_id IS NOT NULL AND source_trd_buy_id > 0
            )
            SELECT src.sid
            FROM src
            WHERE NOT EXISTS (
                    SELECT 1
                    FROM announcements a
                    WHERE a.id = src.sid
                  )
               OR EXISTS (
                    SELECT 1
                    FROM lots l
                    WHERE l.source_trd_buy_id = src.sid
                      AND l.announcement_id IS NULL
                  )
               OR EXISTS (
                    SELECT 1
                    FROM contracts c
                    WHERE c.source_trd_buy_id = src.sid
                      AND c.announcement_id IS NULL
                  )
            ORDER BY src.sid
            """
        )

    ids = [r["sid"] for r in source_ids if r["sid"]]
    if args.limit:
        ids = ids[: args.limit]

    logger.info("source_trd_buy IDs to backfill: %d", len(ids))

    sem = asyncio.Semaphore(max(1, args.concurrency))
    lock = asyncio.Lock()
    stats = {
        "fetched": 0,
        "upserted": 0,
        "lot_links": 0,
        "contract_links": 0,
        "skipped": 0,
        "errors": 0,
    }

    async with OWSClient(token=cfg.ows_token, base_url=cfg.ows_base_url) as client:
        async def process_sid(sid: int) -> None:
            async with sem:
                try:
                    ann = await client.get_trd_buy(sid)
                except Exception:
                    async with lock:
                        stats["errors"] += 1
                    return

                if not ann:
                    async with lock:
                        stats["skipped"] += 1
                    return

                organizer_bin = ann.get("org_bin")
                customer_bin = ann.get("customer_bin") or organizer_bin
                publish_dt = parse_dt(ann.get("publish_date"))
                end_dt = parse_dt(ann.get("end_date"))

                if args.dry_run:
                    async with lock:
                        stats["fetched"] += 1
                    return

                async with pool.acquire() as conn:
                    subject_cache: set[str] = set()
                    if organizer_bin:
                        await ensure_subject(conn, client, organizer_bin, is_supplier=False, cache=subject_cache)
                    if customer_bin and customer_bin != organizer_bin:
                        await ensure_subject(conn, client, customer_bin, is_supplier=False, cache=subject_cache)

                    status_id = safe_int(ann.get("ref_buy_status_id"))
                    trade_method_id = safe_int(ann.get("ref_trade_methods_id"))
                    await ensure_announcement_refs(conn, status_id, trade_method_id)

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

                    lot_tag = await conn.execute(
                        """
                        UPDATE lots
                        SET announcement_id=$1, synced_at=NOW()
                        WHERE source_trd_buy_id=$1 AND announcement_id IS NULL
                        """,
                        sid,
                    )
                    contract_tag = await conn.execute(
                        """
                        UPDATE contracts
                        SET announcement_id=$1, synced_at=NOW()
                        WHERE source_trd_buy_id=$1 AND announcement_id IS NULL
                        """,
                        sid,
                    )

                    lot_links = _rows_affected(lot_tag)
                    contract_links = _rows_affected(contract_tag)

                async with lock:
                    stats["fetched"] += 1
                    stats["upserted"] += 1
                    stats["lot_links"] += lot_links
                    stats["contract_links"] += contract_links

        await asyncio.gather(*(process_sid(sid) for sid in ids))

    status = "ok" if stats["errors"] == 0 else ("partial" if stats["upserted"] > 0 else "failed")
    async with pool.acquire() as conn:
        await conn.execute(
            LOG_ETL_SQL,
            "backfill_announcements",
            stats["fetched"],
            stats["upserted"],
            0,
            stats["skipped"],
            stats["errors"],
            status,
            round(time.monotonic() - t_start, 2),
        )
    await pool.close()

    logger.info(
        "backfill_announcements: fetched=%d upserted=%d lot_links=%d contract_links=%d skipped=%d errors=%d",
        stats["fetched"],
        stats["upserted"],
        stats["lot_links"],
        stats["contract_links"],
        stats["skipped"],
        stats["errors"],
    )


if __name__ == "__main__":
    asyncio.run(main())
