"""
etl/load_treasury_pay_incremental.py
------------------------------------
Incremental checkpointed loader for /v3/treasury-pay into raw_treasury_pay.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.client import OWSClient
from etl.config import get_config
from etl.utils import (
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
logger = logging.getLogger("etl.treasury_pay_incremental")

STATE_ENTITY = "treasury_pay_worker"
STATE_SCOPE = "__global__"

UPSERT_RAW_SQL = """
INSERT INTO raw_treasury_pay (
    id, contract_id, kato, item_description, pay_amount, pay_date, index_date, payload, synced_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
ON CONFLICT (id) DO UPDATE SET
    contract_id = EXCLUDED.contract_id,
    kato = EXCLUDED.kato,
    item_description = EXCLUDED.item_description,
    pay_amount = EXCLUDED.pay_amount,
    pay_date = EXCLUDED.pay_date,
    index_date = EXCLUDED.index_date,
    payload = EXCLUDED.payload,
    synced_at = NOW()
"""

LOG_ETL_SQL = """
INSERT INTO etl_runs (entity, customer_bin, records_fetched, records_inserted,
                      records_updated, records_skipped, errors, status, duration_sec, completed_at)
VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8, NOW())
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--refresh-mode",
        choices=("incremental", "full"),
        default=os.environ.get("TREASURY_PAY_REFRESH_MODE", "incremental"),
    )
    p.add_argument(
        "--checkpoint-every",
        type=int,
        default=int(os.environ.get("RAW_WORKER_CHECKPOINT_EVERY", "1")),
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=int(os.environ.get("RAW_WORKER_BATCH_SIZE", "500")),
    )
    p.add_argument("--limit-pages", type=int, default=0)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


async def _ensure_tables(conn) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_treasury_pay (
            id BIGINT PRIMARY KEY,
            contract_id BIGINT,
            kato VARCHAR(20),
            item_description TEXT,
            pay_amount NUMERIC(18,4),
            pay_date DATE,
            index_date TIMESTAMPTZ,
            payload JSONB NOT NULL,
            synced_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_treasury_contract ON raw_treasury_pay(contract_id)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_treasury_kato ON raw_treasury_pay(kato)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_treasury_pay_date ON raw_treasury_pay(pay_date)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_treasury_index_date ON raw_treasury_pay(index_date)")


async def main() -> None:
    import asyncpg

    args = parse_args()
    cfg = get_config()
    db_url = resolve_db_url(cfg.db_url)

    conn = await asyncpg.connect(db_url, timeout=15)
    await ensure_etl_state_table(conn)
    await _ensure_tables(conn)

    prev_last_id, resume_path = await get_etl_state(conn, STATE_ENTITY, STATE_SCOPE)
    last_id = prev_last_id or 0
    start_path = resume_path or "/v3/treasury-pay"
    resuming = bool(resume_path)
    checkpoint_every = max(1, args.checkpoint_every)
    batch_size = max(1, args.batch_size)

    fetched = 0
    upserted = 0
    skipped = 0
    errors = 0
    scanned_pages = 0
    max_seen_id = last_id
    t0 = time.monotonic()

    async with OWSClient(token=cfg.ows_token, base_url=cfg.ows_base_url) as client:
        final_resume = start_path
        should_clear_resume = False
        limit_hit = False

        async for items, next_page in client.iter_rest_pages("/v3/treasury-pay", start_path=start_path):
            scanned_pages += 1
            if args.limit_pages and scanned_pages > args.limit_pages:
                limit_hit = True
                break

            page_has_new = False
            batch: list[tuple] = []

            for row in items:
                fetched += 1
                row_id = safe_int(row.get("id"))
                if row_id is not None:
                    max_seen_id = max(max_seen_id, row_id)

                if (
                    args.refresh_mode == "incremental"
                    and not resuming
                    and last_id > 0
                    and row_id is not None
                    and row_id <= last_id
                ):
                    skipped += 1
                    continue

                page_has_new = True
                pay_dt = parse_dt(row.get("pay_date"))
                batch.append(
                    (
                        row_id,
                        safe_int(row.get("contract_id")),
                        row.get("kato"),
                        row.get("item_description"),
                        row.get("pay_amount"),
                        pay_dt.date() if pay_dt else None,
                        parse_dt(row.get("index_date")),
                        json.dumps(row, ensure_ascii=False),
                    )
                )

                if not args.dry_run and len(batch) >= batch_size:
                    try:
                        await conn.executemany(UPSERT_RAW_SQL, batch)
                        upserted += len(batch)
                    except Exception:
                        errors += len(batch)
                    batch = []

            if not args.dry_run and batch:
                try:
                    await conn.executemany(UPSERT_RAW_SQL, batch)
                    upserted += len(batch)
                except Exception:
                    errors += len(batch)

            final_resume = next_page
            if not args.dry_run and scanned_pages % checkpoint_every == 0:
                await update_etl_state(conn, STATE_ENTITY, STATE_SCOPE, max_seen_id, final_resume)

            if (
                args.refresh_mode == "incremental"
                and not resuming
                and last_id > 0
                and not page_has_new
            ):
                should_clear_resume = True
                break

            if not next_page:
                should_clear_resume = True
                break

            resuming = False

        if not args.dry_run:
            if limit_hit:
                await update_etl_state(conn, STATE_ENTITY, STATE_SCOPE, max_seen_id, final_resume)
            else:
                await update_etl_state(
                    conn,
                    STATE_ENTITY,
                    STATE_SCOPE,
                    max_seen_id,
                    None if should_clear_resume else final_resume,
                )

    status = "ok" if errors == 0 else ("partial" if upserted > 0 else "failed")
    await conn.execute(
        LOG_ETL_SQL,
        STATE_ENTITY,
        fetched,
        upserted,
        0,
        skipped,
        errors,
        status,
        round(time.monotonic() - t0, 2),
    )
    logger.info(
        "treasury_pay_worker: mode=%s pages=%d fetched=%d upserted=%d skipped=%d errors=%d",
        args.refresh_mode,
        scanned_pages,
        fetched,
        upserted,
        skipped,
        errors,
    )
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
