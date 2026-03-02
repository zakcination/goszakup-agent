"""
etl/load_plans_spec_incremental.py
----------------------------------
Incremental checkpointed loader for /v3/plans/spec.

Writes raw rows to raw_plans_spec and backfills missing numeric plan metadata.
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
logger = logging.getLogger("etl.plans_spec_incremental")

STATE_ENTITY = "plans_spec_worker"
STATE_SCOPE = "__global__"

UPSERT_RAW_SQL = """
INSERT INTO raw_plans_spec (
    id, pln_points_id, ekrb_code, fkrb_program_code, amount, index_date, payload, synced_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
ON CONFLICT (id) DO UPDATE SET
    pln_points_id = EXCLUDED.pln_points_id,
    ekrb_code = EXCLUDED.ekrb_code,
    fkrb_program_code = EXCLUDED.fkrb_program_code,
    amount = EXCLUDED.amount,
    index_date = EXCLUDED.index_date,
    payload = EXCLUDED.payload,
    synced_at = NOW()
"""

LOG_ETL_SQL = """
INSERT INTO etl_runs (entity, customer_bin, records_fetched, records_inserted,
                      records_updated, records_skipped, errors, status, duration_sec, completed_at)
VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8, NOW())
"""


def _rows_affected(tag: str) -> int:
    try:
        return int(tag.rsplit(" ", 1)[-1])
    except Exception:
        return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--bins", type=str, default="", help="Comma-separated BINs (default: TARGET_BINS)")
    p.add_argument("--year-from", type=int, default=None)
    p.add_argument("--year-to", type=int, default=None)
    p.add_argument(
        "--refresh-mode",
        choices=("incremental", "full"),
        default=os.environ.get("PLANS_SPEC_REFRESH_MODE", "incremental"),
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


def parse_bins(arg: str, fallback: tuple[str, ...]) -> list[str]:
    if arg:
        return [b.strip() for b in arg.split(",") if b.strip()]
    return list(fallback)


async def _ensure_tables(conn) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_plans_spec (
            id BIGINT PRIMARY KEY,
            pln_points_id BIGINT NOT NULL,
            ekrb_code VARCHAR(30),
            fkrb_program_code VARCHAR(30),
            amount NUMERIC(18,4),
            index_date TIMESTAMPTZ,
            payload JSONB NOT NULL,
            synced_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_plans_spec_point ON raw_plans_spec(pln_points_id)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_plans_spec_ekrb ON raw_plans_spec(ekrb_code)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_plans_spec_index_date ON raw_plans_spec(index_date)")


async def main() -> None:
    import asyncpg

    args = parse_args()
    cfg = get_config()
    bins = parse_bins(args.bins, cfg.target_bins)
    year_from = args.year_from or cfg.data_year_from
    year_to = args.year_to or cfg.data_year_to

    db_url = resolve_db_url(cfg.db_url)
    conn = await asyncpg.connect(db_url, timeout=15)
    await ensure_etl_state_table(conn)
    await _ensure_tables(conn)

    prev_last_id, resume_path = await get_etl_state(conn, STATE_ENTITY, STATE_SCOPE)
    last_id = prev_last_id or 0
    start_path = resume_path or "/v3/plans/spec"
    resuming = bool(resume_path)
    checkpoint_every = max(1, args.checkpoint_every)
    batch_size = max(1, args.batch_size)

    fetched = 0
    upserted = 0
    backfilled = 0
    skipped = 0
    errors = 0
    scanned_pages = 0
    max_seen_id = last_id
    t0 = time.monotonic()

    async with OWSClient(token=cfg.ows_token, base_url=cfg.ows_base_url) as client:
        final_resume = start_path
        should_clear_resume = False
        limit_hit = False

        async for items, next_page in client.iter_rest_pages("/v3/plans/spec", start_path=start_path):
            scanned_pages += 1
            if args.limit_pages and scanned_pages > args.limit_pages:
                limit_hit = True
                break

            batch: list[tuple] = []
            plan_ids: set[int] = set()
            page_has_new = False

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
                plan_id = safe_int(row.get("pln_points_id"))
                if plan_id:
                    plan_ids.add(plan_id)

                batch.append(
                    (
                        row_id,
                        plan_id,
                        row.get("ekrb_code"),
                        row.get("fkrb_program_code"),
                        row.get("amount"),
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

            if not args.dry_run and plan_ids:
                tag = await conn.execute(
                    """
                    UPDATE plan_points p
                    SET quantity = COALESCE(
                            p.quantity,
                            NULLIF(r.payload->>'count', '')::NUMERIC
                        ),
                        unit_price = COALESCE(
                            p.unit_price,
                            NULLIF(r.payload->>'price', '')::NUMERIC
                        ),
                        total_amount = COALESCE(p.total_amount, r.amount),
                        synced_at = NOW()
                    FROM raw_plans_spec r
                    WHERE p.id = r.pln_points_id
                      AND p.id = ANY($1::BIGINT[])
                      AND p.customer_bin = ANY($2::VARCHAR[])
                      AND p.fin_year BETWEEN $3 AND $4
                      AND (p.quantity IS NULL OR p.unit_price IS NULL OR p.total_amount IS NULL)
                    """,
                    list(plan_ids),
                    bins,
                    year_from,
                    year_to,
                )
                backfilled += _rows_affected(tag)

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
        backfilled,
        skipped,
        errors,
        status,
        round(time.monotonic() - t0, 2),
    )
    logger.info(
        "plans_spec_worker: mode=%s pages=%d fetched=%d upserted=%d backfilled=%d skipped=%d errors=%d",
        args.refresh_mode,
        scanned_pages,
        fetched,
        upserted,
        backfilled,
        skipped,
        errors,
    )
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
