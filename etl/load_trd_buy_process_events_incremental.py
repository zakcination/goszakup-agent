"""
etl/load_trd_buy_process_events_incremental.py
----------------------------------------------
Incremental loader for:
- /v3/trd-buy/{id}/cancel
- /v3/trd-buy/{id}/pause

Persists process-risk evidence into raw_trd_buy_events.
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
logger = logging.getLogger("etl.trd_buy_process_events")

STATE_ENTITY = "trd_buy_process_events_worker"
STATE_SCOPE = "__global__"

UPSERT_EVENT_SQL = """
INSERT INTO raw_trd_buy_events (
    trd_buy_id, event_type, has_event, payload, checked_at, synced_at
)
VALUES ($1, $2, $3, $4, NOW(), NOW())
ON CONFLICT (trd_buy_id, event_type) DO UPDATE SET
    has_event = EXCLUDED.has_event,
    payload = EXCLUDED.payload,
    checked_at = NOW(),
    synced_at = NOW()
"""

LOG_ETL_SQL = """
INSERT INTO etl_runs (entity, customer_bin, records_fetched, records_inserted,
                      records_updated, records_skipped, errors, status, duration_sec, completed_at)
VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8, NOW())
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--year-from", type=int, default=None)
    p.add_argument("--year-to", type=int, default=None)
    p.add_argument("--limit", type=int, default=0, help="Max announcement IDs per run (0 = no limit)")
    p.add_argument(
        "--checkpoint-every",
        type=int,
        default=int(os.environ.get("RAW_WORKER_CHECKPOINT_EVERY", "1")),
        help="Persist checkpoint every N announcements",
    )
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def _has_event(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    items = payload.get("items")
    if isinstance(items, list):
        return len(items) > 0
    total = payload.get("total")
    if isinstance(total, int):
        return total > 0
    # Some endpoints may return flat objects when event exists.
    return bool(payload)


async def _ensure_tables(conn) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_trd_buy_events (
            trd_buy_id BIGINT NOT NULL,
            event_type VARCHAR(20) NOT NULL,
            has_event BOOLEAN NOT NULL DEFAULT FALSE,
            payload JSONB NOT NULL,
            checked_at TIMESTAMPTZ DEFAULT NOW(),
            synced_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (trd_buy_id, event_type)
        )
        """
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_raw_trd_buy_events_type ON raw_trd_buy_events(event_type, checked_at DESC)"
    )


async def main() -> None:
    import asyncpg

    args = parse_args()
    cfg = get_config()
    year_from = args.year_from or cfg.data_year_from
    year_to = args.year_to or cfg.data_year_to
    checkpoint_every = max(1, args.checkpoint_every)

    db_url = resolve_db_url(cfg.db_url)
    conn = await asyncpg.connect(db_url, timeout=15)
    await ensure_etl_state_table(conn)
    await _ensure_tables(conn)

    prev_last_id, _ = await get_etl_state(conn, STATE_ENTITY, STATE_SCOPE)
    last_id = prev_last_id or 0

    params = [year_from, year_to, last_id]
    limit_sql = ""
    if args.limit and args.limit > 0:
        limit_sql = "LIMIT $4"
        params.append(args.limit)

    announcement_rows = await conn.fetch(
        f"""
        SELECT id
        FROM announcements
        WHERE publish_date IS NOT NULL
          AND EXTRACT(YEAR FROM publish_date)::INT BETWEEN $1 AND $2
          AND id > $3
        ORDER BY id
        {limit_sql}
        """,
        *params,
    )
    ann_ids = [r["id"] for r in announcement_rows]

    fetched = 0
    upserted = 0
    skipped = 0
    errors = 0
    max_seen_id = last_id
    t0 = time.monotonic()

    async with OWSClient(token=cfg.ows_token, base_url=cfg.ows_base_url) as client:
        for idx, trd_buy_id in enumerate(ann_ids, start=1):
            fetched += 1
            max_seen_id = max(max_seen_id, trd_buy_id)

            try:
                cancel_payload = await client.get_trd_buy_cancel(trd_buy_id)
            except Exception:
                cancel_payload = {}
            try:
                pause_payload = await client.get_trd_buy_pause(trd_buy_id)
            except Exception:
                pause_payload = {}

            if args.dry_run:
                continue

            try:
                await conn.execute(
                    UPSERT_EVENT_SQL,
                    trd_buy_id,
                    "cancel",
                    _has_event(cancel_payload),
                    json.dumps(cancel_payload or {}, ensure_ascii=False),
                )
                await conn.execute(
                    UPSERT_EVENT_SQL,
                    trd_buy_id,
                    "pause",
                    _has_event(pause_payload),
                    json.dumps(pause_payload or {}, ensure_ascii=False),
                )
                upserted += 2
            except Exception:
                errors += 1

            if not args.dry_run and idx % checkpoint_every == 0:
                await update_etl_state(conn, STATE_ENTITY, STATE_SCOPE, max_seen_id, None)

    if not args.dry_run:
        await update_etl_state(conn, STATE_ENTITY, STATE_SCOPE, max_seen_id, None)

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
        "trd_buy_process_events_worker: fetched=%d upserted=%d errors=%d last_id=%d",
        fetched,
        upserted,
        errors,
        max_seen_id,
    )
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
