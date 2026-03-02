"""
etl/load_contracts.py
--------------------
Spiral 2 — Load contracts and ensure supplier subjects.
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
    ensure_etl_state_table,
    ensure_subject,
    get_etl_state,
    load_env,
    maybe_reexec_in_venv,
    parse_dt,
    resolve_db_url,
    safe_float,
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
logger = logging.getLogger("etl.contracts")


UPSERT_CONTRACT_SQL = """
INSERT INTO contracts (
    id, contract_number, announcement_id, source_trd_buy_id, lot_id, customer_bin, supplier_bin,
    contract_sum, sign_date, start_date, end_date, status_id, fin_year,
    trade_method_id, ref_contract_type_id, system_id, last_updated, synced_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10, $11, $12, $13,
    $14, $15, $16, $17, NOW()
)
ON CONFLICT (id) DO UPDATE SET
    contract_number     = EXCLUDED.contract_number,
    announcement_id     = EXCLUDED.announcement_id,
    source_trd_buy_id   = EXCLUDED.source_trd_buy_id,
    lot_id              = EXCLUDED.lot_id,
    customer_bin        = EXCLUDED.customer_bin,
    supplier_bin        = EXCLUDED.supplier_bin,
    contract_sum        = EXCLUDED.contract_sum,
    sign_date           = EXCLUDED.sign_date,
    start_date          = EXCLUDED.start_date,
    end_date            = EXCLUDED.end_date,
    status_id           = EXCLUDED.status_id,
    fin_year            = EXCLUDED.fin_year,
    trade_method_id     = EXCLUDED.trade_method_id,
    ref_contract_type_id= EXCLUDED.ref_contract_type_id,
    system_id           = EXCLUDED.system_id,
    last_updated        = EXCLUDED.last_updated,
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
        "--resolve-contract-view",
        action="store_true",
        default=os.environ.get("CONTRACTS_RESOLVE_VIEW", "0").lower() in ("1", "true", "yes", "on"),
        help="When key fields are missing, fetch /v3/contract/{id} for enrichment",
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


def _first_int(data: dict, keys: tuple[str, ...]) -> int | None:
    for key in keys:
        value = safe_int(data.get(key))
        if value is not None:
            return value
    return None


def _first_date(data: dict, keys: tuple[str, ...]):
    for key in keys:
        value = parse_dt(data.get(key))
        if value:
            return value
    return None


def _first_str(data: dict, keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str):
            value = value.strip()
            if value:
                return value
        elif value is not None:
            return str(value)
    return None


async def _upsert_contract_with_fallback(conn, params: list):
    try:
        await conn.execute(UPSERT_CONTRACT_SQL, *params)
        return
    except Exception as exc:
        # Some historical rows reuse contract numbers across distinct IDs.
        # Keep the row by dropping contract_number rather than losing the contract.
        if "contracts_contract_number_key" in str(exc):
            params2 = list(params)
            params2[1] = None
            await conn.execute(UPSERT_CONTRACT_SQL, *params2)
            return
        raise


async def load_contracts():
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
        await conn.execute("ALTER TABLE contracts ADD COLUMN IF NOT EXISTS source_trd_buy_id BIGINT")

    async with OWSClient(token=cfg.ows_token, base_url=cfg.ows_base_url) as client:
        sem = asyncio.Semaphore(concurrency)

        async def process_bin(bin_code: str) -> None:
            async with sem:
                async with pool.acquire() as conn:
                    subject_cache: set[str] = set()
                    ann_trade_method_cache: dict[int, int | None] = {}
                    ann_unique_lot_cache: dict[int, int | None] = {}
                    valid_trade_methods: set[int] = {
                        row["id"] for row in await conn.fetch("SELECT id FROM trade_methods_ref")
                    }

                    fetched = 0
                    upserted = 0
                    skipped = 0
                    errors = 0
                    t_start = time.monotonic()

                    try:
                        ann_rows = await conn.fetch(
                            """
                            SELECT id
                            FROM announcements
                            WHERE organizer_bin=$1 OR customer_bin=$1
                            """,
                            bin_code,
                        )
                        ann_ids: set[int] = {row["id"] for row in ann_rows}
                        ann_checked: set[int] = set(ann_ids)

                        state_last_id, state_resume_path = await get_etl_state(conn, "contracts", bin_code)
                        backstep = args.resume_backstep if args.resume_backstep is not None else cfg.resume_backstep
                        base_path = f"/v3/contract/customer/{bin_code}"
                        start_path = build_start_path(base_path, state_resume_path, state_last_id, backstep)

                        limit_reached = False
                        async for items, next_page in client.iter_rest_pages(base_path, start_path=start_path):
                            page_last_id: int | None = None
                            for c in items:
                                if args.limit and fetched >= args.limit:
                                    limit_reached = True
                                    break
                                fetched += 1

                                contract_id = safe_int(c.get("id"))
                                if contract_id is not None:
                                    page_last_id = contract_id

                                if contract_id is None:
                                    skipped += 1
                                    continue

                                sign_dt = _first_date(c, ("crdate", "sign_date", "contract_date"))
                                if sign_dt and (sign_dt.year < year_from or sign_dt.year > year_to):
                                    skipped += 1
                                    continue

                                supplier_bin = _first_str(c, ("supplier_biin", "supplier_bin"))
                                if supplier_bin:
                                    await ensure_subject(conn, client, supplier_bin, is_supplier=True, cache=subject_cache)

                                trd_buy_id = _first_int(c, ("trd_buy_id", "trdBuyId", "announcement_id", "trd_buy"))
                                ann_id = trd_buy_id or None
                                lot_id = _first_int(c, ("lot_id", "lotId", "ref_lot_id"))
                                trade_method_id = _first_int(
                                    c,
                                    ("ref_trade_methods_id", "trade_method_id", "ref_trade_method_id"),
                                )
                                start_dt = _first_date(c, ("start_date", "date_begin", "date_start"))
                                end_dt = _first_date(c, ("end_date", "date_end", "date_finish"))
                                customer_bin = _first_str(c, ("customer_bin", "customer_biin", "org_bin", "bin")) or bin_code
                                contract_sum = safe_float(c.get("contract_sum"))
                                if args.resolve_contract_view and (
                                    lot_id is None or trade_method_id is None or start_dt is None or end_dt is None
                                ):
                                    try:
                                        c_view = await client.get_contract(contract_id)
                                    except Exception:
                                        c_view = {}
                                    if c_view:
                                        trd_buy_id = trd_buy_id or _first_int(
                                            c_view,
                                            ("trd_buy_id", "trdBuyId", "announcement_id", "trd_buy"),
                                        )
                                        ann_id = trd_buy_id or ann_id
                                        lot_id = lot_id or _first_int(c_view, ("lot_id", "lotId", "ref_lot_id"))
                                        trade_method_id = trade_method_id or _first_int(
                                            c_view,
                                            ("ref_trade_methods_id", "trade_method_id", "ref_trade_method_id"),
                                        )
                                        start_dt = start_dt or _first_date(c_view, ("start_date", "date_begin", "date_start"))
                                        end_dt = end_dt or _first_date(c_view, ("end_date", "date_end", "date_finish"))
                                        if not customer_bin:
                                            customer_bin = _first_str(c_view, ("customer_bin", "customer_biin", "org_bin", "bin")) or bin_code
                                        if contract_sum is None:
                                            contract_sum = safe_float(c_view.get("contract_sum"))
                                fin_year = (
                                    sign_dt.year
                                    if sign_dt
                                    else (start_dt.year if start_dt else (end_dt.year if end_dt else safe_int(c.get("fin_year"))))
                                )

                                if ann_id and trade_method_id is None:
                                    if ann_id not in ann_trade_method_cache:
                                        ann_trade_method_cache[ann_id] = await conn.fetchval(
                                            "SELECT trade_method_id FROM announcements WHERE id=$1",
                                            ann_id,
                                        )
                                    trade_method_id = ann_trade_method_cache[ann_id]

                                if ann_id and lot_id is None:
                                    if ann_id not in ann_unique_lot_cache:
                                        lot_rows = await conn.fetch(
                                            "SELECT id FROM lots WHERE announcement_id=$1 ORDER BY id LIMIT 2",
                                            ann_id,
                                        )
                                        ann_unique_lot_cache[ann_id] = (
                                            lot_rows[0]["id"] if len(lot_rows) == 1 else None
                                        )
                                    lot_id = ann_unique_lot_cache[ann_id]

                                if trade_method_id is not None and trade_method_id not in valid_trade_methods:
                                    trade_method_id = None

                                if ann_id:
                                    if ann_id not in ann_checked:
                                        ann_checked.add(ann_id)
                                        if ann_id not in ann_ids:
                                            try:
                                                ann = await client.get_trd_buy(ann_id)
                                                if ann:
                                                    organizer_bin = ann.get("org_bin")
                                                    customer_bin = ann.get("customer_bin") or organizer_bin
                                                    if organizer_bin:
                                                        await ensure_subject(conn, client, organizer_bin, is_supplier=False, cache=subject_cache)
                                                    if customer_bin and customer_bin != organizer_bin:
                                                        await ensure_subject(conn, client, customer_bin, is_supplier=False, cache=subject_cache)

                                                    publish_dt = parse_dt(ann.get("publish_date"))
                                                    end_dt = parse_dt(ann.get("end_date"))
                                                    await conn.execute(
                                                        """
                                                        INSERT INTO announcements (
                                                            id, number_anno, customer_bin, organizer_bin, name_ru, total_sum,
                                                            publish_date, end_date, status_id, trade_method_id, system_id, last_updated, synced_at
                                                        )
                                                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
                                                        ON CONFLICT (id) DO NOTHING
                                                        """,
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
                                                    ann_ids.add(ann_id)
                                                else:
                                                    ann_id = None
                                            except Exception:
                                                ann_id = None

                                if args.dry_run:
                                    upserted += 1
                                    continue

                                try:
                                    params = [
                                        contract_id,
                                        c.get("contract_number"),
                                        ann_id,
                                        trd_buy_id,
                                        lot_id,
                                        customer_bin,
                                        supplier_bin,
                                        contract_sum,
                                        sign_dt.date() if sign_dt else None,
                                        start_dt.date() if start_dt else None,
                                        end_dt.date() if end_dt else None,
                                        safe_int(c.get("ref_contract_status_id")),
                                        fin_year,
                                        trade_method_id,
                                        safe_int(c.get("ref_contract_type_id")),
                                        safe_int(c.get("system_id")),
                                        parse_dt(c.get("index_date")),
                                    ]
                                    await _upsert_contract_with_fallback(conn, params)
                                    upserted += 1
                                except Exception as exc:
                                    errors += 1
                                    logger.debug("BIN %s contract %s upsert failed: %s", bin_code, contract_id, exc)

                            if not args.dry_run:
                                new_last_id = page_last_id or state_last_id
                                resume_path = next_page
                                if limit_reached and page_last_id:
                                    resume_path = f"{base_path}?page=next&search_after={page_last_id}"
                                await update_etl_state(conn, "contracts", bin_code, new_last_id, resume_path)
                                state_last_id = new_last_id

                            if limit_reached:
                                break
                    except Exception as e:
                        errors += 1
                        logger.exception("BIN %s: fatal contracts ingestion error: %s", bin_code, e)

                    status = "ok" if errors == 0 else ("partial" if upserted > 0 else "failed")
                    await conn.execute(
                        LOG_ETL_SQL,
                        "contracts",
                        bin_code,
                        fetched,
                        upserted,
                        0,
                        skipped,
                        errors,
                        status,
                        round(time.monotonic() - t_start, 2),
                    )
                    logger.info("BIN %s: contracts upserted=%d fetched=%d skipped=%d errors=%d", bin_code, upserted, fetched, skipped, errors)

        await asyncio.gather(*(process_bin(b) for b in bins))

    await pool.close()


if __name__ == "__main__":
    asyncio.run(load_contracts())
