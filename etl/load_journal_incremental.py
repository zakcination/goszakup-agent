"""
etl/load_journal_incremental.py
-------------------------------
Incremental updates using /v3/journal (last day by default).
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.client import OWSClient
from etl.config import get_config
from etl.utils import (
    ensure_enstru_ref,
    ensure_unit_ref,
    ensure_subject,
    load_env,
    maybe_reexec_in_venv,
    normalize_name,
    parse_dt,
    resolve_db_url,
    safe_float,
    safe_int,
)

maybe_reexec_in_venv()
load_env()

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("etl.journal")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--date-from", type=str, default=None, help="YYYY-MM-DD")
    p.add_argument("--date-to", type=str, default=None, help="YYYY-MM-DD")
    return p.parse_args()


def _first_int(data: dict, keys: tuple[str, ...]) -> int | None:
    for key in keys:
        value = safe_int(data.get(key))
        if value is not None:
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


def _first_dt(data: dict, keys: tuple[str, ...]):
    for key in keys:
        dt = parse_dt(data.get(key))
        if dt:
            return dt
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


async def _upsert_contract_with_fallback(conn, params: list):
    try:
        await conn.execute(UPSERT_CONTRACT_SQL, *params)
        return
    except Exception as exc:
        if "contracts_contract_number_key" in str(exc):
            params2 = list(params)
            params2[1] = None
            await conn.execute(UPSERT_CONTRACT_SQL, *params2)
            return
        raise


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


async def load_incremental():
    import asyncpg

    args = parse_args()
    cfg = get_config()

    if args.date_from and args.date_to:
        date_from = args.date_from
        date_to = args.date_to
    else:
        today = date.today()
        date_from = (today - timedelta(days=1)).isoformat()
        date_to = today.isoformat()

    db_url = resolve_db_url(cfg.db_url)
    conn = await asyncpg.connect(db_url, timeout=15)
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS journal_entries (
            id          BIGSERIAL PRIMARY KEY,
            entity      VARCHAR(40),
            object_id   BIGINT,
            customer_bin VARCHAR(12),
            payload     JSONB,
            occurred_at TIMESTAMPTZ,
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    await conn.execute("ALTER TABLE lots ADD COLUMN IF NOT EXISTS source_trd_buy_id BIGINT")
    await conn.execute("ALTER TABLE contracts ADD COLUMN IF NOT EXISTS source_trd_buy_id BIGINT")
    await conn.execute("ALTER TABLE contract_items ADD COLUMN IF NOT EXISTS pln_point_id BIGINT")

    async with OWSClient(token=cfg.ows_token, base_url=cfg.ows_base_url) as client:
        entries = await client.get_journal(date_from, date_to)
        logger.info("journal entries: %d", len(entries))

        subject_cache: set[str] = set()
        enstru_cache: set[str] = set()
        unit_cache: set[int] = set()
        ann_trade_method_cache: dict[int, int | None] = {}
        ann_unique_lot_cache: dict[int, int | None] = {}
        valid_trade_methods: set[int] = {
            row["id"] for row in await conn.fetch("SELECT id FROM trade_methods_ref")
        }

        for e in entries:
            entity = e.get("entity") or e.get("table") or e.get("model") or e.get("entity_type")
            object_id = e.get("object_id") or e.get("id") or e.get("entity_id")
            customer_bin = e.get("customer_bin") or e.get("bin") or e.get("org_bin")
            occurred_at = parse_dt(e.get("occurred_at") or e.get("created_at") or e.get("date"))
            await conn.execute(
                "INSERT INTO journal_entries (entity, object_id, customer_bin, payload, occurred_at) VALUES ($1,$2,$3,$4,$5)",
                entity,
                safe_int(object_id),
                customer_bin,
                e,
                occurred_at,
            )

            if not object_id:
                continue

            # Update affected entities
            if entity in ("trd_buy", "announcements", "announcement"):
                ann = await client.get_trd_buy(int(object_id))
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

            elif entity in ("lots", "lot"):
                lot = await client.get_lot(int(object_id))
                if lot:
                    ann_id = _first_int(lot, ("trd_buy_id", "trdBuyId", "announcement_id")) or None
                    amount = safe_float(lot.get("amount"))
                    quantity = safe_float(lot.get("count"))
                    if quantity is None:
                        quantity = safe_float(lot.get("quantity"))
                    unit_price = safe_float(lot.get("unit_price"))
                    if unit_price is None:
                        unit_price = safe_float(lot.get("price"))
                    if unit_price is None and amount and quantity:
                        unit_price = (amount / quantity) if quantity != 0 else None
                    enstru_code = _first_str(lot, ("ref_enstru_code", "enstru_code", "tru_code"))
                    unit_code = _first_int(lot, ("ref_units_code", "ref_unit_code", "unit_code", "unit_id"))
                    point_list = lot.get("point_list") or []
                    if point_list and (not enstru_code or not unit_code):
                        # Use first plan point for units/enstru if available
                        pid = safe_int(point_list[0])
                        if pid:
                            row = await conn.fetchrow("SELECT enstru_code, unit_code FROM plan_points WHERE id=$1", pid)
                            if row:
                                enstru_code = enstru_code or row["enstru_code"]
                                unit_code = unit_code or row["unit_code"]
                    if enstru_code:
                        await ensure_enstru_ref(conn, enstru_code, enstru_cache)
                    await ensure_unit_ref(conn, unit_code, unit_cache)
                    kato_code = (
                        _first_str(lot, ("ref_kato_code", "kato_delivery", "kato_code"))
                        or _extract_kato_from_delivery_places(lot)
                    )
                    await conn.execute(
                        UPSERT_LOT_SQL,
                        safe_int(lot.get("id")),
                        ann_id,
                        _first_int(lot, ("trd_buy_id", "trdBuyId", "announcement_id")),
                        _first_str(lot, ("customer_bin", "customer_biin", "org_bin")),
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

            elif entity in ("contract", "contracts"):
                c = await client.get_contract(int(object_id))
                if c:
                    supplier_bin = _first_str(c, ("supplier_biin", "supplier_bin"))
                    if supplier_bin:
                        await ensure_subject(conn, client, supplier_bin, is_supplier=True, cache=subject_cache)
                    trd_buy_id = _first_int(c, ("trd_buy_id", "trdBuyId", "announcement_id", "trd_buy"))
                    ann_id = trd_buy_id or None
                    sign_dt = _first_dt(c, ("crdate", "sign_date", "contract_date"))
                    start_dt = _first_dt(c, ("start_date", "date_begin", "date_start"))
                    end_dt = _first_dt(c, ("end_date", "date_end", "date_finish"))
                    lot_id = _first_int(c, ("lot_id", "lotId", "ref_lot_id"))
                    trade_method_id = _first_int(
                        c,
                        ("ref_trade_methods_id", "trade_method_id", "ref_trade_method_id"),
                    )
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
                    params = [
                        safe_int(c.get("id")),
                        c.get("contract_number"),
                        ann_id,
                        trd_buy_id,
                        lot_id,
                        _first_str(c, ("customer_bin", "customer_biin", "org_bin", "bin")),
                        supplier_bin,
                        safe_float(c.get("contract_sum")),
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

            elif entity in ("contract_items", "contract_units"):
                units = await client.get_contract_units(int(object_id))
                for u in units:
                    item_id = safe_int(u.get("id"))
                    if not item_id:
                        continue
                    pln_point_id = safe_int(u.get("pln_point_id"))
                    enstru_code = None
                    unit_code = None
                    name_ru = None
                    if pln_point_id:
                        row = await conn.fetchrow("SELECT enstru_code, unit_code, name_ru FROM plan_points WHERE id=$1", pln_point_id)
                        if row:
                            enstru_code, unit_code, name_ru = row["enstru_code"], row["unit_code"], row["name_ru"]
                    if enstru_code:
                        await ensure_enstru_ref(conn, enstru_code, enstru_cache)
                    await ensure_unit_ref(conn, unit_code, unit_cache)
                    quantity = u.get("quantity")
                    unit_price = u.get("item_price")
                    total_price = u.get("total_sum")
                    await conn.execute(
                        UPSERT_ITEM_SQL,
                        item_id,
                        int(object_id),
                        pln_point_id,
                        enstru_code,
                        name_ru,
                        normalize_name(name_ru),
                        unit_code,
                        quantity,
                        unit_price,
                        total_price,
                        bool(quantity) and bool(unit_price),
                    )

    await conn.close()


if __name__ == "__main__":
    asyncio.run(load_incremental())
