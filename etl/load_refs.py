"""
etl/load_refs.py
---------------
Spiral 2 — Load reference tables (units, trade methods, statuses, KATO).
"""

import asyncio
import argparse
import logging
import os
import sys
import time
from pathlib import Path

# Ensure project root is in path
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.client import OWSClient
from etl.config import get_config
from etl.utils import load_env, maybe_reexec_in_venv, normalize_name, resolve_db_url, safe_int

maybe_reexec_in_venv()
load_env()

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("etl.refs")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--force-kato",
        action="store_true",
        help="Force full /v3/refs/ref_kato reload even when kato_ref already looks complete",
    )
    return p.parse_args()


UPSERT_UNITS_SQL = """
INSERT INTO units_ref (code, name_ru, name_kz, name_norm, aliases)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (code) DO UPDATE SET
    name_ru   = EXCLUDED.name_ru,
    name_kz   = EXCLUDED.name_kz,
    name_norm = EXCLUDED.name_norm,
    aliases   = EXCLUDED.aliases
"""

UPSERT_TRADE_METHODS_SQL = """
INSERT INTO trade_methods_ref (id, name_ru, name_kz, code)
VALUES ($1, $2, $3, $4)
ON CONFLICT (id) DO UPDATE SET
    name_ru = EXCLUDED.name_ru,
    name_kz = EXCLUDED.name_kz,
    code    = EXCLUDED.code
"""

UPSERT_LOT_STATUS_SQL = """
INSERT INTO lot_statuses_ref (id, name_ru, name_kz)
VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE SET
    name_ru = EXCLUDED.name_ru,
    name_kz = EXCLUDED.name_kz
"""

UPSERT_CONTRACT_STATUS_SQL = """
INSERT INTO contract_statuses_ref (id, name_ru, name_kz)
VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE SET
    name_ru = EXCLUDED.name_ru,
    name_kz = EXCLUDED.name_kz
"""

UPSERT_KATO_SQL = """
INSERT INTO kato_ref (code, name_ru, name_kz, level, parent_code, region_id)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (code) DO UPDATE SET
    name_ru = EXCLUDED.name_ru,
    name_kz = EXCLUDED.name_kz,
    level = EXCLUDED.level,
    parent_code = EXCLUDED.parent_code,
    region_id = EXCLUDED.region_id
"""

LOG_ETL_SQL = """
INSERT INTO etl_runs (entity, customer_bin, records_fetched, records_inserted,
                      records_updated, records_skipped, errors, status, duration_sec, completed_at)
VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8, NOW())
"""


def _derive_region_id(code: str | None, parent_code: str | None) -> str | None:
    candidate = (code or parent_code or "").strip()
    if len(candidate) < 3:
        return None
    prefix = candidate[:3]
    if not prefix.isdigit():
        return None
    return f"{prefix}000000"


async def _fetch_ref_items_or_skip(
    conn,
    fetch_coro,
    *,
    table_name: str,
    entity_name: str,
) -> tuple[list[dict], bool]:
    """
    Fetch reference payload from OWS.
    If transient failures happen and local reference table is already populated,
    keep existing rows and skip this refresh to avoid aborting full recovery.
    """
    existing = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
    try:
        items = await fetch_coro()
        return items, False
    except Exception as e:
        if int(existing or 0) > 0:
            logger.warning(
                "%s refresh skipped: fetch failed (%s), keeping %d existing rows",
                entity_name,
                e,
                existing,
            )
            await conn.execute(
                LOG_ETL_SQL,
                entity_name,
                0,
                0,
                0,
                0,
                1,
                "partial",
                0,
            )
            return [], True
        raise


async def load_refs():
    import asyncpg

    args = parse_args()
    cfg = get_config()
    db_url = resolve_db_url(cfg.db_url)

    conn = await asyncpg.connect(db_url, timeout=15)

    async with OWSClient(token=cfg.ows_token, base_url=cfg.ows_base_url) as client:
        # Units
        section_start = time.monotonic()
        errors = 0
        items, skipped = await _fetch_ref_items_or_skip(
            conn,
            client.get_ref_units,
            table_name="units_ref",
            entity_name="ref_units",
        )
        if skipped:
            logger.info("ref_units: skipped (existing rows retained)")
        fetched = len(items)
        if skipped:
            fetched = 0
        upserted = 0
        for it in items:
            code = safe_int(it.get("code"))
            if code is None:
                continue
            name_ru = it.get("name_ru") or it.get("name_kz") or str(code)
            name_kz = it.get("name_kz")
            name_norm = normalize_name(name_ru) or str(code)
            aliases = [a for a in [it.get("alpha_code"), it.get("code2")] if a]
            try:
                await conn.execute(UPSERT_UNITS_SQL, code, name_ru, name_kz, name_norm, aliases)
                upserted += 1
            except Exception:
                errors += 1
        if not skipped:
            await conn.execute(LOG_ETL_SQL, "ref_units", fetched, upserted, 0, fetched - upserted, errors, "ok", round(time.monotonic() - section_start, 2))
            logger.info("ref_units: %d upserted (%d fetched)", upserted, fetched)

        # Trade methods
        section_start = time.monotonic()
        errors = 0
        items, skipped = await _fetch_ref_items_or_skip(
            conn,
            client.get_ref_trade_methods,
            table_name="trade_methods_ref",
            entity_name="ref_trade_methods",
        )
        if skipped:
            logger.info("ref_trade_methods: skipped (existing rows retained)")
        fetched = len(items)
        upserted = 0
        for it in items:
            id_ = safe_int(it.get("id"))
            if id_ is None:
                continue
            name_ru = it.get("name_ru") or str(id_)
            name_kz = it.get("name_kz")
            code = it.get("code")
            try:
                await conn.execute(UPSERT_TRADE_METHODS_SQL, id_, name_ru, name_kz, code)
                upserted += 1
            except Exception:
                errors += 1
        if not skipped:
            await conn.execute(LOG_ETL_SQL, "ref_trade_methods", fetched, upserted, 0, fetched - upserted, errors, "ok", round(time.monotonic() - section_start, 2))
            logger.info("ref_trade_methods: %d upserted (%d fetched)", upserted, fetched)

        # Lot statuses
        section_start = time.monotonic()
        errors = 0
        items, skipped = await _fetch_ref_items_or_skip(
            conn,
            client.get_ref_lot_statuses,
            table_name="lot_statuses_ref",
            entity_name="ref_lot_statuses",
        )
        if skipped:
            logger.info("ref_lot_statuses: skipped (existing rows retained)")
        fetched = len(items)
        upserted = 0
        for it in items:
            id_ = safe_int(it.get("id"))
            if id_ is None:
                continue
            name_ru = it.get("name_ru") or str(id_)
            name_kz = it.get("name_kz")
            try:
                await conn.execute(UPSERT_LOT_STATUS_SQL, id_, name_ru, name_kz)
                upserted += 1
            except Exception:
                errors += 1
        if not skipped:
            await conn.execute(LOG_ETL_SQL, "ref_lot_statuses", fetched, upserted, 0, fetched - upserted, errors, "ok", round(time.monotonic() - section_start, 2))
            logger.info("ref_lot_statuses: %d upserted (%d fetched)", upserted, fetched)

        # Contract statuses
        section_start = time.monotonic()
        errors = 0
        items, skipped = await _fetch_ref_items_or_skip(
            conn,
            client.get_ref_contract_statuses,
            table_name="contract_statuses_ref",
            entity_name="ref_contract_statuses",
        )
        if skipped:
            logger.info("ref_contract_statuses: skipped (existing rows retained)")
        fetched = len(items)
        upserted = 0
        for it in items:
            id_ = safe_int(it.get("id"))
            if id_ is None:
                continue
            name_ru = it.get("name_ru") or str(id_)
            name_kz = it.get("name_kz")
            try:
                await conn.execute(UPSERT_CONTRACT_STATUS_SQL, id_, name_ru, name_kz)
                upserted += 1
            except Exception:
                errors += 1
        if not skipped:
            await conn.execute(LOG_ETL_SQL, "ref_contract_statuses", fetched, upserted, 0, fetched - upserted, errors, "ok", round(time.monotonic() - section_start, 2))
            logger.info("ref_contract_statuses: %d upserted (%d fetched)", upserted, fetched)

        # KATO reference (paginated endpoint).
        # Insert in level order so parent_code FK is already present.
        skip_min_rows = int(os.environ.get("KATO_SKIP_MIN_ROWS", "16000"))
        existing_kato = await conn.fetchval("SELECT COUNT(*) FROM kato_ref")
        if existing_kato >= skip_min_rows and not args.force_kato:
            await conn.execute(
                LOG_ETL_SQL,
                "ref_kato",
                0,
                0,
                0,
                0,
                0,
                "ok",
                0,
            )
            logger.info(
                "ref_kato: skipped (%d rows already in kato_ref, threshold=%d; use --force-kato to refresh)",
                existing_kato,
                skip_min_rows,
            )
        else:
            section_start = time.monotonic()
            errors = 0
            fetched = 0
            upserted = 0
            parsed_rows: list[tuple[str, str, str | None, int, str | None, str | None]] = []
            async for it in client.iter_ref_kato():
                fetched += 1
                code = str(it.get("code") or "").strip()
                if not code:
                    continue
                name_ru = it.get("name_ru") or it.get("full_name_ru") or code
                name_kz = it.get("name_kz") or it.get("full_name_kz")
                parent_code = str(it.get("parent_code") or "").strip() or None
                level_raw = safe_int(it.get("level_")) or safe_int(it.get("level")) or 1
                level = max(1, min(5, level_raw))
                region_id = _derive_region_id(code, parent_code)
                parsed_rows.append((code, name_ru, name_kz, level, parent_code, region_id))

            parsed_rows.sort(key=lambda r: (r[3], r[0]))
            existing_codes: set[str] = set()
            for code, name_ru, name_kz, level, parent_code, region_id in parsed_rows:
                # Drop broken parent reference if it does not exist yet.
                parent_for_insert = parent_code if (not parent_code or parent_code in existing_codes) else None
                try:
                    await conn.execute(
                        UPSERT_KATO_SQL,
                        code,
                        name_ru,
                        name_kz,
                        level,
                        parent_for_insert,
                        region_id,
                    )
                    upserted += 1
                    existing_codes.add(code)
                except Exception:
                    errors += 1
            await conn.execute(
                LOG_ETL_SQL,
                "ref_kato",
                fetched,
                upserted,
                0,
                fetched - upserted,
                errors,
                "ok" if errors == 0 else ("partial" if upserted > 0 else "failed"),
                round(time.monotonic() - section_start, 2),
            )
            logger.info("ref_kato: %d upserted (%d fetched)", upserted, fetched)

    await conn.close()


if __name__ == "__main__":
    asyncio.run(load_refs())
