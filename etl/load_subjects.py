"""
etl/load_subjects.py
─────────────────────
Spiral 1 — ETL Step 1: Load Subjects

Loads full profiles for all 27 target BINs from OWS v3 API
and upserts them into the subjects table.
Also checks each BIN against the RNU (bad suppliers) registry.

Run:
    python etl/load_subjects.py

Expected output:
    ✅ 000740001307 — Департамент образования...
    ✅ 020240002363 — Управление здравоохранения...
    ...
    ✅ Done: 27 subjects loaded, 0 in RNU
"""

import asyncio
import logging
import os
import sys
import time
from pathlib import Path

# Ensure project root is in path
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.client import OWSClient
from etl.config import TARGET_BINS

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("etl.subjects")


# ── DB helpers ────────────────────────────────────────────────────────────────
UPSERT_SUBJECT_SQL = """
INSERT INTO subjects (
    bin, name_ru, name_kz, full_name_ru,
    is_customer, is_supplier, is_organizer,
    ref_kopf_code, email, phone, website,
    is_qvazi, pid, is_rnu, rnu_checked_at,
    last_synced_at, updated_at
)
VALUES (
    $1, $2, $3, $4,
    $5, $6, $7,
    $8, $9, $10, $11,
    $12, $13, $14, NOW(),
    NOW(), NOW()
)
ON CONFLICT (bin) DO UPDATE SET
    name_ru         = EXCLUDED.name_ru,
    name_kz         = EXCLUDED.name_kz,
    full_name_ru    = EXCLUDED.full_name_ru,
    is_customer     = EXCLUDED.is_customer,
    is_supplier     = EXCLUDED.is_supplier,
    is_organizer    = EXCLUDED.is_organizer,
    ref_kopf_code   = EXCLUDED.ref_kopf_code,
    email           = EXCLUDED.email,
    phone           = EXCLUDED.phone,
    website         = EXCLUDED.website,
    is_qvazi        = EXCLUDED.is_qvazi,
    pid             = EXCLUDED.pid,
    is_rnu          = EXCLUDED.is_rnu,
    rnu_checked_at  = EXCLUDED.rnu_checked_at,
    last_synced_at  = NOW(),
    updated_at      = NOW()
"""

LOG_ETL_SQL = """
INSERT INTO etl_runs (entity, customer_bin, records_inserted, records_updated,
                      errors, status, duration_sec, completed_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
"""


async def load_subjects():
    import asyncpg

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set — check your .env")

    token = os.environ.get("OWS_TOKEN")
    if not token:
        raise RuntimeError("OWS_TOKEN not set — check your .env")

    logger.info("Connecting to database...")
    conn = await asyncpg.connect(db_url, timeout=15)
    logger.info("Database connected ✅")

    inserted = 0
    errors   = 0
    rnu_count = 0
    t_start = time.monotonic()

    async with OWSClient(token=token) as client:
        for bin_ in TARGET_BINS:
            try:
                # ── Fetch subject profile ─────────────────────────────────
                subject = await client.get_subject_by_bin(bin_)

                name_ru      = subject.get("name_ru") or f"Организация {bin_}"
                name_kz      = subject.get("name_kz")
                full_name_ru = subject.get("full_name_ru")
                is_customer  = bool(subject.get("customer", 0))
                is_supplier  = bool(subject.get("supplier", 0))
                is_organizer = bool(subject.get("organizer", 0))
                kopf         = subject.get("ref_kopf_code")
                email        = subject.get("email")
                phone        = subject.get("phone")
                website      = subject.get("website")
                is_qvazi     = bool(subject.get("qvazi", 0))
                pid          = subject.get("pid")

                # ── Check RNU ─────────────────────────────────────────────
                rnu_records = await client.check_rnu(bin_)
                is_rnu = len(rnu_records) > 0
                if is_rnu:
                    rnu_count += 1
                    logger.warning("⚠️  BIN %s is in RNU (bad suppliers registry)!", bin_)

                # ── Upsert ────────────────────────────────────────────────
                await conn.execute(
                    UPSERT_SUBJECT_SQL,
                    bin_, name_ru, name_kz, full_name_ru,
                    True,          # always is_customer for our target BINs
                    is_supplier, is_organizer,
                    kopf, email, phone, website,
                    is_qvazi, pid, is_rnu,
                )
                inserted += 1
                status_icon = "⚠️ " if is_rnu else "✅"
                logger.info("%s %s — %s", status_icon, bin_, (name_ru or "")[:60])

            except Exception as e:
                errors += 1
                logger.error("❌ Failed to load BIN %s: %s", bin_, e)

    # ── Log ETL run ──────────────────────────────────────────────────────────
    duration = time.monotonic() - t_start
    status   = "ok" if errors == 0 else ("partial" if inserted > 0 else "failed")
    await conn.execute(
        LOG_ETL_SQL,
        "subjects", None, inserted, 0, errors, status, round(duration, 2)
    )

    await conn.close()

    logger.info("─" * 50)
    logger.info("✅ Done: %d/%d subjects loaded in %.1fs", inserted, len(TARGET_BINS), duration)
    logger.info("   RNU hits: %d | Errors: %d", rnu_count, errors)

    if errors > 0:
        logger.warning("Some BINs failed — check logs above")
        sys.exit(1)


if __name__ == "__main__":
    # Load .env if running locally
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())

    asyncio.run(load_subjects())
