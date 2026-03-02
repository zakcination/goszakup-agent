"""
etl/load_macro_indices.py
------------------------
Load national macro indices (inflation/GDP) as per-year constants.
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Ensure project root is in path
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.config import get_config
from etl.utils import load_env, maybe_reexec_in_venv, resolve_db_url

maybe_reexec_in_venv()
load_env()

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("etl.macro_indices")


DATA = {
    2024: {"inflation_pct": 8.6, "gdp_growth_pct": 5.0, "basket_price_kzt": None},
    2025: {"inflation_pct": 12.3, "gdp_growth_pct": 6.5, "basket_price_kzt": None},
    2026: {"inflation_pct": 12.2, "gdp_growth_pct": 4.7, "basket_price_kzt": None},
}

UPSERT_SQL = """
INSERT INTO macro_indices (year, inflation_pct, gdp_growth_pct, basket_price_kzt, source, updated_at)
VALUES ($1, $2, $3, $4, $5, NOW())
ON CONFLICT (year) DO UPDATE SET
    inflation_pct = EXCLUDED.inflation_pct,
    gdp_growth_pct = EXCLUDED.gdp_growth_pct,
    basket_price_kzt = EXCLUDED.basket_price_kzt,
    source = EXCLUDED.source,
    updated_at = NOW()
"""


async def load_macro_indices():
    import asyncpg

    cfg = get_config()
    db_url = resolve_db_url(cfg.db_url)
    conn = await asyncpg.connect(db_url, timeout=15)

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS macro_indices (
            year             SMALLINT PRIMARY KEY,
            inflation_pct    NUMERIC(6,3) NOT NULL,
            gdp_growth_pct   NUMERIC(6,3) NOT NULL,
            basket_price_kzt NUMERIC(18,2),
            source           TEXT DEFAULT 'manual',
            created_at       TIMESTAMPTZ DEFAULT NOW(),
            updated_at       TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )

    upserted = 0
    for year, vals in DATA.items():
        await conn.execute(
            UPSERT_SQL,
            year,
            vals["inflation_pct"],
            vals["gdp_growth_pct"],
            vals["basket_price_kzt"],
            "manual",
        )
        upserted += 1

    await conn.close()
    logger.info("macro_indices: %d upserted", upserted)


if __name__ == "__main__":
    asyncio.run(load_macro_indices())
