"""
etl/utils.py
------------
Shared helpers for ETL scripts (Spiral 2+).
"""

from __future__ import annotations

import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


def maybe_reexec_in_venv() -> None:
    """Re-exec inside .venv if available and not already active."""
    if os.environ.get("VIRTUAL_ENV"):
        return
    venv_python = Path(__file__).resolve().parent.parent / ".venv" / "bin" / "python"
    if not venv_python.exists():
        return
    if Path(sys.executable).resolve() == venv_python.resolve():
        return
    os.execv(str(venv_python), [str(venv_python), *sys.argv])


def load_env() -> None:
    """Load .env from project root without clobbering existing env."""
    env_file = Path(__file__).resolve().parent.parent / ".env"
    try:
        from dotenv import load_dotenv

        load_dotenv(env_file, override=False)
        return
    except Exception:
        pass

    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())


def resolve_db_url(db_url: str) -> str:
    """Use localhost DSN when running on host."""
    if not Path("/.dockerenv").exists():
        return db_url.replace("@postgres:", "@localhost:")
    return db_url


def normalize_name(text: str | None) -> str | None:
    if not text:
        return None
    text = text.lower()
    text = re.sub(r"[^\w\s]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def safe_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return int(value)
        except ValueError:
            try:
                return int(float(value))
            except ValueError:
                return None
    return None


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            return None
    return None


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


async def ensure_enstru_ref(conn, code: str | None, cache: set[str] | None = None) -> None:
    if not code:
        return
    if cache is not None and code in cache:
        return
    await conn.execute(
        """
        INSERT INTO enstru_ref (code, name_ru, name_kz, section, division, group_code, is_active)
        VALUES ($1, 'UNKNOWN', NULL, NULL, NULL, NULL, TRUE)
        ON CONFLICT (code) DO NOTHING
        """,
        code,
    )
    if cache is not None:
        cache.add(code)


async def ensure_unit_ref(conn, code: int | None, cache: set[int] | None = None) -> None:
    if not code:
        return
    if cache is not None and code in cache:
        return
    await conn.execute(
        """
        INSERT INTO units_ref (code, name_ru, name_kz, name_norm, aliases)
        VALUES ($1, $2, NULL, $3, ARRAY[]::TEXT[])
        ON CONFLICT (code) DO NOTHING
        """,
        code,
        f"UNKNOWN_UNIT_{code}",
        str(code),
    )
    if cache is not None:
        cache.add(code)


async def ensure_subject(conn, client, bin_code: str | None, is_supplier: bool = False, cache: set[str] | None = None) -> None:
    if not bin_code:
        return
    if cache is not None and bin_code in cache:
        return

    row = await conn.fetchrow("SELECT bin, is_supplier FROM subjects WHERE bin=$1", bin_code)
    if row:
        if is_supplier and not row["is_supplier"]:
            await conn.execute("UPDATE subjects SET is_supplier=TRUE, updated_at=NOW() WHERE bin=$1", bin_code)
        if cache is not None:
            cache.add(bin_code)
        return

    try:
        subject = await client.get_subject_by_bin(bin_code)
    except Exception:
        subject = {}

    if isinstance(subject, list):
        subject = subject[0] if subject else {}
    elif isinstance(subject, dict) and isinstance(subject.get("items"), list):
        items = subject.get("items") or []
        subject = items[0] if items else subject
    if not isinstance(subject, dict):
        subject = {}

    name_ru = subject.get("name_ru") or f"Организация {bin_code}"
    name_kz = subject.get("name_kz")
    full_name_ru = subject.get("full_name_ru")
    is_customer = bool(subject.get("customer", 0))
    api_supplier = bool(subject.get("supplier", 0))
    is_organizer = bool(subject.get("organizer", 0))
    kopf = subject.get("ref_kopf_code")
    email = subject.get("email")
    phone = subject.get("phone")
    website = subject.get("website")
    is_qvazi = bool(subject.get("qvazi", 0))
    pid = subject.get("pid")

    await conn.execute(
        """
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
            $12, $13, FALSE, NULL,
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
            last_synced_at  = NOW(),
            updated_at      = NOW()
        """,
        bin_code,
        name_ru,
        name_kz,
        full_name_ru,
        is_customer,
        (api_supplier or is_supplier),
        is_organizer,
        kopf,
        email,
        phone,
        website,
        is_qvazi,
        pid,
    )
    if cache is not None:
        cache.add(bin_code)


async def kato_exists(conn, code: str | None, cache: set[str] | None = None) -> bool:
    if not code:
        return False
    if cache is not None and code in cache:
        return True
    exists = await conn.fetchval("SELECT 1 FROM kato_ref WHERE code=$1", code)
    if exists and cache is not None:
        cache.add(code)
    return bool(exists)


async def ensure_etl_state_table(conn) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_state (
            entity VARCHAR(40) NOT NULL,
            customer_bin VARCHAR(12),
            last_id BIGINT,
            resume_path TEXT,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (entity, customer_bin)
        )
        """
    )


async def get_etl_state(conn, entity: str, customer_bin: str | None) -> tuple[int | None, str | None]:
    row = await conn.fetchrow(
        """
        SELECT last_id, resume_path
        FROM etl_state
        WHERE entity=$1 AND customer_bin IS NOT DISTINCT FROM $2
        """,
        entity,
        customer_bin,
    )
    if not row:
        return None, None
    return row["last_id"], row["resume_path"]


async def update_etl_state(conn, entity: str, customer_bin: str | None, last_id: int | None, resume_path: str | None) -> None:
    await conn.execute(
        """
        INSERT INTO etl_state (entity, customer_bin, last_id, resume_path, updated_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (entity, customer_bin) DO UPDATE SET
            last_id = EXCLUDED.last_id,
            resume_path = EXCLUDED.resume_path,
            updated_at = NOW()
        """,
        entity,
        customer_bin,
        last_id,
        resume_path,
    )
