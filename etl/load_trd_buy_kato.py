"""Pull KATO metadata straight from a trd-buy payload."""

import argparse
import asyncio
import json
import logging
from typing import Any, Iterable

import asyncpg

from etl.client import OWSClient
from etl.config import get_config

logger = logging.getLogger(__name__)

REGION_PARENT_MAP = {
    "100": "100000000",  # Абайская область
    "110": "110000000",  # Акмолинская область
    "150": "150000000",  # Актюбинская область
    "190": "190000000",  # Алматинская область
    "230": "230000000",  # Атырауская область
    "270": "270000000",  # Западно-Казахстанская область
    "310": "310000000",  # Жамбылская область
    "330": "330000000",  # Жетісу
    "350": "350000000",  # Карагандинская область
    "390": "390000000",  # Костанайская область
    "430": "430000000",  # Кызылординская область
    "470": "470000000",  # Мангистауская область
    "510": "510000000",  # Южно-Казахстанская (Түркістан)
    "550": "550000000",  # Павлодарская область
    "590": "590000000",  # Северо-Казахстанская область
    "610": "610000000",  # Туркестанская область
    "620": "620000000",  # Ұлытау
    "630": "630000000",  # Восточно-Казахстанская область
    "710": "710000000",  # г.Астана
    "750": "750000000",  # г.Алматы
    "790": "790000000",  # г.Шымкент
}

CITY_KEYS = ("cityNameRu", "city_ru", "cityName", "city", "name_ru", "name")


def _infer_parent_code(code: str | None) -> str | None:
    if not code or len(code) < 3:
        return None
    return REGION_PARENT_MAP.get(code[:3])


def _normalize_candidate(candidate: dict[str, Any], fallback_parent: str | None = None) -> dict[str, Any] | None:
    code = (
        candidate.get("refKatoCode")
        or candidate.get("ref_kato_code")
        or candidate.get("katoCode")
        or candidate.get("kato_code")
        or candidate.get("code")
    )
    if not code:
        return None
    parent = (
        candidate.get("parentCode")
        or candidate.get("parent_code")
        or candidate.get("parent")
        or fallback_parent
    )
    city_ru = next((candidate.get(key) for key in CITY_KEYS if candidate.get(key)), None)
    city_kz = candidate.get("cityNameKz") or candidate.get("city_kz")
    parent = parent or _infer_parent_code(code)
    return {
        "kato_code": code,
        "parent_kato": parent,
        "city_ru": city_ru,
        "city_kz": city_kz,
        "raw": candidate,
    }


def _collect_candidates_from_lot(lot: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for key, value in lot.items():
        if "kato" not in key.lower():
            continue
        if isinstance(value, list):
            for entry in value:
                if isinstance(entry, dict):
                    normalized = _normalize_candidate(entry, fallback_parent=lot.get("parent_kato"))
                    if normalized:
                        candidates.append(normalized)
                elif isinstance(entry, str):
                    normalized = _normalize_candidate({"code": entry}, fallback_parent=lot.get("parent_kato"))
                    if normalized:
                        candidates.append(normalized)
        elif isinstance(value, dict):
            normalized = _normalize_candidate(value, fallback_parent=lot.get("parent_kato"))
            if normalized:
                candidates.append(normalized)
        elif isinstance(value, str):
            normalized = _normalize_candidate({"code": value}, fallback_parent=lot.get("parent_kato"))
            if normalized:
                candidates.append(normalized)
    return candidates


def _extract_recordings(trd_buy: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    # top-level kato info
    for entry in trd_buy.get("kato", []):
        if isinstance(entry, dict):
            normalized = _normalize_candidate(entry)
            if normalized:
                records.append({**normalized, "lot_id": None, "trd_buy_id": trd_buy.get("id")})

    # lots nested inside payload (helpers for common key variants)
    for lot_key in ("lotList", "lot_list", "lots", "lot"):  # order matters
        lot_payload = trd_buy.get(lot_key)
        if not lot_payload:
            continue
        if isinstance(lot_payload, dict):
            lot_payload = [lot_payload]
        if not isinstance(lot_payload, list):
            continue
        for lot in lot_payload:
            lot_id = lot.get("lotId") or lot.get("id")
            for normalized in _collect_candidates_from_lot(lot):
                records.append({**normalized, "lot_id": lot_id, "trd_buy_id": trd_buy.get("id")})
    return records


async def _persist_records(records: Iterable[dict[str, Any]], db_url: str, dry_run: bool) -> None:
    async with asyncpg.connect(db_url, timeout=15) as conn:
        async with conn.transaction():
            for record in records:
                if not record:
                    continue
                if dry_run:
                    logger.info("[dry] data=%s", record)
                    continue
                await conn.execute(
                    """
                    INSERT INTO trd_buy_kato_metadata (
                        trd_buy_id, lot_id, kato_code, parent_kato, city_ru, city_kz, raw_json
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7)
                    ON CONFLICT (trd_buy_id, lot_id, kato_code)
                    DO UPDATE SET
                        parent_kato = EXCLUDED.parent_kato,
                        city_ru = EXCLUDED.city_ru,
                        city_kz = EXCLUDED.city_kz,
                        raw_json = EXCLUDED.raw_json,
                        updated_at = NOW()
                    """,
                    record["trd_buy_id"],
                    record.get("lot_id"),
                    record["kato_code"],
                    record.get("parent_kato"),
                    record.get("city_ru"),
                    record.get("city_kz"),
                    json.dumps(record.get("raw", {}), ensure_ascii=False),
                )


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch KATO metadata from a trd-buy payload.")
    parser.add_argument("--trd-buy-id", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)
    config = get_config()

    try:
        async with OWSClient(token=config.ows_token, base_url=config.ows_base_url) as client:
            trd_buy = await client.get_trd_buy(args.trd_buy_id)
    except Exception as exc:  # pragma: no cover - best-effort fetch
        logger.error("Failed to fetch trd-buy %s: %s", args.trd_buy_id, exc)
        return
    if not trd_buy:
        logger.warning("trd-buy %s returned no payload", args.trd_buy_id)
        return

    records = _extract_recordings(trd_buy)
    if not records:
        logger.info("No kato candidates extracted for trd-buy %s", args.trd_buy_id)
        return

    await _persist_records(records, config.db_url, args.dry_run)
    logger.info("Persisted %s kato records for trd-buy %s", len(records), args.trd_buy_id)


if __name__ == "__main__":
    asyncio.run(main())
