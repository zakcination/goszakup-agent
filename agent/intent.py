"""
agent/intent.py
---------------
Natural-language intent classification to strict tool parameters.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from agent.schemas import IntentResult, IntentType


ENSTRU_RE = re.compile(r"\b\d{6}\.\d{3}\.\d{6}\b")
BIN_RE = re.compile(r"\b\d{12}\b")
YEAR_RE = re.compile(r"\b20\d{2}\b")
TOPK_RE = re.compile(r"(?:top|топ)\s*[-:]?\s*(\d{1,3})", re.IGNORECASE)
THRESHOLD_PCT_RE = re.compile(r"(?:>|>=|больше|не\s*менее)?\s*(\d+(?:[.,]\d+)?)\s*%", re.IGNORECASE)
# Handles RU cases like "договор №123", "договора №123", and EN "contract 123".
CONTRACT_ID_RE = re.compile(
    r"(?:\bдоговор(?:а|у|ом|е)?\b|\bcontract\b)\s*№?\s*([A-Za-zА-Яа-я0-9\\-_/]+)",
    re.IGNORECASE,
)
KATO_RE = re.compile(
    r"(?:\bkato\b|като|регион|область|город|г\.)\s*[:№]?\s*([1-9]\d{2,8})",
    re.IGNORECASE,
)
LOT_REF_RE = re.compile(r"(?:\bлот(?:а|у|ом|е)?\b|\blot\b|trd-buy|закупк[аи])\s*№?\s*(\d{4,})", re.IGNORECASE)


class ClarificationRequired(Exception):
    def __init__(self, message: str, missing_fields: list[str] | None = None):
        super().__init__(message)
        self.message = message
        self.missing_fields = missing_fields or []


@dataclass
class ParsedEntities:
    bins: list[str]
    enstru: str | None
    years: list[int]
    top_k: int | None
    threshold_pct: float | None
    contract_token: str | None
    region_id: str | None
    lot_ref: int | None


def _parse_entities(question: str) -> ParsedEntities:
    bins = BIN_RE.findall(question)
    enstru = ENSTRU_RE.search(question)
    years = [int(y) for y in YEAR_RE.findall(question)]
    top_k = TOPK_RE.search(question)
    threshold = THRESHOLD_PCT_RE.search(question)
    ctoken = CONTRACT_ID_RE.search(question)
    region = KATO_RE.search(question)
    lot_ref = LOT_REF_RE.search(question)
    return ParsedEntities(
        bins=bins,
        enstru=enstru.group(0) if enstru else None,
        years=sorted(set(years)),
        top_k=int(top_k.group(1)) if top_k else None,
        threshold_pct=float(threshold.group(1).replace(",", ".")) if threshold else None,
        contract_token=ctoken.group(1) if ctoken else None,
        region_id=region.group(1) if region else None,
        lot_ref=int(lot_ref.group(1)) if lot_ref else None,
    )


def _period_bounds(years: list[int]) -> tuple[int, int]:
    if not years:
        return 2024, 2026
    if len(years) == 1:
        return years[0], years[0]
    return min(years), max(years)


def _confidence(primary_hits: int, secondary_hits: int) -> float:
    score = 0.35 + (0.25 * primary_hits) + (0.1 * secondary_hits)
    return max(0.0, min(1.0, score))


def classify_intent(question: str) -> IntentResult:
    text = (question or "").strip()
    if len(text) < 3:
        raise ClarificationRequired("Запрос слишком короткий.", [])

    t = text.lower()
    ent = _parse_entities(text)
    p_from, p_to = _period_bounds(ent.years)
    params: dict[str, Any] = {"period_from": p_from, "period_to": p_to}

    if any(k in t for k in ("нет договора", "без договора", "не законтракт", "coverage gap", "что в плане, но нет договора")):
        if not ent.bins:
            raise ClarificationRequired("Укажите БИН заказчика.", ["customer_bin"])
        if not ent.years:
            raise ClarificationRequired("Укажите финансовый год.", ["fin_year"])
        return IntentResult(
            intent=IntentType.COVERAGE_GAPS,
            confidence=_confidence(2, 1),
            params={"customer_bin": ent.bins[0], "fin_year": ent.years[0], "limit": ent.top_k or 50},
            missing_fields=[],
        )

    if any(k in t for k in ("объем", "объём", "колич", "завышение количества", "рост количества", "volume")):
        filters: dict[str, Any] = {}
        if ent.bins:
            filters["customer_bin"] = ent.bins[0]
        if ent.enstru:
            filters["enstru_code"] = ent.enstru
        return IntentResult(
            intent=IntentType.TOP_K,
            confidence=_confidence(2, 1),
            params={
                "dimension": "volume_anomalies",
                "period_from": p_from,
                "period_to": p_to,
                "k": ent.top_k or 10,
                "filters": filters,
            },
            missing_fields=[],
        )

    if any(k in t for k in ("топ", "top")):
        dimension = "expensive_lots"
        if "аномал" in t:
            dimension = "anomalies"
        elif "договор" in t or "контракт" in t:
            dimension = "expensive_contracts"
        elif "поставщик" in t or "supplier" in t:
            dimension = "suppliers"
        return IntentResult(
            intent=IntentType.TOP_K,
            confidence=_confidence(2, 1),
            params={
                "dimension": dimension,
                "period_from": p_from,
                "period_to": p_to,
                "k": ent.top_k or 10,
                "filters": {"customer_bin": ent.bins[0]} if ent.bins else {},
            },
            missing_fields=[],
        )

    if any(k in t for k in ("выполнение плана", "plan vs fact", "исполнение плана")):
        if not ent.bins:
            raise ClarificationRequired("Укажите БИН заказчика.", ["customer_bin"])
        if not ent.years:
            raise ClarificationRequired("Укажите год.", ["fin_year"])
        return IntentResult(
            intent=IntentType.PLAN_VS_FACT,
            confidence=_confidence(2, 1),
            params={"customer_bin": ent.bins[0], "fin_year": ent.years[0]},
            missing_fields=[],
        )

    if any(k in t for k in ("история договора", "audit trail", "аудит")):
        if not ent.contract_token:
            raise ClarificationRequired("Укажите номер или ID договора.", ["contract_number_or_id"])
        token = ent.contract_token
        payload: dict[str, Any]
        if token.isdigit():
            payload = {"contract_id": int(token)}
        else:
            payload = {"contract_number": token}
        return IntentResult(
            intent=IntentType.AUDIT_TRAIL,
            confidence=_confidence(2, 1),
            params=payload,
            missing_fields=[],
        )

    if any(k in t for k in ("поставляет", "поставщик", "supplier")):
        if not ent.enstru:
            raise ClarificationRequired("Укажите ЕНСТРУ/КТРУ код.", ["enstru_code"])
        return IntentResult(
            intent=IntentType.SUPPLIER_CHECK,
            confidence=_confidence(1, 2),
            params={
                "enstru_code": ent.enstru,
                "customer_bins": ent.bins,
                "period_from": p_from,
                "period_to": p_to,
                "limit": ent.top_k or 20,
            },
            missing_fields=[],
        )

    if any(k in t for k in ("подозр", "аномал", "отклон")):
        return IntentResult(
            intent=IntentType.ANOMALY_SCAN,
            confidence=_confidence(2, 1),
            params={
                "customer_bin": ent.bins[0] if ent.bins else None,
                "period_from": p_from,
                "period_to": p_to,
                "confidence_min": "LOW",
                "threshold_pct": ent.threshold_pct or 30.0,
                "limit": ent.top_k or 20,
            },
            missing_fields=[],
        )

    if any(k in t for k in ("сводка", "summary", "итог по бин", "по организации")):
        if not ent.bins:
            raise ClarificationRequired("Укажите БИН заказчика.", ["customer_bin"])
        return IntentResult(
            intent=IntentType.ORG_SUMMARY,
            confidence=_confidence(2, 1),
            params={
                "customer_bin": ent.bins[0],
                "period_from": p_from,
                "period_to": p_to,
            },
            missing_fields=[],
        )

    if any(k in t for k in ("цена", "рыноч", "справедлив", "адекват")):
        if not ent.enstru and ent.lot_ref is None:
            raise ClarificationRequired("Укажите ЕНСТРУ/КТРУ код.", ["enstru_code"])
        return IntentResult(
            intent=IntentType.PRICE_CHECK,
            confidence=_confidence(1, 2),
            params={
                "enstru_code": ent.enstru,
                "lot_id": ent.lot_ref,
                "trd_buy_id": ent.lot_ref,
                "region_id": ent.region_id,
                "period_from": p_from,
                "period_to": p_to,
                "top_k": ent.top_k or 5,
            },
            missing_fields=[],
        )

    raise ClarificationRequired(
        "Не удалось определить тип запроса. Уточните: цена, аномалии, сводка, аудит, топ, покрытие.",
        ["intent"],
    )
