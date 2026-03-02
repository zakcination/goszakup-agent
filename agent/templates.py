"""
agent/templates.py
------------------
Deterministic narration templates for RU/KZ.
"""

from __future__ import annotations

from typing import Any

from agent.schemas import IntentType, ToolResult


def _fmt_money(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):,.2f} ₸".replace(",", " ")
    except Exception:
        return str(value)


def _fmt_pct(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):.2f}%"
    except Exception:
        return str(value)


def render_l0(intent: IntentType, result: ToolResult, lang: str = "ru") -> str:
    d = result.data
    if result.verdict == "insufficient_data":
        reason = d.get("reason")
        if reason:
            return f"Недостаточно данных: {reason}"
        n = result.n or d.get("n")
        if n is not None:
            return f"Недостаточно данных для надежной оценки (N={n}, требуется минимум 20)."
        return "Недостаточно данных для надежного вывода по заданным фильтрам."

    if intent == IntentType.PRICE_CHECK:
        return (
            f"Рыночная ориентировочная цена: {_fmt_money(d.get('fair_price'))} "
            f"(N={result.n}, scope={result.scope_level})."
        )
    if intent == IntentType.ORG_SUMMARY:
        return (
            f"По БИН {d.get('customer_bin')}: план {_fmt_money(d.get('total_planned'))}, "
            f"контракты {_fmt_money(d.get('total_contracted'))}, "
            f"исполнение {_fmt_pct((d.get('execution_rate') or 0) * 100 if d.get('execution_rate') is not None else None)}."
        )
    if intent == IntentType.ANOMALY_SCAN:
        return (
            f"Найдено аномалий: {d.get('count', 0)}. "
            f"Максимальное отклонение: {_fmt_pct(d.get('max_deviation_pct'))}."
        )
    if intent == IntentType.SUPPLIER_CHECK:
        return (
            f"Поставщиков по коду {d.get('enstru_code')}: {d.get('supplier_count', 0)}. "
            f"Концентрация (HHI): {d.get('concentration_score', 0):.4f}."
        )
    if intent == IntentType.AUDIT_TRAIL:
        completeness = d.get("completeness", {})
        coverage = d.get("coverage_ratio", 0.0)
        return (
            f"Аудит-цепочка построена. Покрытие: {coverage:.0%}. "
            f"Plan={completeness.get('has_plan')}, Announcement={completeness.get('has_announcement')}, "
            f"Lot={completeness.get('has_lot')}, Contract={completeness.get('has_contract')}."
        )
    if intent == IntentType.PLAN_VS_FACT:
        return (
            f"План {_fmt_money(d.get('planned_sum'))}, факт {_fmt_money(d.get('contracted_sum'))}, "
            f"gap {_fmt_money(d.get('gap'))}."
        )
    if intent == IntentType.TOP_K:
        return f"Сформирован TOP-{d.get('k')} по {d.get('dimension')}."
    if intent == IntentType.COVERAGE_GAPS:
        return f"Найдено незаконтрактованных плановых позиций: {d.get('count', 0)}."
    return "Результат сформирован."
