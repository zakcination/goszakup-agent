from __future__ import annotations

from datetime import datetime, timezone

import pytest

from agent.agent import AgentWorkflow
from agent.schemas import FairPriceParams, IntentResult, IntentType, ToolResult
from agent.tools import TOOL_REGISTRY, ToolSpec


def test_intent_price_check():
    from agent.intent import classify_intent

    res = classify_intent("Проверь справедливую цену по ЕНСТРУ 331312.100.000000 за 2025")
    assert res.intent == IntentType.PRICE_CHECK
    assert res.params["enstru_code"] == "331312.100.000000"
    assert res.params["period_from"] == 2025
    assert res.params["period_to"] == 2025


def test_intent_audit_trail_contract_id():
    from agent.intent import classify_intent

    res = classify_intent("История договора №24764966")
    assert res.intent == IntentType.AUDIT_TRAIL
    assert res.params["contract_id"] == 24764966


def test_intent_price_check_by_lot_reference():
    from agent.intent import classify_intent

    res = classify_intent("Оцени адекватность цены лота № 6751790 за 2025")
    assert res.intent == IntentType.PRICE_CHECK
    assert res.params["lot_id"] == 6751790
    assert res.params["trd_buy_id"] == 6751790
    assert res.params["period_from"] == 2025
    assert res.params["period_to"] == 2025


def test_intent_volume_route_to_top_k_volume():
    from agent.intent import classify_intent

    res = classify_intent(
        "Выяви нетипичное завышение количества ТРУ по сравнению с предыдущими годами по ЕНСТРУ 172213.000.000002 за 2025 год"
    )
    assert res.intent == IntentType.TOP_K
    assert res.params["dimension"] == "volume_anomalies"
    assert res.params["period_from"] == 2025
    assert res.params["period_to"] == 2025


@pytest.mark.asyncio
async def test_clarification_response():
    wf = AgentWorkflow()
    resp = await wf.handle_query("???")
    assert resp.intent == "CLARIFY"
    assert resp.verdict == "insufficient_data"
    assert resp.confidence == "LOW"


@pytest.mark.asyncio
async def test_no_hallucination_on_structured_output(monkeypatch):
    wf = AgentWorkflow()

    def fake_classifier(_question: str):
        return IntentResult(
            intent=IntentType.PRICE_CHECK,
            confidence=0.99,
            params={
                "enstru_code": "331312.100.000000",
                "region_id": "710000000",
                "period_from": 2025,
                "period_to": 2025,
                "top_k": 3,
            },
            missing_fields=[],
        )

    def fake_tool(_params: FairPriceParams) -> ToolResult:
        return ToolResult(
            verdict="ok",
            confidence="HIGH",
            n=34,
            method="Median + IQR",
            freshness_ts=datetime(2026, 3, 2, tzinfo=timezone.utc),
            data={"fair_price": 285000.0, "enstru_code": "331312.100.000000"},
            top_k=[{"contract_id": 123, "unit_price": 280000.0}],
        )

    monkeypatch.setattr("agent.agent.classify_intent", fake_classifier)
    orig = TOOL_REGISTRY[IntentType.PRICE_CHECK]
    monkeypatch.setitem(
        TOOL_REGISTRY,
        IntentType.PRICE_CHECK,
        ToolSpec("get_fair_price", FairPriceParams, fake_tool),
    )
    try:
        resp = await wf.handle_query("любой текст")
        assert resp.verdict == "ok"
        assert resp.analytics["fair_price"] == 285000.0
        assert resp.top_k[0]["contract_id"] == 123
    finally:
        monkeypatch.setitem(TOOL_REGISTRY, IntentType.PRICE_CHECK, orig)
