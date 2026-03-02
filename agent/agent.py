"""
agent/agent.py
--------------
Main agent workflow:
intent -> validated tool call -> cache -> execution -> layered response.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from agent.cache import ToolCache, build_cache_key
from agent.intent import ClarificationRequired, classify_intent
from agent.schemas import IntentType, QueryResponse
from agent.templates import render_l0
from agent.tools import TOOL_REGISTRY


def _confidence_label(score: float) -> str:
    if score >= 0.85:
        return "HIGH"
    if score >= 0.65:
        return "MEDIUM"
    return "LOW"


def _select_language(user_hint: str | None, question: str) -> str:
    if user_hint in {"ru", "kz"}:
        return user_hint
    q = question.lower()
    kz_markers = ("қ", "ғ", "ң", "ә", "ө", "ұ", "ү", "і")
    return "kz" if any(ch in q for ch in kz_markers) else "ru"


def _follow_ups(intent: IntentType, result_top_k: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prompts: list[dict[str, Any]] = []
    if intent == IntentType.PRICE_CHECK:
        prompts.append({"action": "drilldown_anomalies", "prompt": "Покажи аномалии по этому ЕНСТРУ в том же периоде"})
    if intent == IntentType.ANOMALY_SCAN and result_top_k:
        first = result_top_k[0]
        cid = first.get("contract_id")
        if cid:
            prompts.append({"action": "audit_trail", "prompt": f"Покажи аудит-цепочку по договору {cid}"})
    if intent == IntentType.COVERAGE_GAPS:
        prompts.append({"action": "plan_execution", "prompt": "Покажи выполнение плана по тем же параметрам"})
    return prompts


def _build_layers(intent: IntentType, result_data: dict[str, Any], result_top_k: list[dict[str, Any]], method: str | None) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    l1 = {
        "summary_table": result_top_k[:5],
    }
    l2 = {
        "method": method,
        "analytics": result_data,
    }
    l3 = {
        "comparables": result_top_k[:3],
        "extremes": result_top_k[:3],
    }
    l4 = {
        "audit_chain_ready": intent in {IntentType.AUDIT_TRAIL, IntentType.ANOMALY_SCAN, IntentType.PRICE_CHECK},
    }
    return l1, l2, l3, l4


class AgentWorkflow:
    def __init__(self) -> None:
        self.cache = ToolCache()

    async def handle_query(self, question: str, language: str | None = None) -> QueryResponse:
        lang = _select_language(language, question)
        try:
            intent_res = classify_intent(question)
        except ClarificationRequired as e:
            return QueryResponse(
                intent="CLARIFY",
                verdict="insufficient_data",
                confidence="LOW",
                l0=e.message,
                l1={"missing_fields": e.missing_fields},
                l2={},
                l3={},
                l4={},
                parameters={},
                analytics={},
                top_k=[],
                meta={"reason": "clarification_required"},
                follow_ups=[],
            )

        spec = TOOL_REGISTRY[intent_res.intent]
        try:
            validated_params = spec.param_model(**intent_res.params)
        except ValidationError as e:
            return QueryResponse(
                intent="CLARIFY",
                verdict="insufficient_data",
                confidence="LOW",
                l0="Параметры запроса некорректны. Уточните формулировку.",
                l1={"validation_errors": e.errors()},
                l2={},
                l3={},
                l4={},
                parameters={},
                analytics={},
                top_k=[],
                meta={"reason": "parameter_validation_failed"},
                follow_ups=[],
            )

        param_dict = validated_params.model_dump()
        analytics_db = Path(os.environ.get("ANALYTICS_DB_PATH", "data/analytics.duckdb"))
        cache_ver = int(analytics_db.stat().st_mtime) if analytics_db.exists() else 0
        cache_key = build_cache_key(spec.name, {"cache_version": cache_ver, **param_dict})
        cached = await self.cache.get(cache_key)
        if cached:
            return QueryResponse.model_validate(cached)

        result = spec.handler(validated_params)
        l0 = render_l0(intent_res.intent, result, lang=lang)
        l1, l2, l3, l4 = _build_layers(intent_res.intent, result.data, result.top_k, result.method)
        response = QueryResponse(
            intent=intent_res.intent,
            verdict=result.verdict,
            confidence=result.confidence,
            l0=l0,
            l1=l1,
            l2=l2,
            l3=l3,
            l4=l4,
            parameters=param_dict,
            analytics={
                **result.data,
                "sample_n": result.n,
                "method": result.method,
                "scope_level": result.scope_level,
            },
            top_k=result.top_k,
            meta={
                "tool_name": spec.name,
                "cache": "MISS",
                "intent_confidence_score": round(float(intent_res.confidence), 3),
                "intent_confidence_label": _confidence_label(intent_res.confidence),
                "freshness_ts": result.freshness_ts.isoformat() if result.freshness_ts else None,
                "limitations": result.limitations,
                "evidence": result.evidence,
            },
            follow_ups=_follow_ups(intent_res.intent, result.top_k),
        )
        payload = response.model_dump()
        payload["meta"]["cache"] = "HIT"
        await self.cache.set(cache_key, payload)
        return response
