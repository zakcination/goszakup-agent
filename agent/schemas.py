"""
agent/schemas.py
----------------
Strict request/response and tool schemas.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class IntentType(str, Enum):
    PRICE_CHECK = "PRICE_CHECK"
    ORG_SUMMARY = "ORG_SUMMARY"
    ANOMALY_SCAN = "ANOMALY_SCAN"
    SUPPLIER_CHECK = "SUPPLIER_CHECK"
    AUDIT_TRAIL = "AUDIT_TRAIL"
    PLAN_VS_FACT = "PLAN_VS_FACT"
    TOP_K = "TOP_K"
    COVERAGE_GAPS = "COVERAGE_GAPS"


class Period(BaseModel):
    model_config = ConfigDict(extra="forbid")

    period_from: int = Field(..., ge=2000, le=2100)
    period_to: int = Field(..., ge=2000, le=2100)

    @model_validator(mode="after")
    def _validate_range(self) -> "Period":
        if self.period_to < self.period_from:
            raise ValueError("period_to must be >= period_from")
        return self


class IntentResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    intent: IntentType
    confidence: float = Field(..., ge=0.0, le=1.0)
    params: dict[str, Any]
    missing_fields: list[str] = Field(default_factory=list)


class ToolResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    verdict: Literal["ok", "anomaly", "insufficient_data"]
    data: dict[str, Any] = Field(default_factory=dict)
    evidence: dict[str, Any] = Field(default_factory=dict)
    top_k: list[dict[str, Any]] = Field(default_factory=list)
    confidence: Literal["HIGH", "MEDIUM", "LOW"] = "LOW"
    method: str | None = None
    n: int | None = None
    freshness_ts: datetime | None = None
    limitations: list[str] = Field(default_factory=list)
    scope_level: str | None = None


class QueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str = Field(..., min_length=3, max_length=4000)
    language: Literal["ru", "kz"] | None = None


class QueryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    intent: IntentType | Literal["CLARIFY"]
    verdict: str
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    l0: str
    l1: dict[str, Any] = Field(default_factory=dict)
    l2: dict[str, Any] = Field(default_factory=dict)
    l3: dict[str, Any] = Field(default_factory=dict)
    l4: dict[str, Any] = Field(default_factory=dict)
    parameters: dict[str, Any] = Field(default_factory=dict)
    analytics: dict[str, Any] = Field(default_factory=dict)
    top_k: list[dict[str, Any]] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)
    follow_ups: list[dict[str, Any]] = Field(default_factory=list)


class FairPriceParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enstru_code: str | None = None
    lot_id: int | None = Field(default=None, ge=1)
    trd_buy_id: int | None = Field(default=None, ge=1)
    region_id: str | None = None
    period_from: int = Field(..., ge=2000, le=2100)
    period_to: int = Field(..., ge=2000, le=2100)
    top_k: int = Field(default=5, ge=1, le=100)

    @model_validator(mode="after")
    def _validate_identifier(self) -> "FairPriceParams":
        if not self.enstru_code and self.lot_id is None and self.trd_buy_id is None:
            raise ValueError("enstru_code or lot_id/trd_buy_id is required")
        return self


class OrgSummaryParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_bin: str = Field(..., min_length=12, max_length=12)
    period_from: int = Field(..., ge=2000, le=2100)
    period_to: int = Field(..., ge=2000, le=2100)


class AnomalyScanParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_bin: str | None = Field(default=None, min_length=12, max_length=12)
    period_from: int = Field(..., ge=2000, le=2100)
    period_to: int = Field(..., ge=2000, le=2100)
    confidence_min: Literal["LOW", "MEDIUM", "HIGH"] = "LOW"
    threshold_pct: float = Field(default=30.0, ge=0.0, le=1000.0)
    limit: int = Field(default=20, ge=1, le=200)


class SupplierCheckParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enstru_code: str
    customer_bins: list[str] = Field(default_factory=list)
    period_from: int = Field(..., ge=2000, le=2100)
    period_to: int = Field(..., ge=2000, le=2100)
    limit: int = Field(default=20, ge=1, le=200)


class AuditTrailParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_id: int | None = None
    contract_number: str | None = None

    @model_validator(mode="after")
    def _validate_any_identifier(self) -> "AuditTrailParams":
        if self.contract_id is None and not self.contract_number:
            raise ValueError("contract_id or contract_number is required")
        return self


class PlanVsFactParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_bin: str = Field(..., min_length=12, max_length=12)
    fin_year: int = Field(..., ge=2000, le=2100)


class TopKParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dimension: Literal["expensive_lots", "expensive_contracts", "anomalies", "suppliers", "volume_anomalies"] = "expensive_lots"
    period_from: int = Field(..., ge=2000, le=2100)
    period_to: int = Field(..., ge=2000, le=2100)
    k: int = Field(default=10, ge=1, le=200)
    filters: dict[str, Any] = Field(default_factory=dict)


class CoverageGapsParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_bin: str = Field(..., min_length=12, max_length=12)
    fin_year: int = Field(..., ge=2000, le=2100)
    limit: int = Field(default=50, ge=1, le=500)
