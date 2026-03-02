"""
services/analytics_api/main.py
------------------------------
FastAPI service to expose analytics marts (DuckDB).
"""

from __future__ import annotations

from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from analytics.engine import (
    query_anomalies,
    query_compare,
    query_fair_price,
    query_lot_fair_price,
    query_search,
    query_volume_anomalies,
)


app = FastAPI(title="goszakup-analytics-api", version="0.1")


class FairPriceRequest(BaseModel):
    enstru_code: str
    unit_code: int | None = None
    year: int | None = None
    region_id: str | None = None
    limit: int = 10


class AnomalyRequest(BaseModel):
    enstru_code: str | None = None
    year: int | None = None
    min_deviation: float = 30.0
    limit: int = 50


class CompareRequest(BaseModel):
    bin_a: str
    bin_b: str
    enstru_code: str
    year: int | None = None


class SearchRequest(BaseModel):
    mode: Literal["market", "fair_price", "anomaly", "volume"] = "market"
    enstru_code: str | None = None
    unit_code: int | None = None
    customer_bin: str | None = None
    region_id: str | None = None
    year: int | None = None
    year_from: int | None = None
    year_to: int | None = None
    verdict: Literal["ok", "anomaly", "insufficient_data"] | None = None
    fallback_level: int | None = None
    min_n: int | None = None
    min_deviation: float | None = None
    max_deviation: float | None = None
    sort_by: str | None = None
    limit: int = 20


@app.post("/fair-price")
def fair_price(req: FairPriceRequest):
    try:
        data = query_fair_price(req.enstru_code, req.unit_code, req.year, req.region_id, req.limit)
        return {"items": data}
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))


class FairPriceLotRequest(BaseModel):
    lot_id: int
    limit: int = 10


@app.post("/fair-price-lot")
def fair_price_lot(req: FairPriceLotRequest):
    try:
        return query_lot_fair_price(req.lot_id, req.limit)
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))


class VolumeRequest(BaseModel):
    enstru_code: str | None = None
    customer_bin: str | None = None
    year: int | None = None
    min_ratio: float = 1.5
    min_prev_qty: float = 50.0
    limit: int = 20


@app.post("/volume-anomaly")
def volume_anomaly(req: VolumeRequest):
    try:
        data = query_volume_anomalies(
            enstru_code=req.enstru_code,
            customer_bin=req.customer_bin,
            year=req.year,
            min_ratio=req.min_ratio,
            min_prev_qty=req.min_prev_qty,
            limit=req.limit,
        )
        return {"items": data}
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.post("/anomaly")
def anomaly(req: AnomalyRequest):
    try:
        data = query_anomalies(req.enstru_code, req.year, req.min_deviation, req.limit)
        return {"items": data}
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.post("/compare")
def compare(req: CompareRequest):
    try:
        return query_compare(req.bin_a, req.bin_b, req.enstru_code, req.year)
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.post("/search")
def search(req: SearchRequest):
    try:
        data = query_search(
            mode=req.mode,
            enstru_code=req.enstru_code,
            unit_code=req.unit_code,
            customer_bin=req.customer_bin,
            region_id=req.region_id,
            year=req.year,
            year_from=req.year_from,
            year_to=req.year_to,
            verdict=req.verdict,
            fallback_level=req.fallback_level,
            min_n=req.min_n,
            min_deviation=req.min_deviation,
            max_deviation=req.max_deviation,
            sort_by=req.sort_by,
            limit=req.limit,
        )
        return {"items": data}
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/tools")
def tools():
    return {
        "search_modes": ["market", "fair_price", "anomaly", "volume"],
        "search_filters": [
            "enstru_code",
            "unit_code",
            "customer_bin",
            "region_id",
            "year",
            "year_from",
            "year_to",
            "verdict",
            "fallback_level",
            "min_n",
            "min_deviation",
            "max_deviation",
            "sort_by",
            "limit",
        ],
        "search_sort_by": {
            "market": ["n", "median", "mean", "year"],
            "fair_price": ["deviation", "abs_deviation", "year", "n", "price"],
            "anomaly": ["deviation", "abs_deviation", "year", "n", "price"],
            "volume": ["ratio", "year", "qty", "prev_qty"],
        },
    }
