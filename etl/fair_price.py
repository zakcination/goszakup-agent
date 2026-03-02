"""
etl/fair_price.py
----------------
Spiral 2 scaffolding for fair price computation.
"""

from __future__ import annotations


K_VOL = 1.0  # TODO: enable volume coefficient later


def verdict_insufficient_data(n: int, min_n: int = 20) -> dict:
    return {"verdict": "insufficient_data", "reason": f"N={n} < {min_n}"}


def compute_fair_price(*args, **kwargs):
    raise NotImplementedError("Fair price computation is implemented in Spiral 3")
