"""
agent/cache.py
--------------
Redis-backed cache for tool outputs with in-memory fallback.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def build_cache_key(tool_name: str, params: dict[str, Any]) -> str:
    payload = json.dumps({"tool": tool_name, "params": params}, sort_keys=True, default=_json_default, ensure_ascii=False)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"tool:{tool_name}:{digest}"


@dataclass
class ToolCache:
    ttl_sec: int = field(default_factory=lambda: int(os.environ.get("AGENT_CACHE_TTL_SEC", "3600")))
    redis_url: str | None = field(default_factory=lambda: os.environ.get("REDIS_URL"))
    redis_password: str | None = field(default_factory=lambda: os.environ.get("REDIS_PASSWORD"))
    _local: dict[str, tuple[datetime, dict[str, Any]]] = field(default_factory=dict)
    _redis: Any | None = None
    _ready: bool = False

    async def _init_redis(self) -> None:
        if self._ready:
            return
        self._ready = True
        if not self.redis_url:
            return
        try:
            from redis import asyncio as redis_async  # type: ignore

            self._redis = redis_async.from_url(
                self.redis_url,
                password=self.redis_password if self.redis_password else None,
                decode_responses=True,
            )
            await self._redis.ping()
        except Exception:
            self._redis = None

    async def get(self, key: str) -> dict[str, Any] | None:
        await self._init_redis()
        if self._redis is not None:
            try:
                raw = await self._redis.get(key)
                if raw:
                    return json.loads(raw)
            except Exception:
                pass

        row = self._local.get(key)
        if not row:
            return None
        ts, payload = row
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        if age > self.ttl_sec:
            self._local.pop(key, None)
            return None
        return payload

    async def set(self, key: str, payload: dict[str, Any]) -> None:
        await self._init_redis()
        if self._redis is not None:
            try:
                await self._redis.setex(key, self.ttl_sec, json.dumps(payload, default=_json_default, ensure_ascii=False))
                return
            except Exception:
                pass
        self._local[key] = (datetime.now(timezone.utc), payload)

