"""
etl/client.py
─────────────
OWS v3 API Client — Spiral 1

Handles:
  - Auth (Bearer token)
  - Rate limiting (token bucket, respects portal limits)
  - Automatic pagination (REST: next_page cursor; GraphQL: lastId cursor)
  - Retries with exponential backoff
  - Structured logging

Usage:
    async with OWSClient(token=os.environ["OWS_TOKEN"]) as client:
        subject = await client.get_subject_by_bin("210240019348")
        plans   = [p async for p in client.iter_plans_by_bin("210240019348")]
"""

import asyncio
import logging
import os
import time
from typing import AsyncIterator

import httpx

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
BASE_URL      = os.environ.get("OWS_BASE_URL", "https://ows.goszakup.gov.kz")
TOKEN         = os.environ.get("OWS_TOKEN", "")
RATE_LIMIT    = float(os.environ.get("OWS_RATE_LIMIT_RPS", "500"))   # req/sec
TIMEOUT       = float(os.environ.get("OWS_TIMEOUT_SECONDS", "30"))
MAX_RETRIES   = 3
PAGE_SIZE     = int(os.environ.get("OWS_GRAPHQL_PAGE_SIZE", "50"))
ADAPTIVE_RPS  = os.environ.get("OWS_RPS_ADAPTIVE", "1").lower() in ("1", "true", "yes", "on")
RPS_MIN       = float(os.environ.get("OWS_RPS_MIN", "1"))
RPS_MAX       = float(os.environ.get("OWS_RPS_MAX", str(RATE_LIMIT)))
RPS_START     = float(os.environ.get("OWS_RPS_START", str(min(20.0, RPS_MAX))))
RPS_STEP      = float(os.environ.get("OWS_RPS_STEP", "1"))
RPS_WIN       = int(os.environ.get("OWS_RPS_SUCCESS_WINDOW", "50"))
TRD_BUY_CACHE_MAX = int(os.environ.get("OWS_TRD_BUY_CACHE_MAX", "20000"))
TRD_BUY_CACHE_TTL = int(os.environ.get("OWS_TRD_BUY_CACHE_TTL", "3600"))
TRD_BUY_CACHE_MISS_TTL = int(os.environ.get("OWS_TRD_BUY_CACHE_MISS_TTL", "300"))


def _suppress_unhandled_future_exception(fut: asyncio.Future) -> None:
    # Avoid "Future exception was never retrieved" for in-flight cache futures.
    try:
        fut.exception()
    except Exception:
        pass


class RateLimiter:
    """Simple token bucket — ensures we never exceed OWS rate limit."""
    def __init__(
        self,
        rps: float,
        adaptive: bool = False,
        min_rps: float = 1.0,
        max_rps: float = 5.0,
        step: float = 1.0,
        success_window: int = 50,
        cooldown_sec: float = 10.0,
    ):
        self._min_rps = max(0.1, min_rps)
        self._max_rps = max(self._min_rps, max_rps)
        self._rps = max(self._min_rps, min(self._max_rps, rps))
        self._tokens = self._rps
        self._last = time.monotonic()
        self._lock = asyncio.Lock()
        self._adaptive = adaptive
        self._step = step
        self._success_window = max(1, success_window)
        self._success_streak = 0
        self._last_adjust = time.monotonic()
        self._cooldown = cooldown_sec

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self._rps, self._tokens + elapsed * self._rps)
            self._last = now
            if self._tokens < 1:
                wait = (1 - self._tokens) / self._rps
                logger.debug("Rate limit: sleeping %.2fs", wait)
                await asyncio.sleep(wait)
                self._tokens = 0
            else:
                self._tokens -= 1

    async def _set_rps(self, new_rps: float, reason: str) -> None:
        if not self._adaptive:
            return
        old = None
        async with self._lock:
            new_rps = max(self._min_rps, min(self._max_rps, new_rps))
            if abs(new_rps - self._rps) < 1e-6:
                return
            old = self._rps
            self._rps = new_rps
            self._tokens = min(self._tokens, self._rps)
            self._last_adjust = time.monotonic()
            self._success_streak = 0
        if old is not None:
            logger.info("Adaptive RPS: %.2f -> %.2f (%s)", old, new_rps, reason)

    async def on_success(self) -> None:
        if not self._adaptive:
            return
        old = None
        new = None
        async with self._lock:
            self._success_streak += 1
            now = time.monotonic()
            if self._success_streak < self._success_window:
                return
            if now - self._last_adjust < self._cooldown:
                return
            old = self._rps
            new = min(self._max_rps, self._rps + self._step)
            if abs(new - old) < 1e-6:
                self._success_streak = 0
                return
            self._rps = new
            self._tokens = min(self._tokens, self._rps)
            self._last_adjust = now
            self._success_streak = 0
        if old is not None and new is not None:
            logger.info("Adaptive RPS: %.2f -> %.2f (stable)", old, new)

    async def on_throttle(self) -> None:
        await self._set_rps(self._rps * 0.7, "429")

    async def on_error(self) -> None:
        await self._set_rps(self._rps * 0.85, "error")


class OWSClient:
    """
    Async client for OWS v3 REST + GraphQL API.

    Always use as async context manager:
        async with OWSClient() as client:
            ...
    """

    def __init__(self, token: str = TOKEN, base_url: str = BASE_URL, graphql_url: str | None = None):
        if not token:
            raise ValueError("OWS_TOKEN is required — check your .env file")
        self._token = token
        self._base_url = base_url.rstrip("/")
        self._graphql_url = graphql_url or os.environ.get("OWS_GRAPHQL_URL", f"{self._base_url}/v3/graphql")
        start_rps = max(RPS_MIN, min(RPS_MAX, RPS_START))
        self._limiter = RateLimiter(
            start_rps,
            adaptive=ADAPTIVE_RPS,
            min_rps=RPS_MIN,
            max_rps=RPS_MAX,
            step=RPS_STEP,
            success_window=RPS_WIN,
        )
        self._client: httpx.AsyncClient | None = None
        # Simple in-memory cache for frequently repeated lookups.
        self._trd_buy_cache: dict[int, tuple[float, dict | None]] = {}
        # Prevent duplicate concurrent requests for the same trd_buy_id.
        self._trd_buy_inflight: dict[int, asyncio.Future] = {}
        self._trd_buy_lock = asyncio.Lock()

    # ── Context manager ──────────────────────────────────────────────────────
    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type":  "application/json",
                "Accept":        "application/json",
                "User-Agent":    "goszakup-agent/1.0",
            },
            timeout=httpx.Timeout(TIMEOUT),
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    # ── Internal request helpers ─────────────────────────────────────────────
    async def _get(self, path: str, params: dict | None = None) -> dict:
        """Single GET with rate-limiting + retry."""
        assert self._client, "Use 'async with OWSClient() as client:'"
        for attempt in range(1, MAX_RETRIES + 1):
            await self._limiter.acquire()
            try:
                r = await self._client.get(path, params=params)
                r.raise_for_status()
                await self._limiter.on_success()
                return r.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    await self._limiter.on_throttle()
                    wait = 10 * attempt
                    logger.warning("429 Too Many Requests — waiting %ds", wait)
                    await asyncio.sleep(wait)
                elif e.response.status_code in (500, 502, 503):
                    await self._limiter.on_error()
                    wait = 2 ** attempt
                    logger.warning("Server error %d — retry %d/%d in %ds",
                                   e.response.status_code, attempt, MAX_RETRIES, wait)
                    await asyncio.sleep(wait)
                else:
                    logger.error("HTTP %d for %s", e.response.status_code, path)
                    raise
            except (httpx.TransportError, httpx.ReadTimeout) as e:
                await self._limiter.on_error()
                wait = 2 ** attempt
                logger.warning("Network error: %s — retry %d/%d in %ds", e, attempt, MAX_RETRIES, wait)
                await asyncio.sleep(wait)
        raise RuntimeError(f"All {MAX_RETRIES} retries failed for GET {path}")

    async def _graphql(self, query: str, variables: dict | None = None) -> dict:
        """Single GraphQL POST with rate-limiting + retry."""
        assert self._client
        payload = {"operationName": None, "query": query, "variables": variables or {}}
        for attempt in range(1, MAX_RETRIES + 1):
            await self._limiter.acquire()
            try:
                r = await self._client.post(self._graphql_url, json=payload)
                r.raise_for_status()
                data = r.json()
                if "errors" in data:
                    logger.error("GraphQL errors: %s", data["errors"])
                    raise ValueError(f"GraphQL error: {data['errors']}")
                await self._limiter.on_success()
                return data
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    await self._limiter.on_throttle()
                    wait = 10 * attempt
                    logger.warning("GraphQL 429 — waiting %ds", wait)
                    await asyncio.sleep(wait)
                elif e.response.status_code in (500, 502, 503):
                    await self._limiter.on_error()
                    wait = 2 ** attempt
                    logger.warning("GraphQL server error %d — retry %d/%d in %ds",
                                   e.response.status_code, attempt, MAX_RETRIES, wait)
                    await asyncio.sleep(wait)
                else:
                    logger.error("GraphQL HTTP %d", e.response.status_code)
                    raise
            except (httpx.TransportError, httpx.ReadTimeout) as e:
                await self._limiter.on_error()
                wait = 2 ** attempt
                logger.warning("GraphQL network error: %s — retry %d/%d in %ds", e, attempt, MAX_RETRIES, wait)
                await asyncio.sleep(wait)
        raise RuntimeError(f"All {MAX_RETRIES} GraphQL retries failed")

    # ── REST pagination helper ────────────────────────────────────────────────
    async def _paginate_rest(self, path: str, params: dict | None = None) -> AsyncIterator[dict]:
        """Walks REST next_page cursor and yields each item."""
        next_path = path
        next_params = params or {}
        page = 0
        while next_path:
            page += 1
            data = await self._get(next_path, next_params if page == 1 else None)
            items = data.get("items", [])
            logger.debug("REST %s — page %d, %d items", path, page, len(items))
            for item in items:
                yield item
            # next_page is a full path like "/v3/plans/BIN?page=next&search_after=123"
            raw_next = data.get("next_page", "")
            next_path = raw_next if raw_next else None
            next_params = {}   # params are encoded in next_page URL

    async def _paginate_rest_pages(
        self,
        path: str,
        params: dict | None = None,
        start_path: str | None = None,
    ) -> AsyncIterator[tuple[list[dict], str | None]]:
        """Walks REST next_page cursor and yields (items, next_page)."""
        next_path = start_path or path
        next_params = None if start_path else (params or {})
        page = 0
        while next_path:
            page += 1
            data = await self._get(next_path, next_params if page == 1 else None)
            items = data.get("items", [])
            logger.debug("REST %s — page %d, %d items", path, page, len(items))
            raw_next = data.get("next_page", "")
            next_page = raw_next if raw_next else None
            yield items, next_page
            next_path = next_page
            next_params = None

    # ── GraphQL pagination helper ─────────────────────────────────────────────
    async def _paginate_graphql(self, query: str, key: str, variables: dict) -> AsyncIterator[dict]:
        """Walks GraphQL lastId cursor and yields each record."""
        after = 0
        page = 0
        while True:
            page += 1
            variables["after"] = after
            variables["limit"] = PAGE_SIZE
            data = await self._graphql(query, variables)
            records = data.get("data", {}).get(key, [])
            logger.debug("GraphQL %s — page %d, %d records", key, page, len(records))
            for rec in records:
                yield rec
            ext = data.get("extensions", {}).get("pageInfo", {})
            if not ext.get("hasNextPage"):
                break
            after = ext.get("lastId", 0)

    # ══════════════════════════════════════════════════════════════════════════
    # PUBLIC API METHODS
    # ══════════════════════════════════════════════════════════════════════════

    # ── Journal (incremental updates) ─────────────────────────────────────────
    async def get_journal(self, date_from: str, date_to: str) -> list[dict]:
        """GET /v3/journal — returns changed objects between two dates.
        date_from/date_to format: 'YYYY-MM-DD'
        """
        data = await self._get("/v3/journal", {"date_from": date_from, "date_to": date_to})
        return data.get("items", [])

    # ── Subjects ──────────────────────────────────────────────────────────────
    async def get_subject_by_bin(self, bin: str) -> dict:
        """GET /v3/subject/biin/{bin} — full profile of one organisation."""
        return await self._get(f"/v3/subject/biin/{bin}")

    async def check_rnu(self, bin: str) -> list[dict]:
        """GET /v3/rnu/{bin} — is this BIN in the bad suppliers registry?"""
        try:
            data = await self._get(f"/v3/rnu/{bin}")
            return data.get("items", [])
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return []   # not in RNU — good
            raise

    # ── Plans ─────────────────────────────────────────────────────────────────
    async def iter_plans_by_bin(self, bin: str) -> AsyncIterator[dict]:
        """GET /v3/plans/{bin} with pagination — all plan points for a BIN."""
        async for item in self._paginate_rest(f"/v3/plans/{bin}"):
            yield item

    async def iter_rest_pages(
        self,
        path: str,
        params: dict | None = None,
        start_path: str | None = None,
    ) -> AsyncIterator[tuple[list[dict], str | None]]:
        """Expose page-wise REST pagination for checkpointing."""
        async for items, next_page in self._paginate_rest_pages(path, params=params, start_path=start_path):
            yield items, next_page

    # ── Announcements (trd-buy) ───────────────────────────────────────────────
    async def iter_trd_buy(self) -> AsyncIterator[dict]:
        """GET /v3/trd-buy — announcements registry."""
        async for item in self._paginate_rest("/v3/trd-buy"):
            yield item

    async def iter_trd_buy_all(self) -> AsyncIterator[dict]:
        """GET /v3/trd-buy/all — announcements registry (all records)."""
        async for item in self._paginate_rest("/v3/trd-buy/all"):
            yield item

    async def iter_trd_buy_by_org_bin(self, org_bin: str) -> AsyncIterator[dict]:
        """GET /v3/trd-buy/bin/{bin} — announcements by organiser BIN."""
        async for item in self._paginate_rest(f"/v3/trd-buy/bin/{org_bin}"):
            yield item

    async def get_trd_buy(self, trd_buy_id: int) -> dict:
        """GET /v3/trd-buy/{id} — one announcement."""
        now = time.monotonic()
        cached = self._trd_buy_cache.get(trd_buy_id)
        if cached:
            ts, data = cached
            ttl = TRD_BUY_CACHE_MISS_TTL if data is None else TRD_BUY_CACHE_TTL
            if now - ts < ttl:
                return data or {}
            self._trd_buy_cache.pop(trd_buy_id, None)

        created = False
        async with self._trd_buy_lock:
            in_flight = self._trd_buy_inflight.get(trd_buy_id)
            if in_flight is None:
                in_flight = asyncio.get_running_loop().create_future()
                in_flight.add_done_callback(_suppress_unhandled_future_exception)
                self._trd_buy_inflight[trd_buy_id] = in_flight
                created = True

        if not created:
            data = await in_flight
            return data or {}

        try:
            data = await self._get(f"/v3/trd-buy/{trd_buy_id}")
            now = time.monotonic()
            if len(self._trd_buy_cache) >= TRD_BUY_CACHE_MAX:
                # Drop an arbitrary item to cap memory.
                self._trd_buy_cache.pop(next(iter(self._trd_buy_cache)), None)
            self._trd_buy_cache[trd_buy_id] = (now, data or None)
            if not in_flight.done():
                in_flight.set_result(data or {})
            return data
        except Exception as exc:
            if not in_flight.done():
                in_flight.set_exception(exc)
            raise
        finally:
            async with self._trd_buy_lock:
                self._trd_buy_inflight.pop(trd_buy_id, None)

    async def get_trd_buy_by_number_anno(self, number_anno: str) -> dict:
        """GET /v3/trd-buy/number-anno/{numberAnno} — announcement by number."""
        return await self._get(f"/v3/trd-buy/number-anno/{number_anno}")

    async def get_trd_buy_cancel(self, trd_buy_id: int) -> dict:
        """GET /v3/trd-buy/{id}/cancel — court cancellation info (if any)."""
        return await self._get(f"/v3/trd-buy/{trd_buy_id}/cancel")

    async def get_trd_buy_pause(self, trd_buy_id: int) -> dict:
        """GET /v3/trd-buy/{id}/pause — paused procurement info (if any)."""
        return await self._get(f"/v3/trd-buy/{trd_buy_id}/pause")

    async def iter_trd_buy_commission(self, trd_buy_id: int) -> AsyncIterator[dict]:
        """GET /v3/trd-buy/{id}/commission — procurement commission members."""
        async for item in self._paginate_rest(f"/v3/trd-buy/{trd_buy_id}/commission"):
            yield item

    # ── Lots ──────────────────────────────────────────────────────────────────
    async def iter_lots(self) -> AsyncIterator[dict]:
        """GET /v3/lots — lots registry."""
        async for item in self._paginate_rest("/v3/lots"):
            yield item

    async def iter_lots_by_number_anno(self, number_anno: str) -> AsyncIterator[dict]:
        """GET /v3/lots/number-anno/{numberAnno} — lots by announcement number."""
        async for item in self._paginate_rest(f"/v3/lots/number-anno/{number_anno}"):
            yield item

    async def iter_lots_by_customer_bin(self, customer_bin: str) -> AsyncIterator[dict]:
        """GET /v3/lots/bin/{bin} — lots by customer BIN."""
        async for item in self._paginate_rest(f"/v3/lots/bin/{customer_bin}"):
            yield item

    async def get_lot(self, lot_id: int) -> dict:
        """GET /v3/lots/{id} — one lot."""
        return await self._get(f"/v3/lots/{lot_id}")

    # ── Contracts ─────────────────────────────────────────────────────────────
    async def iter_contracts(self) -> AsyncIterator[dict]:
        """GET /v3/contract — contracts registry."""
        async for item in self._paginate_rest("/v3/contract"):
            yield item

    async def iter_contracts_by_number_anno(self, number_anno: str) -> AsyncIterator[dict]:
        """GET /v3/contract/number-anno/{numberAnno} — contracts by announcement number."""
        async for item in self._paginate_rest(f"/v3/contract/number-anno/{number_anno}"):
            yield item

    async def iter_contracts_by_customer_bin(self, customer_bin: str) -> AsyncIterator[dict]:
        """GET /v3/contract/customer/{bin} — contracts by customer BIN."""
        async for item in self._paginate_rest(f"/v3/contract/customer/{customer_bin}"):
            yield item

    async def iter_contracts_by_supplier_bin(self, supplier_bin: str) -> AsyncIterator[dict]:
        """GET /v3/contract/supplier/{bin} — contracts by supplier BIN."""
        async for item in self._paginate_rest(f"/v3/contract/supplier/{supplier_bin}"):
            yield item

    async def get_contract(self, contract_id: int) -> dict:
        """GET /v3/contract/{id} — one contract."""
        return await self._get(f"/v3/contract/{contract_id}")

    async def get_contract_units(self, contract_id: int) -> list[dict]:
        """GET /v3/contract/{id}/units — contract line items/units."""
        data = await self._get(f"/v3/contract/{contract_id}/units")
        return data.get("items", data if isinstance(data, list) else [])

    # ── Plan point details ─────────────────────────────────────────────────────
    async def get_plan_point_view(self, pln_point_id: int) -> dict:
        """GET /v3/plans/view/{id} — enriched plan point view (incl. ENSTRU + KATO array)."""
        return await self._get(f"/v3/plans/view/{pln_point_id}")

    # ── Acts ──────────────────────────────────────────────────────────────────
    async def iter_acts(self) -> AsyncIterator[dict]:
        """GET /v3/acts — contract acts registry."""
        async for item in self._paginate_rest("/v3/acts"):
            yield item

    async def get_act(self, act_id: int) -> dict:
        """GET /v3/acts/{id} — one act."""
        return await self._get(f"/v3/acts/{act_id}")

    # ── Treasury payments ──────────────────────────────────────────────────────
    async def iter_treasury_pay(self) -> AsyncIterator[dict]:
        """GET /v3/treasury-pay — payments registry."""
        async for item in self._paginate_rest("/v3/treasury-pay"):
            yield item

    async def get_treasury_pay_by_contract(self, contract_id: int) -> list[dict]:
        """GET /v3/treasury-pay/{contract_id} — payments by contract."""
        data = await self._get(f"/v3/treasury-pay/{contract_id}")
        return data.get("items", data if isinstance(data, list) else [])

    # ── Refs ──────────────────────────────────────────────────────────────────
    async def get_ref_units(self) -> list[dict]:
        data = await self._get("/v3/refs/ref_units")
        return data.get("items", data if isinstance(data, list) else [])

    async def get_ref_trade_methods(self) -> list[dict]:
        data = await self._get("/v3/refs/ref_trade_methods")
        return data.get("items", data if isinstance(data, list) else [])

    async def get_ref_lot_statuses(self) -> list[dict]:
        data = await self._get("/v3/refs/ref_lots_status")
        return data.get("items", data if isinstance(data, list) else [])

    async def get_ref_contract_statuses(self) -> list[dict]:
        data = await self._get("/v3/refs/ref_contract_status")
        return data.get("items", data if isinstance(data, list) else [])

    async def iter_ref_kato(self) -> AsyncIterator[dict]:
        """GET /v3/refs/ref_kato — KATO reference rows (paginated)."""
        async for item in self._paginate_rest("/v3/refs/ref_kato"):
            yield item

    async def iter_plans_kato(self) -> AsyncIterator[dict]:
        """GET /v3/plans/kato — plan point delivery KATO rows (paginated)."""
        async for item in self._paginate_rest("/v3/plans/kato"):
            yield item

    async def iter_plans_spec(self) -> AsyncIterator[dict]:
        """GET /v3/plans/spec — budget/specification rows for plan points."""
        async for item in self._paginate_rest("/v3/plans/spec"):
            yield item
