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
    client = OWSClient(token=os.environ["OWS_TOKEN"])
    async with client:
        subject = await client.get_subject_by_bin("210240019348")
        plans   = await client.get_plans_by_bin("210240019348", year=2024)
"""

import asyncio
import logging
import os
import time
from typing import Any, AsyncIterator

import httpx

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
BASE_URL      = os.environ.get("OWS_BASE_URL", "https://ows.goszakup.gov.kz")
GRAPHQL_URL   = os.environ.get("OWS_GRAPHQL_URL", f"{BASE_URL}/v3/graphql")
TOKEN         = os.environ.get("OWS_TOKEN", "")
RATE_LIMIT    = float(os.environ.get("OWS_RATE_LIMIT_RPS", "5"))   # req/sec
TIMEOUT       = float(os.environ.get("OWS_TIMEOUT_SECONDS", "30"))
MAX_RETRIES   = 3
PAGE_SIZE     = 50   # OWS v3 default max


class RateLimiter:
    """Simple token bucket — ensures we never exceed OWS rate limit."""
    def __init__(self, rps: float):
        self._rps = rps
        self._tokens = rps
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

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


class OWSClient:
    """
    Async client for OWS v3 REST + GraphQL API.

    Always use as async context manager:
        async with OWSClient() as client:
            ...
    """

    def __init__(self, token: str = TOKEN, base_url: str = BASE_URL):
        if not token:
            raise ValueError("OWS_TOKEN is required — check your .env file")
        self._token = token
        self._base_url = base_url.rstrip("/")
        self._limiter = RateLimiter(RATE_LIMIT)
        self._client: httpx.AsyncClient | None = None

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
                return r.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    wait = 10 * attempt
                    logger.warning("429 Too Many Requests — waiting %ds", wait)
                    await asyncio.sleep(wait)
                elif e.response.status_code in (500, 502, 503):
                    wait = 2 ** attempt
                    logger.warning("Server error %d — retry %d/%d in %ds",
                                   e.response.status_code, attempt, MAX_RETRIES, wait)
                    await asyncio.sleep(wait)
                else:
                    logger.error("HTTP %d for %s", e.response.status_code, path)
                    raise
            except (httpx.ConnectError, httpx.ReadTimeout) as e:
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
                r = await self._client.post(GRAPHQL_URL, json=payload)
                r.raise_for_status()
                data = r.json()
                if "errors" in data:
                    logger.error("GraphQL errors: %s", data["errors"])
                    raise ValueError(f"GraphQL error: {data['errors']}")
                return data
            except (httpx.ConnectError, httpx.ReadTimeout) as e:
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

    # ── Announcements ─────────────────────────────────────────────────────────
    _TRD_BUY_QUERY = """
    query($limit: Int, $after: Int, $filter: TrdBuyFiltersInput!) {
      trd_buy(limit: $limit, after: $after, filters: $filter) {
        id nameRu numberAnno totalSum publishDate endDate
        refBuyStatusId refTradeMethodsId customerBin orgBin
        customerNameRu orgNameRu systemId lastUpdateDate
      }
    }
    """

    async def iter_announcements_by_bin(
        self, customer_bin: str, date_from: str = "2024-01-01", date_to: str = "2026-12-31"
    ) -> AsyncIterator[dict]:
        """GraphQL TrdBuy — all announcements for a customer BIN."""
        variables = {"filter": {
            "customerBin": customer_bin,
            "publishDate": [date_from, date_to],
        }}
        async for rec in self._paginate_graphql(self._TRD_BUY_QUERY, "trd_buy", variables):
            yield rec

    # ── Lots ──────────────────────────────────────────────────────────────────
    _LOTS_QUERY = """
    query($limit: Int, $after: Int, $filter: LotsFiltersInput!) {
      Lots(limit: $limit, after: $after, filters: $filter) {
        id nameRu nameKz descriptionRu amount count refUnitsCode
        refEnstruCode lotAmountSum customerBin refTradeMethodsId
        refLotStatusId systemId
        deliveryPlaces { refKatoCode fullDeliveryPlaceNameRu count }
      }
    }
    """

    async def iter_lots_by_bin(self, customer_bin: str) -> AsyncIterator[dict]:
        """GraphQL Lots — all lots for a customer BIN."""
        variables = {"filter": {"customerBin": customer_bin}}
        async for rec in self._paginate_graphql(self._LOTS_QUERY, "Lots", variables):
            yield rec

    # ── Contracts ─────────────────────────────────────────────────────────────
    _CONTRACT_QUERY = """
    query($limit: Int, $after: Int, $filter: ContractFiltersInput!) {
      Contract(limit: $limit, after: $after, filters: $filter) {
        id contractNumber lotId announcementId
        customerBin supplierBiin contractSum
        signDate startDate endDate
        refContractStatusId refTradeMethodsId
        systemId lastUpdateDate crdate
      }
    }
    """

    async def iter_contracts_by_bin(
        self, customer_bin: str, date_from: str = "2024-01-01", date_to: str = "2026-12-31"
    ) -> AsyncIterator[dict]:
        """GraphQL Contract — all contracts for a customer BIN."""
        variables = {"filter": {
            "customerBin": customer_bin,
            "crdate": [date_from, date_to],
        }}
        async for rec in self._paginate_graphql(self._CONTRACT_QUERY, "Contract", variables):
            yield rec

    # ── Contract items ─────────────────────────────────────────────────────────
    _ITEMS_QUERY = """
    query($limit: Int, $after: Int, $filter: ContractUnitsFiltersInput!) {
      ContractUnits(limit: $limit, after: $after, filters: $filter) {
        id contractId nameRu refUnitsCode count price totalSum
        refEnstruCode
      }
    }
    """

    async def iter_contract_items(self, contract_id: int) -> AsyncIterator[dict]:
        """GraphQL ContractUnits — line items for a specific contract."""
        variables = {"filter": {"contractId": contract_id}}
        async for rec in self._paginate_graphql(self._ITEMS_QUERY, "ContractUnits", variables):
            yield rec

    # ── Refs ──────────────────────────────────────────────────────────────────
    async def get_ref_units(self) -> list[dict]:
        data = await self._get("/v3/ref/units")
        return data.get("items", data if isinstance(data, list) else [])

    async def get_ref_trade_methods(self) -> list[dict]:
        data = await self._get("/v3/ref/trade-methods")
        return data.get("items", data if isinstance(data, list) else [])

    async def get_ref_lot_statuses(self) -> list[dict]:
        data = await self._get("/v3/ref/lot-statuses")
        return data.get("items", data if isinstance(data, list) else [])
