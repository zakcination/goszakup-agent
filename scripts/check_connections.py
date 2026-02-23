"""
scripts/check_connections.py
──────────────────────────────
Spiral 1 — Connection & Environment Validator

Checks:
  1. .env file exists and has required keys
  2. PostgreSQL is reachable and schema is applied
  3. OWS v3 API is reachable and token is valid
  4. Redis is reachable

Run from project root:
    python scripts/check_connections.py

Or inside docker (after spin-up):
    docker compose exec etl python /app/scripts/check_connections.py
"""

import asyncio
import os
import sys
import time
from pathlib import Path

# ── Colour helpers (no deps) ──────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
BOLD   = "\033[1m"
RESET  = "\033[0m"
OK     = f"{GREEN}✅{RESET}"
FAIL   = f"{RED}❌{RESET}"
WARN   = f"{YELLOW}⚠️ {RESET}"
INFO   = f"{BLUE}ℹ️ {RESET}"


def section(title: str):
    print(f"\n{BOLD}{'─'*50}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'─'*50}{RESET}")


def ok(msg):   print(f"  {OK}  {msg}")
def fail(msg): print(f"  {FAIL}  {msg}")
def warn(msg): print(f"  {WARN}  {msg}")
def info(msg): print(f"  {INFO}  {msg}")


# ════════════════════════════════════════════════════════════════════════════
# CHECK 1: .env file
# ════════════════════════════════════════════════════════════════════════════
def check_env() -> bool:
    section("1. Environment Variables")

    env_path = Path(".env")
    if not env_path.exists():
        fail(".env file not found!")
        info("Run: cp .env.example .env && nano .env")
        return False
    ok(".env file found")

    required = ["OWS_TOKEN", "POSTGRES_PASSWORD", "DATABASE_URL"]
    optional = ["AI_API_KEY", "REDIS_URL", "AI_MODEL"]
    all_ok = True

    for key in required:
        val = os.environ.get(key, "")
        if not val or val.startswith("your_") or val.startswith("change_"):
            fail(f"{key} is missing or still placeholder")
            all_ok = False
        else:
            masked = val[:6] + "..." + val[-4:] if len(val) > 10 else "***"
            ok(f"{key} = {masked}")

    for key in optional:
        val = os.environ.get(key, "")
        if not val or val.startswith("your_"):
            warn(f"{key} not set (optional for Spiral 1)")
        else:
            ok(f"{key} set")

    return all_ok


# ════════════════════════════════════════════════════════════════════════════
# CHECK 2: PostgreSQL
# ════════════════════════════════════════════════════════════════════════════
async def check_postgres() -> bool:
    section("2. PostgreSQL")
    try:
        import asyncpg
    except ImportError:
        fail("asyncpg not installed — run: pip install asyncpg")
        return False

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        fail("DATABASE_URL not set")
        return False

    try:
        t0 = time.monotonic()
        conn = await asyncpg.connect(db_url, timeout=10)
        elapsed = (time.monotonic() - t0) * 1000
        ok(f"Connected in {elapsed:.0f}ms")

        # Check schema
        tables = await conn.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename"
        )
        table_names = [r["tablename"] for r in tables]
        expected = ["subjects", "plan_points", "lots", "contracts", "contract_items", "etl_runs"]
        for t in expected:
            if t in table_names:
                ok(f"Table '{t}' exists")
            else:
                fail(f"Table '{t}' missing — schema not applied?")

        # Check seeded BINs
        count = await conn.fetchval("SELECT COUNT(*) FROM subjects WHERE is_customer=TRUE")
        ok(f"{count}/27 target BINs seeded")
        if count < 27:
            warn("Not all BINs seeded — check init.sql ran successfully")

        # Postgres version
        version = await conn.fetchval("SELECT version()")
        ok(f"Version: {version[:50]}")

        await conn.close()
        return True

    except Exception as e:
        fail(f"PostgreSQL connection failed: {e}")
        info("Is docker compose up? Check: docker compose ps")
        return False


# ════════════════════════════════════════════════════════════════════════════
# CHECK 3: OWS v3 API
# ════════════════════════════════════════════════════════════════════════════
async def check_ows_api() -> bool:
    section("3. OWS v3 API")
    try:
        import httpx
    except ImportError:
        fail("httpx not installed — run: pip install httpx")
        return False

    token = os.environ.get("OWS_TOKEN", "")
    if not token:
        fail("OWS_TOKEN not set")
        return False

    base_url = os.environ.get("OWS_BASE_URL", "https://ows.goszakup.gov.kz")

    try:
        # Test 1: REST — get one subject
        test_bin = "210240019348"
        t0 = time.monotonic()
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"{base_url}/v3/subject/biin/{test_bin}",
                headers={"Authorization": f"Bearer {token}"}
            )
            elapsed = (time.monotonic() - t0) * 1000

            if r.status_code == 200:
                data = r.json()
                name = data.get("name_ru", "N/A")
                ok(f"REST /v3/subject/biin/{test_bin} → {elapsed:.0f}ms")
                ok(f"  Name: {name[:60]}")
            elif r.status_code == 401:
                fail("401 Unauthorized — OWS_TOKEN is invalid or expired")
                info("Get a new token from your goszakup.gov.kz profile")
                return False
            elif r.status_code == 404:
                warn(f"BIN {test_bin} not found (404) — token OK but BIN unknown")
            else:
                fail(f"Unexpected status {r.status_code}: {r.text[:200]}")
                return False

        # Test 2: GraphQL
        t0 = time.monotonic()
        async with httpx.AsyncClient(timeout=15) as client:
            gql_payload = {
                "operationName": None,
                "query": "query { trd_buy(limit: 1, filters: {}) { id numberAnno } }",
                "variables": {}
            }
            r = await client.post(
                f"{base_url}/v3/graphql",
                json=gql_payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                }
            )
            elapsed = (time.monotonic() - t0) * 1000
            if r.status_code == 200:
                data = r.json()
                if "errors" not in data:
                    ok(f"GraphQL /v3/graphql → {elapsed:.0f}ms")
                else:
                    warn(f"GraphQL returned errors: {data['errors']}")
            else:
                fail(f"GraphQL failed: {r.status_code}")

        # Test 3: Journal endpoint
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"{base_url}/v3/journal",
                params={"date_from": "2024-01-01", "date_to": "2024-01-02"},
                headers={"Authorization": f"Bearer {token}"}
            )
            if r.status_code == 200:
                data = r.json()
                ok(f"Journal /v3/journal → {data.get('total', 0)} entries")
            else:
                warn(f"Journal endpoint returned {r.status_code}")

        return True

    except httpx.ConnectError:
        fail(f"Cannot reach {base_url} — check internet connection")
        return False
    except Exception as e:
        fail(f"OWS API check failed: {e}")
        return False


# ════════════════════════════════════════════════════════════════════════════
# CHECK 4: Redis
# ════════════════════════════════════════════════════════════════════════════
async def check_redis() -> bool:
    section("4. Redis")
    try:
        import redis.asyncio as aioredis
    except ImportError:
        warn("redis[asyncio] not installed — optional for Spiral 1")
        info("Install: pip install 'redis[asyncio]'")
        return True   # not blocking

    redis_url = os.environ.get("REDIS_URL", "")
    if not redis_url:
        warn("REDIS_URL not set — skipping Redis check")
        return True

    try:
        t0 = time.monotonic()
        r = aioredis.from_url(redis_url, socket_timeout=5)
        pong = await r.ping()
        elapsed = (time.monotonic() - t0) * 1000
        if pong:
            ok(f"Redis PING → PONG ({elapsed:.0f}ms)")
        await r.aclose()
        return True
    except Exception as e:
        fail(f"Redis connection failed: {e}")
        return False


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════
async def main():
    print(f"\n{BOLD}{'═'*50}{RESET}")
    print(f"{BOLD}  goszakup-agent · Spiral 1 · Connection Check{RESET}")
    print(f"{BOLD}{'═'*50}{RESET}")

    # Load .env if running locally (not in docker)
    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())

    results = []
    results.append(("Environment", check_env()))
    results.append(("PostgreSQL",  await check_postgres()))
    results.append(("OWS API",     await check_ows_api()))
    results.append(("Redis",       await check_redis()))

    # Summary
    section("SUMMARY")
    all_pass = True
    for name, passed in results:
        if passed:
            ok(f"{name}")
        else:
            fail(f"{name}")
            all_pass = False

    print()
    if all_pass:
        print(f"  {GREEN}{BOLD}🚀 All checks passed — ready for Spiral 1 ETL!{RESET}")
        print(f"  Next: python etl/load_subjects.py")
    else:
        print(f"  {RED}{BOLD}Fix failing checks before proceeding.{RESET}")
        sys.exit(1)
    print()


if __name__ == "__main__":
    asyncio.run(main())
