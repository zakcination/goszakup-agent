"""
scripts/check_connections.py — Spiral 1 connection validator
Runs on the HOST machine (outside Docker), connects via 127.0.0.1 ports.
"""

import asyncio, os, sys, time
from pathlib import Path


def _maybe_reexec_in_venv() -> None:
    """Re-exec inside .venv if available and not already active."""
    if os.environ.get("VIRTUAL_ENV"):
        return
    venv_python = (Path(__file__).resolve().parent.parent / ".venv" / "bin" / "python")
    if not venv_python.exists():
        return
    if Path(sys.executable).resolve() == venv_python.resolve():
        return
    os.execv(str(venv_python), [str(venv_python), *sys.argv])


_maybe_reexec_in_venv()

GREEN="\033[92m"; RED="\033[91m"; YELLOW="\033[93m"; BLUE="\033[94m"
BOLD="\033[1m";   RESET="\033[0m"
OK=f"{GREEN}✅{RESET}"; FAIL=f"{RED}❌{RESET}"
WARN=f"{YELLOW}⚠️ {RESET}"; INFO=f"{BLUE}ℹ️ {RESET}"

def section(t): print(f"\n{BOLD}{'─'*50}\n  {t}\n{'─'*50}{RESET}")
def ok(m):   print(f"  {OK}  {m}")
def fail(m): print(f"  {FAIL}  {m}")
def warn(m): print(f"  {WARN}  {m}")
def info(m): print(f"  {INFO}  {m}")


# ── 1. Env ────────────────────────────────────────────────────────────────────
def check_env() -> bool:
    section("1. Environment Variables")
    if not Path(".env").exists():
        fail(".env not found — run: cp .env.example .env")
        return False
    ok(".env found")

    all_ok = True
    for key in ["OWS_TOKEN", "POSTGRES_PASSWORD", "DATABASE_URL"]:
        val = os.environ.get(key, "")
        if not val or "your_" in val or "change_me" in val:
            fail(f"{key} missing or placeholder"); all_ok = False
        else:
            masked = val[:6]+"..."+val[-4:] if len(val)>10 else "***"
            ok(f"{key} = {masked}")

    for key in ["AI_API_KEY", "REDIS_URL", "AI_MODEL"]:
        val = os.environ.get(key, "")
        if not val or "your_" in val: warn(f"{key} not set (optional S1)")
        else: ok(f"{key} set")

    return all_ok


# ── 2. PostgreSQL ─────────────────────────────────────────────────────────────
async def check_postgres() -> bool:
    section("2. PostgreSQL")
    try: import asyncpg
    except ImportError:
        fail("asyncpg not installed: pip install asyncpg"); return False

    # Build a localhost URL regardless of what DATABASE_URL says
    # (DATABASE_URL uses 'postgres' hostname — only valid inside Docker)
    db_url = os.environ.get("DATABASE_URL", "")
    local_url = db_url.replace("@postgres:", "@localhost:").replace("@redis:", "@localhost:")
    if not local_url:
        fail("DATABASE_URL not set"); return False

    try:
        t0 = time.monotonic()
        conn = await asyncpg.connect(local_url, timeout=10)
        ok(f"Connected in {(time.monotonic()-t0)*1000:.0f}ms  →  127.0.0.1:5432")

        tables = [r["tablename"] for r in await conn.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")]
        for t in ["subjects","plan_points","lots","contracts","contract_items","etl_runs"]:
            ok(f"Table '{t}' ✓") if t in tables else fail(f"Table '{t}' missing")

        count = await conn.fetchval("SELECT COUNT(*) FROM subjects WHERE is_customer=TRUE")
        raw_bins = os.environ.get("TARGET_BINS", "")
        expected = len([b for b in raw_bins.split(",") if b.strip()]) if raw_bins else 0
        if expected:
            ok(f"{count}/{expected} target BINs present in subjects (is_customer=TRUE)")
        else:
            ok(f"{count} customer BINs present in subjects (is_customer=TRUE)")

        ver = await conn.fetchval("SELECT version()")
        ok(f"Version: {ver[:55]}")
        await conn.close()
        return True
    except Exception as e:
        fail(f"PostgreSQL: {e}")
        info("Make sure 'docker compose up -d' is running")
        return False


# ── 3. OWS v3 API ────────────────────────────────────────────────────────────
async def check_ows_api() -> bool:
    section("3. OWS v3 API")
    try: import httpx
    except ImportError:
        fail("httpx not installed: pip install httpx"); return False

    token = os.environ.get("OWS_TOKEN","")
    if not token: fail("OWS_TOKEN not set"); return False

    base = os.environ.get("OWS_BASE_URL","https://ows.goszakup.gov.kz")
    hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    try:
        async with httpx.AsyncClient(timeout=20) as c:
            # REST
            t0 = time.monotonic()
            r = await c.get(f"{base}/v3/subject/biin/210240019348", headers=hdrs)
            if r.status_code == 200:
                ok(f"REST /subject/biin → {(time.monotonic()-t0)*1000:.0f}ms")
                ok(f"  Name: {r.json().get('name_ru','')[:60]}")
            elif r.status_code == 401:
                fail("401 — token invalid/expired"); return False
            else:
                warn(f"REST returned {r.status_code}")

            # GraphQL health check: __typename should work regardless of schema style.
            t0 = time.monotonic()
            r = await c.post(f"{base}/v3/graphql", headers=hdrs, json={
                "operationName": None,
                "query": "query { __typename }",
                "variables": {}
            })
            d = r.json()
            if r.status_code == 200 and "errors" not in d:
                ok(f"GraphQL → {(time.monotonic()-t0)*1000:.0f}ms ✓")
            else:
                errs = d.get("errors", [])
                fail(f"GraphQL: {errs[0].get('message') if errs else r.status_code}")

            # Journal
            r = await c.get(f"{base}/v3/journal",
                            params={"date_from":"2024-01-01","date_to":"2024-01-02"},
                            headers=hdrs)
            if r.status_code == 200:
                ok(f"Journal → {r.json().get('total',0)} entries")
            else:
                warn(f"Journal returned {r.status_code}")

        return True
    except httpx.ConnectError:
        fail(f"Cannot reach {base}"); return False
    except Exception as e:
        fail(f"OWS API: {e}"); return False


# ── 4. Redis ──────────────────────────────────────────────────────────────────
async def check_redis() -> bool:
    section("4. Redis")
    try: import redis.asyncio as aioredis
    except ImportError:
        warn("redis not installed (optional S1): pip install redis"); return True

    # Always connect via localhost, not Docker hostname
    redis_url = os.environ.get("REDIS_URL","redis://localhost:6379/0")
    local_url = redis_url.replace("redis://redis:", "redis://localhost:")

    # Inject password if REDIS_PASSWORD is set
    pw = os.environ.get("REDIS_PASSWORD","redis_change_me")
    if "://" in local_url and "@" not in local_url:
        local_url = local_url.replace("redis://", f"redis://:{pw}@")

    try:
        t0 = time.monotonic()
        r = aioredis.from_url(local_url, socket_timeout=5)
        assert await r.ping()
        ok(f"Redis PING → PONG ({(time.monotonic()-t0)*1000:.0f}ms)  →  127.0.0.1:6379")
        await r.aclose()
        return True
    except Exception as e:
        fail(f"Redis: {e}"); return False


# ── Main ─────────────────────────────────────────────────────────────────────
async def main():
    print(f"\n{BOLD}{'═'*50}")
    print(f"  goszakup-agent · Spiral 1 · Connection Check")
    print(f"{'═'*50}{RESET}")

    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

    results = [
        ("Environment", check_env()),
        ("PostgreSQL",  await check_postgres()),
        ("OWS API",     await check_ows_api()),
        ("Redis",       await check_redis()),
    ]

    section("SUMMARY")
    all_pass = True
    for name, passed in results:
        (ok if passed else fail)(name)
        if not passed: all_pass = False

    print()
    if all_pass:
        print(f"  {GREEN}{BOLD}🚀 All checks passed — run: python etl/load_subjects.py{RESET}")
    else:
        print(f"  {RED}{BOLD}Fix failing checks above before proceeding.{RESET}")
        sys.exit(1)
    print()

if __name__ == "__main__":
    asyncio.run(main())
