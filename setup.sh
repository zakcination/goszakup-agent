#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════════════════════
# goszakup-agent · setup.sh
# One script to rule them all — run this first on a fresh machine
#
# Usage:
#   chmod +x setup.sh
#   ./setup.sh          # full setup
#   ./setup.sh check    # env check only
#   ./setup.sh up       # docker up only
#   ./setup.sh reset    # tear down volumes and restart fresh
# ══════════════════════════════════════════════════════════════════════════════
set -euo pipefail

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; RESET='\033[0m'

ok()   { echo -e "  ${GREEN}✅${RESET}  $*"; }
fail() { echo -e "  ${RED}❌${RESET}  $*"; exit 1; }
warn() { echo -e "  ${YELLOW}⚠️ ${RESET}  $*"; }
info() { echo -e "  ${BLUE}ℹ️ ${RESET}  $*"; }
step() { echo -e "\n${BOLD}▶ $*${RESET}"; }

# ── Prereq checks ─────────────────────────────────────────────────────────────
check_prerequisites() {
    step "Checking prerequisites"

    command -v docker   >/dev/null 2>&1 || fail "Docker not found. Install: https://docs.docker.com/get-docker/"
    ok "Docker: $(docker --version)"

    # Support both 'docker compose' (v2) and 'docker-compose' (v1)
    if docker compose version >/dev/null 2>&1; then
        COMPOSE="docker compose"
    elif command -v docker-compose >/dev/null 2>&1; then
        COMPOSE="docker-compose"
    else
        fail "Docker Compose not found. Install Docker Desktop or 'docker-compose'"
    fi
    ok "Compose: $($COMPOSE version)"

    docker ps >/dev/null 2>&1 || fail "Docker daemon not running. Start Docker Desktop."
    ok "Docker daemon running"

    # Prefer Python 3.12/3.11 for binary wheels (duckdb/pyarrow)
    PYTHON_BIN=""
    for cand in python3.12 python3.11 python3.10 python3; do
        if command -v "$cand" >/dev/null 2>&1; then
            PYTHON_BIN="$cand"
            break
        fi
    done
    if [ -z "$PYTHON_BIN" ]; then
        fail "Python3 not found"
    fi
    PYTHON_VER=$($PYTHON_BIN -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    if $PYTHON_BIN -c "import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)"; then
        ok "Python $PYTHON_VER ($PYTHON_BIN)"
        if $PYTHON_BIN -c "import sys; sys.exit(0 if sys.version_info >= (3,13) else 1)"; then
            warn "Python 3.13 detected — duckdb/pyarrow wheels may be missing. Prefer 3.12 for analytics."
        fi
    else
        fail "Python >= 3.10 required (found $PYTHON_VER)"
    fi

    command -v git >/dev/null 2>&1 || fail "Git not found"
    ok "Git: $(git --version)"

    # Check ports
    step "Checking required ports"
    for port in 5432 8080 6379; do
        if lsof -i ":$port" >/dev/null 2>&1; then
            warn "Port $port is in use — may conflict"
        else
            ok "Port $port free"
        fi
    done

    # Check disk
    AVAILABLE_GB=$(df -g . | tail -1 | awk '{print $4}' | tr -d 'G')
    if [ "${AVAILABLE_GB:-0}" -lt 10 ]; then
        warn "Less than 10GB disk space available (${AVAILABLE_GB}GB). Recommend 20GB+"
    else
        ok "Disk space: ${AVAILABLE_GB}GB available"
    fi

    # Check RAM (macOS)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        RAM_GB=$(( $(sysctl -n hw.memsize) / 1024 / 1024 / 1024 ))
        if [ "$RAM_GB" -lt 8 ]; then
            warn "Only ${RAM_GB}GB RAM. Recommend 8GB+ for smooth operation"
        else
            ok "RAM: ${RAM_GB}GB"
        fi
    fi
}

# ── .env setup ────────────────────────────────────────────────────────────────
setup_env() {
    step "Setting up .env"

    if [ ! -f ".env" ]; then
        cp .env.example .env
        warn ".env created from template — YOU MUST FILL IN SECRETS!"
        echo ""
        echo -e "  ${BOLD}Required changes in .env:${RESET}"
        echo "    OWS_TOKEN=<your bearer token from goszakup.gov.kz>"
        echo "    POSTGRES_PASSWORD=<strong password>"
        echo "    DATABASE_URL=postgresql://goszakup_user:<password>@localhost:5432/goszakup"
        echo "    AI_API_KEY=<optional (Spiral 3+)>"
        echo ""
        echo -e "  ${YELLOW}Edit .env now, then re-run: ./setup.sh up${RESET}"
        exit 0
    else
        ok ".env already exists"

        # Warn if still has placeholders
        if grep -q "your_.*_here\|change_me" .env 2>/dev/null; then
            warn ".env still has placeholder values — fill in real secrets!"
        else
            ok ".env looks configured"
        fi
    fi
}

# ── Python venv ───────────────────────────────────────────────────────────────
setup_python() {
    step "Setting up Python environment"

    if [ ! -d ".venv" ]; then
        ${PYTHON_BIN:-python3} -m venv .venv
        ok "Created .venv"
    else
        ok ".venv already exists"
    fi

    source .venv/bin/activate
    pip install --upgrade pip -q
    pip install httpx asyncpg "redis[asyncio]" python-dotenv -q   # minimal for check_connections
    ok "Core packages installed"

    info "To install all packages: source .venv/bin/activate && pip install -r requirements.txt"
}

# ── Docker up ────────────────────────────────────────────────────────────────
docker_up() {
    step "Starting Docker services (Spiral 1)"

    $COMPOSE up -d postgres redis
    ok "postgres + redis starting..."

    # Wait for postgres
    echo -n "  Waiting for PostgreSQL"
    for i in $(seq 1 30); do
        if $COMPOSE exec -T postgres pg_isready -q 2>/dev/null; then
            echo ""
            ok "PostgreSQL ready"
            break
        fi
        echo -n "."
        sleep 2
    done

    # Also start adminer in dev mode
    $COMPOSE --profile dev up -d adminer 2>/dev/null || true
    ok "Adminer UI available at http://localhost:8080"
    info "  Server: postgres | DB: goszakup | User: goszakup_user"
}

# ── Check connections ─────────────────────────────────────────────────────────
run_checks() {
    step "Running connection checks"
    source .venv/bin/activate 2>/dev/null || true
    python3 scripts/check_connections.py
}

# ── Git init ──────────────────────────────────────────────────────────────────
git_init() {
    step "Initialising git repository"
    if [ ! -d ".git" ]; then
        git init
        git add .
        git commit -m "feat: Spiral 1 — project skeleton, schema, API client"
        ok "Git repo initialised with initial commit"
    else
        ok "Git repo already exists"
    fi
}

# ── Reset ─────────────────────────────────────────────────────────────────────
reset() {
    step "Resetting all data (volumes will be deleted!)"
    read -rp "  Are you sure? This deletes all data [y/N]: " confirm
    if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
        $COMPOSE down -v
        ok "Containers and volumes removed"
        docker_up
    else
        info "Reset cancelled"
    fi
}

# ── Print status ──────────────────────────────────────────────────────────────
print_status() {
    echo ""
    echo -e "${BOLD}══════════════════════════════════════════════${RESET}"
    echo -e "${BOLD}  goszakup-agent · Spiral 1 · Status${RESET}"
    echo -e "${BOLD}══════════════════════════════════════════════${RESET}"
    $COMPOSE ps 2>/dev/null || true
    echo ""
    echo -e "${GREEN}${BOLD}Next steps:${RESET}"
    echo "  1. Fill in .env (OWS_TOKEN, passwords)"
    echo "  2. ./setup.sh up              — start services"
    echo "  3. python scripts/check_connections.py   — verify everything"
    echo "  4. python etl/load_subjects.py           — first data load!"
    echo ""
}

# ── Entry point ───────────────────────────────────────────────────────────────
case "${1:-full}" in
    check)
        run_checks
        ;;
    up)
        check_prerequisites
        setup_env
        docker_up
        run_checks
        ;;
    reset)
        reset
        ;;
    status)
        print_status
        ;;
    full|*)
        check_prerequisites
        setup_env
        setup_python
        docker_up
        git_init
        run_checks
        print_status
        ;;
esac
