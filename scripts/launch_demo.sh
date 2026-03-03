#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "▶ Starting demo services (postgres, redis, analytics_api, agent_api, ui)"
docker compose up -d postgres redis analytics_api agent_api ui --remove-orphans

echo ""
echo "▶ Health checks"
curl -fsS http://127.0.0.1:8002/health >/dev/null && echo "  OK  agent_api /health"
curl -fsS http://127.0.0.1:8001/tools  >/dev/null && echo "  OK  analytics_api /tools"

echo ""
echo "▶ URLs"
echo "  UI:           http://127.0.0.1:8501"
echo "  Agent API:    http://127.0.0.1:8002/docs"
echo "  Analytics API:http://127.0.0.1:8001/docs"
echo "  Adminer:      http://127.0.0.1:8080 (dev profile)"

