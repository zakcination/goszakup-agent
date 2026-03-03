#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PY="$ROOT/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="$(command -v python3)"
fi

ts="$(date -u +%Y%m%d_%H%M%S)"
out_dir="$ROOT/artifacts/db_discovery_${ts}"
mkdir -p "$out_dir"

echo "▶ Discovering Postgres"
$PY scripts/discover_postgres.py "$@" | tee "$out_dir/postgres.txt" >/dev/null

echo ""
echo "▶ Discovering DuckDB"
$PY scripts/discover_duckdb.py "$@" | tee "$out_dir/duckdb.txt" >/dev/null

echo ""
echo "▶ Saved"
echo "  $out_dir/postgres.txt"
echo "  $out_dir/duckdb.txt"

