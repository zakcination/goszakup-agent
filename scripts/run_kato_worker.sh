#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="$ROOT/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY=$(command -v python3)
fi

# Load .env if present so OWS_TOKEN is available
if [[ -f "$ROOT/.env" ]]; then
  set -a
  source "$ROOT/.env"
  set +a
fi

IDS=${KATO_TRD_BUY_IDS:-82661999}
LOG_LEVEL=${KATO_TRD_LOG_LEVEL:-INFO}

cd "$ROOT"

for id in $IDS; do
  echo "▶ fetching kato metadata for trd-buy $id"
  PYTHONPATH="$ROOT" "$PY" etl/load_trd_buy_kato.py --trd-buy-id "$id" --log-level "$LOG_LEVEL"
done
