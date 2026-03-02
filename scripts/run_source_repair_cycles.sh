#!/usr/bin/env bash
# Run bounded source-link repair cycles:
#   1) repair missing lots.source_trd_buy_id (+kato/enstru where available)
#   2) backfill announcements for newly discovered source IDs
#   3) print compact quality snapshot
#
# Usage:
#   ./scripts/run_source_repair_cycles.sh --cycles 5 --chunk-size 1000

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="$ROOT/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="$(command -v python3)"
fi

CYCLES=1
CHUNK_SIZE=1000
REPAIR_CONCURRENCY=8
BACKFILL_CONCURRENCY=4

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cycles)
      CYCLES="$2"; shift 2;;
    --chunk-size)
      CHUNK_SIZE="$2"; shift 2;;
    --repair-concurrency)
      REPAIR_CONCURRENCY="$2"; shift 2;;
    --backfill-concurrency)
      BACKFILL_CONCURRENCY="$2"; shift 2;;
    *)
      echo "Unknown arg: $1"; exit 1;;
  esac
done

cd "$ROOT"

export OWS_TIMEOUT_SECONDS="${OWS_TIMEOUT_SECONDS:-60}"
export OWS_RPS_ADAPTIVE="${OWS_RPS_ADAPTIVE:-1}"
export OWS_RATE_LIMIT_RPS="${OWS_RATE_LIMIT_RPS:-8}"
export OWS_RPS_START="${OWS_RPS_START:-8}"
export OWS_RPS_MIN="${OWS_RPS_MIN:-3}"
export OWS_RPS_MAX="${OWS_RPS_MAX:-14}"
export OWS_RPS_STEP="${OWS_RPS_STEP:-0.5}"
export OWS_RPS_SUCCESS_WINDOW="${OWS_RPS_SUCCESS_WINDOW:-80}"

remaining_source_ids () {
  "$PY" - <<'PY'
import asyncio, asyncpg
from etl.utils import load_env, resolve_db_url
from etl.config import get_config
load_env(); cfg = get_config()
async def main():
    conn = await asyncpg.connect(resolve_db_url(cfg.db_url))
    q = '''
    WITH src AS (
      SELECT DISTINCT source_trd_buy_id AS sid FROM lots WHERE source_trd_buy_id IS NOT NULL AND source_trd_buy_id > 0
      UNION
      SELECT DISTINCT source_trd_buy_id AS sid FROM contracts WHERE source_trd_buy_id IS NOT NULL AND source_trd_buy_id > 0
    )
    SELECT count(*) FROM src
    WHERE NOT EXISTS (SELECT 1 FROM announcements a WHERE a.id = src.sid)
       OR EXISTS (SELECT 1 FROM lots l WHERE l.source_trd_buy_id = src.sid AND l.announcement_id IS NULL)
       OR EXISTS (SELECT 1 FROM contracts c WHERE c.source_trd_buy_id = src.sid AND c.announcement_id IS NULL)
    '''
    v = await conn.fetchval(q)
    print(v)
    await conn.close()
asyncio.run(main())
PY
}

echo "Settings:"
echo "  cycles=$CYCLES"
echo "  chunk_size=$CHUNK_SIZE"
echo "  repair_concurrency=$REPAIR_CONCURRENCY"
echo "  backfill_concurrency=$BACKFILL_CONCURRENCY"
echo "  OWS_RATE_LIMIT_RPS=$OWS_RATE_LIMIT_RPS (adaptive=$OWS_RPS_ADAPTIVE, range=$OWS_RPS_MIN..$OWS_RPS_MAX)"

for ((i=1; i<=CYCLES; i++)); do
  echo ""
  echo "===== Cycle $i/$CYCLES ====="

  echo "▶ repair_missing_source_links (lots)"
  "$PY" etl/repair_missing_source_links.py \
    --mode lots \
    --skip-announcements \
    --concurrency "$REPAIR_CONCURRENCY" \
    --checkpoint-every 500 \
    --resume-backstep 0 \
    --limit "$CHUNK_SIZE"

  rem="$(remaining_source_ids)"
  echo "remaining_source_ids_to_backfill=$rem"
  if [[ "$rem" -gt 0 ]]; then
    to_run="$CHUNK_SIZE"
    if [[ "$rem" -lt "$CHUNK_SIZE" ]]; then
      to_run="$rem"
    fi
    echo "▶ backfill_announcements_from_sources (limit=$to_run)"
    "$PY" etl/backfill_announcements_from_sources.py \
      --concurrency "$BACKFILL_CONCURRENCY" \
      --limit "$to_run"
  else
    echo "▶ backfill_announcements_from_sources skipped (no remaining source IDs)"
  fi

  echo "▶ quality snapshot"
  "$PY" scripts/check_quality_gaps.py | sed -n '1,10p'
done

echo ""
echo "Done."
