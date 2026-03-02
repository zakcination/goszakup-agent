#!/usr/bin/env bash
# Strict recovery runner:
#   1) reload refs
#   2) reprocess Spiral-2 ingestion with finite backstep
#   3) run checkpointed high-volume workers
#   4) capture quality snapshot + verify Spiral-2
#   5) optionally rebuild analytics + verify Spiral-3
# Notes:
#   - default deep backstep is finite (RESUME_BACKSTEP or 20000) for restart safety
#   - use --full-rescan for full rewind from the beginning
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="$ROOT/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="$(command -v python3)"
fi

BIN_ARG=""
YFROM_ARG=""
YTO_ARG=""
LIMIT_ARG=""
DRY_RUN=""
CONCURRENCY_ARG=""
BACKSTEP_ARG="--resume-backstep 999999999"
FULL_RESCAN="0"
REBUILD_ANALYTICS="1"
RPS_START="${OWS_RPS_START:-20}"
RPS_MAX="${OWS_RPS_MAX:-120}"
REF_FORCE_KATO=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bins)
      BIN_ARG="--bins $2"; shift 2;;
    --year-from)
      YFROM_ARG="--year-from $2"; shift 2;;
    --year-to)
      YTO_ARG="--year-to $2"; shift 2;;
    --limit)
      LIMIT_ARG="--limit $2"; shift 2;;
    --dry-run)
      DRY_RUN="--dry-run"; shift 1;;
    --resume-backstep)
      BACKSTEP_ARG="--resume-backstep $2"; shift 2;;
    --full-rescan)
      FULL_RESCAN="1"
      BACKSTEP_ARG="--resume-backstep 999999999"; shift 1;;
    --concurrency)
      CONCURRENCY_ARG="--concurrency $2"; shift 2;;
    --rps-start)
      RPS_START="$2"; shift 2;;
    --rps-max)
      RPS_MAX="$2"; shift 2;;
    --skip-analytics)
      REBUILD_ANALYTICS="0"; shift 1;;
    *)
      echo "Unknown arg: $1"; exit 1;;
  esac
done

if [[ "$FULL_RESCAN" == "0" && "$BACKSTEP_ARG" == "--resume-backstep 999999999" ]]; then
  # Default strict mode now uses a finite deep backstep to allow restart without full rewind.
  BACKSTEP_ARG="--resume-backstep ${RESUME_BACKSTEP:-20000}"
fi

run_step () {
  local name="$1"
  local cmd="$2"
  echo ""
  echo "▶ $name"
  eval "$cmd"
}

cd "$ROOT"

export OWS_RPS_ADAPTIVE=1
export OWS_RPS_START="$RPS_START"
export OWS_RPS_MAX="$RPS_MAX"
export PLANS_KATO_REFRESH_MODE="${PLANS_KATO_REFRESH_MODE:-incremental}"
export PLANS_SPEC_REFRESH_MODE="${PLANS_SPEC_REFRESH_MODE:-incremental}"
export ACTS_REFRESH_MODE="${ACTS_REFRESH_MODE:-incremental}"
export TREASURY_PAY_REFRESH_MODE="${TREASURY_PAY_REFRESH_MODE:-incremental}"

if [[ "$FULL_RESCAN" == "1" ]]; then
  export PLANS_KATO_REFRESH_MODE="full"
  export PLANS_SPEC_REFRESH_MODE="full"
  export ACTS_REFRESH_MODE="full"
  export TREASURY_PAY_REFRESH_MODE="full"
  REF_FORCE_KATO="--force-kato"
fi

echo "Recovery settings:"
echo "  OWS_RPS_START=$OWS_RPS_START"
echo "  OWS_RPS_MAX=$OWS_RPS_MAX"
echo "  $BACKSTEP_ARG"
echo "  PLANS_KATO_REFRESH_MODE=$PLANS_KATO_REFRESH_MODE"
echo "  PLANS_SPEC_REFRESH_MODE=$PLANS_SPEC_REFRESH_MODE"
echo "  ACTS_REFRESH_MODE=$ACTS_REFRESH_MODE"
echo "  TREASURY_PAY_REFRESH_MODE=$TREASURY_PAY_REFRESH_MODE"

run_step "load_refs (incl. KATO)" "$PY etl/load_refs.py $REF_FORCE_KATO"
run_step "load_plans (strict reprocess)" "$PY etl/load_plans.py $BIN_ARG $YFROM_ARG $YTO_ARG $LIMIT_ARG $BACKSTEP_ARG $CONCURRENCY_ARG $DRY_RUN"
run_step "load_plans_kato_incremental" "$PY etl/load_plans_kato_incremental.py $BIN_ARG $YFROM_ARG $YTO_ARG $DRY_RUN"
run_step "load_plans_spec_incremental" "$PY etl/load_plans_spec_incremental.py $BIN_ARG $YFROM_ARG $YTO_ARG $DRY_RUN"
run_step "load_announcements (strict reprocess)" "$PY etl/load_announcements.py $BIN_ARG $YFROM_ARG $YTO_ARG $LIMIT_ARG $BACKSTEP_ARG $CONCURRENCY_ARG $DRY_RUN"
run_step "load_lots (strict reprocess)" "$PY etl/load_lots.py $BIN_ARG $YFROM_ARG $YTO_ARG $LIMIT_ARG $BACKSTEP_ARG $CONCURRENCY_ARG $DRY_RUN"
run_step "load_contracts (strict reprocess)" "$PY etl/load_contracts.py $BIN_ARG $YFROM_ARG $YTO_ARG $LIMIT_ARG $BACKSTEP_ARG $CONCURRENCY_ARG $DRY_RUN"
run_step "repair missing source links from detail endpoints" "$PY etl/repair_missing_source_links.py $LIMIT_ARG $CONCURRENCY_ARG $DRY_RUN"
run_step "load_trd_buy_process_events_incremental" "$PY etl/load_trd_buy_process_events_incremental.py $YFROM_ARG $YTO_ARG $LIMIT_ARG $DRY_RUN"
run_step "backfill announcements from source_trd_buy_id" "$PY etl/backfill_announcements_from_sources.py $LIMIT_ARG $CONCURRENCY_ARG $DRY_RUN"
run_step "load_contract_items (strict reprocess + repair missing)" "$PY etl/load_contract_items.py $BIN_ARG $YFROM_ARG $YTO_ARG $LIMIT_ARG $BACKSTEP_ARG $CONCURRENCY_ARG $DRY_RUN --repair-missing"
run_step "load_acts_incremental" "$PY etl/load_acts_incremental.py $DRY_RUN"
run_step "load_treasury_pay_incremental" "$PY etl/load_treasury_pay_incremental.py $DRY_RUN"
run_step "capture_quality_snapshot" "$PY scripts/capture_quality_snapshot.py"
run_step "verify_spiral2" "$PY scripts/verify_spiral2.py"

if [[ "$REBUILD_ANALYTICS" == "1" ]]; then
  run_step "export_parquet" "$PY analytics/export_parquet.py"
  run_step "build_marts" "$PY analytics/build_marts.py"
  run_step "verify_spiral3" "$PY scripts/verify_spiral3.py"
fi

echo ""
echo "✅ Strict recovery completed."
