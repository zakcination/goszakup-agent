#!/usr/bin/env bash
# Spiral 2 runner:
# refs → plans → plans_kato_worker → plans_spec_worker → announcements → lots
# → contracts → announcements linkage backfill → contract_items
# → acts_worker → treasury_pay_worker → quality_snapshot → verify
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
BACKSTEP_ARG=""
CONCURRENCY_ARG=""

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
    --concurrency)
      CONCURRENCY_ARG="--concurrency $2"; shift 2;;
    *)
      echo "Unknown arg: $1"; exit 1;;
  esac
done

run_step () {
  local name="$1"
  local cmd="$2"
  echo ""
  echo "▶ $name"
  eval "$cmd"
}

cd "$ROOT"

export PLANS_KATO_REFRESH_MODE="${PLANS_KATO_REFRESH_MODE:-incremental}"
export PLANS_SPEC_REFRESH_MODE="${PLANS_SPEC_REFRESH_MODE:-incremental}"
export ACTS_REFRESH_MODE="${ACTS_REFRESH_MODE:-incremental}"
export TREASURY_PAY_REFRESH_MODE="${TREASURY_PAY_REFRESH_MODE:-incremental}"

run_step "load_refs" "$PY etl/load_refs.py"
run_step "load_plans" "$PY etl/load_plans.py $BIN_ARG $YFROM_ARG $YTO_ARG $LIMIT_ARG $BACKSTEP_ARG $CONCURRENCY_ARG $DRY_RUN"
run_step "load_plans_kato_incremental" "$PY etl/load_plans_kato_incremental.py $BIN_ARG $YFROM_ARG $YTO_ARG $DRY_RUN"
run_step "load_plans_spec_incremental" "$PY etl/load_plans_spec_incremental.py $BIN_ARG $YFROM_ARG $YTO_ARG $DRY_RUN"
run_step "load_announcements" "$PY etl/load_announcements.py $BIN_ARG $YFROM_ARG $YTO_ARG $LIMIT_ARG $BACKSTEP_ARG $CONCURRENCY_ARG $DRY_RUN"
run_step "load_lots" "$PY etl/load_lots.py $BIN_ARG $YFROM_ARG $YTO_ARG $LIMIT_ARG $BACKSTEP_ARG $CONCURRENCY_ARG $DRY_RUN"
run_step "load_contracts" "$PY etl/load_contracts.py $BIN_ARG $YFROM_ARG $YTO_ARG $LIMIT_ARG $BACKSTEP_ARG $CONCURRENCY_ARG $DRY_RUN"
run_step "load_trd_buy_process_events_incremental" "$PY etl/load_trd_buy_process_events_incremental.py $YFROM_ARG $YTO_ARG $LIMIT_ARG $DRY_RUN"
run_step "backfill_announcements_from_sources" "$PY etl/backfill_announcements_from_sources.py $LIMIT_ARG $CONCURRENCY_ARG $DRY_RUN"
run_step "load_contract_items" "$PY etl/load_contract_items.py $BIN_ARG $YFROM_ARG $YTO_ARG $LIMIT_ARG $BACKSTEP_ARG $CONCURRENCY_ARG $DRY_RUN"
run_step "load_acts_incremental" "$PY etl/load_acts_incremental.py $DRY_RUN"
run_step "load_treasury_pay_incremental" "$PY etl/load_treasury_pay_incremental.py $DRY_RUN"
run_step "capture_quality_snapshot" "$PY scripts/capture_quality_snapshot.py"
run_step "verify_spiral2" "$PY scripts/verify_spiral2.py"
