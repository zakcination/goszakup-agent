#!/usr/bin/env bash
# Daily incremental run: journal → parquet export → marts → verify
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="$ROOT/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="$(command -v python3)"
fi

DATE_FROM=""
DATE_TO=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --date-from)
      DATE_FROM="--date-from $2"; shift 2;;
    --date-to)
      DATE_TO="--date-to $2"; shift 2;;
    *)
      echo "Unknown arg: $1"; exit 1;;
  esac
done

cd "$ROOT"

echo "▶ journal incremental"
$PY etl/load_journal_incremental.py $DATE_FROM $DATE_TO

echo "▶ export parquet"
$PY analytics/export_parquet.py

echo "▶ build marts"
$PY analytics/build_marts.py

echo "▶ verify spiral3"
$PY scripts/verify_spiral3.py
