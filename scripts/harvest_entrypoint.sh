#!/usr/bin/env bash
# Smart harvest entrypoint for sharded workflow.
# Tries harvest_shard.py, then coverage_harvest.py, then run_once.py (only on shard 0),
# else falls back to copying data/*.csv (only on shard 0).

set -euo pipefail
OUT_DIR="${1:-shard_out}"
SHARD_INDEX="${SHARD_INDEX:-0}"
SHARD_COUNT="${SHARD_COUNT:-1}"

mkdir -p "$OUT_DIR"
mkdir -p dataset

have_any=false

run_and_collect() {
  # Move any per-shard outputs into OUT_DIR if they exist; otherwise move dataset CSVs.
  local moved=0
  shopt -s nullglob
  for f in dataset/*_shard_${SHARD_INDEX}_of_${SHARD_COUNT}.csv; do
    mv -f "$f" "$OUT_DIR/"; moved=$((moved+1))
  done
  if (( moved == 0 )); then
    for f in dataset/*.csv; do
      # avoid scooping previous merges; prefer document_* / codes_* names
      case "$(basename "$f")" in
        document_*|codes_*.csv) cp -f "$f" "$OUT_DIR/"; moved=$((moved+1)) ;;
      esac
    done
  fi
  if (( moved > 0 )); then have_any=true; fi
  echo "Collected $moved CSV(s) into $OUT_DIR/"
}

if [[ -f "scripts/harvest_shard.py" ]]; then
  echo "▶ Running scripts/harvest_shard.py (shard ${SHARD_INDEX}/${SHARD_COUNT})"
  python scripts/harvest_shard.py
  run_and_collect
  $have_any && exit 0 || { echo "⚠️ No outputs found after harvest_shard.py"; exit 2; }
fi

if [[ -f "scripts/coverage_harvest.py" ]]; then
  echo "▶ Running scripts/coverage_harvest.py (shard ${SHARD_INDEX}/${SHARD_COUNT})"
  python scripts/coverage_harvest.py --shard-index "$SHARD_INDEX" --shard-count "$SHARD_COUNT" --out "$OUT_DIR" || true
  # Some versions write into dataset/, not --out
  run_and_collect
  $have_any &&_
