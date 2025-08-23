#!/usr/bin/env bash
# Smart harvest entrypoint for sharded workflow.
# Tries harvest_shard.py, then coverage_harvest.py, then run_once.py (only shard 0),
# else falls back to copying data/*.csv (only shard 0).

set -euo pipefail

OUT_DIR="${1:-shard_out}"
SHARD_INDEX="${SHARD_INDEX:-0}"
SHARD_COUNT="${SHARD_COUNT:-1}"

# Make sure Python can import from the repo root
export PYTHONPATH="${PYTHONPATH:-$PWD}"

mkdir -p "$OUT_DIR" dataset

have_any=false
run_and_collect() {
  local moved=0
  shopt -s nullglob
  # Prefer per-shard outputs if present
  for f in dataset/*_shard_${SHARD_INDEX}_of_${SHARD_COUNT}.csv; do
    mv -f "$f" "$OUT_DIR/"; moved=$((moved+1))
  done
  # Otherwise collect well-known dataset files
  if (( moved == 0 )); then
    for f in dataset/*.csv; do
      case "$(basename "$f")" in
        document_*|codes_*.csv) cp -f "$f" "$OUT_DIR/"; moved=$((moved+1)) ;;
      esac
    done
  fi
  (( moved > 0 )) && have_any=true
  echo "Collected $moved CSV(s) into $OUT_DIR/"
}

if [[ -f "scripts/harvest_shard.py" ]]; then
  echo "▶ Running python -m scripts.harvest_shard (shard ${SHARD_INDEX}/${SHARD_COUNT})"
  python -m scripts.harvest_shard
  run_and_collect
  $have_any && exit 0 || { echo "⚠️ No outputs found after harvest_shard.py"; exit 2; }
fi

if [[ -f "scripts/coverage_harvest.py" ]]; then
  echo "▶ Running python -m scripts.coverage_harvest (shard ${SHARD_INDEX}/${SHARD_COUNT})"
  python -m scripts.coverage_harvest \
    --shard-index "$SHARD_INDEX" \
    --shard-count "$SHARD_COUNT" \
    --out "$OUT_DIR" || true
  run_and_collect
  $have_any && exit 0 || { echo "⚠️ No outputs found after coverage_harvest.py"; exit 2; }
fi

if [[ -f "scripts/run_once.py" ]]; then
  if [[ "$SHARD_INDEX" != "0" ]]; then
    echo "ℹ️ run_once.py is non-sharded; skipping on shard ${SHARD_INDEX}"
    exit 0
  fi
  echo "▶ Running python -m scripts.run_once (single-shot on shard 0)"
  python -m scripts.run_once
  run_and_collect
  $have_any && exit 0 || { echo "⚠️ No outputs found after run_once.py"; exit 2; }
fi

# Fallback: only shard 0 copies seed CSVs
if [[ "$SHARD_INDEX" != "0" ]]; then
  echo "ℹ️ Fallback mode: non-zero shard ${SHARD_INDEX} does nothing."
  exit 0
fi
echo "⚠️ Fallback mode: no harvester found. Copying data/*.csv into ${OUT_DIR}/ ..."
shopt -s nullglob
copied=0
for f in data/*.csv; do cp -f "$f" "$OUT_DIR/"; copied=$((copied+1)); done
(( copied > 0 )) && echo "✅ Fallback copied $copied file(s) to ${OUT_DIR}/" || { echo "❌ No data/*.csv to copy"; exit 2; }
