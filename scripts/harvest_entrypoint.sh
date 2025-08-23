#!/usr/bin/env bash
# Smart harvest entrypoint for sharded workflow.
# Prefers: python -m scripts.harvest_shard
# Falls back to: python -m scripts.coverage_harvest (with shard args)
# Lastly (shard 0 only): copy data/*.csv to OUT_DIR

set -euo pipefail

OUT_DIR="${1:-shard_out}"
SHARD_INDEX="${SHARD_INDEX:-0}"
# unify shard total: prefer SHARD_TOTAL, then SHARD_COUNT, else 1
SHARD_TOTAL="${SHARD_TOTAL:-${SHARD_COUNT:-1}}"

export PYTHONPATH="${PYTHONPATH:-$PWD}"
export SHARD_INDEX SHARD_TOTAL

mkdir -p "$OUT_DIR" dataset

echo "[ENTRYPOINT] SHARD_INDEX=${SHARD_INDEX} SHARD_TOTAL=${SHARD_TOTAL}"

have_any=false
run_and_collect() {
  local moved=0
  shopt -s nullglob
  # Prefer per-shard outputs
  for f in dataset/*_shard_${SHARD_INDEX}_of_${SHARD_TOTAL}.csv; do
    mv -f "$f" "$OUT_DIR/"; moved=$((moved+1))
  done
  # Otherwise collect common outputs
  if (( moved == 0 )); then
    for f in dataset/*.csv; do
      case "$(basename "$f")" in
        document_*|codes_*.csv) cp -f "$f" "$OUT_DIR/"; moved=$((moved+1)) ;;
      esac
    done
  fi
  (( moved > 0 )) && have_any=true
  echo "[ENTRYPOINT] Collected $moved CSV(s) into $OUT_DIR/"
}

if [[ -f "scripts/harvest_shard.py" ]]; then
  echo "▶ Running python -m scripts.harvest_shard (shard ${SHARD_INDEX}/${SHARD_TOTAL})"
  python -m scripts.harvest_shard
  run_and_collect
  $have_any && exit 0 || { echo "⚠️ No outputs found after harvest_shard.py"; exit 2; }
fi

if [[ -f "scripts/coverage_harvest.py" ]]; then
  echo "▶ Running python -m scripts.coverage_harvest (shard ${SHARD_INDEX}/${SHARD_TOTAL})"
  python -m scripts.coverage_harvest \
    --shard-index "$SHARD_INDEX" \
    --shard-count "$SHARD_TOTAL" \
    --out "$OUT_DIR" || true
  run_and_collect
  $have_any && exit 0 || { echo "⚠️ No outputs found after coverage_harvest.py"; exit 2; }
fi

# Fallback: only
