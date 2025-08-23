#!/usr/bin/env bash
# Harvest entrypoint used by the sharded workflow.
# If scripts/coverage_harvest.py exists, run it with shard params.
# Else (fallback), on shard 0 copy data/*.csv -> OUT_DIR/ (others do nothing).
set -euo pipefail
OUT_DIR="${1:-shard_out}"
SHARD_INDEX="${SHARD_INDEX:-0}"
SHARD_COUNT="${SHARD_COUNT:-1}"

mkdir -p "$OUT_DIR"

if [[ -f "scripts/coverage_harvest.py" ]]; then
  echo "▶ Running scripts/coverage_harvest.py (shard ${SHARD_INDEX}/${SHARD_COUNT})"
  python scripts/coverage_harvest.py \
    --shard-index "$SHARD_INDEX" \
    --shard-count "$SHARD_COUNT" \
    --out "$OUT_DIR"
  exit 0
fi

if [[ "$SHARD_INDEX" != "0" ]]; then
  echo "ℹ️ Fallback mode: non-zero shard ${SHARD_INDEX} does nothing."
  exit 0
fi

echo "⚠️ Fallback mode: scripts/coverage_harvest.py not found."
echo "   Copying CSVs from data/ into ${OUT_DIR}/ ..."
shopt -s nullglob
found=false
for f in data/*.csv; do
  found=true
  cp -f "$f" "$OUT_DIR/"
done
if ! $found; then
  echo "❌ No data/*.csv found to harvest. Aborting."
  exit 2
fi
echo "✅ Fallback harvest complete in ${OUT_DIR}/"
