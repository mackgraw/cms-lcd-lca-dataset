#!/usr/bin/env bash
set -euo pipefail

KEEP1=".github/workflows/coverage-harvest.sharded.yml"
KEEP2=".github/workflows/release-dataset.yml"
ARCHIVE=".github/workflows.disabled"

mkdir -p "$ARCHIVE"

echo "Archiving all workflows except:"
echo " - $KEEP1"
echo " - $KEEP2"
echo

shopt -s nullglob
for wf in .github/workflows/*.yml .github/workflows/*.yaml; do
  if [[ "$wf" != "$KEEP1" && "$wf" != "$KEEP2" ]]; then
    echo "Archiving: $wf"
    git mv "$wf" "$ARCHIVE"/
  fi
done

echo
echo "Done. Archived workflows can be re-enabled by moving them back."
