#!/usr/bin/env bash
set -euo pipefail
# Usage:
#   tools/git/merge-to-main.sh [branch]   # default: current branch

feature="${1:-$(git rev-parse --abbrev-ref HEAD)}"
if [[ -z "${feature}" || "${feature}" == "HEAD" ]]; then
  echo "Error: not on a branch; specify a branch explicitly."; exit 2
fi

# Ensure working tree is clean
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Error: uncommitted changes present. Commit or stash first."; exit 2
fi

# Push feature branch first
git push origin "${feature}"

# Fast‑forward main, merge, and push
git checkout main
git pull origin main
git merge --no-ff "${feature}" -m "merge: ${feature} into main"
git push origin main

# Optional: delete feature branch locally and remotely
git branch -d "${feature}" || true
git push origin ":${feature}" || true

echo "[ok] Merged ${feature} -> main"
