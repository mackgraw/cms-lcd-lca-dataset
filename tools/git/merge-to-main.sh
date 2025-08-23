#!/usr/bin/env bash
set -euo pipefail
# Usage:
#   tools/git/merge-to-main.sh <branch>
#
# Merges the given branch into main with a merge commit and pushes.

branch="${1:-}"
if [[ -z "$branch" ]]; then
  echo "Usage: $0 <branch>" >&2
  exit 2
fi

git fetch origin
git checkout main
git pull --ff-only origin main
git merge --no-ff "$branch" -m "merge($branch): merge into main"
git push origin main
echo "Merged $branch into main."
