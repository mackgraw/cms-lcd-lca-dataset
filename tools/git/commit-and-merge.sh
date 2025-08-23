#!/usr/bin/env bash
# Commit current changes and merge to main via tools/git/merge-to-main.sh
# Usage:
#   tools/git/commit-and-merge.sh -m "Your message" [-b new/branch] [--all]
# Defaults:
#   - commits only modified/deleted tracked files (no new files) unless --all is passed
#   - uses current branch unless -b is provided (creates branch if it doesn't exist)

set -euo pipefail

MSG=""
NEW_BRANCH=""
INCLUDE_ALL=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    -m|--message) MSG="${2:-}"; shift 2 ;;
    -b|--branch)  NEW_BRANCH="${2:-}"; shift 2 ;;
    --all)        INCLUDE_ALL=true; shift ;;
    -h|--help)
      echo "Usage: $0 -m \"Commit message\" [-b feature/branch] [--all]"
      exit 0 ;;
    *) echo "Unknown arg: $1"; exit 2 ;;
  esac
done

if [[ -z "${MSG}" ]]; then
  echo "ERROR: commit message required. Try:  $0 -m \"Your message\"" >&2
  exit 2
fi

# Ensure merge helper exists
MERGER="tools/git/merge-to-main.sh"
if [[ ! -x "$MERGER" ]]; then
  echo "ERROR: $MERGER not found or not executable." >&2
  exit 1
fi

# Choose / create branch
CURRENT="$(git rev-parse --abbrev-ref HEAD)"
if [[ -n "$NEW_BRANCH" ]]; then
  if git rev-parse --verify "$NEW_BRANCH" >/dev/null 2>&1; then
    git checkout "$NEW_BRANCH"
  else
    git checkout -b "$NEW_BRANCH"
  fi
  BRANCH="$NEW_BRANCH"
else
  BRANCH="$CURRENT"
fi

# Stage changes
if $INCLUDE_ALL; then
  git add -A           # include new files too
else
  git add -u           # modified & deleted tracked files only
fi

# Commit if there is anything staged
if git diff --cached --quiet; then
  echo "No staged changes; nothing to commit."
else
  git commit -m "$MSG"
fi

# Push (set upstream if needed)
if git rev-parse --symbolic-full-name --abbrev-ref @{u} >/dev/null 2>&1; then
  git push
else
  git push -u origin "$BRANCH"
fi

# Merge into main
"$MERGER" "$BRANCH"

echo "✅ Commit + merge complete. Branch '$BRANCH' merged into 'main'."
