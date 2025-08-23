#!/usr/bin/env bash
# Commit current changes and merge to main via tools/git/merge-to-main.sh
# Safe for repos where a tag named 'main' ever existed (uses explicit refspecs).
# Usage:
#   tools/git/commit-and-merge.sh -m "Your message" [-b new/branch] [--all] [--fix-ambiguous]
#     --all           stage new files too (git add -A)
#     --fix-ambiguous remove local/remote tag named 'main' if found

set -euo pipefail

MSG=""
NEW_BRANCH=""
INCLUDE_ALL=false
FIX_AMBIGUOUS=false
MAIN_BRANCH="main"
MAIN_HEAD="refs/heads/${MAIN_BRANCH}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -m|--message) MSG="${2:-}"; shift 2 ;;
    -b|--branch)  NEW_BRANCH="${2:-}"; shift 2 ;;
    --all)        INCLUDE_ALL=true; shift ;;
    --fix-ambiguous) FIX_AMBIGUOUS=true; shift ;;
    -h|--help)
      echo "Usage: $0 -m \"Commit msg\" [-b feature/branch] [--all] [--fix-ambiguous]"
      exit 0 ;;
    *) echo "Unknown arg: $1"; exit 2 ;;
  esac
done
[[ -n "$MSG" ]] || { echo "ERROR: -m|--message is required"; exit 2; }

MERGER="tools/git/merge-to-main.sh"
[[ -x "$MERGER" ]] || { echo "ERROR: $MERGER not found/executable"; exit 1; }

# Optional: auto-fix ambiguous main (tag vs branch)
if $FIX_AMBIGUOUS; then
  if git show-ref --verify --quiet "refs/tags/${MAIN_BRANCH}"; then
    echo "Removing local tag '${MAIN_BRANCH}' to avoid ambiguity…"
    git tag -d "${MAIN_BRANCH}" || true
  fi
  # Try to remove remote tag too (ignore errors)
  git ls-remote --tags origin | grep -q "refs/tags/${MAIN_BRANCH}$" && \
    git push origin ":refs/tags/${MAIN_BRANCH}" || true
fi

CURRENT="$(git rev-parse --abbrev-ref HEAD)"
if [[ -n "$NEW_BRANCH" ]]; then
  git rev-parse --verify "$NEW_BRANCH" >/dev/null 2>&1 && git checkout "$NEW_BRANCH" || git checkout -b "$NEW_BRANCH"
  BRANCH="$NEW_BRANCH"
else
  BRANCH="$CURRENT"
fi

# Stage
$INCLUDE_ALL && git add -A || git add -u

# Commit if staged
if git diff --cached --quiet; then
  echo "No staged changes; nothing to commit."
else
  git commit -m "$MSG"
fi

# Push branch (set upstream if needed)
if git rev-parse --symbolic-full-name --abbrev-ref @{u} >/dev/null 2>&1; then
  git push
else
  git push -u origin "$BRANCH"
fi

# Merge into main via your helper
"$MERGER" "$BRANCH"

# Push main explicitly to avoid ambiguity (uses full refspec)
echo "Pushing ${MAIN_BRANCH} with explicit refspec…"
git push origin "${MAIN_HEAD}:${MAIN_HEAD}"

echo "✅ Commit + merge complete. '${BRANCH}' merged into '${MAIN_BRANCH}'."
