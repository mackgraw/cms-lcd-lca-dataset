#!/usr/bin/env bash
# Commit helper
# Usage:
#   tools/commit.sh [-b <branch>] [-m "<message>"] [--] [files...]
#
# Examples:
#   tools/commit.sh -b "release/20250823-0830" -m "Packaging updates" \
#       .github/workflows/release-dataset.yml scripts/make_samples.py
#   tools/commit.sh -m "Tidy up"                 # stages ALL changes
#
# Notes:
# - If no files are provided, the script stages ALL changes (`git add -A`).
# - If -b is provided, it creates/switches to that branch (from current HEAD).
# - Safe warnings for missing files; won’t fail the whole run.
# - Exits cleanly if there’s nothing to commit.

set -euo pipefail

BRANCH=""
MSG="chore: update"
FILES=()

print_usage() {
  sed -n '1,50p' "$0" | sed -n '2,30p' | sed 's/^# \{0,1\}//'
  exit 1
}

# --- parse args ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    -b|--branch)
      [[ $# -ge 2 ]] || { echo "ERR: -b|--branch requires a value"; exit 2; }
      BRANCH="$2"; shift 2;;
    -m|--message)
      [[ $# -ge 2 ]] || { echo "ERR: -m|--message requires a value"; exit 2; }
      MSG="$2"; shift 2;;
    -h|--help)
      print_usage;;
    --)
      shift; break;;
    -*)
      echo "ERR: unknown flag $1"; print_usage;;
    *)
      FILES+=("$1"); shift;;
  esac
done

# Any remaining args after '--' are files too
if [[ $# -gt 0 ]]; then
  FILES+=("$@")
fi

# --- branch handling ---
CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [[ -n "$BRANCH" ]]; then
  if git rev-parse --verify "$BRANCH" >/dev/null 2>&1; then
    echo ">> Switching to existing branch: $BRANCH"
    git checkout "$BRANCH"
  else
    echo ">> Creating and switching to new branch: $BRANCH"
    git checkout -b "$BRANCH"
  fi
else
  BRANCH="$CURRENT_BRANCH"
  echo ">> Using current branch: $BRANCH"
fi

# --- stage files ---
if [[ ${#FILES[@]} -eq 0 ]]; then
  echo ">> No file list provided; staging ALL changes (git add -A)"
  git add -A
else
  echo ">> Staging specified files:"
  for f in "${FILES[@]}"; do
    if compgen -G "$f" > /dev/null; then
      # Expand globs safely
      for match in $f; do
        echo "   + $match"
        git add "$match" || true
      done
    else
      echo "   ! WARN: no matches for '$f' (skipped)"
    fi
  done
fi

# --- commit (if needed) ---
if git diff --cached --quiet; then
  echo ">> No staged changes. Nothing to commit."
else
  echo ">> Committing: $MSG"
  git commit -m "$MSG"
fi

# --- push ---
UPSTREAM_EXISTS="$(git rev-parse --symbolic-full-name --abbrev-ref @{u} 2>/dev/null || true)"
if [[ -z "$UPSTREAM_EXISTS" ]]; then
  echo ">> Pushing and setting upstream: origin/$BRANCH"
  git push -u origin "$BRANCH"
else
  echo ">> Pushing to existing upstream"
  git push
fi

# --- summary ---
LAST_COMMIT="$(git rev-parse --short HEAD)"
echo
echo "✅ Done."
echo "   Branch : $BRANCH"
echo "   Commit : $LAST_COMMIT"
echo "   Message: $MSG"
