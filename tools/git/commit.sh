#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   tools/git/commit.sh -b <branch> -m "message" [files...]
#
# If -b is omitted, uses current branch; if it doesn't exist, creates one:
#   work/$(date +%Y%m%d)-auto
# If no files are provided, stages modified/deleted tracked files (git add -u).

branch=""
msg=""

while getopts ":b:m:" opt; do
  case "$opt" in
    b) branch="$OPTARG" ;;
    m) msg="$OPTARG" ;;
    *) echo "Usage: $0 -m \"message\" [-b branch] [files...]"; exit 2 ;;
  esac
done
shift $((OPTIND-1))

if [[ -z "${msg}" ]]; then
  echo "Error: commit message (-m) is required"; exit 2
fi

current_branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo detached)"
if [[ -z "${branch}" ]]; then
  if [[ "${current_branch}" == "HEAD" || "${current_branch}" == "detached" ]]; then
    branch="work/$(date +%Y%m%d)-auto"
    git checkout -b "${branch}"
  else
    branch="${current_branch}"
  fi
else
  if ! git rev-parse --verify --quiet "${branch}" >/dev/null; then
    git checkout -b "${branch}"
  else
    git checkout "${branch}"
  fi
fi

if [[ $# -gt 0 ]]; then
  git add "$@"
else
  git add -u
fi

git commit -m "${msg}"
git push -u origin "${branch}"
echo "[ok] Pushed ${branch}"
