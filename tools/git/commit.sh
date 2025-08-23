#!/usr/bin/env bash
set -euo pipefail
# Usage:
#   tools/git/commit.sh -b <new-branch-name> -m "<commit message>" [files...]
#
# Creates a branch (if -b is provided), adds files, commits with the message,
# and pushes the branch to origin.

branch=""
msg=""

while getopts ":b:m:" opt; do
  case $opt in
    b) branch="$OPTARG" ;;
    m) msg="$OPTARG" ;;
    *) echo "Usage: $0 -b <branch> -m <message> [files...]" >&2; exit 2 ;;
  esac
done
shift $((OPTIND-1))

if [[ -n "$branch" ]]; then
  git checkout -b "$branch" || git checkout "$branch"
fi

if [[ -z "${msg:-}" ]]; then
  echo "Commit message (-m) is required" >&2
  exit 2
fi

git add "$@"
git commit -m "$msg"
git push -u origin "$(git rev-parse --abbrev-ref HEAD)"
echo "Pushed branch $(git rev-parse --abbrev-ref HEAD) with commit: $msg"
