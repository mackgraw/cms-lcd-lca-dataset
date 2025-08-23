#!/usr/bin/env bash
set -euo pipefail

MSG="${1:-"chore: prune repo and keep only required packaging files"}"
BUMP="${2:-patch}"

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

# 1) Ensure dataset keeper exists
mkdir -p dataset
[ -f dataset/.gitkeep ] || : > dataset/.gitkeep

# 2) Disable extra workflows (archive them)
if [ -x tools/maintenance/disable_extra_workflows.sh ]; then
  tools/maintenance/disable_extra_workflows.sh
fi

# 3) Prune repo (apply)
if [ -x tools/maintenance/prune_repo.sh ]; then
  tools/maintenance/prune_repo.sh apply
fi

# 4) Use your existing commit + merge helpers
BRANCH="cleanup/$(date +%Y%m%d-%H%M%S)"
if [ -x ./tools/commit.sh ]; then
  ./tools/commit.sh -b "$BRANCH" -m "$MSG" .
elif [ -x ./tools/git/commit.sh ]; then
  ./tools/git/commit.sh -b "$BRANCH" -m "$MSG" .
else
  git checkout -b "$BRANCH"
  git add -A
  git commit -m "$MSG"
  git push -u origin "$BRANCH"
fi

if [ -x ./tools/merge-to-main.sh ]; then
  ./tools/merge-to-main.sh "$BRANCH"
elif [ -x ./tools/git/merge-to-main.sh ]; then
  ./tools/git/merge-to-main.sh "$BRANCH"
else
  git checkout main
  git pull --ff-only
  git merge --no-ff "$BRANCH" -m "$MSG"
  git push
fi

# 5) Compute next tag and push (triggers release workflow)
LAST=$(git tag --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -n1 || true)
if [[ -z "$LAST" ]]; then MAJ=0; MIN=1; PAT=0; else
  MAJ=$(echo "${LAST#v}" | cut -d. -f1)
  MIN=$(echo "${LAST#v}" | cut -d. -f2)
  PAT=$(echo "${LAST#v}" | cut -d. -f3)
fi
case "$BUMP" in
  major) MAJ=$((MAJ+1)); MIN=0; PAT=0 ;;
  minor) MIN=$((MIN+1)); PAT=0 ;;
  *)     PAT=$((PAT+1)) ;;
esac
NEXT="v${MAJ}.${MIN}.${PAT}"
git tag "$NEXT"
git push origin "$NEXT"
echo "Tag $NEXT pushed."
