#!/usr/bin/env bash
set -euo pipefail

TAG="${GITHUB_REF_NAME:-$(git describe --tags --abbrev=0 || echo 'unreleased')}"
DATE_UTC="$(date -u +%Y-%m-%d)"
PREV_TAG="$(git describe --tags --abbrev=0 "${TAG}^" 2>/dev/null || true)"

{
  echo "# Changelog"
  echo
  echo "## ${TAG} - ${DATE_UTC}"
  if [[ -n "${PREV_TAG}" ]]; then
    RANGE="${PREV_TAG}..${TAG}"
  else
    RANGE="${TAG}"
  fi
  echo
  echo "### Changes"
  git log --pretty=format:'- %s (%h)' ${RANGE} || echo "- Initial release"
  echo
} > CHANGELOG.md
