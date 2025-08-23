#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-dry-run}"  # dry-run | apply

# ---- allowlist (exact files or glob patterns) ------------------------------
ALLOW=(
  ".gitignore"
  ".gitattributes"
  "README.md"
  "DATA_DICTIONARY.md"
  "requirements.txt"
  "dataset/.gitkeep"
  "scripts/**"
  "tools/**"
  ".github/workflows/coverage-harvest.sharded.yml"
  ".github/workflows/release-dataset.yml"
)
# ---------------------------------------------------------------------------

# Convert ALLOW globs to a grep -E pattern
pattern_from_allow() {
  local pat=""
  for a in "${ALLOW[@]}"; do
    # escape dots, replace ** with .*, * with [^/]* (no slash), allow end anchor
    local re="$a"
    re="${re//\./\\.}"
    re="${re//\*\*/.*}"
    re="${re//\*/[^/]*}"
    pat+="^$re$|"
  done
  echo "${pat%|}"
}

PATTERN="$(pattern_from_allow)"

echo "Mode: $MODE"
echo "Allowlist pattern: $PATTERN"
echo

# List tracked files and filter out allowlisted ones
MAPFILE -t ALL < <(git ls-files)
TO_REMOVE=()
for f in "${ALL[@]}"; do
  if [[ ! "$f" =~ $PATTERN ]]; then
    TO_REMOVE+=("$f")
  fi
done

if ((${#TO_REMOVE[@]}==0)); then
  echo "Nothing to remove. Repo already minimal."
  exit 0
fi

echo "Would remove ${#TO_REMOVE[@]} tracked paths:"
for f in "${TO_REMOVE[@]}"; do echo "  $f"; done
echo

if [[ "$MODE" == "apply" ]]; then
  git rm -r --cached --quiet -- "${TO_REMOVE[@]}" || true
  rm -rf -- "${TO_REMOVE[@]}" || true
  echo "Removed. Run a commit to finalize."
else
  echo "Dry run only. Re-run with:  tools/maintenance/prune_repo.sh apply"
fi
