#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MODE="tracked"
if [[ "${1:-}" == "--all" ]]; then
  MODE="all"
  shift
fi

EXCLUDE_FILES='(^|/)(\.git|\.venv|\.firecrawl|\.pytest_cache|\.ruff_cache|\.mypy_cache|__pycache__|\.codex-home|\.codex-log|logs|runtime|data/(raw|processed|latest)|vendor)(/|$)'

if [[ "$MODE" == "all" ]]; then
  exec uvx --from detect-secrets detect-secrets scan \
    --all-files \
    --exclude-files "$EXCLUDE_FILES" \
    "$@" \
    .
fi

mapfile -t TRACKED_FILES < <(git ls-files)
if [[ "${#TRACKED_FILES[@]}" -eq 0 ]]; then
  echo "No git-tracked files found." >&2
  exit 1
fi

exec uvx --from detect-secrets detect-secrets scan "$@" "${TRACKED_FILES[@]}"
