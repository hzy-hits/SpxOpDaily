#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export CODEX_HOME="$ROOT/.codex-home"
export CODEX_SQLITE_HOME="$ROOT/.codex-home"

mkdir -p "$CODEX_HOME" "$ROOT/.codex-log"

exec codex \
  -c "log_dir=$ROOT/.codex-log" \
  --cd "$ROOT" \
  "$@"

