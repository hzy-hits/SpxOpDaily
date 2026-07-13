#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENTRYPOINT="$ROOT/.venv/bin/spx-spark-ibkr-stream"
if [[ ! -x "$ENTRYPOINT" ]]; then
  printf 'missing executable: %s (run uv sync first)\n' "$ENTRYPOINT" >&2
  exit 1
fi

exec "$ENTRYPOINT" "$@"
