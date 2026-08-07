#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v flock >/dev/null 2>&1; then
  echo '{"status":"failed","reason":"flock_command_unavailable"}' >&2
  exit 127
fi

LOCK_ROOT="${XDG_RUNTIME_DIR:-/tmp}"
RUN_LOCK="$LOCK_ROOT/spx-spark-session-finalize.lock"
exec 9>"$RUN_LOCK"
if ! flock -n 9; then
  echo '{"status":"skipped","reason":"session_finalize_already_running"}'
  exit 0
fi

if uv run --no-sync python -m spx_spark.session_finalize "$@"; then
  finalize_status=0
else
  finalize_status=$?
fi
if (( finalize_status != 0 )); then
  echo "{\"status\":\"failed\",\"reason\":\"session_finalize_failed\",\"exit_code\":${finalize_status}}" >&2
  exit "$finalize_status"
fi
