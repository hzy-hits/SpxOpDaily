#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LOCK_ROOT="${XDG_RUNTIME_DIR:-/tmp}"
RUN_LOCK="$LOCK_ROOT/spx-spark-data-compact.lock"
exec 9>"$RUN_LOCK"
if ! flock -n 9; then
  echo '{"status":"skipped","reason":"compaction_already_running"}'
  exit 0
fi

uv run --no-sync python -m spx_spark.data_platform.lake.compact "$@"

replay_status=0
uv run --no-sync python -m spx_spark.data_platform.cli replay-spool || replay_status=$?
uv run --no-sync python -m spx_spark.data_platform.cli sync-manifests
exit "$replay_status"
