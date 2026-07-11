#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

uv run python -m spx_spark.data_platform.lake.compact "$@"

replay_status=0
uv run python -m spx_spark.data_platform.cli replay-spool || replay_status=$?
uv run python -m spx_spark.data_platform.cli sync-manifests
exit "$replay_status"
