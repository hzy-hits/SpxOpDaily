#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Keep journald bounded; the command writes the detailed JSON report to logs/.
exec uv run --no-sync spx-spark-maintenance dry-run
