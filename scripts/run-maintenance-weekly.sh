#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Raw JSONL deletion is intentionally disabled until retention is verified
# against Parquet manifests. Keep the scheduled weekly pass audit-only.
# The detailed JSON report is written to logs/ while journald stays concise.
exec uv run spx-spark-maintenance prune
