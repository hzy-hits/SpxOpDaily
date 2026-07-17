#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Threshold-gated retention: below the prune watermark the pass stays
# audit-only; at or above MAINTENANCE_PRUNE_PCT (action levels prune /
# critical_stop_raw) it executes deletion. Every decision is logged so
# journald shows exactly what ran. The detailed JSON report goes to logs/.
report_json="$(uv run --no-sync spx-spark-maintenance dry-run --json --no-write)"
action_level="$(printf '%s' "$report_json" | uv run --no-sync python -c 'import json, sys; print(json.load(sys.stdin)["action_level"])')"
used_pct="$(printf '%s' "$report_json" | uv run --no-sync python -c 'import json, sys; print(json.load(sys.stdin)["disk_used_pct"])')"

echo "[maintenance-weekly] disk_used_pct=${used_pct} action_level=${action_level}"
case "$action_level" in
  prune|critical_stop_raw)
    echo "[maintenance-weekly] usage >= prune threshold; running prune --execute"
    uv run --no-sync spx-spark-maintenance prune --execute
    ;;
  *)
    echo "[maintenance-weekly] usage below prune threshold; audit-only prune"
    uv run --no-sync spx-spark-maintenance prune
    ;;
esac

# Ledger retention: purge terminal outbox rows (VACUUM only happens here, in
# the weekly off-market window) and trim the review audit log.
echo "[maintenance-weekly] purging acked domain-event outbox rows"
uv run --no-sync spx-spark-maintenance purge-outbox --vacuum
echo "[maintenance-weekly] trimming alert review audit log"
uv run --no-sync spx-spark-maintenance trim-review-audit
