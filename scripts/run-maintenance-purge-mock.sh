#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
exec uv run spx ops maintenance purge-latest-provider --provider mock --json
