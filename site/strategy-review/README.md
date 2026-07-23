# SPX Strategy Review Site

This directory serves the validated 2026-07-23 all-signal and risk review as a
private, self-contained static site.

## Access

The nginx sidecar shares the existing code-server network namespace and listens
on port `18081`. It is intentionally reachable only through the authenticated
code-server proxy:

`https://code.zh3nyu.com/proxy/18081/`

No repository root, environment file, account statement, order detail, or raw
fill record is mounted into the container.

## Rebuild

Regenerate `public/index.html` from the canonical artifact with the packaged
report delivery tool, then restart only if nginx is not already running. Static
file changes are visible immediately because the public directory is mounted
read-only.

```bash
REPORT_BUILDER_ROOT=/home/ubuntu/.codex/plugins/cache/openai-curated-remote/data-analytics/0.2.8-13ceeea1f599
cd "$REPORT_BUILDER_ROOT"
npm run report:deliver -- \
  --input /home/ubuntu/spx-spark/docs/spx-0dte-signal-risk-review-2026-07-23.artifact.json \
  --output /home/ubuntu/spx-spark/site/strategy-review/public/index.html

docker compose -f /home/ubuntu/spx-spark/site/strategy-review/compose.yaml up -d
docker compose -f /home/ubuntu/spx-spark/site/strategy-review/compose.yaml ps
```

The page is a fixed validation snapshot through 2026-07-22, not a live trading
dashboard. It separates all-history RTH results from the recent three-day
window, reconciles telemetry rows to semantic opportunities, and keeps
in-sample parameter winners in shadow when expanding walk-forward fails.
Automatic ordering remains disabled, and readiness never promotes a policy
automatically.
