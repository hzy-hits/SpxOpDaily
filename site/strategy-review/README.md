# SPX Strategy Review Site

<!-- documentation-status: 2026-10-05 -->
> **文档定位：退役界面与固定历史页面。** 退役界面或固定历史页面记录；不是实时策略入口，不按旧命令恢复已退役服务。
> [全仓文档、当前运行状态与合同优先级](../../docs/README.md)（目录核对：2026-10-05）。

This directory retains the 2026-07-23 all-signal and risk review as a private,
self-contained historical static site. Its telemetry-based opportunity
reconciliation is not a raw-broker replay of the current strategy policy and
cannot establish current edge. New strategy evaluation must start from original
IBKR/Schwab option and underlying data, as required by the root AGENTS.md.

## Access

The nginx sidecar shares the existing code-server network namespace and listens
on port `18081`. It is intentionally reachable only through the authenticated
code-server proxy:

`https://code.zh3nyu.com/proxy/18081/`

No repository root, environment file, account statement, order detail, or raw
fill record is mounted into the container.

## Historical rebuild recipe

The command below records the original pinned report-builder environment.
Confirm that package is installed before reuse; this documentation update does
not reinstall the plugin, rebuild the report or attest that the private site is
currently running. The fixed dataset and sample dates remain unchanged.

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
