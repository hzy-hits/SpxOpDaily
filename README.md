# SPX Spark

Near-real-time SPX/SPXW 0DTE dashboard and alert research system.

Current scope:

- Verify IBKR market data permissions.
- Record the boundary between live, delayed, and missing feeds.
- Keep the project isolated from the machine's default Codex setup.
- Market-data only: no order placement, account polling, position polling, or credential storage.

## Quick Start

```bash
cd /home/ubuntu/spx-spark
cp .env.example .env
uv sync
scripts/run-ibkr-verifier.sh
```

IBKR requirements:

- TWS or IB Gateway must be running.
- API socket must be enabled.
- Use paper first: IB Gateway paper usually listens on `127.0.0.1:4002`.
- Keep IBKR Gateway's Read-Only API setting enabled for this project.

## Isolated Codex Wrapper

```bash
scripts/run-codex-isolated.sh "summarize this project"
```

The wrapper uses project-local `.codex-home` and `.codex-log` directories. It does not modify `~/.codex`.

## Runtime Mode

```bash
uv run spx-spark-runtime-mode status
uv run spx-spark-runtime-mode ibkr-on --ttl-minutes 120 --reason "manual monitor request"
uv run spx-spark-runtime-mode protected --ttl-minutes 180 --reason "phone trading"
uv run spx-spark-runtime-mode clear
```

The runtime mode file is local state under `runtime/`. It lets an agent temporarily allow or block IBKR collection without changing permanent config.

## IBKR Collector

```bash
scripts/run-ibkr-collector.sh --dry-run
scripts/run-ibkr-collector.sh --skip-options
scripts/run-ibkr-collector.sh --force --skip-options
scripts/run-ibkr-collector.sh --force
```

The collector writes normalized IBKR quotes into the same raw/latest-state path as the mock
collector. By default it respects runtime mode and will not connect if IBKR is protected or
outside the allowed schedule. Use `--force` only when you intentionally want this SSH host to
connect to TWS/IB Gateway.

Suggested real-data acceptance sequence:

```bash
scripts/start-ibgateway-xvfb.sh
scripts/start-ibgateway-vnc.sh
uv run spx-spark-runtime-mode ibkr-on --ttl-minutes 120 --reason "manual IBKR data test"
scripts/run-ibkr-collector.sh --force --skip-options --json
scripts/show-latest-state.sh --all-providers
scripts/run-ibkr-collector.sh --force --json
```

Trading-hours entitlement report:

```bash
IBKR_PORT=4001 scripts/run-ibkr-trading-hours-report.sh --skip-options
IBKR_PORT=4001 IBKR_MAX_OPTION_LINES=40 scripts/run-ibkr-trading-hours-report.sh
```

The report writes `logs/ibkr-trading-hours-report-*.json` and classifies each
requested row as `ok`, `stale`, `delayed`, `frozen`, `missing_price`,
`missing_bid_ask`, `missing_greeks`, `missing`, or `error`. Run it during
regular U.S. trading hours for the real acceptance result; weekend or overnight
runs are marked `not_rth` unless `--allow-outside-rth` is set.

On a headless host, view the Gateway login window through an SSH tunnel:

```bash
ssh -L 5909:127.0.0.1:5909 ubuntu@YOUR_SERVER
```

Then connect a local VNC viewer to `127.0.0.1:5909`. The VNC bridge is bound
to localhost and is only for manual Gateway login/configuration.

Keep the P0 index set focused on SPX and vol-regime data:
`SPX,VIX,VIX1D,VIX9D,VIX3M,VVIX,SKEW`. Optional cross-index checks can be added
temporarily with explicit exchanges, for example:

```bash
IBKR_VERIFY_INDEXES='SPX,VIX,VIX1D,VIX9D,VIX3M,VVIX,SKEW,NDX@NASDAQ,RUT@RUSSELL,DJX@CBOE' \
  scripts/run-ibkr-collector.sh --force --skip-options --json
```

For NDX/RUT/Dow context, ETF proxies `QQQ/IWM/DIA` are often enough for alerts and
use ordinary US stock data lines. Add official cash indexes only when they add a
clear signal or entitlement check.

## Schwab Verifier

```bash
scripts/create-schwab-token.sh
scripts/run-schwab-verifier.sh --offline
scripts/run-schwab-verifier.sh --print-config
scripts/run-schwab-verifier.sh
```

The token helper runs Schwab's manual OAuth flow for SSH/headless hosts. The verifier reads
`SCHWAB_ACCESS_TOKEN` or `SCHWAB_TOKEN_FILE`. It checks candidate index quotes, ETF/futures
quotes, and option chains without placing orders.

## Maintenance Dry Run

```bash
scripts/run-maintenance-dry-run.sh
scripts/run-maintenance-dry-run.sh --json --no-write
```

The dry run scans disk usage and cleanup candidates only. It does not delete files.

## Sampling Plan

```bash
scripts/run-sampling-plan.sh --underlier 7500 --expiry 20260706 --next-expiry 20260707
scripts/run-sampling-plan.sh --underlier 7500 --mode degraded --summary-json
```

The planner produces the SPXW hot lane and rolling quote groups for collectors. It does not request market data.

## Alert Profile

```bash
scripts/run-alert-profile.sh
scripts/run-alert-profile.sh --schedule
scripts/run-alert-profile.sh --at 2026-07-06T14:30:00
scripts/run-alert-engine.sh --at 2026-07-07T03:15:00
scripts/run-options-map.sh
scripts/run-iv-surface.sh
scripts/run-24h-service.sh --print-config
scripts/send-openclaw-test-alert.sh
```

The alert profile is the 24h monitoring layer. It maps New York and Beijing
time to the current monitoring window, source priority, alert cadence, summary
cadence, and SPXW sampling mode. The IBKR trading-hours report remains a data
entitlement check; it does not replace premarket, after-hours, futures,
Hyperliquid, or Polymarket monitoring.

The alert engine reads normalized latest state and emits data-health and
price-move alerts. Notification is optional and disabled by default. Enable it
with `ALERT_NOTIFY_ENABLED=true`; `ALERT_NOTIFY_OPENCLAW_DRY_RUN=true` keeps the
OpenClaw path in dry-run mode while testing.

OpenClaw Weixin is supported through the `openclaw message send` CLI. The Weixin
channel requires a valid conversation `context_token`; a raw login `userId` may
dry-run successfully but real sends can fail until the user has messaged the
OpenClaw bot and the gateway has cached that context.

For fast agent-confirmed pushes, use the OpenClaw agent sink with Codex Spark
and keep raw message pushes off:

```env
ALERT_NOTIFY_ENABLED=true
ALERT_NOTIFY_OPENCLAW_ENABLED=false
ALERT_NOTIFY_OPENCLAW_AGENT_ENABLED=true
ALERT_NOTIFY_OPENCLAW_AGENT_DELIVER=true
ALERT_NOTIFY_OPENCLAW_AGENT_MODEL=gpt-5.3-codex-spark
ALERT_NOTIFY_OPENCLAW_AGENT_THINKING=high
```

Minimal OpenClaw test:

```bash
openclaw gateway status
openclaw channels status
scripts/send-openclaw-test-alert.sh
ALERT_NOTIFY_OPENCLAW_DRY_RUN=false scripts/send-openclaw-test-alert.sh
```

`run-options-map.sh` is the current options-intelligence feature layer. It reads
SPXW option quotes from latest state and computes ATM strike, ATM straddle,
expected move, IV/skew ratios, Greek coverage, and an open-interest-based GEX
prototype for zero gamma, put wall, and call wall when OI is available. Without
open interest it intentionally reports `unknown_no_open_interest` instead of
pretending that gamma-only data is a real wall map.

`run-iv-surface.sh` writes a 5-minute surface snapshot under
`data/features/iv_surface/` and `data/latest/iv_surface.json`. It tracks ATM IV,
skew, surface shift, smile curvature, 0DTE-vs-next-expiry IV gap, and quote
quality. `run-24h-service.sh` is the modular long-running loop. It runs
Hyperliquid, IV surface, and alert tasks by default; IBKR is disabled unless
`SPX_SERVICE_ENABLE_IBKR=true` is set in `.env`.

Install the 24h user service:

```bash
mkdir -p ~/.config/systemd/user
ln -sfn /home/ubuntu/spx-spark/systemd/spx-spark-24h.service ~/.config/systemd/user/spx-spark-24h.service
systemctl --user daemon-reload
systemctl --user enable --now spx-spark-24h.service
journalctl --user -u spx-spark-24h.service -f
```

## Mock Data Loop

```bash
scripts/run-mock-collector.sh --underlier 7500 --expiry 20260706 --next-expiry 20260707
scripts/show-latest-state.sh --instrument index:SPX
scripts/show-latest-state.sh --all-providers
```

The mock collector generates normalized `Quote` rows, writes raw JSONL files under
`MARKET_DATA_DATA_ROOT/raw/`, and updates `MARKET_DATA_LATEST_STATE_PATH`. It is the
local no-broker test path for sampler, storage, latest-state, and fallback logic.

## Hyperliquid Collector

```bash
scripts/run-hyperliquid-collector.sh --print-config
scripts/run-hyperliquid-collector.sh --list-coins
scripts/run-hyperliquid-collector.sh --coin 'S&P500-USDC' --json
scripts/run-hyperliquid-collector.sh --dex xyz --coin xyz:SP500 --json
scripts/show-latest-state.sh --all-providers --instrument crypto_perp:xyz:SP500
```

The Hyperliquid collector uses public `POST /info` endpoints and does not need an API key.
It writes a normalized perp quote plus a Hyperliquid context row with funding, OI,
oracle premium, book imbalance, and recent-trade burst fields.

Live verification found the S&P 500-like perpetual on HIP-3 dex `xyz` as `xyz:SP500`
around the 7,500 index level. The default-dex `SPX` symbol trades around `0.43`, so it is a
different Hyperliquid crypto/perp asset and must not be mixed with `index:SPX`.

## MrMicopedia Guidance

```bash
scripts/run-micopedia-guidance.sh --underlier 7502 --vix1d 12.5 --gamma-state pin --event opex,jpm_collar
scripts/run-micopedia-guidance.sh --from-latest-state --time-phase open --event cpi --json
```

This produces an observational `MicopediaSignal`: regime, map focus, trigger
watchlist, candidate expression shape, risk guardrails, data warnings, and the
suggested SPXW sampling mode. It is an explanation/checklist layer only and does
not place orders.

## Market Data Model

IBKR, Schwab, and Hyperliquid payloads enter through provider adapters and become
`ProviderSnapshot` objects before they reach storage, fallback, sampling, features,
greeks, alerts, or dashboard code. Downstream code should compare normalized
`Quote.quality` and provider priority instead of branching on provider-specific fields.

## Secret Scan

```bash
scripts/scan-secrets.sh
scripts/scan-secrets.sh --all
```

Default mode scans only git-tracked files. `--all` scans the working tree but excludes
local runtime noise such as `.venv/`, `.firecrawl/`, cache folders, logs, runtime state,
and raw data.

## Notes

- Architecture plan: `docs/architecture-plan.md`
- Headless deployment: `docs/headless-deployment.md`
- Data source decision memo: `docs/data-source-decision.md`
- IBKR API research: `docs/ibkr-api-research.md`
- Storage plan: `docs/storage-plan.md`
- Market data model: `docs/market-data-model.md`
- Sampling engine design: `docs/sampling-engine-design.md`
- Operations schedule: `docs/operations-schedule.md`
- Trend spread framework: `docs/trend-spread-framework.md`
- MrMicopedia agent guidance: `docs/micopedia-agent-guidance.md`
- MrMicopedia background knowledge: `docs/micopedia-background-knowledge.md`
