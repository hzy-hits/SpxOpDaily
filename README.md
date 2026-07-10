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

### Streaming Collector

`spx-spark-ibkr-stream` is the persistent alternative to the snapshot
collector. It keeps one read-only connection (own client id 172) with base
contracts always subscribed, a hot SPXW lane near ATM, and the remaining
option-line budget rotating through the sampling planner's strike groups. It
flushes to raw storage and latest state every 5 seconds, re-plans when SPX
drifts 10+ points, reconnects with exponential backoff, backs off politely on
a competing session (IBKR 10197), and re-checks runtime mode continuously.

```bash
scripts/run-ibkr-stream.sh --print-config
scripts/run-ibkr-stream.sh --force --skip-options --duration-seconds 60
scripts/run-ibkr-stream.sh --force
```

Run it as a service (keep `SPX_SERVICE_ENABLE_IBKR=false` in the 24h loop so
only one IBKR writer is active):

```bash
ln -sfn /home/ubuntu/spx-spark/systemd/spx-spark-ibkr-stream.service ~/.config/systemd/user/spx-spark-ibkr-stream.service
systemctl --user daemon-reload
systemctl --user enable --now spx-spark-ibkr-stream.service
```

### IBKR Index CFDs

`IBKR_VERIFY_CFDS` (default `IBUS500`) adds IBKR index CFDs to the collector
and verifier universe. `IBUS500` tracks the S&P 500 cash index at the same
price level and trades nearly 24h on weekdays, so it doubles as an off-hours
SPX price proxy and as an extra ATM-reference fallback (`SPX -> ES -> IBUS500 ->
SPY*10`). Rows appear as `cfd:IBUS500` and the trading-hours report groups them
under `cfd_proxies` (optional group; it never fails the overall status). CFD
market data requires the account's CFD permission; without it the row shows an
entitlement error and everything else keeps working. Set `IBKR_VERIFY_CFDS=` to
disable.

### Session Recovery

If a manual phone/desktop login preempts the automated Gateway session, the
recovery chain is: IBC yields (`ExistingSessionDetectedAction=secondary`) ->
systemd restarts the service every 60s indefinitely (`StartLimitIntervalSec=0`)
-> login succeeds once the manual session ends -> the collector conflict probe
returns to IBKR automatically. A watchdog timer (`ibc-watchdog.timer`) also
restarts the Gateway when the process is alive but the API port stays dead.
See `docs/headless-deployment.md` (Session Recovery Chain) for details.

Keep the P0 index set focused on SPX, vol-regime data, and a small cross-index
context set: `SPX,VIX,VIX1D,VIX9D,VIX3M,VVIX,SKEW,NDX,RUT,DJX,DJU`.
Use explicit exchanges when a broker symbol needs correction, for example:

```bash
IBKR_VERIFY_INDEXES='SPX,VIX,VIX1D,VIX9D,VIX3M,VVIX,SKEW,NDX@NASDAQ,RUT@RUSSELL,DJX@CBOE,DJU@CBOE' \
  scripts/run-ibkr-collector.sh --force --skip-options --json
```

For NDX/RUT/Dow/utilities context, ETF proxies `QQQ/IWM/DIA/XLU` are often
enough for alerts and use ordinary US stock data lines. The alert payload keeps
official cash indexes separate from proxies; missing official index data degrades
that layer instead of being silently replaced.

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

Human-facing alerts are intentionally SPX-only. The visible push surface is
limited to SPX, SPXW option structure, and ES confirmation. VIX-family indexes,
ETF proxies, on-chain data, prediction markets, and macro/risk proxies may feed
the internal score, but they are not shown to the human and cannot directly
trigger a push as separate trading instruments.

OpenClaw Weixin is supported through the `openclaw message send` CLI. The Weixin
channel requires a valid conversation `context_token`; a raw login `userId` may
dry-run successfully but real sends can fail until the user has messaged the
OpenClaw bot and the gateway has cached that context.

For fast agent-confirmed pushes, use the local Codex CLI sink. It uses this
machine's Codex/ChatGPT login and then delivers the short confirmation through
OpenClaw Weixin. Keep raw message pushes off:

```env
ALERT_NOTIFY_ENABLED=true
ALERT_NOTIFY_OPENCLAW_ENABLED=false
ALERT_NOTIFY_CODEX_ENABLED=true
ALERT_NOTIFY_CODEX_DELIVER=true
ALERT_NOTIFY_CODEX_MODEL=gpt-5.3-codex-spark
ALERT_NOTIFY_CODEX_REASONING_EFFORT=high
ALERT_NOTIFY_CODEX_REQUIRE_DELIVERY_CUE=true
```

With `ALERT_NOTIFY_CODEX_REQUIRE_DELIVERY_CUE=true`, Weixin delivery happens
only when Codex starts with an explicit cue such as `需要看盘:`. Smoke tests or
degraded-data conclusions that start with `不需要推送:` are recorded but not
forwarded. The Codex prompt receives `human_focus_context`, which contains only
SPX, SPXW walls/gamma/IV surface, ES confirmation, Micopedia guidance, and the
past-hour SPXW IV-surface summary.

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
quality. The alert engine also reads the last hour of these snapshots when
deciding whether an SPXW alert is worth waking the human. `run-24h-service.sh` is
the modular long-running loop. It runs
Hyperliquid, IV surface, and alert tasks by default; IBKR is disabled unless
`SPX_SERVICE_ENABLE_IBKR=true` is set in `.env`.

Order-map and human-focus payloads also include a strict same-day SPXW
`spxw_0dte_greeks_reference.v1` shadow layer. It derives Delta, Gamma, Theta,
Vega, Charm, Color, Speed, Vanna, Vomma, and Zomma plus bounded spot/time/IV
scenarios. Aggregates are OI-only gross magnitudes with position sign and
direction explicitly `unknown`; they cannot change candidate direction,
ranking, or limits. Delivered snapshots are persisted under
`data/features/spxw_0dte_greeks_reference/` and summarized in the post-close
review. See [docs/zero-dte-greeks-reference.md](docs/zero-dte-greeks-reference.md).

The same 5-second SPX/ES path monitor now freezes the pre-move flip band and
call wall. Two fresh synchronized confirmations can produce a short-lived
`flip_reclaim_call` or `call_wall_breakout_call` bias and direct alert; the
15-minute order map then replaces the invalidated same-level Put with that Call
while retaining the other risk play. Shock/reclaim events are scored after 5/15/30
minutes with directional MFE/MAE in daily
`data/features/intraday_event_outcomes/date=YYYY-MM-DD/` partitions, while the higher-Greeks shadow can sample
every 60 seconds during RTH with `SPX_SERVICE_ENABLE_GREEK_SHADOW=true`.

Post-close SPX/SPXW review:

```bash
scripts/run-post-close-review.sh --date auto
scripts/run-post-close-review.sh --date 2026-07-06 --json
```

The review is designed to run after the US close delay and to be appended by the
local Hermes daily report. It writes:

- `data/reports/spx_options_review/date=YYYY-MM-DD/review.md`
- `data/reports/spx_options_review/date=YYYY-MM-DD/review.json`
- `data/latest/spx_options_review.md`
- `/home/ubuntu/research/finance/daily/spx-options-review/latest-spx-options-review.md`

The systemd timer `spx-spark-post-close-review.timer` runs Monday through
Friday at 17:15 America/New_York. The application calendar still suppresses
holidays and verifies report identity before publishing.
`complete` is emitted only when the structured SPX/ES bucket, edge-recency,
live-ratio, SPXW breadth/IV, and IV-surface coverage checks all pass; otherwise
the JSON and Markdown reports remain explicitly `degraded` with measured checks.

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
- Design review and improvement plan: `docs/design-review.md`
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
