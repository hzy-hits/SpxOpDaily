# Operations Schedule

Date: 2026-07-04

## Host Budget

Target host:

- Oracle Linux headless host
- user-reported size: 4 cores, 24 GB RAM
- ARM64

This is enough for the MVP if the system avoids full-market OPRA capture.

Official Oracle Always Free documentation currently describes Ampere A1 Always Free as 2 OCPUs and 12 GB total memory, plus 200 GB block volume. If the actual instance in the console is 4 OCPUs and 24 GB RAM, treat that as more than enough for the safe MVP profile.

## Capacity Decision

Safe MVP budget:

- live selected option contracts: 40-80
- ETF/index/futures symbols: 20-40
- chain/prediction feeds: selected markets only
- feature loop: 1 second fast loop, 5 second medium loop, 1 minute slow loop
- storage target: 100 MB to 1 GB compressed per active trading day

Aggressive but still feasible:

- live selected option contracts: 150-300
- storage target: 1 GB to 5 GB compressed per active trading day
- requires disk monitoring and stricter retention

Not acceptable on this host without a separate data budget:

- full OPRA tick capture
- full SPX option chain tick capture all expiries
- indefinite raw tick retention
- uncompressed JSON as the primary raw store

## Runtime Priorities

During market-sensitive windows, spend CPU on:

- quote ingest
- latest-state updates
- SPXW selected-contract Greeks
- alert rules
- dashboard state

Move expensive or non-urgent work to maintenance windows:

- Parquet compaction
- daily reports
- retention pruning
- smart-wallet historical scans
- replay/backtest jobs
- feature quality audits
- model recalculation with changed assumptions

## Sensitive Market Windows

Use higher sampling cadence and richer alerts around four recurring windows.

Times below are U.S. Eastern Time:

- pre-open 1h: 08:30-09:30 ET
- open 1h: 09:30-10:30 ET
- close 1h: 15:00-16:00 ET
- post-close 1h: 16:00-17:00 ET

During U.S. daylight saving time, these map to Beijing time:

- pre-open 1h: 20:30-21:30
- open 1h: 21:30-22:30
- close 1h: 03:00-04:00
- post-close 1h: 04:00-05:00

During U.S. standard time, these map to Beijing time:

- pre-open 1h: 21:30-22:30
- open 1h: 22:30-23:30
- close 1h: 04:00-05:00
- post-close 1h: 05:00-06:00

### Pre-Open 1h

Purpose:

- detect overnight repricing
- compare ES/MES, SPY premarket, Hyperliquid SPX, and Polymarket
- identify whether a dip is liquidity noise, macro repricing, or real risk-off

Signals:

- ES/SPY premarket divergence
- HL SPX premium or funding pressure
- Polymarket probability jumps
- VIX/VIX1D/VIX9D changes if available
- ETF risk proxies after premarket liquidity improves

Sampling:

- VIX/Cboe index context: 5-15 seconds if available
- SPY/QQQ/IWM premarket: 5-15 seconds
- SPXW option wide context: 16-60 second rolling scan if quotes are usable
- `device_required` promotes execution monitor for about 20 minutes

### Open 1h

Purpose:

- capture price discovery and liquidity normalization
- avoid false premarket signals
- detect opening range, trend day risk, and failed gap setups

Signals:

- opening range high/low
- ES/SPY/SPX alignment
- ATM straddle decay or expansion
- spread quality improvement
- HYG/LQD and QQQ/IWM confirmation

Sampling:

- ATM/hot lane: 1-4 seconds
- +/-200 rolling option scan: 16 seconds if broker supports batch quotes
- VIX/Cboe index context: 5-15 seconds
- `device_required` promotes execution monitor for about 30 minutes

### Close 1h

Purpose:

- 0DTE gamma, theta, and dealer hedging behavior matter most
- close auction and rebalancing can distort signals
- option spreads and IV can move quickly

Signals:

- ATM straddle change
- gamma concentration by strike
- underlying approach to large strikes
- VIX1D/VIX9D movement
- spread quality degradation

Sampling:

- ATM/hot lane: 1-3 seconds
- target spread legs: 1-3 seconds when alert is active
- wider option scan: 16-30 seconds
- IBKR preferred if available
- `device_required` promotes execution monitor for about 60 minutes

### Post-Close 1h

Purpose:

- review after-hours repricing
- capture earnings/macro/geopolitical reaction
- write initial session report
- avoid unnecessary broker line usage if no event is active

Signals:

- ES/MES and ETF after-hours moves
- HL SPX divergence
- Polymarket event jumps
- late news feed events when available

Sampling:

- broker options: reduce or stop unless an active alert needs monitoring
- futures/ETF/chain context: 15-60 seconds
- reports and feature materialization can start after critical buffers are flushed

## Daily Schedule

Times are Asia/Shanghai.

### User Attention Model

The system should separate windows where alerts are likely actionable from windows where the user is usually not watching.

Actionable windows:

- Beijing 14:00 through the U.S. open and open 1h
- focus: low-liquidity dips, pre-open repricing, opening range confirmation
- alert style: `watch`, `device_required`, `avoid`

Unattended discovery windows:

- Beijing 02:00 through U.S. close plus about 2 hours
- during U.S. daylight saving time: roughly 14:00-18:00 ET
- during U.S. standard time: roughly 13:00-18:00 ET if starting at 02:00 Beijing
- focus: information extraction, close behavior, next-session prep, alert validation
- alert style: fewer push alerts, richer report events, high-severity only unless explicitly requested

This avoids waking the user for low-quality manual trades while still capturing information that is useful the next day.

### 14:00-21:30 Pre-Open Watch

Purpose:

- low-liquidity opportunity scan
- macro/news shock monitor
- futures/HL/Polymarket context
- device-required alerts

Data priority:

- Schwab if verified
- Hyperliquid SPX
- Polymarket event markets
- ES/MES if available
- SPY/QQQ/IWM premarket after 04:00 ET

Compute policy:

- keep latency low
- no heavy compaction
- no wide historical replay
- no full-chain refetch loops
- `device_required` alerts automatically enable execution monitoring for about 20 minutes

### 21:30-01:05 RTH Manual Protected

Purpose:

- user may be actively trading on phone/desktop
- keep alerts running without disturbing manual trading

Data priority:

- Schwab
- Hyperliquid
- Polymarket
- ETF risk proxies
- IBKR if available without conflict

Compute policy:

- feature loop normal
- alert loop normal
- IBKR conflict probe when unavailable
- no session fighting
- `device_required` alerts automatically enable execution monitoring for about 30 minutes

### 01:05-02:00 Late RTH Transition

Purpose:

- transition from manual trading to unattended monitoring
- reduce user-facing alert frequency
- keep capturing useful market structure

Data priority:

- IBKR when available
- Schwab fallback
- Hyperliquid/Polymarket always-on

Compute policy:

- continue normal feature collection
- start aggregating report context
- reduce `device_required` pushes unless severity is high
- no session fighting

### 02:00-Close+2h Unattended Discovery

Purpose:

- capture late-session and close behavior when the user usually cannot watch
- extract information for next-day planning
- record high-quality data around 0DTE gamma/theta and close auction behavior
- observe post-close repricing and event reaction
- validate whether earlier alerts had forward value

During U.S. daylight saving time:

- Beijing 02:00 = 14:00 ET
- U.S. cash close 16:00 ET = Beijing 04:00
- close+2h = Beijing 06:00

During U.S. standard time:

- Beijing 02:00 = 13:00 ET
- U.S. cash close 16:00 ET = Beijing 05:00
- close+2h = Beijing 07:00

Data priority:

- IBKR if available without competing-session takeover
- Schwab fallback
- SPXW selected quotes and calculated Greeks
- official VIX/VVIX/SKEW/Cboe indices if available
- ES/MES, SPY/QQQ/IWM, Hyperliquid SPX, Polymarket

Compute policy:

- close 1h: raise sampling cadence automatically
- post-close 2h: lower option sampling unless an event remains active
- preserve raw ticks around high-severity alerts and close-window anomalies
- write initial session report before maintenance starts
- user push alerts should be high-severity only by default
- ordinary IBKR reconnect allowed
- competing-session takeover disabled
- final-hour high-severity alerts automatically enable execution monitoring for about 60 minutes

Information to mine:

- close-window ATM straddle expansion or collapse
- large strike pin/magnet behavior
- gamma concentration shifts
- VIX1D/VIX9D and short-vol pressure
- spread quality deterioration into the close
- ES/SPY/HL divergence after close
- Polymarket probability jumps
- whether dips/rallies from earlier windows followed through or failed

Outputs:

- high-severity alert if something needs immediate attention
- next-session watchlist
- daily report summary
- alert forward-return labels
- candidate thresholds to adjust

### 08:00-13:30 Daily Maintenance

Purpose:

- compact yesterday's data
- write session reports
- prune expired raw data
- refresh metadata

Jobs:

- flush raw buffers
- compact small Parquet files
- materialize 1 minute features
- calculate alert forward returns
- update data-quality report
- delete raw data beyond retention, except preserved alert windows

Compute policy:

- limit maintenance to 1-2 cores
- avoid starving always-on collectors
- stop maintenance if disk free space is below threshold and pruning is pending

## Weekend Schedule

Weekend jobs should be allowed to use more CPU because U.S. markets are closed, but avoid running heavy work right when futures reopen.

In `auto` mode, IBKR collection should be paused on weekends. Schwab and chain collectors can either run in light mode or pause according to provider needs. Use the weekend window for cleanup, compaction, audit, and reports. A deliberate `ibkr_on` override can still enable IBKR for a manual weekend or holiday check.

Recommended Beijing windows:

- Saturday 10:00-24:00: heavy compaction and replay
- Sunday 10:00-Monday 04:00: historical scans, reports, and weekly research jobs
- Monday 04:00 until Globex reopen: light jobs only, prepare for futures and pre-open monitoring

Heavy weekend jobs:

- compact raw selected tick files into larger Parquet chunks
- build weekly DuckDB summary tables
- run smart-wallet cohort scans
- recalculate Greeks for preserved alert windows with current model version
- validate alert forward returns
- generate weekly report
- audit provider quality and missing fields

Do not run at weekend:

- jobs that require broker sessions unless explicitly requested
- destructive pruning without a dry-run report
- full historical scans that can run into Monday pre-open

## Storage Maintenance

Daily:

- check disk usage
- check file counts under `data/raw`
- compact previous day's small files
- write data-quality summary

Weekly:

- prune raw selected ticks according to the current retention policy
- preserve high-severity alert windows
- compact feature bars
- verify that 1 minute bars can replay the session narrative

Monthly:

- export reports and alert summaries
- archive or delete old raw preserved windows after review
- review whether storage profile is still safe

Disk thresholds:

- above 70% used: include warning in daily report
- above 80% used: pause aggressive collection and run compaction
- above 85% used: switch sampler to degraded mode
- above 90% used: prune non-preserved raw ticks
- above 95% used: stop raw tick writes except alerts and latest state

## Process Layout

Suggested always-on services:

- `collector-schwab`
- `collector-hyperliquid`
- `collector-polymarket`
- `collector-ibkr`, gated by runtime policy
- `feature-engine`
- `alert-engine`
- `dashboard-api`

Suggested maintenance services:

- `maintenance-daily`
- `maintenance-weekly`
- `report-daily`
- `report-weekly`

Keep the collector and alert services separate from maintenance. Maintenance failures should not stop alerts.

## Agent Control

Agent commands should change runtime mode, not permanent config.

Examples:

```bash
uv run spx-spark-runtime-mode status
uv run spx-spark-runtime-mode ibkr-on --ttl-minutes 120 --reason "manual monitor request"
uv run spx-spark-runtime-mode protected --ttl-minutes 180 --reason "phone trading"
uv run spx-spark-runtime-mode clear
```

Future agent commands:

- `maintenance dry-run`
- `maintenance compact --date YYYY-MM-DD`
- `maintenance prune --dry-run`
- `report daily --date YYYY-MM-DD`
- `replay alert --alert-id ID`

## Schwab Index Verification

Public documentation and SDK docs are not enough to guarantee Schwab account-level access for Cboe indices.

What public sources support:

- Schwab/Schwab-py quote endpoints can request symbols through `get_quote()` and `get_quotes()`.
- `get_quotes()` is preferred for symbols containing special characters.
- `schwab-py` examples and docs expose `$SPX` as an index-style symbol.

What must be verified on the account:

- `$VIX`
- `$VIX1D`
- `$VIX9D`
- `$VIX3M`
- `$VVIX`
- `$SKEW`
- live/delayed/stale status
- quote timestamps
- streaming availability, not only REST quote availability

Decision:

- Search can identify symbol candidates.
- Only the verifier can confirm this account's actual entitlement and data quality.

## Sources

- Oracle Always Free resources: https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm
- Oracle Cloud Free Tier: https://www.oracle.com/cloud/free/
- Schwab-py quote docs: https://schwab-py.readthedocs.io/en/latest/client.html
- Cboe VIX methodology: https://cdn.cboe.com/resources/indices/Volatility_Index_Methodology_Cboe_Volatility_Index.pdf
