# Storage Plan

Date: 2026-07-04

## Decision

Do not store full-market OPRA tick data in the MVP.

Store only:

- selected live quotes needed for the dashboard and alerts
- compressed feature bars
- alert events
- session summaries
- enough raw data to replay important decisions

This keeps storage manageable on the Oracle Linux host.

## Schwab And Cboe Index Data

Schwab may be able to quote some index symbols through the quote API, but VIX/VVIX/SKEW availability, symbol format, and real-time entitlement must be verified on the actual account.

Candidate Schwab symbols to test:

- `$SPX`
- `$VIX`
- `$VIX1D`
- `$VIX9D`
- `$VIX3M`
- `$VVIX`
- `$SKEW`

Fallback policy:

- If Schwab returns live Cboe index quotes, use them as broker data and tag `provider=schwab`.
- If IBKR returns Cboe Streaming Market Indexes, prefer IBKR during its available window and tag `provider=ibkr`.
- If neither source returns the official index, mark the official field `missing`.
- Do not rename synthetic estimates as official VIX, VVIX, or SKEW.

Synthetic fields are allowed, but must be clearly named:

- `vix_proxy`
- `near_vol_proxy`
- `tail_risk_proxy`
- `skew_proxy`

## What To Store

### Raw Ticks

Store raw ticks only for:

- selected SPXW or XSP contracts
- SPY/QQQ/IWM option contracts selected for fallback
- SPX, ES/MES, SPY, QQQ, IWM
- Cboe index values when available
- Hyperliquid SPX trades, BBO, mark, funding, and OI
- Polymarket markets being watched

Do not store:

- all OPRA options
- full SPX option chain as tick streams
- every strike for every expiry
- full L2 order books indefinitely

### Latest State

Keep an in-memory and on-disk latest state table:

- latest quote by instrument
- latest feature by feature name
- latest provider quality
- latest alert state and cooldown

This supports dashboard rendering without scanning raw tick files.

### Feature Bars

Persist compact bars:

- 1 second bars for selected instruments during active windows
- 5 second bars for option microstructure features
- 1 minute bars for session analytics and replay

Example feature rows:

- `atm_straddle_mid`
- `atm_iv`
- `atm_delta`
- `atm_gamma`
- `atm_theta`
- `atm_vega`
- `quote_spread_bps`
- `quote_age_ms`
- `es_return_1m`
- `spy_return_1m`
- `hl_spx_return_1m`
- `vix1d_vix9d`
- `tail_risk_proxy`

### Alerts

Always store alert events. They are small and valuable.

Fields:

- timestamp
- trigger name
- severity
- provider state
- feature snapshot
- text sent to user
- whether user opened device or acknowledged, if available later
- forward returns after 5m, 15m, 60m, and close

## Storage Estimates

These are planning ranges, not hard promises.

## SPXW Contract Universe Sizing

For a selected strike window:

```text
contracts = expiries * option_types * strikes_per_expiry
strikes_per_expiry ~= floor((window_percent * 2 * spx_level) / strike_step) + 1
```

Where:

- `expiries`: 2 for 0DTE + 1DTE
- `option_types`: 2 for calls + puts
- `window_percent`: 0.02 for +/-2%
- `strike_step`: usually plan around 5 SPX points for SPXW near-the-money live streaming, unless the provider returns a denser chain

Examples with 5-point strikes:

```text
SPX 5000: +/-2% = 4900-5100, about 41 strikes per expiry
0DTE + 1DTE calls/puts ~= 2 * 2 * 41 = 164 contracts

SPX 6000: +/-2% = 5880-6120, about 49 strikes per expiry
0DTE + 1DTE calls/puts ~= 2 * 2 * 49 = 196 contracts

SPX 7000: +/-2% = 6860-7140, about 57 strikes per expiry
0DTE + 1DTE calls/puts ~= 2 * 2 * 57 = 228 contracts

SPX 7500: +/-2% = 7350-7650, about 61 strikes per expiry
0DTE + 1DTE calls/puts ~= 2 * 2 * 61 = 244 contracts
```

So a 150-300 contract budget is reasonable for 0DTE + 1DTE +/-2% if using 5-point strikes.

If a provider exposes 1-point strikes and the system subscribes to every strike, the same SPX 6000 example becomes about 964 contracts. Do not live-stream that in the MVP. Downselect to 5-point strikes, liquidity-filtered strikes, or a smaller window.

Provider line limits matter separately from server capacity:

- Oracle 4-core/24 GB can compute and store 150-300 selected contracts.
- IBKR may not allow 150-300 simultaneous live option lines unless the account has enough market-data lines.
- For IBKR, start with a smaller live set and use periodic chain snapshots for the wider +/-2% context.
- For Schwab, verify streaming stability and rate behavior before assuming all 150-300 contracts can stream continuously.

Default live-subscription ramp:

- Start: 20-40 contracts, 0DTE ATM +/- 25 to 50 points
- Step 2: 40-80 contracts, 0DTE ATM +/- 50 to 100 points
- Step 3: add selected 1DTE contracts around ATM
- Step 4: only after broker verification, expand toward the full 0DTE + 1DTE +/-2% universe

The full 150-300 contract universe is a sizing target for server capacity and storage planning, not the default broker subscription count.

## SPXW Subscription Tiling

Do not treat every strike inside +/-2% as equally urgent.

For 0DTE trading, the useful cadence depends on distance from ATM:

```text
Core ring:
  0DTE ATM +/- 25 to 50 SPX points
  5-point strikes
  calls + puts
  about 22-42 contracts
  target cadence: live stream or 1 second features

Near ring:
  0DTE ATM +/- 50 to 100 SPX points
  5-point strikes
  calls + puts
  about 40-82 contracts total including core
  target cadence: 5-15 second refresh/features

Outer ring:
  0DTE ATM +/- 1.0% to 2.0%
  5-point strikes, liquidity filtered
  target cadence: 30-60 second chain refresh or event-triggered refresh

Next-day ring:
  1DTE ATM +/- 50 to 100 SPX points first
  wider +/-2% only as snapshot/refresh context
  target cadence: 30-60 seconds unless it becomes the next 0DTE
```

SPX daily movement context:

- 1.0% at SPX 6000 is 60 points
- 1.5% at SPX 6000 is 90 points
- 2.0% at SPX 6000 is 120 points
- 1.0% at SPX 7500 is 75 points
- 1.5% at SPX 7500 is 112.5 points
- 2.0% at SPX 7500 is 150 points

So at SPX 7500, 0DTE ATM +/- 75 to 115 points is closer to a 1.0% to 1.5% normal-move window. A full +/-2% window is useful for tail context, gamma map, and event days, but it does not need tick-level updates by default.

If broker line limits are tight, rotate the outer ring:

```text
second 00: core ring
second 05: near-left slice
second 10: near-right slice
second 15: outer-left slice
second 20: outer-right slice
...
```

For a 16-slice rotation, the full outer window refreshes about once every 16 cycles. This is acceptable only for broad context, not for the ATM decision layer.

Implementation rule:

- Keep the core ring continuously subscribed when possible.
- Refresh the near ring frequently enough for alerts.
- Refresh the outer ring by slices.
- When SPX moves and ATM changes, rebalance rings rather than adding unlimited new contracts.
- Preserve old ATM strikes for a short grace period so alerts do not jump during fast moves.

## Rolling Slice Sampler

When broker line limits are tight, use a simple rotating sampler instead of trying to keep the full window subscribed.

Example at SPX 7500:

```text
window: 7500 +/- 200 points = 7300-7700
strike step: 5 points
strikes: about 81
contracts per expiry: 81 * calls/puts = 162
0DTE + 1DTE full window: about 324 contracts
```

Split the strike window into 20 slices:

```text
slices: 20
slice width: about 20 SPX points
strikes per slice: about 4-5
contracts per slice per expiry: about 8-10
contracts per slice for 0DTE + 1DTE: about 16-20
cadence: one slice every 3 seconds
full scan: about 60 seconds
```

This is a good fit for broad context:

- gamma map refresh
- skew/tail proxy refresh
- stale wide-strike quote detection
- event-day window expansion

It is not enough by itself for the immediate ATM decision layer, because the ATM slice could be stale for up to a minute. Use two lanes:

```text
hot lane:
  ATM +/- 25 to 50 points
  every 1-3 seconds
  used for straddle, ATM IV, Greeks, and alerts

rolling lane:
  full +/- 200 point window
  20 slices
  one slice every 3 seconds
  full scan every minute
  used for context and surface shape
```

If the provider supports a whole-chain endpoint efficiently, prefer one chain refresh per minute over 20 separate quote calls. If the provider only supports quote batches, use the slice sampler.

Avoid rapid streaming subscribe/unsubscribe churn if the broker penalizes it. The rolling sampler should use snapshot/batch quote calls when available. Streaming lines should be reserved for the hot lane.

## Human Alert Sampling Modes

The MVP is an alert and decision-support system, not an automatic execution engine. Therefore it does not need every wide-strike quote at tick speed.

Use sampling modes:

```text
human_alert:
  purpose: notify user to open the trading device
  ATM/near-ATM cadence: 4-15 seconds
  wide-window cadence: 30-60 seconds
  acceptable latency: human-scale, usually tens of seconds

execution_monitor:
  purpose: monitor a possible active order or very near-term 0DTE entry
  ATM/near-ATM cadence: 1-3 seconds
  wide-window cadence: 15-30 seconds
  acceptable latency: low single-digit seconds
```

Default to `human_alert`. Switch to `execution_monitor` only when:

- a high-severity alert is active
- a `device_required` alert is fired
- the system is inside a sensitive market window
- ATM straddle or underlier movement crosses an event threshold
- user manually requests it

`device_required` should automatically promote the relevant symbols to `execution_monitor`; do not require a separate confirmation.

Default promotion TTL:

- normal RTH: 30 minutes
- final RTH hour: 60 minutes
- pre-open watch: 20 minutes
- open 1h: 30 minutes
- post-close 1h: 20 minutes if an event is active; otherwise do not promote
- macro-event window: 60 minutes
- user manual override: user-specified TTL, default 60 minutes

The TTL can be shortened if:

- the alert invalidation level is hit
- quote quality becomes poor for several consecutive checks
- the triggering regime normalizes
- the market closes

Simple 4-group sampler:

```text
window: ATM +/- 200 points
groups: 4
group width: about 100 points each
cadence: one group every 4 seconds
full scan: about 16 seconds
```

At SPX 7500 with 5-point strikes:

```text
full window strikes: about 81
strikes per group: about 20-21
contracts per group, 0DTE only: about 40-42
contracts per group, 0DTE + 1DTE: about 80-84
```

This is simpler and more responsive than a 20-slice one-minute sampler, but each request batch is larger. It is a good compromise if the broker handles batch quotes well.

Recommended starting point:

- `human_alert`: 4 groups, 4 seconds per group, 16 second full scan
- `execution_monitor`: hot lane every 1-3 seconds plus 4-group scan every 16 seconds
- `degraded`: 20 groups, 3 seconds per group, 60 second full scan

Use official or broker-provided VIX/VVIX/SKEW values as separate low-rate context. They can update in real time, but they do not replace option quotes for ATM straddle, spread, and Greeks.

Sensitive market windows can automatically raise cadence:

```text
pre-open 1h:
  VIX/ETF/futures/HL/Polymarket context: 5-15 seconds
  options wide scan: 16-60 seconds if quotes are usable

open 1h:
  ATM/hot lane: 1-4 seconds
  +/-200 rolling scan: about 16 seconds if broker supports batch quotes

close 1h:
  ATM/hot lane: 1-3 seconds
  target spread legs: 1-3 seconds when alert is active
  wider scan: 16-30 seconds

post-close 1h:
  reduce or stop option sampling unless an event is active
  futures/ETF/chain context: 15-60 seconds
```

### Safe MVP

Universe:

- 40-80 live option contracts
- 20-40 ETF/index/futures symbols
- Hyperliquid SPX context
- Polymarket selected markets

Expected raw compressed storage:

- 100 MB to 1 GB per active trading day
- 3 GB to 25 GB per month

This is manageable.

### Aggressive MVP

Universe:

- 150-300 live option contracts
- more frequent quote updates
- wider raw replay retention
- more chain and smart-wallet context

Expected raw compressed storage:

- 1 GB to 5 GB per active trading day
- 25 GB to 125 GB per month

Still manageable with retention rules, but needs monitoring.

### Not MVP

Universe:

- full OPRA chain
- full SPX chain all expiries
- full depth/order-book data
- indefinite raw tick retention

Expected storage:

- can become tens or hundreds of GB per day depending on scope

Do not do this without a dedicated data vendor and storage budget.

## Retention Policy

Recommended default for the current 150 GB mounted disk, with about 80 GB reserved for project data:

- raw selected ticks: 7 trading days
- raw high-severity alert windows: 60 days
- 1 second feature bars: 30 days
- 5 second feature bars: 90 days
- 1 minute feature bars: keep indefinitely
- alert events: keep indefinitely
- daily session reports: keep indefinitely

If disk usage stays comfortably below 70% after two weeks, consider increasing raw selected ticks to 10 trading days. Do not expand to 14 trading days until the real daily footprint is known.

High-severity alert windows should preserve raw ticks from:

- 15 minutes before the alert
- 60 minutes after the alert
- through close if the alert was close-related

## File Layout

Use partitioned Parquet for append-heavy data:

```text
data/
  raw/
    provider=ibkr/date=YYYY-MM-DD/*.parquet
    provider=schwab/date=YYYY-MM-DD/*.parquet
    provider=hyperliquid/date=YYYY-MM-DD/*.parquet
    provider=polymarket/date=YYYY-MM-DD/*.parquet
  features/
    interval=1s/date=YYYY-MM-DD/*.parquet
    interval=5s/date=YYYY-MM-DD/*.parquet
    interval=1m/date=YYYY-MM-DD/*.parquet
  alerts/
    date=YYYY-MM-DD/*.jsonl
  reports/
    date=YYYY-MM-DD/session.md
```

Use DuckDB for local querying across Parquet files.

SQLite is acceptable for:

- runtime state
- subscriptions
- alert cooldowns
- small metadata tables

Do not use SQLite as the main raw tick store.

## Write Strategy

Use buffered writes:

- collect rows in memory for a short interval
- flush every 1-5 seconds or every N rows
- write Parquet chunks by provider/date/instrument class
- write alert JSONL immediately

Avoid:

- one file per tick
- one SQLite insert per tick without batching
- uncompressed raw JSON as the primary store

## Greeks And Calculation Storage

For SPXW Greeks:

- store raw quote
- store model inputs
- store calculated IV and Greeks
- store calculation version
- store quote quality flags

Do not recalculate historical alerts with a changed model without recording the model version.

Suggested fields:

- `model_name`
- `model_version`
- `underlier_source`
- `rate_source`
- `dividend_or_forward_source`
- `iv`
- `delta`
- `gamma`
- `theta`
- `vega`
- `calculation_status`

## Sources

- Cboe VIX methodology: https://cdn.cboe.com/resources/indices/Volatility_Index_Methodology_Cboe_Volatility_Index.pdf
- Cboe VIX mathematics methodology: https://cdn.cboe.com/resources/indices/Cboe_Volatility_Index_Mathematics_Methodology.pdf
- Cboe VIX historical data: https://www.cboe.com/tradable_products/vix/vix_historical_data/
- Cboe SKEW dashboard: https://www.cboe.com/us/indices/dashboard/skew/
- Schwab-py quote docs: https://schwab-py.readthedocs.io/en/latest/client.html
