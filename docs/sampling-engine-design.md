# Sampling Engine Design

Date: 2026-07-04

## Goal

Drive broker-friendly option quote collection for SPXW/XSP/SPY-family options without requiring full-chain tick subscriptions.

The sampler supports:

- human-alert mode
- execution-monitor mode
- sensitive market windows
- 4-group rolling scans
- 150 GB local disk budget
- automatic cleanup and audit windows

## Open Design Issues Resolved

### Broker Limits

Server capacity is not the main limiter. Broker line limits and pacing are.

Rules:

- Use streaming lines only for the smallest hot set.
- Use batch quote or option-chain calls for rolling scans when available.
- Avoid rapid subscribe/unsubscribe churn on IBKR.
- Track provider errors as first-class data-quality events.
- Degrade cadence automatically when errors increase.

### Time Sensitivity

The system is for manual spread decisions, not automatic execution.

Rules:

- human-alert mode is acceptable at 4-60 second cadence depending on instrument class.
- execution-monitor mode is only for a relevant active alert or sensitive window.
- unattended windows should produce reports and high-severity alerts, not noisy trade prompts.

### Storage

The host has about 150 GB mounted storage.

Rules:

- Do not store full OPRA.
- Do not store all SPXW strikes as tick data.
- Store selected raw quotes, compact feature bars, alerts, and reports.
- Preserve raw windows only around high-value events.

## Modes

### `human_alert`

Default mode.

Use when:

- no active high-severity alert
- user is not in an execution-monitor TTL
- normal market conditions

Cadence:

- VIX/VVIX/SKEW and index context: 5-15 seconds when available
- SPY/QQQ/IWM/ES/HL context: 5-15 seconds
- option wide scan: 16-60 seconds
- feature bars: 1 minute plus selected 5 second bars

### `execution_monitor`

Temporary high-cadence mode.

Enter when:

- `device_required` alert fires
- high-severity alert fires
- close-window anomaly fires
- user manually requests it

Default TTL:

- pre-open: 20 minutes
- open 1h: 30 minutes
- normal RTH: 30 minutes
- close 1h: 60 minutes
- post-close: 20 minutes only if an event remains active

Cadence:

- hot option legs and ATM area: 1-3 seconds
- +/-200 rolling scan: about 16 seconds if batch quotes are stable
- VIX/VVIX/SKEW context: 5-15 seconds

Exit early when:

- invalidation level is hit
- quote quality is poor for several consecutive checks
- trigger regime normalizes
- market closes and no post-close event remains active
- disk pressure requires degradation

### `degraded`

Use when broker errors, line limits, API pacing, or disk pressure appear.

Cadence:

- hot lane only if possible
- wide window: 20 slices, 3 seconds per slice, 60 second full scan
- optional full-chain refresh every 1-5 minutes if provider supports it

## Sampling Universe

Default underlier:

- SPXW 0DTE
- optional XSP or SPY fallback

Default strike step:

- 5 SPX points for SPXW

Default window:

- ATM +/- 200 points for rolling context
- ATM +/- 25 to 50 points for hot lane

At SPX 7500:

- +/-200 contains about 81 strikes
- 0DTE call/put window is about 162 contracts
- 0DTE + 1DTE window is about 324 contracts

Do not subscribe to all 324 contracts as continuous streaming lines unless a provider has been verified to support it.

## 4-Group Rolling Sampler

Default human-alert sampler:

```text
window: ATM +/- 200 points
groups: 4
cadence: 1 group every 4 seconds
full scan: 16 seconds
```

At SPX 7500:

```text
strikes per group: about 20-21
contracts per group, 0DTE: about 40-42
contracts per group, 0DTE + 1DTE: about 80-84
```

Default group construction is interleaved, not contiguous:

```text
group 0: 7300, 7320, 7340, ...
group 1: 7305, 7325, 7345, ...
group 2: 7310, 7330, 7350, ...
group 3: 7315, 7335, 7355, ...
```

This means every 4-second batch sees a sparse view across the whole +/-200 point window. The full 16-second scan fills in every 5-point strike.

Use contiguous groups only for diagnostic runs or if a provider explicitly favors compact strike ranges.

## Hot Lane

Hot lane is a small, always-fresh set.

Default:

- 0DTE ATM +/- 25 to 50 points
- calls and puts
- target spread candidate legs when active
- old ATM area preserved for 3-5 minutes after ATM moves

Cadence:

- 4-8 seconds in human-alert mode if batch quotes only
- 1-3 seconds in execution-monitor mode
- continuous stream if provider line limits allow it

## ATM Rebalance

ATM changes as SPX moves.

Rules:

- Recompute ATM from best available underlier every 5-15 seconds.
- Do not rebalance on every tiny move.
- Rebalance when ATM strike changes by at least one strike step and remains changed for two checks.
- Keep old ATM strikes in the hot lane for 3-5 minutes.
- Cap hot lane size; if exceeded, drop the oldest non-target strikes first.

## Provider Strategy

### Schwab

Preferred for:

- always-on non-IBKR collection
- batch quotes
- option chains if verified
- ETF/index/futures quotes if verified

Need to test:

- SPX/SPXW/XSP option chain support
- batch quote limits
- level-one option streaming stability
- Cboe index symbols such as `$VIX`, `$VVIX`, `$SKEW`

### IBKR

Preferred for:

- paid OPRA/Cboe/CME verification
- high-quality SPXW/Cboe checks when available
- hot lane streaming if market-data lines permit

Rules:

- never fight mobile/desktop session
- no `compete=true`
- weekend auto mode pauses IBKR
- use snapshots/chain refresh for wide context where possible
- avoid rotating streaming subscriptions every few seconds

### Hyperliquid / Polymarket

Preferred for:

- 24/7 context
- weekend/holiday lightweight monitoring
- event probability and smart-flow research

## Data Model

Provider-specific payloads must be normalized before sampler, feature, greeks,
alert, or dashboard code consumes them. The canonical implementation is
`src/spx_spark/marketdata.py`; see `docs/market-data-model.md`.

Raw quote row:

```text
timestamp
provider
instrument_id
expiry
strike
right
bid
ask
last
bid_size
ask_size
volume
open_interest
quote_time
trade_time
source_latency_ms
quality
sampling_mode
sampling_group
```

Feature row:

```text
timestamp
feature_name
value
provider
quality
sampling_mode
underlier
expiry
strike
model_version
source_quote_age_ms
```

Alert row:

```text
timestamp
alert_id
severity
trigger_name
mode_before
mode_after
provider_state
feature_snapshot
message
cooldown_key
preserve_raw_window
```

## 150 GB Disk Budget

Reserve disk:

- 20 GB: OS, packages, logs, buffers
- 25 GB: DuckDB scratch and compaction workspace
- 25 GB: emergency free space and unexpected logs
- 80 GB: project data

Project data budget:

- raw selected ticks: 30 GB
- preserved alert windows: 15 GB
- feature bars: 15 GB
- reports and alert JSONL: 5 GB
- staging/temporary files: 15 GB

Default retention under 150 GB:

- raw selected ticks: 7 trading days
- raw high-severity alert windows: 60 days
- 1 second feature bars: 30 days
- 5 second feature bars: 90 days
- 1 minute feature bars: keep indefinitely while under budget
- alerts and reports: keep indefinitely

If disk usage remains low after two weeks, consider increasing raw selected ticks to 10 trading days. Do not expand to 14 trading days until the real daily footprint is known.

Disk watermarks:

- 70%: log warning, include in daily report
- 80%: stop aggressive sampling and compact
- 85%: switch sampler to degraded mode
- 90%: prune non-preserved raw ticks
- 95%: stop raw tick writes except alerts/latest state

## Write Strategy

Use buffered batch writes.

Rules:

- write alert JSONL immediately
- buffer raw quotes for 1-5 seconds or N rows
- write Parquet chunks by provider/date/instrument class
- compact small files daily
- never write one file per tick
- do not use SQLite for high-volume raw quotes

Recommended files:

```text
data/raw/provider=schwab/date=YYYY-MM-DD/hour=HH/*.parquet
data/raw/provider=ibkr/date=YYYY-MM-DD/hour=HH/*.parquet
data/features/interval=1s/date=YYYY-MM-DD/*.parquet
data/features/interval=1m/date=YYYY-MM-DD/*.parquet
data/alerts/date=YYYY-MM-DD/*.jsonl
data/reports/date=YYYY-MM-DD/session.md
```

## Cleanup And Audit

Daily maintenance:

- compact prior day's small Parquet files
- materialize 1 minute bars
- label alert forward returns
- write data-quality report
- run retention dry-run

Weekend maintenance:

- run actual retention pruning after dry-run
- compact weekly data
- audit provider missing fields
- recalculate preserved alert-window features if model version changed
- generate weekly report

Deletion safety:

- never delete preserved alert windows automatically without a manifest
- write deletion manifest before pruning
- retain manifest indefinitely
- prefer moving to `data/trash/date=YYYY-MM-DD` first if disk pressure allows

## Failure Modes

Broker pacing:

- increase interval
- switch to degraded sampler
- reduce 1DTE first
- reduce outer window before hot lane

Market-data line cap:

- stop adding live lines
- keep hot lane
- use batch/chain refresh for rolling context

Provider stale quotes:

- mark quality stale
- suppress spread-quality alerts
- continue underlier and index context

Disk pressure:

- stop wide raw writes
- keep alerts/latest state
- run compaction/pruning

Clock/timezone mismatch:

- store all timestamps in UTC
- derive market windows from `America/New_York`
- display local schedule in Asia/Shanghai

## Implementation Order

1. Schwab verifier
2. Maintenance dry-run
3. Raw quote schema and JSONL writer
4. 4-group sampler using mock/provider stubs
5. Runtime mode integration
6. Sensitive-window scheduler
7. Execution-monitor TTL state
8. Disk watermarks and degradation policy
9. Alert preservation manifests

Current status:

- raw normalized quote schema is implemented in `spx_spark.marketdata`
- JSONL raw writer is implemented in `spx_spark.storage`
- latest-state fallback store is implemented in `spx_spark.storage`
- mock collector is implemented in `spx_spark.mock_collector`
- Parquet writer and compaction are deferred until real quote volume is measured
