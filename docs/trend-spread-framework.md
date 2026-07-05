# Trend Spread Framework

Date: 2026-07-05

## Purpose

Build an independent SPX 0DTE spread-scoring layer inspired by public descriptions
of structured SPX credit-spread workflows. The public Alpha Crunching page describes
its Trend Spread Engine as combining time of day, intraday trend, and strike
selection for SPX 0DTE credit spread opportunities. We use that public framing as
a product benchmark, not as proprietary rule replication.

Source: https://www.alphacrunching.com/#pricing

Project boundary remains unchanged:

- observational only
- no automatic order placement
- no hidden copy-trading rule
- every output must include data quality and missing-data warnings

## Pipeline Position

```text
ProviderSnapshot
  -> LatestState
  -> feature bars
  -> TrendSpreadInputs
  -> TrendSpreadScore
  -> MicopediaSignal / alert explanation / audit
```

The framework must not consume broker-specific fields directly. It consumes
normalized quotes, provider states, and derived feature rows.

## Data First

The scoring layer is not useful until the data layer proves these inputs.

### P0 Required

- official `index:SPX`
- SPXW 0DTE selected option quotes
- SPXW bid, ask, mid, quote time, and provider quality
- SPXW model Greeks when IBKR provides them
- `index:VIX1D`, `index:VIX9D`, `index:VIX`
- `equity:SPY`, `equity:QQQ`, `equity:IWM`, `equity:DIA`
- latest provider states and stale/degraded flags

### P1 Useful

- `index:VVIX`, `index:SKEW`, `index:VIX3M`
- `future:ES` / `future:MES` when available
- `equity:HYG`, `equity:LQD`, `equity:TLT`, `equity:IEF`
- Hyperliquid `crypto_perp:xyz:SP500` context
- official `index:NDX`, `index:RUT`, `index:DJX`, `index:DJU` only when
  entitlement and contract definitions are verified

ETF proxies are enough for many alerts:

```text
QQQ -> NDX context
IWM -> RUT context
DIA -> Dow context
XLU -> utilities/DJU context
```

## Core Feature Groups

### 1. Time-Of-Day

The first version should use named time buckets, not learned magic thresholds.

Examples in New York time:

- premarket
- open 0-30m
- open 30-60m
- midday
- power hour
- final 15m
- post-close

Feature examples:

- `time_bucket`
- `minutes_from_open`
- `minutes_to_close`
- `is_event_window`
- `is_close_risk_window`

Later validation can attach historical win rate, MFE, MAE, and spread payoff
statistics by bucket.

### 2. Intraday Trend

Trend must be measured from data, not inferred from narrative.

Feature examples:

- `spx_return_from_open`
- `spx_return_1m`, `spx_return_5m`, `spx_return_15m`
- `spx_opening_range_position`
- `spx_vwap_distance_bps` when VWAP is available
- `spy_confirmation`
- `qqq_iwm_dia_confirmation`
- `hyg_lqd_risk_confirmation`
- `hl_sp500_premium_bps` as context only

Initial trend labels:

- `uptrend`
- `downtrend`
- `range`
- `failed_breakout`
- `failed_breakdown`
- `unknown`

### 3. Vol And Expected Range

SPX 0DTE spreads are sensitive to implied range and IV reset.

Feature examples:

- `vix1d`
- `vix1d_vix9d`
- `vix9d_vix`
- `atm_straddle_mid`
- `atm_straddle_implied_move_points`
- `realized_range_points`
- `realized_vs_implied_range`
- `iv_crush_risk`

If the option chain is missing, the framework may still produce a checklist, but
spread selection must be marked degraded.

### 4. Strike Selection

Strike selection should be an explicit scoring problem.

Candidate spread fields:

- `right`: put credit spread or call credit spread
- `short_strike`
- `long_strike`
- `width`
- `mid_credit`
- `max_loss`
- `credit_to_width`
- `distance_from_spx_points`
- `distance_from_spx_bps`
- `short_delta`
- `long_delta`
- `spread_mid`
- `spread_bid_ask_width`
- `spread_quality`
- `quote_age_ms`
- `provider_quality`

Useful gates:

- stale quote -> reject
- missing bid/ask -> reject
- negative or crossed market -> reject
- too-wide spread -> reject or degraded
- missing Greeks -> allow only low-confidence liquidity view
- short strike inside noisy ATM zone -> require stronger trigger

## Score Shape

Do not start with one opaque score. Keep components visible:

```json
{
  "time_score": 0.0,
  "trend_score": 0.0,
  "vol_score": 0.0,
  "strike_score": 0.0,
  "liquidity_score": 0.0,
  "risk_score": 0.0,
  "final_score": 0.0,
  "decision": "degraded|watch|candidate|avoid",
  "warnings": []
}
```

Initial decision meaning:

- `degraded`: required data missing or stale
- `avoid`: data present but spread quality/risk is poor
- `watch`: map is interesting but trigger is missing
- `candidate`: data, trigger, and risk gates line up

`candidate` is still not an order. It is an alert/checklist state.

## Direction And Expression

The framework should separate directional read from spread expression.

Examples:

- bullish trend + valid support/reclaim -> evaluate put credit spreads below support
- bearish trend + failed reclaim/rejection -> evaluate call credit spreads above resistance
- mixed/pin regime -> evaluate no-trade or bounded range logic
- high-vol event -> reduce confidence unless IV/range behavior confirms the setup

## Validation Plan

Every generated score should be persisted with enough context to judge it later.

Forward metrics:

- `return_T+5m`
- `return_T+15m`
- `return_T+30m`
- `return_T+60m`
- `return_to_close`
- `MFE`
- `MAE`
- `short_strike_touched`
- `spread_max_profit_possible`
- `spread_stop_would_trigger`
- `quote_quality_at_signal`

Do not promote any rule to "edge" until it survives this audit.

## MVP Order

1. IBKR real-data acceptance for SPX, VIX family, ETF proxies, ES/MES, and SPXW
   selected options.
2. Persist selected raw quotes and latest state.
3. Add 1-minute underlier features: SPX/SPY/QQQ/IWM/DIA returns and time bucket.
4. Add SPXW ATM straddle and selected spread candidate features.
5. Generate transparent `TrendSpreadScore` rows.
6. Feed the score into `MicopediaSignal` explanation and alert audit.

The first working version should prefer being explainable and auditable over
being clever.
