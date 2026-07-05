# SPX Spark Architecture Plan

## Goal

Build a near-real-time SPX/SPXW 0DTE dashboard and alert system. The first version is observational only: no automatic order placement and no direct trade recommendations.

The system should combine exchange data, ETF risk proxies, on-chain derivatives, prediction markets, and an event-triggered Codex/Spark explanation layer.

## Current Data Stack

Provider-specific payloads are normalized at the collector edge. Downstream
sampler, feature, greeks, alert, and dashboard code consume `InstrumentId`,
`Quote`, `OptionGreeks`, and `ProviderState` instead of IBKR/Schwab raw fields.
Fallback is based on normalized data quality first and provider priority second.

### Available Through IBKR

- OPRA L1
  - SPX/SPXW options
  - XSP options
  - Bid, ask, last, size
  - IBKR model Greeks when underlying permissions are available
- Network A/B/C L1
  - SPY, QQQ, IWM, HYG, LQD, UUP, GLD, USO, TLT, IEF, SHY, sector ETFs
- CME Real-Time L1
  - ES/MES
  - NQ/MNQ if included by the current CME subscription
- Cboe Streaming Market Indexes
  - SPX cash
  - VIX, VIX1D, VIX9D, VIX3M, VVIX, SKEW, subject to symbol verification in TWS/API

### Not Currently Available

- CBOT Treasury futures
  - ZT, ZF, ZN, ZB
- CFE VIX futures
  - VX front/second month futures

These are not required for the MVP. Use ETF and Cboe index proxies first.

## ETF Risk Proxies

Use ETF proxies as RTH risk filters, not as direct entry triggers.

### Rates

- SHY: 1-3Y Treasury ETF
- IEI: 3-7Y Treasury ETF
- IEF: 7-10Y Treasury ETF
- TLT: 20Y+ Treasury ETF
- TIP: TIPS proxy

Derived flags:

- `rates_pressure`: TLT/IEF falling quickly
- `growth_scare`: SPY down, TLT up, HYG/LQD down
- `duration_pressure`: QQQ weak while TLT/IEF are falling

### Credit

- HYG: high yield credit proxy
- JNK: alternative high yield proxy
- LQD: investment grade credit proxy

Derived flags:

- `credit_stress`: HYG/LQD falling
- `weak_rally`: SPX up but HYG/LQD down
- `risk_off_confirmed`: SPX down and HYG/LQD down

### Dollar And Commodities

- UUP: dollar proxy
- GLD/IAU: gold proxy
- USO/BNO/DBO: crude proxy
- CPER: copper proxy
- XLE: energy equity proxy

Derived flags:

- `dollar_pressure`: UUP rising quickly
- `inflation_scare`: oil up, dollar/rates pressure, equities weak
- `hedge_demand`: GLD up while SPX weak

### Equity Breadth And Leadership

- QQQ/SPY: tech leadership
- IWM/SPY: small-cap risk appetite
- RSP/SPY: equal-weight breadth if RSP is available
- XLY/XLP: cyclical vs defensive
- XLK/XLU: growth/tech vs defensive utilities
- DJU when available, with XLU as the ETF proxy

Derived flags:

- `breadth_weak`: SPX up but RSP/SPY or IWM/SPY weak
- `trend_quality_high`: SPX up with QQQ/SPY, RSP/SPY, and HYG/LQD all strong

## Cboe Vol Regime

Use Cboe index data as a vol-regime layer.

Inputs:

- SPX cash
- VIX1D
- VIX9D
- VIX
- VIX3M
- VVIX
- SKEW
- NDX/RUT/DJX/DJU as cross-index context when entitlement and contract
  definitions are verified

Derived features:

- `short_vol_pressure = VIX1D / VIX9D`
- `near_vol_pressure = VIX9D / VIX`
- `term_stress = VIX / VIX3M`
- `vvix_change`
- `skew_level`

## Options Layer

Subscribe to SPXW 0DTE ATM +/- N strikes first.

Features:

- ATM strike
- ATM straddle mid
- expected move
- IV
- delta
- gamma
- theta
- vega
- bid/ask spread
- quote age
- quote stale flag
- gamma concentration by strike

The options layer is the execution-quality and 0DTE volatility truth source.

## Underlier Layer

Inputs:

- ES/MES
- SPY
- SPX cash
- Hyperliquid S&P perp as context only

Features:

- ES return 1s, 5s, 1m, 5m
- SPY return 1s, 5s, 1m, 5m
- SPX cash anchor
- ES-SPX basis
- SPY implied SPX
- opening range position
- VWAP distance

## Hyperliquid And Trade[XYZ] Context

Treat Hyperliquid S&P perp as a 24/7 sentiment and microstructure source, not as an ES replacement.

Useful feeds:

- `allMids`
- `bbo`
- `l2Book`
- `trades`
- `activeAssetCtx`
- candles

Useful fields:

- mid
- markPx
- oraclePx
- best bid/ask
- L2 depth
- spread
- book imbalance
- trade price
- trade size
- trade side
- funding
- openInterest
- dayNotionalVolume
- mark-oracle premium

Derived features:

- `hl_spx_return_1m`
- `hl_spx_return_5m`
- `hl_spx_premium_vs_es`
- `hl_spx_premium_vs_spx_cash`
- `hl_spx_mark_oracle_premium`
- `hl_spx_funding_pressure`
- `hl_spx_oi_change`
- `hl_spx_book_imbalance`
- `hl_spx_large_trade_burst`
- `hl_spx_weekend_gap_proxy`

## Hyperliquid Smart-Money Research Module

This is a research module first. It should not directly trigger trades until validated against ES/SPY/SPXW outcomes.

### Raw Data

From Hyperliquid SPX perp trades:

- buyer address
- seller address
- trade side
- price
- size
- timestamp
- trade id

From asset context:

- funding
- open interest
- mark price
- oracle price
- volume

### Wallet Flow Tables

Tables:

- `hl_wallet_trade`
- `hl_wallet_flow_1m`
- `hl_wallet_flow_5m`
- `hl_wallet_flow_15m`
- `hl_wallet_score_daily`
- `hl_smart_cohort_state`

Per-wallet features:

- net signed volume
- taker buy volume
- taker sell volume
- average trade size
- trade frequency
- large trade count
- position-flip estimate
- realized forward performance after 5m, 15m, 60m

### Wallet Scoring

Do not define smart money by size alone. Score wallets by demonstrated leading behavior.

Candidate score:

- future return alpha
- consistency
- risk-adjusted PnL proxy
- drawdown control
- timing quality
- size quality
- market-specific performance on SPX perp
- chase penalty
- suspicious activity penalty

Wallet tiers:

- Tier A: validated smart cohort
- Tier B: whale flow, not yet proven smart
- Tier C: market-maker or arbitrage-like flow
- Tier D: noisy or suspicious wallets

### Smart-Money Signals

Signals:

- `smart_net_flow_5m`
- `smart_net_flow_15m`
- `smart_flip_count`
- `smart_vs_whale_divergence`
- `smart_flow_against_price`
- `smart_flow_plus_oi`
- `smart_flow_pre_tradfi_open`
- `smart_news_shock_reaction`

Example alert context:

- HL SPX smart cohort net short 15m
- HL SPX mark-oracle premium turns negative
- OI rises
- ES/SPY weak
- SPXW ATM straddle expands

This should produce a high-severity context alert, not an automatic trade.

## Hyperliquid SPX TACO Cohort Scanner

Research idea: identify wallets that bought SPX perp before major geopolitical de-escalation or "TACO" reversals, then check whether those wallets remain active today.

The goal is not to blindly follow historical winners. The goal is to build a watchlist of wallets that repeatedly positioned for de-escalation before the broader market confirmed it, then validate whether the cohort still has forward-looking value.

Event windows:

- `escalation_headline`: threats, strikes, Hormuz risk, ultimatums, military escalation
- `pre_taco_window`: 1h, 3h, 6h, and 24h before a de-escalation headline
- `reaction_window`: 1h, 6h, and 24h after the de-escalation headline

Candidate wallet behavior:

- net bought SPX perp during the pre-TACO window
- increased risk-on exposure before de-escalation was confirmed
- potentially shorted oil perp if available
- potentially bought BTC/ETH or other risk assets
- did not simply chase after the de-escalation headline

Candidate score:

- pre-TACO SPX net-buy notional
- timing quality before de-escalation headline
- post-event forward return
- event repeat count
- cross-asset consistency
- current activity score
- sample size
- chase-after-headline penalty
- one-hit-wonder penalty
- suspicious-flow penalty

Suggested fields:

- `wallet`
- `event_hit_count`
- `avg_lead_time_before_taco`
- `pre_taco_net_spx_flow`
- `cross_asset_pattern`
- `post_event_avg_return`
- `active_7d`
- `active_30d`
- `spx_active`
- `last_seen`
- `current_1h_bias`
- `confidence`

Validation rules:

- Keep a separate watchlist and validation period.
- Do not promote wallets to high confidence from one event.
- Track forward returns after cohort flow over 15m, 60m, and 24h.
- Compare against random active wallet cohorts and whale-only cohorts.
- Validate against HL SPX first, then ES/SPY/SPXW vol response.

Implementation phases:

- Phase 3.5: historical TACO cohort scan
- Phase 4.5: live TACO cohort monitor
- Phase 6: forward-performance validation report

### Limitations

- Wallet addresses are not real-world entities.
- One entity may use many wallets.
- A wallet may hedge on CME, IBKR, or other venues.
- Large traders are not necessarily smart.
- Smart cohorts can be regime-dependent.
- Public trade flow is post-trade, not hidden intent.
- Need weeks of data before trusting wallet scores.

## Polymarket Context

Use Polymarket as an event-probability layer.

Market types:

- S&P 500 close above/below level
- SPY close above/below level
- Fed rate cut/hike markets
- CPI/PPI/NFP surprise markets
- recession/geopolitical/election markets

Useful feeds:

- Gamma API for market discovery and token IDs
- CLOB WebSocket for orderbook and price updates

Features:

- best bid
- best ask
- mid probability
- probability change 5m/30m/1h
- spread
- depth
- liquidity
- volume burst
- market resolved/new-market events

Derived flags:

- `event_probability_jump`
- `event_liquidity_score`
- `event_spread_quality`
- `macro_event_risk_score`

## Alert Engine

Rules run continuously. LLM explanation is event-triggered and budgeted.

Alert categories:

- data quality
- vol regime
- risk-off
- option microstructure
- underlier divergence
- chain context
- smart-money context
- prediction-market context

Each alert must include:

- trigger name
- severity
- current value
- threshold
- source data
- timestamp
- cooldown key
- cooldown expiry

## Codex/Spark Explanation Layer

Use Codex/Spark as an event explanation layer, not as a tick engine.

Official positioning from Codex docs:

- GPT-5.3-Codex-Spark is a separate fast Codex model.
- It is optimized for near-instant, real-time text iteration.
- It is less capable than the main frontier Codex models.
- It has its own usage limits.
- During research preview it is available for ChatGPT Pro subscribers.

### Summary Modes

- `off`: no LLM
- `rules`: deterministic template only
- `spark`: Codex/Spark event-triggered summary
- `api`: API-backed summary model if available and preferred

### Trigger Conditions

Call Spark only when one of these occurs:

- SPXW ATM straddle expands rapidly
- VIX1D/VIX9D changes abruptly
- ES/SPY/HL_SPX diverge
- Hyperliquid OI/funding/large-trade burst is abnormal
- smart cohort flips direction
- Polymarket probability jumps
- macro event window starts or ends
- user manually requests an explanation

### Budget

Initial limits:

- normal mode: 10-20 calls per day
- active mode: 50-80 calls per day
- same alert family cooldown: 5-15 minutes
- quota-low mode: fall back to rules templates

### Input Contract

Spark receives a compressed state packet, not raw ticks.

Example:

```json
{
  "market_time": "2026-07-04T09:45:00-04:00",
  "regime_flags": ["vol_expansion", "credit_weak"],
  "underlier": {
    "es_5m": -0.42,
    "spy_5m": -0.39,
    "hl_spx_5m": -0.55
  },
  "options": {
    "atm_straddle_change_5m": 8.2,
    "spread_quality": "ok"
  },
  "vol": {
    "vix1d": 18.4,
    "vix9d": 16.1,
    "vix1d_vix9d": 1.14
  },
  "prediction": {
    "fed_cut_prob_5m_change": 3.5
  },
  "smart_money": {
    "smart_net_flow_15m": "net_short",
    "smart_flip_count": 7,
    "oi_change_15m": 4.6
  },
  "recent_alerts": []
}
```

### Output Contract

Spark should return structured output:

```json
{
  "regime": "risk_off_vol_expansion",
  "severity": "high",
  "summary": "Short explanation of what changed.",
  "watchpoints": ["SPXW ATM straddle", "HL SPX smart flow", "HYG/LQD"],
  "do_not_infer": ["no direct trade recommendation"]
}
```

## MVP Phases

### Phase 0: Project Isolation

- Create `/home/ubuntu/spx-spark`
- Use project-local `.codex-home`
- Do not modify `~/.codex`
- Do not modify the global `codex` binary

### Phase 1: IBKR Verifier

- Verify live/delayed state for SPX, VIX series, ES/MES, SPY, ETFs, and SPXW options
- Verify Greeks availability
- Verify stale quote detection

### Phase 2: Core Collectors

- IBKR collector
- Hyperliquid collector
- Polymarket collector

### Phase 3: Feature Engine

- Options features
- Underlier features
- Vol regime
- ETF risk proxies
- Chain context
- Smart-money research features

### Phase 4: Alerts

- Deterministic alert engine
- Cooldowns
- Severity model
- Push notifications

### Phase 5: Dashboard

- Market state
- SPXW option surface
- Vol regime
- ETF risk proxies
- Hyperliquid context
- Smart-money module
- Polymarket context
- Alert feed
- Spark explanations

### Phase 6: Replay And Validation

- Raw logs
- Feature logs
- Alert logs
- Session replay
- Smart-money forward-performance validation
- Daily report

## First Completion Criteria

- IBKR verifier confirms live data availability.
- SPXW ATM chain is discovered automatically.
- Greeks and ATM straddle display near real time.
- ES/SPY/SPX/VIX series display correctly.
- ETF risk proxy flags update.
- Hyperliquid S&P perp context is live.
- Polymarket event probability data is live for selected markets.
- Smart-money flow is recorded, even if not yet trusted.
- Alerts are explainable and include source data.
- Spark summaries are event-triggered and budgeted.
