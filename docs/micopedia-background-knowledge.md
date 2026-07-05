# MrMicopedia Background Knowledge

Date: 2026-07-05

This file gives the background an agent should know before using
`MicopediaSignal`. It is a knowledge map, not a trading system, recommendation,
or proof of edge.

## Source Framing

The framework comes from a local review of public @MrMicopedia posts about SPX,
0DTE options, gamma, key levels, intraday execution, and post-trade reviews.
The strongest public evidence for a mature SPX/0DTE framework appears around
2025-03 to 2025-05 and becomes more tool-specific in 2026-05 to 2026-07.

The analysis should be treated as a distilled decision style:

```text
regime -> map -> trigger -> expression -> risk -> audit
```

It should not be treated as a direct copy-trading feed or a fixed set of magic
thresholds.

## Core Instruments

- `SPX`: S&P 500 cash index. Main reference level for the framework.
- `SPXW`: Cboe weekly/daily SPX options, including 0DTE expiries.
- `ES`: CME E-mini S&P 500 futures. Useful for high-frequency confirmation, but
  not always available in this project.
- `SPY`: ETF proxy. Useful as fallback and breadth/risk confirmation, not the
  same as SPX options.
- `VIX1D`, `VIX9D`, `VIX`, `VIX3M`, `VVIX`, `SKEW`: vol-regime inputs.
- `xyz:SP500` on Hyperliquid: context only. It is not CME ES and not official
  SPX.

## Options Concepts

- `0DTE`: option expiring the same day. Very sensitive to price path, timing,
  IV, and dealer hedging.
- `1DTE`: option expiring the next trading day. Sometimes used when same-day
  premium decay or timing risk is unattractive.
- `Vertical spread`: buy one option and sell another same-expiry option at a
  different strike. The framework often prefers this because maximum loss and
  maximum payoff are known.
- `Call spread`: bullish or upside-defined vertical spread.
- `Put spread`: bearish or downside-defined vertical spread.
- `Single-leg option`: naked long call or put. More convex, but direction, IV,
  and timing can all be right against the holder.
- `ATM straddle`: at-the-money call plus put. A proxy for the market's expected
  move and premium level.
- `IV crush`: implied volatility falling after an event, which can hurt long
  options even when direction is correct.
- `Theta`: time decay. In 0DTE, time decay can dominate unless the move is fast
  or the spread payoff is well-defined.

## Market Structure Concepts

- `Positive gamma`: dealers tend to dampen moves; price may mean-revert around
  important strikes.
- `Negative gamma`: dealers may chase moves; breaks can accelerate.
- `Call wall`: strike or zone where large call exposure may cap or attract price.
- `Put wall`: strike or zone where large put exposure may support or accelerate
  if broken.
- `Pin`: price gravitates near a high-open-interest or gamma-sensitive level,
  especially near expiry.
- `JPM collar`: large quarterly hedging structure that can create important
  reference zones, but should not be used mechanically.
- `OPEX`: options expiration period. Pinning, wall behavior, and flow effects can
  be more important than ordinary directional narratives.
- `Dealer hedging`: market-maker hedging can influence intraday price path, but
  it is not the only actor. Systematic funds, pension flows, macro events,
  liquidity operations, and algorithms can dominate in some regimes.

## Regime Types

Agents should classify the day before interpreting a signal:

- `ordinary_rth`: no clear event or microstructure regime.
- `low_vol_difficult`: VIX1D/realized range is low; 0DTE premium may be hard to
  monetize unless entry is precise.
- `positive_gamma_mean_reversion`: walls and mean reversion matter more than
  chase entries.
- `negative_gamma_trend`: level breaks can accelerate; fading requires evidence.
- `opex_gamma_pin`: close distribution, wall behavior, and pin risk dominate.
- `high_vol_event`: CPI, FOMC, NFP, PCE, geopolitical or tariff headlines, and IV
  reset risk dominate.
- `liquidity_systematic_flow`: TGA/liquidity, CTA/systematic flow, pensions, and
  month-end or quarter-end behavior can override local levels.
- `holiday_liquidity`: holiday schedule and reduced liquidity can distort normal
  statistics.

## Intraday Phases

- `premarket`: build scenarios, levels, event map, and maximum-loss structures.
- `open`: validate the first response to gap, flush, squeeze, or opening-range
  failure.
- `midday`: avoid forcing trades in noise; watch whether the original map still
  holds.
- `late`: focus on close distribution, pin risk, spread max-payoff conditions,
  and whether remaining risk is worth holding.
- `closed`: classify posts as review unless clearly planning the next session.

## Common Setup Language

- `Flush`: fast downside move. Can be a buy trigger only if support/reclaim and
  regime context confirm it.
- `Knife`: sharp falling move. Treat as a hypothesis, not an automatic buy.
- `Spike`: fast upside move. Can mark squeeze continuation or exhaustion,
  depending on wall and acceptance behavior.
- `Reclaim`: price breaks below/above a level, then quickly retakes it.
- `Acceptance`: price spends enough time beyond a level to imply the market has
  accepted the new zone.
- `Close distribution`: the practical question "where is SPX likely to close",
  often more relevant to spreads than raw up/down direction.

## Risk Model

The stable risk logic is more important than any one setup:

- Prefer defined-risk structures before anything execution-facing.
- Avoid undefined overnight risk. Overnight exposure must be explicit, bounded,
  and cheap.
- Protect gains quickly; missing extra upside is better than letting 0DTE
  profits decay into losses.
- Do not average down unless the close-distribution thesis, timing window, and
  maximum-loss structure still make sense.
- Do not treat post-trade review as pre-trade signal.
- Do not use Hyperliquid SP500 as an execution reference for CME ES or official
  SPX.

## Validation Metrics

Future backtests should compute:

- `return_T+15m`, `return_T+30m`, `return_T+60m`, `return_to_close`.
- `MFE`: maximum favorable excursion after signal time.
- `MAE`: maximum adverse excursion after signal time.
- `key_level_touched`: whether price reached the stated level.
- `key_level_accepted`: whether price stayed beyond the level.
- `direction_correct`: whether direction matched the stated bias.
- `is_review`: whether the post/signal was after-the-fact commentary.
- `parent_context_status`: cached, missing, not_found, or not_required.

## Anti-Patterns For Agents

- Do not turn `candidate_expression` into an order.
- Do not collapse gamma, macro, and flow into one reason.
- Do not assume fixed VIX1D thresholds are stable without validation.
- Do not infer a strong signal from a reply when parent context is missing.
- Do not overfit sparse months or short holiday samples.
- Do not claim realized edge before SPX/ES 1-minute data and option-chain history
  validate the setup.

