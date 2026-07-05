# Alert Reasoning and SPXW Strategy Model Review

Date: 2026-07-05

Scope: SPX/SPXW manual option trading alerts. ES is allowed as confirmation. VIX family, ETFs, Hyperliquid, Polymarket, and on-chain smart-money data are algorithm context only unless explicitly promoted by future validation.

This document incorporates a DeepSeek architecture review run on 2026-07-05 against the current alert design. The review is advisory; production rules below are the accepted system contract.

## Current Production Rules

1. Human-visible alerts must be SPX/SPXW/ES scoped.
   Non-focus symbols and cross-market signals may affect internal scoring, but they must not appear in the human alert text.

2. Stale SPXW option quotes cannot drive wall, gamma, IV, or surface alerts.
   The options map tracks `live`, `stale`, `delayed`, `unknown_age`, and `max_age_ms`. `stale`, `error`, `missing`, and `unknown` quotes are excluded from IV/GEX/wall calculations.

3. Option-derived alerts require fresh coverage.
   If the live quote ratio is below `ALERT_MIN_OPTION_LIVE_RATIO`, the max quote age is above `ALERT_MAX_OPTION_QUOTE_AGE_MS`, or timestamp coverage is insufficient when enabled, wall and gamma alerts are suppressed and only an `option_quote_freshness_degraded` alert is emitted.

4. IV surface alerts require a recent snapshot.
   If the current surface is older than `ALERT_MAX_IV_SURFACE_AGE_SECONDS`, the system emits `iv_surface_stale` and suppresses IV surface alerts.

5. Hyperliquid SPX proxy is gated by TradFi anchors.
   The proxy must be compared against live ES, MES, or SPX. With no anchor, it is `unanchored_context_only`. If basis breaches `HYPERLIQUID_PROXY_BASIS_WARN_BPS` or `HYPERLIQUID_PROXY_BASIS_BLOCK_BPS`, it cannot score human alerts.

6. Broker-session fallback is a watch prompt, not strategy confirmation.
   If recent IBKR provider state is unavailable/degraded and no ES/MES/SPX anchor is live, Hyperliquid may emit `broker_unavailable_proxy_watch` after a large move. This alert means "open the trading device and verify real SPX/SPXW quotes." It cannot trigger wall, gamma, IV, or SPXW strategy conclusions.

7. On-chain smart-money signals are research-only.
   Any alert marked `research_only`, or with `smart`, `wallet`, `onchain`, or `hyperliquid_proxy` in the kind/source gate, is blocked from human notification selection.

8. Codex/Spark confirmation is a delivery gate, not a strategy oracle.
   It can only accept or reject an already selected alert using the local compact JSON payload. It must not invent missing data, mention non-focus symbols, or override freshness/source gates.

## Codex/Spark Prompt Contract

The prompt used for fast alert confirmation must enforce:

- Use only the provided local JSON payload.
- Do not give automated order instructions.
- Human-visible text may mention only SPX, SPXW, ES, option walls, gamma, and IV surface.
- Hidden context may influence delivery, but non-focus symbols must not appear in the explanation.
- `research_only`, stale data, missing data, unknown data, insufficient coverage, and stale IV surface default to no external push.
- Alerts with `source_gate` default to no external push except `broker_unavailable_fallback`, which can only prompt device verification.
- If SPXW option freshness fails, do not base a watch decision on wall, gamma, or IV.
- If ES/SPX anchor is missing, do not treat Hyperliquid or other proxy data as confirmation. It can only justify a degraded "verify on trading device" prompt when broker state is recently unavailable.
- Output must include conclusion, reason, data quality, snapshot time, and SPX/SPXW checks.
- Delivery requires an explicit first-line cue: `需要看盘:`. Suppression requires `不需要推送:`.

## Accepted DeepSeek Review Items

- Add freshness gates before option wall/gamma alerts.
- Add IV surface age gating.
- Add Hyperliquid proxy basis gating against ES/MES/SPX.
- Keep smart-money and wallet signals research-only until validated.
- Force Codex/Spark to reject alerts when data quality is degraded.
- Add source-level degradation monitoring before trusting recovered data feeds.

## Backlog Review Items

These are useful, but not implemented yet:

- Consecutive-good-quote gate for hot 0DTE strikes before they can re-enter wall/gamma calculations.
- Cooldown after Hyperliquid basis returns inside threshold, so one clean sample does not immediately restore scoring.
- Align IV surface snapshot time with the latest ES/SPX anchor before using surface changes in high-severity alerts.
- Track 60-minute provider missing-rate and hold a feed in degraded state until recovery is stable.
- Forward-test wallet cohorts for at least 30 trading days before allowing any smart-money feature into scoring.
- Compare candidate wallets against random active-wallet and whale-only control cohorts.

## Strategy Model Boundary

Human alerts can be produced only by SPX/SPXW-native events with sufficient freshness:

- SPX move or ES-confirmed SPX proxy move during a sensitive window.
- SPXW wall proximity or wall migration after freshness gates pass.
- Gamma regime changes after freshness gates pass.
- IV surface shifts, term gaps, skew steepening, or ATM IV jumps after surface age gates pass.
- Micopedia regime context when it agrees with fresh SPX/SPXW data.

Background-only features:

- VIX, VVIX, SKEW, VIX1D/VIX9D/VIX3M.
- SPY, QQQ, IWM, DIA, HYG/LQD, TLT/IEF, DXY proxies, crude, gold.
- Hyperliquid SPX proxy unless live ES/MES/SPX basis is healthy.
- Broker-unavailable Hyperliquid fallback, except as a degraded verification prompt.
- Polymarket and other prediction markets.
- On-chain smart-money and wallet cohorts.

## Failure Policy

When in doubt, suppress the human alert and preserve the diagnostic record. A missed noisy context signal is acceptable; a stale 0DTE option alert sent to the human is not.
