# MrMicopedia Agent Guidance

Date: 2026-07-05

## Purpose

This document turns the local MrMicopedia SPX/0DTE research into an agent-readable
guidance layer for SPX Spark. It is observational only. It does not place orders,
recommend trades, or bypass the existing "no automatic order placement" project
boundary.

Read `docs/micopedia-background-knowledge.md` before using this guidance. That
file defines the SPX/0DTE, gamma, wall, spread, regime, and validation vocabulary
that this signal layer assumes.

The guidance belongs after normalized market data and before alert/dashboard
explanation:

```text
ProviderSnapshot -> LatestState/features -> MicopediaSignal -> alert/explanation/audit
```

## Evidence Status

The distilled framework is useful for thought-structure and checklist generation.
It is not yet a proven edge. Current known gaps from the local research:

- Parent context for replies is incomplete, so context-dependent posts must be
  treated carefully.
- SPX/ES 1-minute historical coverage is incomplete, so MFE, MAE, timing window,
  key-level hit, and threshold claims remain hypotheses.
- Hyperliquid SP500 is useful context, but it is not CME ES and not official SPX.
- The 2026-03 monthly review was low-confidence and should not anchor strong
  conclusions by itself.

## Decision Stack

Use this stack when an agent summarizes the day or creates an alert explanation:

1. Regime first: low vol, high vol, OPEX/gamma pin, event, fiscal/systematic flow,
   holiday liquidity, crowded positioning, or ordinary RTH.
2. Map second: SPX key levels, 0DTE call/put walls, JPM collar zones, VIX1D/IV,
   opening range, VWAP, ES/SPY confirmation, and timing windows.
3. Trigger third: require actual price action at the mapped level before treating
   a thesis as active.
4. Tool fourth: in the mature framework, prefer defined-risk SPX/SPXW vertical
   spreads when a view is expressed. Single-leg options are exception cases.
5. Risk always: protect green quickly, invalidate fast, and avoid undefined
   overnight exposure.
6. Audit after: separate pre-trade thesis, intraday revision, and post-trade
   review.

## Signal Schema

Machine-readable output is described in `docs/micopedia-signal-schema.json`.

Core fields:

- `regime`: current framework mode.
- `directional_bias`: `bullish`, `bearish`, `mixed_tactical`, or
  `neutral_unclear`.
- `map_focus`: what the agent should inspect before explanation.
- `trigger_watchlist`: price-action confirmation conditions.
- `candidate_expression`: observational expression shape, never an order.
- `risk_policy`: guardrails that must be shown before any execution-facing use.
- `data_warnings`: missing data and model-risk caveats.
- `suggested_sampling_mode`: how aggressively SPXW options should be monitored.

## CLI

Build a manual signal:

```bash
scripts/run-micopedia-guidance.sh \
  --underlier 7502 \
  --vix1d 12.5 \
  --gamma-state pin \
  --bias mixed_tactical \
  --event opex,jpm_collar \
  --key-level 7500 \
  --key-level 7525
```

Read missing prices from latest state:

```bash
scripts/run-micopedia-guidance.sh --from-latest-state --time-phase open --event cpi --json
```

## Integration Rules

- A `MicopediaSignal` is not a trading instruction. It is a structured checklist
  and explanation object.
- The alert layer may display it only with its `data_warnings`.
- The execution layer must not consume `candidate_expression` as an order.
- Any future backtest must store the original signal inputs, later returns,
  MFE/MAE, key-level hit, and whether the tweet or alert was pre-trade or review.
- Numeric rules such as VIX1D thresholds, FOMC timing, holiday effects, and
  "knife" setups remain hypotheses until validated with SPX/ES 1-minute data and
  option-chain history.

## Coexistence with Steven

Steven (`docs/steven-framework-integration.md`, `strategy/steven.py`) is a parallel
observe-only guidance stack: regime → map → flow → trigger → expression → exit.
It must not contradict this Micopedia decision stack.

Shared hard rules for agents and LLM writers:

- Both layers are observational checklists, never execution authority.
- House exposure metrics that need dealer-sign or unpublished vendor formulas use
  `_proxy` names (`net_dex_proxy`, `dagex_proxy`, …); never treat them as vendor
  Net DEX / DAGEX.
- Confidence from proxy-driven regime/map is capped at medium.
- Hyperliquid SP500 is research context only; it is not the SPX/ES cash anchor
  and cannot alone confirm a wall break or setup.
- Steven defaults stay off (`steven.enabled=false`,
  `alert_context_enabled=false`) until RTH acceptance.

## Next Work

- Feed current option-chain gamma/OI into `MicopediaInputs.has_option_chain` and a
  future wall map object.
- Add feature rows for VIX1D/VIX9D/VIX, SPX opening range, VWAP distance, ES/SPY
  confirmation, and Hyperliquid context.
- Persist `MicopediaSignal` rows so later market windows can compute return,
  MFE, MAE, and key-level touch outcomes.
