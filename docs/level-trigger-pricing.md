# Wall/Flip Realtime Repricing

## Runtime chain

```text
RealtimeEngine tick
  -> level_decision_shadow
  -> TriggerCoordinate resolution
  -> level_trigger_repricing
  -> execution quote gate
  -> parity-forward Black-76 scenarios
  -> pricing_outcomes
  -> empirical touch-time calibration
```

The 15-minute order map is a planning artifact. It does not place or preserve a static
option limit. Active `TESTING`, pending, accepted/rejected, `RETEST`, and `CONFIRMED`
events are repriced on every realtime-engine cycle.

## Coordinate contract

| Session/data state | Observed instrument | Trigger level |
| --- | --- | --- |
| RTH with actionable SPX | `index:SPX` | SPX wall/flip level |
| GTH with actionable SPXW pairs | `synthetic:SPXW_PARITY` | SPX wall/flip level |
| SPX coordinate unavailable, qualified basis available | `future:ES` | SPX level + ES-SPX basis |

`TriggerCoordinate` transforms both the observed value and every key level. A coordinate
change invalidates an active event and forces a re-arm. Public state keeps `level` on the
SPX display scale and exposes the actual `trigger_level`, `trigger_value`, instrument,
and basis separately.

## Quote gate

An exact conditional price is emitted only when all checks pass:

- actionable live quote with positive bid and ask;
- absolute and relative spread limits;
- same-expiry/right spread percentile;
- local transport age and provider source age;
- IBKR/Schwab mid divergence when both sources are present.

Failure changes `execution_quote_status` to `range_only`. The candidate retains an
early/late scenario range for risk context, while `projected_mid`, `limit_aggressive`,
and `limit_conservative` are null.

## Pricing model

The model derives the current forward from near-ATM put-call parity, fits a weighted
local quadratic IV surface, and prices the fixed strike with Black-76. The result remains
ratio-anchored to the current market mid. It records:

- parity forward now and at touch;
- smoothed IV now and at touch;
- time remaining now and at touch;
- early, base, and late touch prices;
- pricing kernel and quote-gate diagnostics.

Until empirical calibration is ready, touch time uses the existing Brownian distance/EM
heuristic. Completed outcomes are grouped by distance/EM, session bucket, volatility
regime, and trend regime. A cohort is not activated until it has at least 20 touched
events across five sessions.

## Outcome records

Open state:

`latest/level_trigger_pricing_outcomes.json`

Completed records:

`features/pricing_outcomes/date=YYYY-MM-DD/outcomes.jsonl`

Each completed record includes first touch time, actual touch mid, model error, whether
the option ask crossed the reference before underlier touch, and 1/5/15-minute return,
MFE, and MAE. Incomplete untouched records expire after six hours.

## Acceptance

1. RTH tests select official SPX; GTH tests select chain parity; ES fallback transforms
   both the observation and the target.
2. Wide, stale, one-sided, or provider-divergent quotes never emit a conditional limit.
3. Every active level event writes `latest/level_trigger_repricing.json` and an append-only
   audit record without waiting for the next report.
4. Pricing output identifies `black76_parity_forward` and contains ordered early/base/late
   scenario bounds.
5. A synthetic touch automatically produces touch error, prefill attribution, and all
   three MFE/MAE horizons.
6. Touch calibration remains `collecting_outcomes` below the sample/session threshold and
   automatically switches to `empirical_first_touch_cohort` after the threshold.
