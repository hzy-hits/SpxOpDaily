# Wall/Flip Level-Decision Shadow

## Purpose

The level-decision shadow prevents the order map from treating a wall or flip
location as an immediate trade trigger. It observes one nearest frozen level at
a time, makes breakout and fade mutually exclusive, and records outcomes without
changing orders or candidate generation. State transitions remain in the
append-only audit. Low-level transition pushes are optional and disabled in the
production profile; user-facing execution notification is reserved for the
narrower TradeReady lane.

The same machine supports two deployment modes. With
`formal_signal_enabled=false`, every public result carries `mode=shadow` and
`actionable=false`. An explicit operator override may set the flag to `true`;
only `CONFIRMED` then carries `formal_signal=true` and `actionable=true`.
No mode submits an order automatically.

## Reviewed RTH upside pilot

The explicit override currently exposes one deliberately narrow notification
lane: `long_0dte_rth_upside_breakout_pilot`. It accepts only an RTH
`CONFIRMED` breakout with direction `up`, the current session's exact 0DTE Call,
and a two-sided selected-contract quote whose source and transport ages are at
most 15 seconds. The quote is checked again immediately before enqueue. A
jointly negative ES 1-minute and 5-minute return, an opposite active regime, a
pre-event macro window, an explicitly blocked breakout filter, an expired event,
or a wall drift over 10 points still blocks the ticket. Aggregate-chain L1, SPY
confirmation, price/volume alignment, and the individual 1-minute/5-minute
checks are retained as diagnostics instead of duplicative vetoes. Missing
expected move is also disclosed as a diagnostic; target room must still come
from the outward wall or the documented five-point fallback. The 15-minute time
stop is capped at the exact SPXW expiry-session close.

The 2026-07-25 point-in-time replay through 2026-07-24 uses Call ask entry and
executable bid exit. Its matching RTH upside-breakout **control** contains 8
fills over 4 trading days: 7 wins, gross PnL +$1,930 per one-contract sequence,
and +$660 after removing the best day. This is not an end-to-end replay of every
new live pilot gate. It excludes commissions, explicit slippage, queueing,
partial fills, market impact, and human delay. The deployed rule is therefore
an in-sample canary, not statistically established alpha; the acceptance report
must keep `acceptance_gates_passed=false` until its normal thresholds pass.

Delta, Gamma, the 15-minute Theta scenario, and a three-vol-point IV-crush
scenario are displayed on the ticket when available. They explain
contract/holding risk only; they never reverse or manufacture the ES/wall
direction or select the contract in the current pilot.
`quantity=operator_selected` and `automatic_ordering=false` remain part of every
ticket.

## MA20/MA50 location context

The RTH report and TradeReady ticket now carry closed-bar **5-minute RTH
SMA20/SMA50** as read-only location context. SMA20 is treated as a fast
reclaim/pullback reference and SMA50 as a slower regime/support reference. A
single intrabar touch or cross is not a breakout: the wall/flip lifecycle,
closed-bar acceptance, ATR-sized distance, and retest still provide the actual
trigger. The moving averages do not add another direction score or hard gate
because they substantially overlap VWAP and HH/HL.

ES and cash SPX are not the same price series. The displayed SPX-equivalent
levels use the synchronized current basis:

```text
SPX_MA_proxy = ES_RTH_5m_SMA - current_synchronized_(ES - SPX)_basis
```

This is a coordinate projection, not SPX's own historical moving average; the
exact identity is `MA(SPX) = MA(ES) - MA(basis)`. In the available ten-session
synchronized sample, ES and SPX agreed on which side of SMA20/SMA50 price was
on 97.67%/98.50% of observations. Current-basis projection error had P90
1.07/1.55 points and maxima 3.85/4.11 points, so a projected line within roughly
four SPX points is near-line context rather than proof of a cash-index break.

The small matching upside-control join did not support a bullish-stack veto:
none of its eight events had `price > SMA20 > SMA50`, including seven winners.
That is not evidence against moving averages; it is evidence that imposing the
stack as a hard requirement would delete the whole current sample. ES contract
identity is persisted on each new bar and bar history resets on a known futures
roll so the discontinuity cannot manufacture a moving-average break. Legacy
bars without a verifiable contract identity are not backfilled.

## State machine

```text
FAR
  -> APPROACHING
  -> TESTING
  -> BREAK_PENDING | REJECT_PENDING
  -> ACCEPTED      | REJECTED
  -> RETEST
  -> CONFIRMED
  -> INVALIDATED | EXPIRED
```

- `APPROACHING`: SPX is within the configured distance of the nearest level.
- `TESTING`: SPX is inside the frozen level's test band.
- `BREAK_PENDING`: SPX crossed to the outside of the range.
- `REJECT_PENDING`: SPX moved back toward the inside of the range.
- `ACCEPTED/REJECTED`: the move held and ES confirmed the same direction.
- `RETEST`: price returned to the frozen level after acceptance/rejection.
- `CONFIRMED`: price moved away from the retest and held again.
- `INVALIDATED`: data, structure drift, or the opposite price move broke the thesis.
- `EXPIRED`: a pending phase or the complete event exceeded its deadline.

The tracked levels are `put_wall`, `flip_low`, `flip_high`, and `call_wall`.
The closest eligible level is the only active event. The live options map may
continue to move, but the active level remains frozen; excessive drift
invalidates the event instead of silently moving its threshold.

## Quality gates

The shadow advances during SPX RTH and ES Globex. Runtime health models these as
separate `ready` and `globex_context` modes. During RTH it prefers official SPX.
Outside RTH it projects SPX as live ES minus the persisted, qualified RTH
ES-SPX basis. Provider failover is active in both sessions; Globex health gates
on ES rather than requiring a closed cash-market SPX quote. The shadow requires:

- official `index:SPX`, or live ES with a qualified current-contract RTH basis;
- live OI/GEX structure, or a frozen structure captured from the latest valid chain;
- a live, usable ES quote.

Frozen OI/GEX structure has a trading-session TTL. Its capture session is
persisted with the structure, and an expired structure fails closed with
`frozen_structure_session_ttl_expired` instead of silently driving another
session.

A short quality grace avoids invalidating a watch on one transient read. A
sustained failure records `data_error` and invalidates the active event.

## Persistence

Current state:

```text
data/latest/level_decision_shadow_state.json
```

Append-only transition audit:

```text
data/features/level_decision_audit/date=YYYY-MM-DD/transitions.jsonl
```

Confirmed-event outcomes:

```text
data/features/level_decision_outcomes/date=YYYY-MM-DD/outcomes.jsonl
```

Per-tick RTH/Globex health evidence:

```text
data/features/level_decision_health/date=YYYY-MM-DD/samples.jsonl
```

Bark delivery audit:

```text
data/features/level_decision_delivery/date=YYYY-MM-DD/deliveries.jsonl
```

Transition records classify failures as `level_error`, `data_error`,
`false_break_or_rejection`, or `no_confirmation`. Confirmed events are sampled
at 30, 60, 180, and 300 seconds and classified as `follow_through`,
`false_confirmation`, `no_follow_through`, `mixed_path`, or `data_incomplete`.
Each outcome contains signed SPX return plus directional MFE and MAE.

The production shock path projects only this machine into `level_strategy`.
Legacy `intraday_strategy` wall/flip branches are retained for historical replay
compatibility but do not run in production. A promoted confirmation maps its
final direction to a Call or Put order-map bias and emits a deduplicated formal
Bark signal; order submission remains outside this path.

## Promotion gate

The shadow must not influence candidate order, alert severity, or notification
delivery until all of the following are true:

1. Five complete RTH sessions pass data-quality acceptance.
2. At least 100 testing events and 20 RTH sessions are recorded.
3. Breakout/fade false-confirmation rates are reported by level kind and regime.
4. Proposed rules improve precision without hiding more than five percentage
   points of valid follow-through events.
5. A separate review explicitly changes `actionable=false` behavior.

Build the evidence report with:

```bash
uv run python -m spx_spark.application.order_map.level_decision_acceptance --json
```

The report is written to `data/latest/level_decision_acceptance.json`. Passing
all numeric gates sets `eligible_for_explicit_review=true`. Normally promotion
requires a separate reviewed configuration change after the shadow outcome
review. When an operator explicitly enables the override before those gates,
the report records `promotion_basis=explicit_operator_override` and keeps
`acceptance_gates_passed=false` so the override cannot be mistaken for completed
statistical acceptance.
