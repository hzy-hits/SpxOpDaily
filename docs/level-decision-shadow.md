# Wall/Flip Level-Decision Shadow

## Purpose

The level-decision shadow prevents the order map from treating a wall or flip
location as an immediate trade trigger. It observes one nearest frozen level at
a time, makes breakout and fade mutually exclusive, and records outcomes without
changing orders or candidate generation. State transitions remain in the
append-only audit. Low-level transition pushes are optional and disabled in the
production profile; promoted `CONFIRMED` signals always deliver independently.

The same machine supports two deployment modes. With
`formal_signal_enabled=false`, every public result carries `mode=shadow` and
`actionable=false`. An explicit operator override may set the flag to `true`;
only `CONFIRMED` then carries `formal_signal=true` and `actionable=true`.
No mode submits an order automatically.

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
