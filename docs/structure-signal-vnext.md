# SPX structure-first signal system

Status: implementation contract for the event-driven operator message and
opportunity replay introduced in August 2026.

## Objective

The system must help a human answer five questions quickly:

1. Where is SPX relative to the frozen Put / Flip / Call structure?
2. Which single path is being tested now?
3. What changed since the previous observation?
4. Is there a fresh, executable option candidate with enough remaining room?
5. What ended the opportunity, and is the evidence complete?

It does not promise a profitable or certain market outcome. “Deterministic”
means that the trigger, lifecycle transition, invalidation, expiry, opportunity
identity and data-quality treatment are reproducible from the recorded inputs.
Automatic broker ordering remains disabled.

## Human-facing message types

### Desk Map

The 15-minute timer continues to persist a normalized audit snapshot, but an
unchanged snapshot is not sent to the operator. A Desk Map is sent on a
material structure or execution-state change and at the following low-frequency
RTH checkpoints:

- 09:30, 10:00 and 10:30 America/New_York;
- then 11:30, 12:30, 13:30, 14:30 and 15:30.

The deterministic card includes:

- `Desk View`: one active path or `NO ACTIVE SETUP`;
- `Location`: SPX, ES, active frozen level and distance;
- `Structure`: frozen event levels separately from current live levels;
- `Primary` and `Alternative`: trigger and invalidation path;
- `Targets` and `Execution`;
- `Data Quality`: OI/GEX provenance, density clipping, snapshot consistency and
  market-data degradation.

### Setup Transition

The hot level loop emits a durable, idempotent card only when the lifecycle
changes through an operator-visible phase:

`APPROACHING → TESTING → BREAK_PENDING / REJECT_PENDING → RETEST → CONFIRMED`

Terminal transitions are `INVALIDATED` and `EXPIRED`. A setup-transition card
has no trade authority. `CONFIRMED` still requires the independent exact-leg,
NBBO, freshness, entitlement, target-room and reward/risk gates.

### Trade Ready

One economic opportunity can produce at most one manual-ready alert per
explicit re-entry generation. The card carries:

- stable opportunity identity;
- direction, thesis and trigger level;
- exact SPXW contract and decision-time NBBO;
- maximum entry price and expiry;
- invalidation, target, time stop and maximum premium loss;
- remaining-distance reward/risk and data provenance.

The default RTH threshold is 1.0. It is explicitly
`underlier_remaining_distance`, not an option-economic payoff estimate. Values
below 1.0 become `late_chase_observation` and cannot be labeled ready.

### Exit / Invalidation

The virtual episode is displayed-quote shadow evidence, not a user position.
The system therefore never claims that the operator entered or exited a real
trade.

Missing underlier or option data marks an episode `DEGRADED`; it does not close
the episode. If an invalidation, target, Greek exit or time stop is triggered
without an executable bid, the original exit reason is latched as
`CLOSE_PENDING`. A later price reversal cannot erase it. Bid recovery closes
the episode once. If no bid appears by the bounded lifecycle/session deadline,
the episode becomes `CENSORED` with no fabricated PnL or exit notification.

## Opportunity identity and monotonic re-arm

The semantic identity is:

```text
session + thesis/play + frozen trigger level + exact contract
```

Repeated evaluations, delivery retries and virtual observations are occurrences
of the same opportunity. Expiry or invalidation cancels the current delivery
occurrence but retains the semantic claim. A second alert is legal only after
the level machine records that price genuinely left the reset band and advances
the explicit `reentry_generation`.

## Opportunity replay artifact

The post-close replay writes `opportunities.jsonl` alongside the normal
backtest artifact. Each row joins:

- all signal occurrences;
- durable delivery events;
- one virtual episode and its event history;
- 0 / 5 / 10 / 20 / 30 second entry-latency replays.

Execution semantics are deliberately conservative:

- single leg: long ask entry, long bid exit;
- vertical: long ask minus short bid entry, long bid minus short ask exit;
- both vertical legs must use one coherent provider;
- status is `quote_reached` or `not_reached`, never `filled`;
- commission is charged per contract side;
- slippage sensitivity is 0 / 0.05 / 0.10 / 0.20 option points per leg per
  side, where 1 point is USD 100 per contract. Single-leg total slippage is
  `2 × s`; vertical total slippage is `4 × s`.

The artifact is evidence for forward comparison, not proof of queue priority,
partial fills, market impact or an out-of-sample edge.

## Cross-index HMM research boundary

The research producer consumes causal normalized SPX, NDX, DJI and RUT
observations, current cross-index relative strength / breadth / dispersion, and
the prior RTH four-index context. Missing indices and source skew remain
explicit.

The output contract contains:

- causal filtered HMM posterior, entropy and update sequence;
- experimental same-day high, low and close range buckets when inputs exist;
- explicit unavailable forecasts with reason codes when they do not;
- close-location bucket probabilities;
- lineage, observation time, availability time and data quality.

The fixed bootstrap state labels are research hints, not verified economic
semantics and never “market-maker behavior.” Every forecast remains
`bootstrap_unvalidated`, `advisory`, `action_authority=none` and
`automatic_ordering=false`. HMM output can iterate immediately in shadow but
cannot independently create or promote a Trade Ready event.

## Production acceptance

Code completion is not end-to-end acceptance. After deployment verify:

1. affected services have one writer, stable restart counts and fresh inputs;
2. unchanged 15-minute snapshots are persisted but absent from the human
   delivery outbox;
3. setup-transition and Trade Ready event IDs are idempotent across retries;
4. an opportunity cannot regress or re-alert without a new generation;
5. `DEGRADED`, `CLOSE_PENDING` and `CENSORED` appear correctly in replay;
6. the cross-index document contains all four observations or explicit
   missing reasons and has no execution authority;
7. outbox targets reach terminal receipts rather than merely being accepted.

