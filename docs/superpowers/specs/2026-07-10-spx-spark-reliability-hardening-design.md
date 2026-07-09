# SPX Spark reliability hardening design

Status: approved in chat on 2026-07-10. This document fixes the approved
architecture before implementation.

## 1. Scope and evidence

This batch addresses one connected reliability problem across five paths:

1. SPXW position PnL and position-event delivery.
2. Quote freshness, especially delayed and delayed-frozen feeds.
3. IBKR slow polling and SPXW ATM subscription re-planning.
4. Hyperliquid SP500 as an off-hours research reference without letting it
   become an actionable pricing anchor.
5. Trading-day rollover, post-close completeness, tests, Ruff, and CI.

The current production baseline is commit `e927694`. The relevant observed
failures are:

- A two-contract long with `avgCost=3200` and mark `25` is reported as
  `+1800` instead of `-1400` because average cost is subtracted once rather
  than once per contract.
- Position state is persisted while alerts are being evaluated, before any
  notification succeeds. A failed or disabled notification therefore consumes
  open, close, quantity-change, and PnL events.
- Delayed quotes never age to stale and remain eligible in several
  time-sensitive paths.
- Nineteen slow labels in chunks of six with a ten-second hold block the only
  stream loop for roughly forty seconds every cycle.
- Live logs show repeated ATM plans around 7480, 7510, and 7545. One direct
  cause is `ES.close - SPX.close` computed from mismatched sessions:
  `7588.75 - 7482.71 = 106.04`, which incorrectly maps a live ES value near
  7587.5 to an SPX reference near 7481.5.
- Re-planning cancels the complete option subscription set before rebuilding
  it, increasing coverage gaps and IBKR pacing pressure.
- The 2026-07-08 post-close report is marked complete even though its final
  front-expiry IV and gamma coverage are only about 28%.
- The current research expiry rolls at 16:15 ET, skips weekends only, and
  disagrees with post-close readiness and other weekday-only calendar logic.
- The current quality baseline is one failing test out of 360 and one Ruff
  F401 error.

## 2. Goals

- Correct PnL for any signed quantity and valid SPXW multiplier.
- Give position events at-least-once delivery semantics with deterministic
  event IDs and per-event acknowledgement.
- Keep recent delayed data available as explicitly degraded research context,
  while preventing delayed, stale, or unanchored data from driving
  time-sensitive alerts or actionable pricing.
- Prevent source ping-pong and unnecessary subscription rebuilds without
  hiding a real sustained market move.
- Keep the hot SPX/SPXW flush cadence independent of slow polling.
- Preserve Hyperliquid's off-hours research value.
- Switch the research 0DTE at 17:00:00 America/New_York to the next valid US
  trading day.
- Make `complete` mean that the post-close dataset has adequate coverage,
  recency, breadth, and quality.
- End with green pytest, Ruff, workflow CI, replay tests, and a production
  shadow soak.

## 3. Non-goals and safety boundary

- No order placement, cancellation, execution, or automatic account action.
- No full asyncio/event-bus rewrite.
- No SQLite or external queue; this remains a single-host, file-backed system.
- No broader IBKR account subscription than the existing explicitly enabled,
  read-only position watcher.
- No use of Hyperliquid as the sole source for executable limits, touch
  probabilities, or model repricing.
- No secrets or account identifiers in logs, fixtures, reports, or commits.

Position monitoring remains opt-in, uses its own client ID, and connects with
`readonly=True` plus `StartupFetch.POSITIONS`. Documentation will distinguish
this explicitly authorized watcher from the market-data-only collectors.

## 4. Architecture

The selected design is a set of small domain controllers coordinated by the
existing synchronous service loops:

- `PositionEventStore`: durable observation state and pending position events.
- `QuoteUseDecision`: one central decision for research, alert, and pricing use.
- `AtmReferenceController`: source provenance, valid ES basis, and stable ATM.
- `OptionReplanController`: hysteresis, confirmations, cooldown, and plan diff.
- `SlowPollScheduler`: cooperative `idle/holding` state machine.
- `SpotResolution`: separate research reference from pricing reference.
- `MarketCalendar`: trading sessions, holidays, early closes, and research
  expiry.
- `ReviewCompletenessPolicy`: structured report checks.

These are pure or nearly pure components. IBKR connection and subscription
calls remain in `StreamCollector`; notification transport remains in the
notifier package; report rendering remains in `post_close_review.py`.

## 5. Position snapshot and PnL

### 5.1 Snapshot version 2

The watcher writes a versioned snapshot with:

- `schema_version=2` and a deterministic `snapshot_id`.
- `fetched_at` and `fetch_complete`.
- managed-account count, raw broker-position count, and filtered SPXW count.
- for each leg: signed quantity, broker average cost, resolved multiplier,
  mark, mark source, mark quality, source quote time, last observed update time,
  and mark age.

`fetch_complete=true` means the read-only connection completed and the broker
position list was obtained without an exception. A successful complete empty
snapshot is different from a failed or incomplete empty snapshot.

Files containing account state are written atomically with mode `0600`. The
writer sets the mode on both the temporary and final path.

### 5.2 PnL formula

For the IBKR option position convention, `avgCost` is cost per contract,
including the multiplier, and `qty` is the separate signed quantity:

~~~text
unit_mark_value = mark * multiplier
unrealized_pnl = qty * (unit_mark_value - avg_cost)
cost_basis = abs(qty * avg_cost)
unrealized_pnl_pct = unrealized_pnl / cost_basis * 100
~~~

Book cost is `sum(abs(qty * avg_cost))` for legs with an accepted mark.
Multiplier comes from the contract. An invalid or missing multiplier falls
back to 100 only for a verified SPXW option.

Structural position events do not require a market mark. PnL events require a
fresh, actionable option mark. A delayed, stale, missing, or unknown mark may
be displayed as degraded reference data but cannot emit a `quality=live` PnL
alert.

A book-PnL event requires fresh, actionable marks for every non-zero SPXW leg.
The snapshot records `priced_leg_count`, `total_leg_count`, and
`book_pnl_complete`. When coverage is incomplete, numerator and denominator
are calculated only for the same priced-leg subset and labeled partial for
display; no book-PnL event is created.

## 6. Durable position-event flow

### 6.1 State model

`PositionEventStore` is a locked, atomically replaced, mode-0600 JSON file:

~~~text
schema_version
observed_snapshot_id
observed_at
observed_positions
pending_events[]
last_acknowledged_book_pnl
updated_at
~~~

Each pending event contains a deterministic `event_id`, snapshot ID, kind,
instrument, old and new quantities or PnL bucket, and creation time. Event IDs
are stable across retries.

Structural events remain ordered and are never coalesced. While a book-PnL
event is still pending, a newer snapshot coalesces it to the latest qualifying
bucket and severity relative to `last_acknowledged_book_pnl`. This prevents one
unsent PnL event per poll while preserving the latest actionable loss or gain.

### 6.2 Transaction order

1. Load the event store and notifier sent state.
2. Before rendering or sending, reconcile any pending event IDs already
   persisted as acknowledged by the notifier.
3. Load and validate the current snapshot.
4. Reject an incomplete, stale, future-dated, or non-monotonic new snapshot
   for event derivation.
5. Compare a valid new snapshot with `observed_positions`.
6. Atomically append new events and advance the observed snapshot in the same
   file replacement.
7. Render every remaining pending event as an alert carrying its `event_id`.
8. Send notifications.
9. Persist successful event IDs in notifier sent state and return them in
   `NotificationResult.acknowledged_event_ids`.
10. Reconcile those IDs into `PositionEventStore` and remove only the matching
   pending events. Update `last_acknowledged_book_pnl` only when its PnL event
   is acknowledged.

At least one real human sink, currently Feishu or Bark, must succeed before a
position event is acknowledged. `--no-notify`, no enabled sink, policy
filtering, and all-sink failure leave the event pending.

The notifier sent-state record also stores acknowledged event IDs. If a
process exits after notifier persistence but before outbox reconciliation, the
next run performs step 2 and removes the pending event without sending it
again. A process exit after transport success but before notifier persistence
can still duplicate a message; that is the accepted at-least-once boundary.

Position PnL changes are measured from the last acknowledged PnL, not the last
observed minute, so a series of small losses cannot continuously move the
baseline and avoid the configured cumulative threshold.

All read-modify-write operations take an advisory file lock before loading the
state and hold it through the atomic replacement, matching the repository's
existing latest-state concurrency pattern.

### 6.3 Freshness and corruption behavior

The default maximum position-snapshot age is
`max(3 * poll_interval_seconds, 180 seconds)` and is configurable. A duplicate
snapshot may replay pending events but does not derive new ones.

Snapshot rejection affects only derivation of new events. Existing pending
events are still reconciled and retried even when the current snapshot is
missing, stale, incomplete, or non-monotonic.

Invalid event-store JSON fails closed: it emits an operations error, derives no
open or close events, and does not replace the file with an empty baseline.
Version-1 state migrates by treating its previous quantities and book PnL as
the initial observed and acknowledged baseline, preventing false historical
open alerts at deployment.

## 7. Quote freshness and use policy

### 7.1 Two independent facts

Feed mode and freshness are not the same:

- feed mode: live, frozen, delayed, or delayed-frozen;
- transport freshness: how recently the source observation advanced.

`Quote` gains an optional `last_update_at`. For persistent IBKR subscriptions,
`snapshot_rows` updates it only when ticker time or a material ticker
fingerprint advances; writing the same cached row again does not refresh it.
This lets a naturally 15-minute-delayed feed remain transport-fresh while it
continues to update.

The normalized fingerprint contains ticker time, bid, ask, last, market price,
close, bid/ask/last sizes, volume, open interest, model IV, delta, gamma, and
model underlier price. The first valid observation sets `last_update_at`.
Later exact equality of all normalized fields does not advance it. Every
comparison uses timezone-aware UTC timestamps and cleaned finite values.

Legacy rows without `last_update_at` remain readable but cannot be proven
actionable. Live rows may fall back to source quote time for freshness;
delayed legacy rows are research-only with freshness marked unknown.

### 7.2 Central decision

A single helper returns:

~~~text
feed_mode
freshness = fresh | stale | unknown
research_usable
alert_allowed
pricing_allowed
reason
~~~

Policy:

- Fresh live data may be used for research, alerts, and pricing.
- Fresh frozen data may be used only where the caller explicitly permits the
  relevant closed-session reference.
- Fresh delayed and delayed-frozen data may be used as labeled research
  context only.
- Stale, missing, error, and unknown data cannot drive alerts or pricing.
- A delayed feed that stops advancing beyond its configured transport
  threshold becomes stale regardless of its market-data type.

Default transport thresholds are 15 seconds for hot instruments, 60 seconds
for delayed or delayed-frozen research feeds, and 300 seconds for configured
slow labels. Environment settings may override each value; a slow-label
threshold wins over the feed-mode default. A quote is fresh while
`as_of - last_update_at <= threshold` and stale when it is greater. A timestamp
more than five seconds in the future produces unknown freshness and fails
closed.

`alert_engine`, `market_context`, `human_focus`, `order_map`, and the position
watcher consume this helper instead of maintaining divergent bad-quality
sets. The existing options-map delayed-data exclusion remains enforced.

## 8. ATM reference and option-plan stability

### 8.1 Reference provenance

`AtmReferenceController` returns a structured candidate:

~~~text
value
rounded_strike
source
observed_at
freshness
basis_value
basis_as_of
basis_contract
reason
~~~

Source policy:

1. During RTH, fresh SPX is authoritative.
2. Outside RTH, a fresh cash-level IBUS500 quote is preferred when available.
3. ES may be basis-adjusted only with persisted basis evidence observed while
   SPX and the same ES contract were simultaneously fresh during RTH.
4. Fresh SPY times ten is the next fallback.
5. The last stable ATM may be reused for an expiry-only rollover.
6. A stale SPX close is allowed only for a one-time bootstrap when no
   controller state or fresh proxy exists; it cannot cause subsequent
   source-driven re-plans.

The current `ES.close - SPX.close` shortcut is removed. Basis samples are
accepted only during RTH when SPX and the same ES contract are fresh and their
source observation timestamps differ by no more than five seconds. The
controller keeps a rolling five-minute sample window, requires at least five
samples spanning at least 30 seconds, rejects absolute basis above 120 points,
rejects a new sample more than 15 points from the current median, and persists
the median rather than a single tick. Valid basis state contains ES contract
month, trading date, sample window, count, median, and observation time. It is
invalidated on an ES contract-month change and expires after three US trading
days. Fresh SPX immediately supersedes it during RTH.

Controller state is persisted atomically with mode `0600` so a service restart
does not lose the valid basis or last stable ATM.

### 8.2 Re-plan controller

Default policy:

- trigger band: accepted ATM differs from plan ATM by at least 20 points;
- re-arm band: difference returns to at most 10 points;
- normal confirmation: the same rounded ATM and source for at least three
  observations spanning at least 15 seconds;
- source grace: retain the current source for a transient loss of freshness up
  to 30 seconds;
- minimum interval between normal rebuilds: 120 seconds;
- emergency movement: at least 40 points with two consistent observations;
- expiry change: immediate and exempt from the cooldown.

The explicit state machine is:

- `steady`: no active candidate; a difference of at least 20 points starts
  `pending`.
- `pending`: the same rounded ATM and source accumulate confirmations. A
  source or rounded-strike change resets the confirmation window; a return to
  at most 10 points cancels it and returns to `steady`.
- `cooldown`: entered after a successful re-plan. Normal candidates are
  ignored for 120 seconds, then begin a new confirmation window.

A 40-point emergency candidate from the same source may bypass the 120-second
cooldown after two observations spanning at least five seconds, but a hard
30-second minimum remains between any two market-movement re-plans. Expiry
rollover bypasses both cooldowns. `accepted` reference and source change only
after a successful initial plan or re-plan, never merely because a raw
candidate was observed.

An expiry change still occurs when the current raw ATM is absent: the last
stable ATM is carried into the new expiry. It occurs exactly once for the new
plan key.

Every decision log records raw and accepted references, provenance, pending
confirmation count, basis evidence, decision reason, and the numbers of
retained, added, and removed contracts.

### 8.3 Subscription reconciliation

Re-planning uses contract-set differences:

1. Retain the intersection of the old and new hot set.
2. Release obsolete rotation or far-tail contracts only as needed to create
   line capacity.
3. Subscribe and qualify added hot contracts.
4. Remove remaining obsolete hot contracts only after replacements succeed.
5. Rebuild rotation slices from the remaining budget.

If a new subscription fails, retained coverage stays active and the controller
backs off instead of repeatedly rebuilding. SPY has an independent plan key
and is not rebuilt when its expiry and rounded ATM are unchanged.

The collector uses one supported synchronization style for contract
qualification; it must not create an unawaited `qualifyContractsAsync`
coroutine.

The option cache retains every expiry allowed by the active sampling plan,
including the next research expiry, until TTL or an actual expiry roll removes
it.

## 9. Cooperative slow polling

`SlowPollScheduler` has two states:

- `idle`: no temporary subscription;
- `holding`: one chunk is subscribed until its hold deadline.

It is advanced once per normal stream iteration:

1. When idle and due, subscribe one chunk and record its deadline.
2. Continue normal five-second hot flushes.
3. When holding and the deadline has elapsed, snapshot, cancel, and cache the
   chunk without sleeping.
4. Schedule the next chunk.

Chunks are spread evenly across the configured cycle; with nineteen labels,
chunk size six, and a 300-second cycle, one of four chunks starts roughly every
75 seconds. Contracts are qualified once per IBKR session and reused in later
cycles; reconnecting starts a new qualification cache.

A chunk error cancels only that temporary chunk, records a degraded slow-lane
event, and retries with backoff. It does not block or tear down the hot stream
unless the underlying IBKR session itself is lost.

## 10. Hyperliquid research versus actionable pricing

`resolve_spx_spot` is replaced by a `SpotResolution`:

~~~text
research_price
research_source
pricing_price
pricing_source
pricing_allowed
gate_state
reason
divergence_bps
~~~

The research reference may be Hyperliquid outside cash hours. The pricing
reference must pass the existing market-context anchor and basis gates and
must come from actionable TradFi or option-chain evidence.

When Hyperliquid is the only usable reference, or the state is `unanchored`,
`basis_warn`, or `basis_blocked`:

- the payload remains valid and explicitly `research_only=true`;
- it may show HL price, directional context, wall distances, scenario strikes,
  and observed option bid/ask;
- `pricing_allowed=false`;
- executable limit fields, model repricing, touch probability, and touch ETA
  are null and are not phrased as recommendations.

When a valid TradFi or chain anchor exists, candidates and limit calculations
use only `pricing_price`. Hyperliquid remains a confirmation or divergence
field and cannot replace that price.

The order-map payload exposes separate `research_reference` and
`pricing_reference` fields. When `pricing_allowed=false`, compatibility
`underlier.price` and `underlier.source` are null, legacy `candidates` and
`wall_ladder` are empty, and every other executable numeric alias is null.
HL-derived scenario strikes, observed quotes, and wall distances live only in
`research_candidates` and `research_wall_ladder`. All candidate selection,
repricing, probability, ETA, and day-move functions require a
`pricing_allowed` resolution before calculation; this is a model-layer gate,
not a renderer label. A legitimate research-only map is not treated as a thin
or failed payload. Periodic off-hours research status may still render the new
research fields with its research-only label; it does not become a direct
actionable alert.

## 11. Unified market calendar and 17:00 rollover

`market_calendar.py` becomes the only source for:

- US trading-day checks;
- observed full-day holidays;
- Good Friday and Juneteenth;
- standard 09:30-16:00 ET sessions;
- scheduled 13:00 ET early closes;
- previous and next trading day;
- RTH-open checks;
- completed review date;
- current and next research expiry.

The implementation uses deterministic calendar rules plus explicit overrides
for exceptional closures. It introduces no large market-calendar dependency.

Research-expiry rule:

- a trading day before 17:00:00 ET uses that date;
- at or after 17:00:00 ET it uses the next trading day;
- a weekend or full-day holiday always uses the next trading day;
- the second expiry is the trading day after the research expiry.

Early close changes the session window and report coverage denominator, but
the user-approved research rollover remains fixed at 17:00 ET.

`default_spxw_expiry` remains as a compatibility wrapper. Sampling,
stream-collector expiry roll, options map, order map, alert profile,
cash-session checks, and post-close review all delegate to the calendar.

Post-close readiness becomes 17:00 ET. The timer runs at 17:15
`America/New_York` on weekdays, independent of server timezone and daylight
saving time. The application still checks holidays and report identity so a
holiday timer cannot resend the previous report.

## 12. Post-close completeness

`ReviewCompletenessPolicy` evaluates structured checks. Each check records its
measured value, threshold, pass/fail result, and reason. Status remains
backward-compatible:

- `complete` only when every required check passes;
- `degraded` otherwise.

Default required checks for the actual session length:

- SPX and ES each cover at least 90% of expected five-minute RTH buckets.
- First usable SPX and ES observations are within 15 minutes of session open.
- Last usable SPX and ES observations are within 15 minutes of actual close.
- SPX and ES live ratios are at least 95%.
- The trading-date SPXW expiry has at least 20 unique contracts, ten strikes,
  both calls and puts, and a strike span of at least 50 points.
- At least 90% of option rows are usable live/frozen RTH observations.
- Option IV coverage is at least 80%, and a front-expiry option observation
  exists within 15 minutes of close.
- The front-expiry IV surface covers at least 60% of expected five-minute
  buckets and ends within 15 minutes of close.
- The latest front-expiry IV and gamma coverage ratios are each at least 50%.

Thresholds are represented by a policy object and may be overridden through
validated environment settings. Report JSON and Markdown show the checks and
warnings. The one-row synthetic case and the current 2026-07-08 low-coverage
case must be degraded.

## 13. Migration and deployment behavior

- Existing quote JSON remains readable because `last_update_at` is optional.
- Version-1 position state migrates without generating historical events.
- Missing ATM controller state causes one initial plan and then normal
  stabilization.
- The unrelated untracked `earlyoom_1.7-2_arm64.deb` remains untouched.
- Code is implemented and tested in an isolated worktree before integration.
- After static and replay verification, relevant user services are restarted
  in a controlled order. The live soak is read-only and places no orders.

## 14. Test and verification design

Implementation is failure-first and test-driven.

### Position tests

- Quantities `+2` and `-2` with `avgCost=3200` and mark `25` produce
  `-1400` and `+1400`.
- Book cost multiplies every leg by absolute quantity.
- Contract multiplier and SPXW-only fallback are covered.
- Complete empty snapshots close positions; incomplete or stale empty
  snapshots do not.
- Sink failure and `--no-notify` retain the same pending event ID.
- One successful sink acknowledges only delivered event IDs.
- Restart reconciliation handles notifier-persisted but outbox-pending events.
- Stale or delayed marks cannot emit live PnL alerts.
- Snapshot and event files are mode `0600`.

### Freshness tests

- A delayed quote whose source timestamp is naturally 15 minutes old but whose
  source observation advances remains fresh research-only data.
- The same feed becomes stale after transport updates stop.
- Delayed data cannot trigger movement alerts, actionable option pricing, or
  position PnL alerts.
- Existing options-map delayed exclusion remains green.

### ATM and stream tests

- Replay `7480 -> 7510 -> 7480 -> 7510 -> 7545 -> 7480` without repeated
  re-plans.
- The mismatched-close evidence cannot generate the erroneous 7480
  basis-adjusted reference.
- A sustained same-source move triggers exactly one re-plan.
- A transient 30-second source outage triggers no re-plan.
- A 17:00 expiry roll with no raw ATM uses the stable ATM exactly once.
- Subscription diff retains overlap and survives partial add failure.
- An unchanged SPY plan is not re-qualified.
- Slow-poll fake-clock tests contain no ten-second blocking sleep, eventually
  cover all labels, and preserve hot flush cadence.
- No test or live log contains an unawaited qualification coroutine warning.

### Calendar and report tests

- Thursday 16:59 ET uses Thursday; 17:00 uses Friday.
- Friday 17:00 uses Monday.
- 2026-07-02 17:00 uses 2026-07-06, skipping the observed July 3 holiday and
  weekend.
- Good Friday, Thanksgiving, cross-year observed holidays, early closes, and
  DST timer behavior are covered.
- The one-row report fixture is degraded.
- The current 2026-07-08 evidence is degraded because front IV/gamma coverage
  is below 50%.
- A full-session, broad, high-quality fixture remains complete.

### Quality gates

The final local-equivalent CI commands are:

~~~text
uv run ruff check .
uv run pytest -q
systemd-analyze verify systemd/*.service systemd/*.timer
~~~

A GitHub Actions workflow on Python 3.12 runs Ruff and pytest. Calendar timer
syntax is verified separately on the Oracle host.

Production acceptance includes a 60-minute read-only stream soak:

- no source ping-pong re-plan;
- every re-plan has an auditable reason;
- hot flush maximum gap stays within 12 seconds under slow polling;
- no new IBKR pacing, session, or unawaited-coroutine error;
- position-event dry-run proves pending retention, stable event IDs, and
  replay without exposing account identifiers.

Per-event acknowledgement is proven by an integration test with deterministic
fake Feishu/Bark success and failure results. A production dry-run never
acknowledges an event. A controlled real-sink test is optional and is performed
only when explicitly enabled for deployment verification.

## 15. Completion criteria

The batch is complete only when every requirement above has direct evidence:

- position math and event reliability tests pass;
- freshness and actionability gates pass;
- ATM replay, option-plan diff, and slow-poll tests pass;
- Hyperliquid remains useful as off-hours research but cannot become the sole
  actionable pricing anchor;
- 17:00 ET and all calendar consumers agree;
- post-close completeness checks correctly classify sparse and low-quality
  days;
- pytest, Ruff, workflow CI, systemd verification, replay checks, and the live
  soak are green.
