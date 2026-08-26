# RTH report clock contract

The scheduled SPX status path uses the exchange session as its only RTH
clock. `America/New_York` determines the trading date, daylight-saving
offset, regular close, and early close. Beijing time remains a presentation
field; it does not own report eligibility.

Python continues to record ET quarter-hour snapshots. Human Desk Maps consume
only the `:00` / `:30` projections, so the first GTH notification is `20:30`
rather than the audit-only `20:15` snapshot.

## Schedule and jitter

- `rth_report_schedule()` returns 15-minute boundaries from the session open
  through the last boundary before the actual close. A regular session has
  26 slots (`09:30` through `15:45` ET); a `13:00` early close has 14.
- Human-report acceptance uses the 13 regular-session `:00` / `:30` slots;
  the other quarter-hour rows remain audit snapshots only.
- `rth_report_slot()` and `rth_report_slot_for_session()` accept a timer start
  up to 120 seconds after a boundary. An arbitrary later in-session call is
  not treated as another scheduled report.
- The systemd timer is expressed in `America/New_York`, has
  `AccuracySec=1s`, and covers the full RTH. Calendar and application gates
  still reject holidays and post-close invocations.
- During RTH, quarter-hour snapshots remain available for audit. Human
  notifications are generated only on half-hour boundaries; a thin delivered
  map is marked `rth_heartbeat_degraded_snapshot` rather than hidden.
- The RTH notification identity uses the resolved slot timestamp and slot
  key, so a process retry cannot create another event for the same boundary.
- The pricing audit stores that slot as `occurred_at`/`report_slot_key`, keeps
  the frozen construction clock in `generated_at`, and records completion in
  `persisted_at`; acceptance never infers slot ownership from completion time.

The shared implementation is
`spx_spark.application.order_map.report_clock`. Daily post-close acceptance
must reuse it instead of implementing another tolerance or timezone rule.
When Rust owns half-hour reports, acceptance reads `scheduled_report`
intents from the Rust delivery ledger for slot/delivery coverage and treats
Python `status_snapshot` audit rows as Spring projection inputs only.

## Spring projection and rolling state

The report accepts a Spring latest projection up to five seconds ahead of
the report process clock. This bounded tolerance covers cross-process write
ordering; larger future timestamps fail closed. Every attach or reject writes
`spring_gamma_v3_projection_diagnostic` with a stable reason and observed
age, and the pricing audit persists that diagnostic.

RTH reports also persist and render `spring_gamma_v3_state_window`. Its
schema is `spring_gamma_v3_state_window.v1` and includes:

- `window_start`, `window_end`, and `window_minutes`;
- `sample_count`, `states`, and per-state `counts`;
- total and per-state five-minute slot counts;
- `latest_state` and `latest_state_as_of`;
- source, future-skew observation, and explicit no-authority fields.

Membership uses the precise causal interval
`window_start < min(prediction_as_of, report_now) <= report_now`. Minute and
five-minute buckets are used only to summarize observations, never to decide
whether an observation belongs to the window. NBBO prices are not
interpolated or synthesized by this report path.
