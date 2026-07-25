# RTH runtime clock and end-to-end acceptance

## One session clock

Every RTH component derives session ownership and boundaries from
`MarketCalendar` in `America/New_York`. Beijing time is presentation only; it
must not decide whether an RTH task is due.

The persisted timestamp roles are:

- `source_at` / quote time: when the provider observed the market value;
- `as_of`: the latest market information included in a derived frame;
- `generated_at`: the report's frozen construction clock;
- `persisted_at`: when the process finished writing the audit record;
- `occurred_at`: the canonical exchange-local report slot used for idempotent
  notification identity.

All persisted values are timezone-aware ISO-8601 timestamps. Comparisons are
performed as absolute instants; ET is used only to derive the trading date,
expiry, session segment and scheduled slot.

## Causal boundaries

The 5-minute state model needs six closed 5-minute bars for 30-minute
efficiency and structure inputs. On a regular day the earliest causal 8/8
state therefore follows the 10:00 ET bar close; 09:45 ET is not a valid 8/8
acceptance target.

Spring Gamma produces one shadow observation per minute. The 15-minute report
summarizes durable Spring observations in its preceding 15-minute window, so a
short `TREND_UP`, `TREND_DOWN` or range state cannot disappear merely because
the report instant lands on `UNCERTAIN`.

The report clock is defined by
`application/order_map/report_clock.py`:

- regular RTH: 26 slots, 09:30 through 15:45 ET;
- scheduled 13:00 early close: 14 slots, 09:30 through 12:45 ET;
- service-manager start grace: at most 120 seconds after the slot;
- a second invocation in the same slot is deduplicated.

RTH slots are heartbeats. `no_material_changes` and a temporarily thin
snapshot cannot suppress them; a thin heartbeat is delivered with an explicit
degraded warning.

## Cross-process projection

The report accepts a Spring projection up to five seconds ahead of the
report's frozen construction clock. This tolerance covers atomic file writes
completed by the minute worker while the report is being assembled. Larger
future skew, stale data, expiry mismatch, session mismatch and invalid segment
remain fail-closed.

Every decision is persisted in
`spring_gamma_v3_projection_diagnostic` with a machine-readable reason. The
report audit also stores `spring_gamma_v3_state_window`, including sample and
5-minute-slot counts, state counts, latest state and maximum future skew.

## Option quote time

NBBO is never interpolated. A merged option leg preserves its provider
last-change source time and its independent provider-observation time.
Freshness is evaluated from that exact field's observation time, while a
future source clock still fails closed. Core live strikes and rotation-cache
strikes are reported separately. IV or structure smoothing may be used only
after the underlying real quote coverage and age are disclosed.

IV, Delta, Gamma, Vanna and Charm are cleared independently on every leg that
does not pass `analytical_allowed`; OI and volume may remain for provenance but
cannot make that leg a complete analytical pair. Spring evaluates the nearest
61 strikes, requires at least 13 accepted C/P pairs in production, and
publishes progress against the 61-pair density target. The target is
diagnostic rather than a 61-pair hard gate.

The final five minutes before 0DTE expiry retain the existing Greek/IV
fail-closed guard. That policy must not be mistaken for a collection outage.

## IBKR competing sessions

IBKR error 10197 opens a non-invasive circuit breaker. Probe delays grow
exponentially from the configured initial delay to a 300-second ceiling. The
circuit closes only after a flush contains genuinely fresh LIVE data; a TCP
reconnect or stale cached quote is insufficient. The collector never
disconnects or takes over a phone, desktop or other live session.

`latest/ibkr_stream_health.json` separates process state from data-plane
health and publishes `policy_blocked`, reason, retry time, circuit state and
conflict count. Its `observed_at`, `max_age_seconds` and `expires_at` prevent a
dead process's last `process_active=true` observation from remaining trusted.

## Daily hard acceptance

At 17:30 ET on each trading day,
`spx-spark-rth-daily-acceptance.timer` writes:

- `reports/rth_daily_acceptance/date=YYYY-MM-DD/acceptance.json`;
- `latest/rth_daily_acceptance.json`;
- a freshly recomputed `latest/level_decision_acceptance.json`.

The operational verdict checks:

- at least 95% of expected Spring RTH minute slots;
- at least 75% ready option overlays;
- 100% of expected RTH report and delivery slots;
- at least 95% Spring projection and rolling-window attachment;
- the post-close raw market-data completeness verdict;
- notification Outbox `quick_check=ok`, rollback journal mode, zero pending or
  claimed targets, zero unacknowledged dead letters and no unknown status;
- formal level-decision authority is either disabled or backed by passed
  statistical acceptance gates.

A historical replay from an intermediate quote-clock implementation is not a
daily acceptance result. In particular, the 2026-07-24 `269/390` replay
predated the final per-leg fail-closed and 13-pair production contract and was
not persisted as a versioned replay artifact. The first full forward
acceptance for this contract is 2026-07-27 RTH, with its verdict scheduled at
17:30 ET. See
[SPXW raw → merge → feature clock contract](spxw-option-clock-contract.md)
for the evidence boundary and exact acceptance targets.

A report slot counts as delivered only when its persisted Outbox event has at
least one target and every target is `delivered`; the legacy
`delivered_ok=any_sink` audit flag cannot satisfy this gate.

A degraded verdict exits non-zero for systemd visibility and queues one
idempotent `ops_transition` alert. Statistical level-decision promotion gates
remain separately reported. Production is shadow-only until those gates pass
and an explicit review re-enables formal TradeReady evaluation; an override
never makes failed evidence gates read as passed.

## Deployment acceptance

Production services load the checked-out repository. A release is complete
only when:

1. `HEAD` is `master` and equals the local `origin/master` reference;
2. the worktree is clean, including previously local-only Outbox changes;
3. tests and static checks pass from that exact commit;
4. user units are reloaded and restarted from the committed files;
5. Outbox integrity, IBKR data-plane health, RTH timers and generated
   acceptance projections pass smoke checks.
