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

The production 5-minute state is attached directly from
`minute_market_frame.diagnostics.rth_market_state`. Spring Gamma computation
and report projection are disabled in production: the research shadow cannot
gate TradeReady, option-structure quality, the opportunity radar or report
delivery.

The report clock is defined by
`application/order_map/report_clock.py`:

- regular RTH: 26 slots, 09:30 through 15:45 ET;
- scheduled 13:00 early close: 14 slots, 09:30 through 12:45 ET;
- service-manager start grace: at most 120 seconds after the slot;
- a second invocation in the same slot is deduplicated.

RTH slots are heartbeats. `no_material_changes` and a temporarily thin
snapshot cannot suppress them; a thin heartbeat is delivered with an explicit
degraded warning.

## ES five-minute bar ownership

`spx-spark-es-bar-sampler.service` is the sole writer of
`latest/es_bars_5m.json`. It observes one real, provider-qualified ES source
timestamp every five seconds, rejects duplicate, out-of-order, future and
contract-conflicted observations, and never interpolates or fills a missed
bucket. A partial bar remains partial; it cannot be promoted merely to restore
an 8/8 state.

The heavy market-feature worker only reads the sampler's last atomic snapshot,
so ES observation is no longer serialized behind option-chain, Greek and
report computation. Shared-host I/O can still affect the sampler, so
`latest/es_bar_sampler.lease.json` records every cycle, including duration,
overrun and consecutive failures. The 24-hour supervisor treats a stale lease
as a data-plane fault even when systemd still reports the process as active.

## Research-only Spring projection

If Spring computation and projection are explicitly re-enabled for research,
the report accepts a Spring projection up to five seconds ahead of the report's
frozen construction clock. This tolerance covers atomic file writes completed
by the minute worker while the report is being assembled. Larger future skew,
stale data, expiry mismatch, session mismatch and invalid segment remain
fail-closed for that research attachment only.

Every enabled research decision is persisted in
`spring_gamma_v3_projection_diagnostic` with a machine-readable reason. The
report audit also stores `spring_gamma_v3_state_window`, including sample and
5-minute-slot counts, state counts, latest state and maximum future skew. When
disabled, no Spring persistence or rolling-window attachment is required.

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

IBKR error 10197 opens a non-invasive circuit breaker. Production probe delay
is capped at 15 seconds (initial and maximum equal), so the collector rechecks
quickly after an external Live session releases entitlement instead of backing
off toward multi-minute waits. The recovery stability window defaults to 30
seconds (`IBKR_CONFLICT_RECOVERY_SECONDS`) and is independent of probe cadence.
The circuit closes only after continuous fresh LIVE flushes for that window; a
TCP reconnect or stale cached quote is insufficient. The collector never
disconnects or takes over a phone, desktop or other live session.

`latest/ibkr_stream_health.json` separates process state from data-plane
health and publishes `policy_blocked`, reason, retry time, circuit state and
conflict count. Its `observed_at`, `max_age_seconds` and `expires_at` prevent a
dead process's last `process_active=true` observation from remaining trusted.

## Daily hard acceptance

At 19:00 ET on each trading day — after `spx-spark-session-finalize` owns the
post-close review artifact — `spx-spark-rth-daily-acceptance.timer` writes:

- `reports/rth_daily_acceptance/date=YYYY-MM-DD/acceptance.json`;
- `latest/rth_daily_acceptance.json`;
- a freshly recomputed `latest/level_decision_acceptance.json`.

The operational verdict always checks:

- 100% of expected RTH report and delivery slots. When
  `SPX_RUST_REPORT_OWNER=true`, slots and human delivery are read from
  `notification.rust_delivery_ledger_path` (`scheduled_report` intents /
  targets). Python `report_kind=status_snapshot` rows are projection inputs
  only and never count as delivered reports;
- 100% of the expected five-minute TradeIntent producer-heartbeat slots, plus
  parseable producer-ledger and TradeIntent audit records;
- every unique manual `strategy_decision` opportunity for the session date has
  a durable `notification_events(source=strategy_decision,lane=trade_ready)`
  event with at least one human target, all targets delivered before signal
  expiry, first delivery within five seconds, and a mirrored success receipt
  for every target; a source-terminal cancellation/expiry is accepted only
  with an explicit mirrored receipt for every target; zero ready opportunities
  passes only when the producer-heartbeat and audit-integrity checks pass;
- the post-close raw market-data completeness verdict;
- notification Outbox `quick_check=ok`, journal mode `delete` or `wal`, and
  zero pending/claimed/uncertain/failed/unknown human transport targets
  (`bark` / `bark_friend` / `feishu`); internal backlog such as
  `alert_pipeline`, `__cancellation__`, or `rust_ingress` is reported as
  diagnostic only and does not fail the human-transport gate;
- notification receipt-store `quick_check=ok` with the unified event/attempt
  schema present on `spx.sqlite`;
- formal level-decision authority is either disabled or backed by passed
  statistical acceptance gates.

Spring is an isolated research dependency. When Spring computation is enabled,
acceptance additionally requires at least 95% of its expected RTH minute slots
and at least 75% ready option overlays. Report Spring projection and
rolling-window attachment (≥95%) are required only when
`spring_gamma_v3.report_enabled=true`. When both production flags are
disabled, those Spring-only checks are omitted; they cannot degrade the live
data, opportunity, report-delivery, or TradeReady verdicts.

A historical replay from an intermediate quote-clock implementation is not a
daily acceptance result. In particular, the 2026-07-24 `269/390` replay
predated the final per-leg fail-closed and 13-pair production contract and was
not persisted as a versioned replay artifact. The first full forward
acceptance for this contract is 2026-07-27 RTH; the daily verdict now runs at
19:00 ET after session finalize. See
[SPXW raw → merge → feature clock contract](spxw-option-clock-contract.md)
for the evidence boundary and exact acceptance targets.

A Rust-owned report slot counts as delivered only when its `scheduled_report`
intent has at least one human transport target and every such target is
`delivered`. The legacy `delivered_ok=any_sink` audit flag cannot satisfy this
gate.

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
