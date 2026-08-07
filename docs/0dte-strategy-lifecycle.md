# 0DTE strategy lifecycle

This document records the production decision boundaries for the SPX intraday
engine.  The implementation is advisory and never submits an order.

## Stable terrain map

Raw OI/GEX analytics remain in the audit payload.  The decision map promotes a
wall change only after three distinct 15-minute buckets agree on a materially
different structure.  Promoted Put Wall, flip and Call Wall values are
published as configurable bands, together with promotion time, duration and
confirmation count.  Active level events keep their frozen coordinate and ES
basis so a price-source change cannot invalidate a valid path merely by
changing coordinates.  During RTH, a live proxy lifecycle may be atomically
rebased to official SPX without resetting its event or confirmation clock.

## ES-led GTH dip reclaim

The GTH fast lane needs a fresh live ES quote but does not require a direct
overnight SPX print.  It evaluates 15- and 60-minute peak-to-trough paths,
requires a minimum descent duration, a configured recovery fraction and a
60-second hold.  Five-second deterministic buckets feed independent causal
15- and 60-minute windows: each horizon becomes usable when its own window is
complete, rather than waiting for a global one-hour warmup.  A 30-second
provider-switch debounce prevents one transient Schwab/IBKR fallback tick from
discarding the active window.  The detector retains the one-hour cooldown and
hard three-signal session ceiling.  A macro pre-event window or an already
active two-leg GTH virtual spread suppresses a new entry advisory.

The confirmed advisory carries an exact debit-spread 埋伏单 only when the
stable structure, expiry, quality-qualified ES→SPX basis and signal all belong
to the same session and the structure inputs are no more than 90 seconds old.
The long strike is the SPX equivalent of ES rounded to the nearest 5-point
strike.  The short strike is the nearest frozen flip_high / Call Wall strictly
above it, inside a 15–75 point width band and capped at 75 points; when the
fresh structure is valid but no wall qualifies, width falls back to half the
expected move (clamped to the band) and then to 50 points.  Missing or stale
inputs suppress exact legs and leave only a non-executable observation asking
for fresh SPXW NBBO.

The exit clock is always 09:45 America/New_York on the current 0DTE expiry:
13:45 UTC during EDT and 14:45 UTC during EST.  It never rolls to the next
expiry.  The virtual episode uses the same signal-carried exit instant, with an
810-minute safety backstop, tracks the exact 1x/-1x legs with five-second quote
age/skew gates, and takes profit when net spread value reaches 85% of width.
Quantity stays operator-selected and automatic ordering remains disabled.
Rationale: `docs/alert-optimization-from-backtest-2026-07-18.md`.

## One primary strategy

Order-map presentation exposes either one TradeReady plan or one observation
strategy.  A candidate in the opposite direction is reduced to an invalidation
condition; it is never rendered as a concurrent plan.  Full raw candidates are
retained for research and replay.

The human action window for one confirmed economic opportunity is five minutes
by default and may be configured only within five to ten minutes. Exact option
quotes have a separate 10--15 second freshness contract. A stale quote pauses
delivery until a fresh executable re-quote passes validation; quote staleness
alone does not mark the economic opportunity expired. Structural invalidation,
the session entry cutoff and the hard exit clock remain terminal boundaries.
The opportunity clock is anchored when the price event is confirmed; an earlier
pending/setup timeout cannot immediately expire the newly confirmed event.
Within that window, a stale exact quote changes publication to "wait for fresh
NBBO", not to a new opportunity generation. A new generation requires the old
structure to terminate and the reset/rearm conditions to be satisfied.

Human delivery is role-specific. `setup` means `WATCH` and carries only the
structure plus the condition needed for confirmation. `trade_ready` is the one
manual action card: it carries the exact contract or spread, executable NBBO
and limit, validity, invalidation, bounded risk, target and reward/risk. `exit`
closes that same opportunity generation. A half-hour report that encounters a
terminal source state is separate from those roles and becomes a deterministic
neutral `STANDBY`; it cannot repeat the prior direction, contract or trigger.

## Durable TradeReady publication

`trade_ready` is externally visible only after the immutable notification
event has been accepted by the durable outbox.  An internally qualified signal
is persisted as `ready_pending_delivery` while a transient enqueue or
reconciliation attempt is outstanding, and as `delivery_blocked` after a
terminal notification-contract failure.  Both states keep
`signal_status=trade_ready` for diagnostic research, but neither grants
execution eligibility.  The Confirmed Gate stays pending for the transient
state, finalizes the terminal state as blocked, and finalizes `trade_ready`
only after durable acceptance.  A notification failure therefore cannot be
reported as an actionable signal and cannot terminate the market-feature
evaluation loop.

## Greeks boundary

Gamma, Speed and Color describe remaining convexity; Theta and Charm describe
waiting cost; Vanna plus IV-down scenarios describe post-event crush risk.
They have no index-direction authority.  Only when exact-expiry usable and OI
coverage both meet the configured gate may they adjust same-direction contract
confidence or a virtual exit.  Otherwise the entire layer is explanation-only.

## Macro clock and virtual lifecycle

Scheduled releases are explicit records in `config/macro_events.toml`; the
model cannot invent event times.  The clock reports normal, pre-event or
post-event mode.  The virtual lifecycle tracks the system's naked RTH option
or exact two-leg GTH debit spread without IBKR positions, records entry/current
net Greeks and 5/15/30-minute
MFE/MAE, and can emit take-profit, reduce or exit advice for target/wall touch,
premium gain, Delta saturation, post-event IV/Vanna drag, Gamma/Color decay,
invalidation or time stop.  Every message states that account positions are
unknown and automatic ordering is disabled.
