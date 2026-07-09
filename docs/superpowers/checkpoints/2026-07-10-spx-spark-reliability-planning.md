# SPX Spark reliability hardening checkpoint

Paused at the user's request on 2026-07-10.

## Authoritative remote state

- Host: `oracle-vm`
- Repository: `/home/ubuntu/spx-spark`
- Branch: `master`
- HEAD: `8f4ebb1 Document SPX Spark reliability hardening design`
- Tracking: local master is one commit ahead of `origin/master`.
- Unrelated untracked file: `earlyoom_1.7-2_arm64.deb`. Do not modify it.
- No production Python, configuration, systemd, or workflow code has been
  changed for this repair batch.
- No service has been restarted and no deployment has occurred.

## Approved artifact

The user approved方案 2 and then approved the written design:

`docs/superpowers/specs/2026-07-10-spx-spark-reliability-hardening-design.md`

The approved design commit is `8f4ebb1` on the Oracle working repository.

## Current phase

The task is paused during the `writing-plans` phase, before implementation.
The skill was read and announced. The next required deliverable is a set of
complete TDD implementation plans; implementation skills and source edits have
not started.

Because the approved spec spans independently reviewable subsystems, the plan
set is decomposed in this order:

1. Position PnL, snapshot v2, durable event outbox, per-event acknowledgement,
   secure state files, and central quote freshness/use policy.
2. Market-calendar foundation, validated ATM reference/basis,
   `OptionReplanController`, subscription diff, next-expiry cache, SPY plan
   key, and cooperative slow polling.
3. `SpotResolution`, HL research-only payload hard gates, 17:00 ET lifecycle
   consumers, and measured post-close completeness.
4. Baseline pytest/Ruff repairs, GitHub Actions, integration replay, controlled
   rollout, and the 60-minute read-only soak.

No plan document has been written yet. Resume by creating:

- `docs/superpowers/plans/2026-07-10-spx-spark-position-reliability.md`
- `docs/superpowers/plans/2026-07-10-spx-spark-stream-stability.md`
- `docs/superpowers/plans/2026-07-10-spx-spark-market-lifecycle.md`
- `docs/superpowers/plans/2026-07-10-spx-spark-integration-rollout.md`

Then run the writing-plans self-review, commit the plan set, and offer the user
the required execution choice: subagent-driven or inline execution.

## Shared interfaces already resolved

### Secure state I/O

Create one L1 helper used by the position outbox and ATM controller:

~~~python
def atomic_write_json_secure(path: Path, payload: Mapping[str, object]) -> None:
    """Atomic JSON replacement with fsync and mode 0600."""


@contextmanager
def exclusive_state_lock(path: Path) -> Iterator[None]:
    """Hold an advisory lock across a complete read-modify-write cycle."""
~~~

The architecture map and tests must register this infrastructure module.

### Quote-use decision

Keep the zero-internal-dependency policy in `marketdata.py`:

~~~python
class QuoteFreshness(str, Enum):
    FRESH = "fresh"
    STALE = "stale"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class QuoteUseDecision:
    feed_mode: MarketDataQuality
    freshness: QuoteFreshness
    research_usable: bool
    alert_allowed: bool
    pricing_allowed: bool
    reason: str


def quote_use_decision(
    quote: Quote,
    *,
    as_of: datetime,
    stale_after_seconds: float = 15.0,
    delayed_stale_after_seconds: float = 60.0,
    allow_frozen: bool = False,
) -> QuoteUseDecision:
    ...
~~~

`Quote` gains optional `last_update_at`. A delayed feed uses transport update
age rather than its naturally delayed source timestamp. Configured slow labels
use 300 seconds by default. A timestamp more than five seconds in the future
fails closed.

`VerifyRow` gains `last_update_at`. `snapshot_rows` advances it only when
ticker time or the approved normalized ticker fingerprint changes; repeated
flushes of the same cache do not refresh it.

### Market calendar

Use a stdlib-only calendar primitive and update the architecture guard
explicitly rather than silently weakening the existing L0 rule:

~~~python
@dataclass(frozen=True)
class MarketSession:
    trading_date: date
    open_at: datetime
    close_at: datetime
    review_ready_at: datetime
    early_close: bool

    @property
    def expected_five_minute_buckets(self) -> int:
        ...


class MarketCalendar:
    def is_trading_day(self, day: date) -> bool: ...
    def next_trading_day(self, day: date) -> date: ...
    def previous_trading_day(self, day: date) -> date: ...
    def session(self, day: date) -> MarketSession | None: ...
    def is_rth_open(self, now: datetime) -> bool: ...
    def research_expiry(self, now: datetime) -> date: ...
    def research_expiries(self, now: datetime) -> tuple[date, date]: ...
    def completed_review_date(self, now: datetime) -> date: ...
~~~

Approved rollover is exactly 17:00:00 America/New_York. Early close changes
the report denominator but not the 17:00 rollover.

Official 2026 calendar detail already checked:

- 2026-07-02 is a normal 16:00 cash session.
- 2026-07-03 is the observed Independence Day full closure.
- Planned 13:00 early closes are 2026-11-27 and 2026-12-24.

## Stream-plan research already completed

The stream plan should contain four commits:

1. `fix(ibkr): derive ATM only from validated source evidence`
   - create `ibkr/atm_reference.py` and `tests/test_atm_reference.py`;
   - basis uses at least five co-fresh SPX/ES samples over at least 30 seconds,
     a rolling five-minute median, source-time skew at most five seconds,
     absolute basis at most 120 points, and a 15-point outlier guard;
   - remove `ES.close - SPX.close`;
   - no raw ES fallback without valid evidence.
2. `fix(ibkr): stabilize option replans with hysteresis`
   - create `ibkr/option_replan.py` and `tests/test_option_replan.py`;
   - trigger/re-arm 20/10 points, three confirmations over 15 seconds,
     120-second cooldown, 40-point/two-observation emergency after a hard
     30-second minimum, and immediate expiry rollover;
   - duplicate flushes with the same `candidate.observed_at` do not count as
     confirmations.
3. `fix(ibkr): reconcile option subscriptions without coverage gaps`
   - retain hot-set intersection, free rotation/far-tail capacity, add new hot,
     then remove obsolete hot only after success;
   - partial failure does not advance the accepted plan;
   - SPY has an independent `(expiry, rounded_atm)` key;
   - option cache accepts the full active expiry set, including next expiry.
4. `fix(ibkr): make slow polling cooperative and qualification session-safe`
   - create `ibkr/slow_poll.py`;
   - schedule four chunks at approximately 0/75/150/225 seconds for a
     300-second cycle;
   - no ten-second blocking sleep;
   - qualify once per IBKR session and clear cache on reconnect;
   - remove CLI use of `util.startLoop()`, which ib_async documents as a
     Jupyter nested-loop helper.

The live acceptance target remains flush gap at most 12 seconds, no unreasoned
or source-ping-pong re-plan, no unnecessary SPY rebuild, and no new pacing,
session, or unawaited-coroutine error.

## Lifecycle-plan research already completed

### Spot resolution

`SpotResolution` has separate research and pricing references. When pricing is
not allowed:

- compatibility `underlier.price/source` are null;
- legacy `candidates` are empty;
- legacy `wall_ladder` is empty;
- day-move actionable values and `rn_density` are null;
- HL values appear only in `research_reference`,
  `research_candidates`, and `research_wall_ladder`;
- no research payload contains numeric projected limits, probabilities, ETA,
  or front-run aliases.

All calculation functions gate on `pricing_allowed` before running. A
research-only payload with a valid research reference is not considered thin.

### Post-close completeness

Use structured checks with measured value, threshold, pass/fail, and reason.
Required defaults remain those in the approved spec:

- 90% SPX/ES five-minute bucket coverage;
- first/last observations within 15 minutes of actual open/close;
- 95% SPX/ES live ratio;
- at least 20 front contracts, ten strikes, both rights, and 50-point span;
- 90% usable front option rows and 80% IV coverage;
- 60% front surface bucket coverage and near-close snapshot;
- latest front IV and gamma coverage each at least 50%.

The existing two-row test becomes degraded. A new broad 78-bucket fixture
proves complete. The 2026-07-08 replay must fail the final IV/gamma checks with
approximately 0.2805 and 0.2683.

### Timer and CI

- Timer target: `Mon..Fri *-*-* 17:15:00 America/New_York`.
- Add scheduled report identity/holiday guard so the previous report is not
  resent.
- Baseline pytest correction: the direct-push fixture enables both Feishu and
  Bark, so expected successful sink count is two.
- Baseline Ruff correction: remove unused `subprocess` from
  `tests/test_order_map.py`.
- Add `.github/workflows/ci.yml` for Python 3.12, locked uv sync, Ruff, and
  pytest.

## Position-plan items still to formalize

The interrupted planning pass must be completed from the approved spec:

1. Secure state helper and mode-0600 tests.
2. Quote freshness/use decision and IBKR update tracking.
3. Snapshot v2 and the exact formula
   `qty * (mark * multiplier - avg_cost)`.
4. Full-book pricing coverage: partial PnL may display as degraded but cannot
   emit a book-PnL event.
5. Durable ordered structural events plus coalesced pending book-PnL event.
6. Version-1 baseline migration and corrupt-state fail-closed behavior.
7. Notification sent state and `NotificationResult` per-event IDs.
8. Pre-send reconciliation, pending retry with an invalid current snapshot,
   `--no-notify` retention, and post-send exact acknowledgement.

## Baseline verification evidence

Before implementation:

- `uv run pytest -q`: `1 failed, 359 passed`.
- Failure: `tests/test_notifier.py::test_direct_push_rewrites_event_with_llm_writer`
  expected one sink although both Feishu and Bark succeeded.
- `uv run ruff check .`: one F401 unused `subprocess` in
  `tests/test_order_map.py:5`.
- Recent live read-only review still observed repeated SPXW re-plans and
  matching unnecessary SPY re-plans.

## Resume instruction

Resume at the writing-plans phase only. Read this checkpoint and the approved
design, finish all four plan documents with exact files, interfaces, RED/GREEN
commands, implementation snippets, and commit boundaries, then perform the
writing-plans self-review. Do not edit implementation code until the plan set
is committed and the user chooses the execution mode.
