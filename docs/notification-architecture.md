# Notification architecture

> **状态（2026-08-07）：本文档的 lane 语义分级（ops/market/trade/position/report）继续有效；
> outbox、claims、receipts、receipt mirror 与 Rust report/delivery lane 的实现已冻结。**
> 该实现将由 `spx-worker` + Huey 单 owner 取代（执行方案 Phase 4，Rust 侧 Phase 6）。
> 不得扩展 outbox/claim/receipt/mirror 状态机，不得为新 lane 增加耐久化机制。

## Monorepo ownership overlay

This document defines the Python notification lanes. Python continues to own
operations, market-warning, trade-ready, position-safety, and legacy report
delivery. The Rust workspace owns only the explicitly cut over quarter-hour
`scheduled_report` lane, using its single SQLite/WAL ledger and receipted
delivery worker as defined in `rust/docs/ARCHITECTURE.md`.

The `SPX_RUST_REPORT_OWNER` fence selects exactly one quarter-hour report producer.
When false, the Python contract below applies to that lane. When true, Python
still publishes the atomic desk projection but must not enqueue the same slot;
Rust report and delivery own it end to end. The two outboxes are not merged and
must never both own the same economic slot.

## Contract

Every human-facing message uses one of five lanes and the shared notifier dispatcher:

| Lane | Purpose | Policy | Examples |
| --- | --- | --- | --- |
| `ops_transition` | State changes requiring operational awareness | Deterministic, no reviewer veto, Bark ops | Schwab to IBKR takeover, Schwab restored, both providers unavailable |
| `market_warning` | Fast market movement warning, not an entry instruction | Deterministic, no LLM latency or veto | SPX/ES shock, reclaim, flip reclaim, call-wall breakout |
| `trade_ready` | Fully gated executable intent | Deterministic strategy gates; LLM is writer only | Contract, entry limit, invalidation, target, expiry |
| `position_safety` | Existing-position or execution safety | Deterministic, never blocked by a reviewer | Open/close/quantity/PnL safety events when account tracking is explicitly enabled |
| `scheduled_report` | Time-based map/status/review | Writer allowed; delivery is still receipted and retryable | Morning map, 15-minute status (including the read-only [Call / Put Skew Spread Shadow](call-skew-spread-shadow.md)), post-close review |

IV, Gamma and option-structure observations enter the reviewer lane. Explicit
data-quality observations remain audit-only. The direct and audit-only sets are
allowlists; their union must never consume the reviewer lane.

## Delivery lifecycle

Every human-facing producer persists a final notification through
`enqueue_notification()` before returning. It owns:

1. a durable SQLite enqueue before any network I/O;
2. an immutable semantic event ID and payload for idempotent producer replay;
3. independent acknowledgement and retry state for every sink, so a delivered
   Bark target is never resent while Feishu is recovering;
4. a content-free SQLite receipt containing semantic event ID, source, lane,
   outcome and per-sink status.

The independent delivery worker polls the outbox every 0.5 seconds and owns all
Feishu/Bark network I/O. The delivery state machine is
`pending -> claimed -> delivered`; failures use the configured
15/60/300/900-second schedule and become `dead_letter` after attempt or age
exhaustion. The 24-hour loop also runs `notification_recovery` every 60 seconds,
so recovery does not depend on a later market alert. Shock events are the sole
latency-critical exception: they still enqueue before attempting delivery
inline.

Claim order is explicit: position/execution safety first, then expiring
`trade_ready` and `gth_manual_candidate` work, market warnings, operations, and
scheduled reports. Within a lane, the earliest expiry wins. Immediately before
transport, the worker atomically rechecks claim ownership, cancellation and
expiry; a rejected claim cannot call Bark or Feishu.

The healthy-path service objectives are at most one second from a confirmed
signal to durable outbox presence and at most five seconds from enqueue to the
first transport result. Every event must end with either a delivered receipt or
an explicit terminal receipt. Expiry immediately before claim or network I/O is
recorded as `expired_before_delivery`; source invalidation is recorded as
`cancelled_before_delivery`. Expired work is not automatically acknowledged,
and an unmirrored terminal receipt keeps operational health degraded.

Every per-sink delivery result first appends a content-free receipt intent in
the same outbox transaction that settles the target. Mirroring that intent into
the receipt database is idempotent and retryable; a mirror backlog fails both
worker health and the daily outbox-integrity check. Source cancellation also
persists a tombstone when no outbox row exists yet, so a concurrent late
enqueue is rejected atomically instead of reviving an invalidated signal.

The receipt database also uses rollback-journal `DELETE` mode with
`synchronous=FULL`. Health is based on its real `quick_check`, schema and exact
receipt-ID mirror rows—not merely the outbox's `recorded_at` projection. New
receipt intents are checked every worker cycle; historical mirrors are
reconciled at startup, after a receipt-file identity change, and at a bounded
60-second cadence.

For `trade_ready`, a fresh, executable final quote may move normally between
decision and enqueue without suppressing the signal. The notification retains
the immutable decision NBBO, entry limit and risk plan; the later quote is
audit-only and tells the receiver to requote. Stale, crossed, excessively wide
or otherwise unexecutable quotes still fail closed for that delivery attempt.
They remain transient re-quote conditions inside the five-minute default
economic-opportunity window, not lifecycle expiry. Quote freshness remains an
independent 10--15 second gate, while typed configuration bounds the human
opportunity window to five--ten minutes.

Operator notification roles have different human projections while the ledger
keeps the complete immutable ingress payload. `setup` is delivered as a compact
`WATCH`: it may show the live structure and the condition that would confirm
it, but it must not look like an entry card or surface an exact contract.
`trade_ready` is the compact manual action card and must retain the opportunity
identity, exact contract or spread, decision NBBO, limit, validity, invalidation,
risk, target and reward/risk. `exit` retains the terminal lifecycle account.
Research prose is audit context and cannot displace those operational fields.

The quarter-hour `scheduled_report` is not a lifecycle notification. If its source
projection is already `invalidated` or `expired`, Rust emits a deterministic
neutral `STANDBY` status. Its current location and reference structure come
from the original typed projection, and its next trigger is fixed to waiting
for a new price event. A model-written title, old LONG/CALL direction, or old
event trigger is never reused in that terminal standing message.

The producer-side inflight lease is bounded by the remaining signal lifetime.
If a process stops between local acceptance and durable enqueue, the short
lease permits an in-lifetime retry. Startup reconciliation repairs either
direction: an outbox row restores missing local acceptance, while local
acceptance without its outbox row is cleared and the exact immutable event is
re-enqueued. Reconciliation compares the persisted payload fingerprint, exact
sink set and live/delivered target states; matching an event ID alone cannot
restore producer acceptance, and cancelled or dead-lettered rows fail closed.
Both `invalidated` and `expired` end the old lifecycle, persist a cancellation
fence, clear semantic dedupe for a later rearm, and keep the old event ID
terminal so replay remains idempotent.

The human-notification outbox uses SQLite rollback-journal (`DELETE`) mode with
`synchronous=FULL`. It intentionally does not use WAL: notifications have
multiple short-lived producer and consumer processes, which can meet the rare
[WAL-reset corruption race](https://sqlite.org/wal.html#the_wal_reset_bug)
present in SQLite versions before 3.51.3. The queue is low-volume, so
serialized writes are preferable to WAL checkpoint risk.

If integrity checking ever fails, stop every notification producer and
consumer before recovery. Move the main database plus any adjacent `-wal`,
`-shm`, or `-journal` files into one timestamped recovery directory; never
separate or delete sidecars from a live database. Start the delivery worker to
create the replacement, then require `PRAGMA journal_mode=DELETE`,
`PRAGMA quick_check=ok`, a stable worker restart count, and one real scheduled
report before restoring normal operation.

During rollout, failed event IDs are mirrored into the old JSONL missed queue.
The SQLite worker imports any pre-existing JSONL entries and removes the shadow
only after the corresponding event is fully delivered. The JSONL flusher is
used only when the delivery outbox feature flag is disabled for rollback.

Periodic alert candidates retain the SQLite domain-event outbox. `acked` means
the candidate reached a terminal policy outcome. The outbox additionally stores
`settlement_outcome` and `delivered_count`; therefore an acknowledged veto or
audit-only observation is no longer indistinguishable from human delivery.

The intraday shock producer remains latency-critical. It may call the notifier
before periodic outbox evaluation, but it uses the same cooldown state,
dispatcher, receipt store and sink policy. The later periodic candidate is
therefore deduplicated without creating a second human push.

The exchange-local heartbeat, isolated research projection and post-close
operational gates are specified in
[RTH runtime clock and end-to-end acceptance](rth-runtime-clock-and-acceptance.md).
