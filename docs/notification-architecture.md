# Notification architecture

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

All human-facing paths call `dispatch_notification()`. It owns:

1. a durable SQLite enqueue before any network I/O;
2. immediate Feishu/Bark fan-out for the newly enqueued event;
3. independent acknowledgement and retry state for every sink, so a delivered
   Bark target is never resent while Feishu is recovering;
4. a content-free SQLite receipt containing semantic event ID, source, lane,
   outcome and per-sink status.

The delivery state machine is `pending -> claimed -> delivered`; failures use
the configured 15/60/300/900-second schedule and become `dead_letter` after
attempt or age exhaustion. The 24-hour loop runs `notification_recovery` every
60 seconds, so recovery does not depend on a later market alert. Shock events
still enqueue and attempt delivery inline, keeping the fast path synchronous.

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

The exchange-local heartbeat, cross-process Spring projection and post-close
operational gates are specified in
[RTH runtime clock and end-to-end acceptance](rth-runtime-clock-and-acceptance.md).
