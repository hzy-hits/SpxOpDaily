# State-machine contract

Status: production design contract for the implemented Rust boundary. It does
not authorize deployment.

## Keep independent axes independent

The SPX decision contract has exactly two trading windows:

```text
MarketSession = GTH | RTH
CalendarState = OPEN | CLOSED
```

`Globex` is a CME futures venue/session label. It may be retained as ES source
metadata by a bridge, but it is not an SPX/SPXW decision window and cannot enter
`MarketSession`. Likewise, `CLOSED` is a calendar gate, not a third session.
Keeping these axes separate prevents impossible combinations from spreading
through provider selection.

The same rule applies elsewhere: provider transport, authentication and
entitlement remain observable facts, while one validated operational state
summarizes whether the provider can be used. A large Cartesian-product enum is
not useful; impossible combinations are rejected at ingress.

## Decision pipeline

Readiness is a one-way pipeline, not a graph with recoverable side effects:

```text
validated envelope
  -> current provider generation
  -> causal decision-time snapshot
  -> permitted provider
  -> exact two-sided live NBBO
  -> fresh 10-point vertical
  -> MANUAL_CANDIDATE

any failed gate -> NO_TRADE(sorted typed reasons)
```

Lifecycle stages should use exhaustive `match` expressions or small transition
functions. Numeric checks such as age, skew, debit and TTL remain flat guard
clauses. They are not states. This avoids deeply nested `if` trees while still
preserving every abstention reason for audit.

Provider generation and availability are hard cache boundaries:

- lower generation: ignore as stale;
- higher generation: discard all quotes from the old connection;
- any non-live state, including IBKR `10197`: discard that provider's quotes;
- returning to `live` without a fresh usable quote flush does not restore
  readiness.

## Normalized bridge transitions

```text
BOOT -> SOCKET_SYNC_FENCE -> SNAPSHOT_SYNC -> READY
  ^             |                 |             |
  +-------------+-----------------+-- transport uncertainty
                                      -> DEGRADED -> reconnect + exact retry

contract poison | stale cursor | state rollback -> HALTED
```

Only one frame is in flight. Before transport, the exact envelope, ID,
generation and sequence are durably stored. EOF or timeout therefore retries
the same bytes after reconnect. A matching typed ACK advances the cursor; an
ACK for another ID is invalid. `replace_provider_snapshot` is a single-frame
commit, so omitted, invalid or zero-price legs remove older cached values rather
than silently inheriting them. `READY` is reached only after both provider
frames receive non-stale accepted ACKs.

The bridge maps explicit `regular/rth` and `gth` quote labels. Schwab is an
explicitly configured SPX/SPXW RTH-only provider, so a genuinely missing
session field may use that provider policy; an explicit unknown label is never
rewritten. Python `globex` remains ES venue metadata, and a session-less IBKR
option is dropped until the normalized producer supplies an auditable SPX
decision session. A process-lifetime state lock fences a second producer, while
source-read failures durably track both provider-clear ACKs and force a full
snapshot resync on recovery even when the restored file has the old fingerprint.

## Delivery target transitions

The outbox has one closed set of legal status transitions:

| Current | Event | Next |
|---|---|---|
| `pending` | claim | `claimed` |
| `pending` | TTL expires | `expired` |
| `pending` | source cancellation | `cancelled` |
| `claimed` | lease expires before transport | `pending` |
| `claimed` | atomic begin; still live and uncancelled | `in_flight` |
| `claimed` | cancellation/expiry at atomic begin | `cancelled` / `expired` |
| `in_flight` | confirmed success | `delivered` |
| `in_flight` | explicit retryable response | `pending` or `dead_letter` |
| `in_flight` | confirmed permanent rejection | `dead_letter` |
| `in_flight` | source cancellation fence | `in_flight` until settlement |
| `in_flight` | lease loss or unknown result | `uncertain` |
| `dead_letter` / `uncertain` | verified operator replay within TTL | `pending` |

`delivered`, `cancelled` and `expired` have no replay transition.
Acknowledgement records operator review but does not change the delivery status.

Each mutation is implemented as a named ledger operation, fenced by owner
generation and an SQL compare-and-set predicate on the expected current state.
The `claimed -> in_flight` operation checks owner/claim leases, cancellation,
TTL, attempt budget, status change and attempt insertion in one immediate
transaction. External I/O starts only after that commit.
Once in flight, a cancellation cannot retract the external side effect; it
fences unsent siblings while the started attempt records its real response (or
becomes `uncertain` on lease loss).
SQLite `CHECK` constraints reject states whose required columns do not match the
status. Application transition logic and database constraints are both needed:
the first makes intent readable; the second prevents bypass writes from storing
an illegal shape. A deferred foreign key and provenance trigger bind each
current attempt to the target's channel, owner/claim generation, replay
generation, attempt number, idempotency key and active claim time window.

## Implementation rule

Use a state machine when values are mutually exclusive and events move a value
through a lifecycle. Use a validator or guard when several independent facts
can fail at once. Never hide readiness failures behind a generic boolean, and
never turn transport uncertainty into an automatic retry.
