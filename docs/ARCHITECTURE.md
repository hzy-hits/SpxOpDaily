# SPX Spark Core architecture

Status: implemented and locally verified migration candidate. Nothing in this
repository is deployed by the presence of these files.

## Objective

The Rust workspace owns a deliberately small production boundary: accept
already-normalized market-data envelopes, build one decision-time snapshot,
apply deterministic readiness rules, persist a manual advisory decision, and
deliver its notification through one auditable ledger.

It never places, changes, or cancels an order. The only strategy actions are
`NO_TRADE` and `MANUAL_CANDIDATE`; `MANUAL_CANDIDATE` is an advisory for a human,
not an executable order.

```text
Python normalized state --> spx-bridge --> Unix socket --> spx-core
                              |                              |
                              +-- no broker/session owner    +--> append-only
                                                        |    normalized frames
                                                        |
                                                        +--> one SQLite/WAL ledger
                                                                   |
                                                                   v
                                                             spx-delivery

append-only frames + ledger decisions
        --> post-close artifact --> Parquet --> Python research
                                              (DuckDB/HMM/replay)
```

The bridge boundary is intentional. The first migration phase does not connect
Rust directly to IB Gateway or Schwab. Python retains provider SDK/session
ownership; `spx-bridge` reads only the atomic normalized projection and
translates it into the closed, versioned contracts in `spx-domain`.

Session, provider, readiness and delivery lifecycle semantics are frozen in the
[state-machine contract](STATE_MACHINES.md). In particular, `MarketSession`
contains only SPX `GTH` and `RTH`; CME `Globex` metadata and the independent
`CLOSED` calendar gate are not session variants.

## Component ownership

| Component | Owns | Must not own |
|---|---|---|
| `spx-domain` | Versioned wire types, enums, validation and canonical hashes | I/O, settings, broker SDKs |
| `spx-bridge` | Bounded source reads, provider mapping, durable generation/sequence/pending frame, typed ACK and health | Broker SDKs, strategy generation, notifications, research |
| `spx-core` | Unix ingress, quote book, decision snapshot, readiness, deterministic policy, health projection and append log | Network delivery, research fitting, orders |
| `spx-ledger` | The single SQLite/WAL database, owner fencing and legal state transitions | Analytical history or provider connections |
| `spx-delivery` | Claim, atomic `InFlight` transition, rendering, transport, retry, receipts, uncertain outcome and DLQ | Strategy decisions or a second outbox database |
| Python research | Post-close artifacts, Parquet, DuckDB, HMM, replay and backtests | Production readiness overrides or notification delivery |

`spx-delivery` is runnable, but outbound I/O is guarded by two independent
permissions: typed configuration and an explicit CLI flag. Examples remain
disabled and are not deployment authority.

## Provider and session policy

Provider selection is fail-closed and session-specific:

| Session | Primary rule | Fallback rule | Failure behavior |
|---|---|---|---|
| GTH | Exact SPXW quotes must come from IBKR | No Schwab execution fallback | Emit `NO_TRADE` when IBKR is not live and entitled |
| RTH | Schwab first | IBKR only when explicitly enabled and recorded | Emit `NO_TRADE` when no permitted provider has fresh exact legs |

Frozen Schwab GTH quotes may be retained for audit but cannot authorize a
candidate. A process being alive does not establish readiness; transport,
entitlement, provider source time, quote quality, both NBBO sides, contract
identity, quote age and cross-leg skew are separate checks.

IBKR error `10197` means another client, including the user's mobile or TWS
session, owns the shared live-data entitlement. The IBKR bridge must publish an
`external_session_owns` state with `competing_session_10197` evidence, stop
aggressive acquisition, and back off. Neither bridge nor core may evict or
compete with the user's trading session. During this state:

- GTH new candidates fail closed;
- RTH may continue through fresh Schwab data;
- TCP reconnection or a running Gateway does not prove recovery;
- only a fresh usable provider flush may return the data plane to live.

The legacy `state.json` is a replaceable projection, not an event queue. The
bridge sends one complete provider snapshot in one frame using
`replace_provider_snapshot`; the core removes all prior quotes for that provider
before installing the accepted replacement. If the frame is rejected, the
bridge leaves the exact payload pending, exits readiness, and cannot submit an
evaluation. Zero or negative sides become missing, crossed books lose both NBBO
sides, and source timestamps are never replaced with file mtime or heartbeat
time.

## State and durability

There is exactly one mutable operational database. It contains ingress
idempotency, owner leases, decisions, notification events, targets, attempts,
receipts, cancellations, DLQ state and operator actions. SQLite runs in WAL mode
and schema constraints reject illegal states.

Large market history does not belong in SQLite. Intraday normalized frames are
append-only and immutable. Daily NDJSON is split into size-bounded numbered
segments. Quote batches use ordinary appends; an evaluation frame is durably
synced before its decision is processed, and rotation syncs the segment being
closed. A configured filesystem reserve is checked before every production
append. Retention deletes only strict regular segments from completed UTC days,
oldest first, while the current UTC day is protected. The live quote book is
also bounded by age and entry count, while a
separate bounded identity cache continues to reject conflicting recent batch
IDs. After the session, a Python job turns the bounded frames and decision
lineage into a versioned replay artifact and Parquet. The
artifact builder must prove that every replayed decision sees only the same data
that was available at its original decision time.

## Delivery outcome semantics

A transport call has four materially different outcomes:

1. confirmed delivered;
2. confirmed retryable failure;
3. confirmed permanent failure;
4. transport started but outcome unknown.

The fourth case is `uncertain`, not `pending` and not `delivered`. It must not be
automatically resent because doing so can duplicate a human-facing alert. An
operator must inspect the external target and record an explicit acknowledgement
or, only after confirming non-delivery and while the intent TTL remains valid,
an explicit replay. Historical attempts and receipts remain immutable. The
target key and delivery channel are frozen together when the intent enters the
ledger; changing a same-named runtime adapter cannot reroute old work.

## Security boundary

- Core accepts local Unix-socket ingress only.
- Broker and notification credentials are never stored in the ledger, config
  examples, logs, fixtures or replay artifacts.
- Secret values are supplied by runtime environment variables whose names, not
  values, may appear in configuration.
- Unknown schema versions, enum values and provider states fail closed.
- Exposure metrics derived from OI or volume remain labeled proxies. The system
  does not claim to know dealer or market-maker inventory.
- No production deployment, service enablement or network change is authorized
  by this architecture document.
