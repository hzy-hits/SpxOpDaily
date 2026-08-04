# Migration plan

Status: Oracle core/bridge shadow is active; half-hour report ownership is a
separate, reversible lane. This document is not deployment authority.

## Principles

Rust is introduced by subtraction and explicit ownership. Each mutable lane has
one writer, a defined rollback and observable lineage. Scheduled Desk Maps do
not need to wait for strategy-policy parity because they are an informational
lane with `action_authority=none`; this does not weaken readiness for
`MANUAL_CANDIDATE` or any future trade-ready lane.

The legacy repository remains the provider integration and research reference
during migration. No Rust crate imports its source code or runtime secrets.
Compatibility is demonstrated with versioned fixtures and differential replay.

## Phase 0: contracts and local verification

Scope:

- freeze `spx-domain` wire versions and enum sets;
- keep `NO_TRADE` and `MANUAL_CANDIDATE` as the only actions;
- validate GTH IBKR-only and RTH Schwab-first readiness;
- validate the single SQLite ledger and `uncertain` delivery outcome;
- run formatting, lint and tests in CI.

Exit gate:

- malformed, stale, wrong-provider and one-sided exact-leg fixtures fail closed;
- error `10197` maps to `external_session_owns` and cannot authorize GTH;
- duplicate ingress and decisions are idempotent;
- a stale `in_flight` attempt becomes `uncertain` rather than retrying;
- no network or broker credential is needed by tests.

The contract suite belongs to this phase; the Oracle core/bridge shadow has
already advanced to Phase 1. Documentation, example config and example units
are not evidence that report ownership has changed.

## Phase 1: normalized bridge shadow

`spx-bridge` reads Python's sanitized atomic normalized, research-context and
desk-map projections and emits versioned envelopes to a local Unix socket while
the existing production report writer remains authoritative. Rust writes only
its own isolated append log, latest projections and SQLite ledger. Delivery
stays disabled.

Required evidence:

- Schwab and IBKR timestamps, quality and entitlement survive normalization;
- `research_context.v2` and `desk_map_projection.v1` pass strict schema,
  freshness, fingerprint and typed-ACK checks independently of quote health;
- quote batches are monotonic and idempotent;
- the total IBKR ticker budget remains owned by the Python collector/supervisor;
- `10197` backs off without Gateway restart or session eviction;
- no dual writer touches the same operational database or projection.

Implemented safeguards include a durable monotonic cursor, exact pending-frame
retry, typed ACK/disposition, atomic full-provider replacement, session-aware
quote identity, bounded source/frame sizes, and a read-only inspection command.
The Oracle frame store also has an append-time free-space reserve and a bounded
completed-day retention timer; exhaustion fails ingress closed without touching
the Python runtime.
Phase 1 still requires live RTH and GTH evidence; a weekend stale snapshot proves
fail-closed behavior but not live parity.

Rollback: stop the bridge first. The existing Python runtime is unchanged. If
the core binary is also rolled back, restore the matching core TOML and unit
from the same release backup before restarting it; strict older binaries reject
newer configuration fields such as the raw-log free-space reserve.

## Half-hour report lane: projection to Rust ownership

This lane can move independently of Rust strategy-policy parity:

1. Python keeps provider sessions and research computation and atomically
   publishes `desk_map_projection.v1` plus optional embedded
   `research_context.v2`.
2. Bridge/core validate, durably audit and expose the latest accepted desk map.
3. `spx-report` owns active GTH/RTH `:00`/`:30` ET slots, calls the fixed
   `deepseek-v4-flash` model with thinking enabled and
   `reasoning_effort=max` plus JSON Output, validates the complete eight-section response, and
   persists one `scheduled_report` v2 intent per source projection/slot.
4. `spx-delivery` claims that v2 intent from the same SQLite/WAL ledger, sends
   the complete body without character/line compression, and records attempts
   and receipts.

The existing user-level `spx-spark-order-map-status.timer` must continue
running because it produces the atomic projection. It stops being a report
writer only when its environment contains `SPX_RUST_REPORT_OWNER=true`.

### Shadow acceptance

- keep checked-in report and delivery network gates disabled;
- prove Python projection ID equals bridge/core's accepted projection ID;
- prove malformed, stale, session-mismatched and expired projections do not call the
  provider or create an intent;
- prove duplicate polling and restarts produce one stable ET slot;
- prove the slot grace bounds when generation may start; once started, a
  completed report remains eligible until its source projection expires;
- exercise DeepSeek through an injected transport and verify that
  `finish_reason=length`, missing sections and unknown sections fail closed;
- prove v2 render/claim/receipt end to end with an injected delivery transport.

### Single-writer production switch

Perform the change between report slots:

1. stop the Python status timer briefly, leaving collectors and other Python
   notification lanes running, and wait until the status oneshot itself is
   inactive;
2. set `SPX_RUST_REPORT_OWNER=true` in the Python runtime environment;
3. run one Python status service invocation and verify it atomically publishes
   a fresh projection but creates no legacy scheduled-report outbox row;
4. confirm Rust bridge/core accepted the same projection ID and that no Rust
   scheduled report already exists for the next slot;
5. install target-aligned report and delivery configs and enable both explicit
   network gates;
6. only after steps 1--5 pass, create the regular root-owned, non-writable
   `/etc/spx-spark-core-shadow/rust-report-owner.enabled` marker. The production
   report and delivery units have `ConditionPathExists` on this marker and must
   remain unable to start while Python owns report enqueueing;
7. start Rust delivery first and then the Rust report system service;
8. restart the Python timer so it continues refreshing projections;
9. accept the cutover only after one complete report has matching source
   projection/slot lineage and a confirmed external receipt.

The marker is a startup fence, not a live process kill switch. Both
network-capable services retain `AF_UNIX` for DNS/resolver operation, while
their systemd sandboxes make `/run/spx-spark-core-shadow` inaccessible so they
cannot reach the core ingress socket.

Do not stop the persistent Python notification worker globally: it may still
own unrelated alert lanes and may drain pre-cutover legacy work. The ownership
switch suppresses only newly scheduled Python Desk Maps.

### Rollback

Disable and stop `spx-report` first so no new v2 intent can be created, then immediately
remove `/etc/spx-spark-core-shadow/rust-report-owner.enabled` so the writer
cannot restart. Disable and stop Rust delivery, then inspect scheduled-report targets and
resolve every `pending`, `claimed`, `in_flight` or `uncertain` outcome; do not
delete the ledger and keep the marker absent. At the next clean slot boundary,
stop the Python status timer, set
`SPX_RUST_REPORT_OWNER=false`, run one status invocation, and restart the timer.
Do not re-enable Python while an unexpired Rust intent for the same economic
slot can still be delivered.

The first cutover retains the established single-user `ubuntu` runtime trust
boundary. Root-owned environment files and systemd path restrictions are
defense in depth, not process-level isolation between services sharing a UID;
dedicated service identities are a separate hardening phase.

## Phase 2: differential replay

At post-close, build one immutable replay artifact from the day's normalized
frames. Re-run both implementations at the same decision timestamps and compare:

- provider selection and readiness reasons;
- exact contract identities and NBBO ages/skew;
- `NO_TRADE` versus `MANUAL_CANDIDATE`;
- semantic notification identity and TTL;
- ledger transitions and health counts.

Differences require a classified reason and a fixture before they are accepted.
Do not compare only final alert counts; missing-data abstentions and delivery
outcomes are part of the result.

## Phase 3: manual-advisory canary

This phase applies to decision-linked manual advisories, not the independent
scheduled Desk Map lane above. Only after the prior strategy gates pass may the
Rust decision ledger become authoritative for a bounded manual-advisory lane.
Automatic ordering remains impossible.
Delivery must first run with a non-human sink, then a single explicitly approved
human target. There must be exactly one delivery owner.

Required evidence includes multiple complete RTH sessions, GTH entitlement-loss
events, restart recovery, TTL expiry, uncertain transport outcome, DLQ inspection
and rollback rehearsal. A service being `active` is not sufficient evidence.

Rollback: disable the Rust producer and delivery owner together, preserve the
ledger and append log, and restore the previously designated single writer. Do
not delete or rewrite evidence during rollback.

The existing Python lane cannot be renamed as Rust v1 parity: its RTH terminal
record may contain one leg and its GTH level strategy selects 5–40 point
verticals. Rust v1 accepts only a two-leg 10-point vertical. Before Phase 3,
either version and port those existing contracts exactly, or introduce a
separately named fixed-10-point advisory lane with independent evidence and a
lane-specific Python notification-owner switch.

## Phase 4: bounded production ownership

Production ownership requires explicit user authorization in a separate change.
Before deployment, confirm:

1. the intended commit/artifact and clean build provenance;
2. local and CI validation are green;
3. the target host configuration contains no repository secrets;
4. only affected services are installed or restarted;
5. post-start provider timestamps, exact-leg readiness, ledger health and actual
   delivery receipts are verified;
6. rollback remains executable without data loss or double delivery.

## Deliberately not migrated

- HMM fitting and calibration;
- DuckDB queries, notebooks and backtests;
- large-scale replay processing;
- Parquet compaction;
- provider SDK sessions in the first phase;
- real or paper order placement;
- claims about actual dealer or market-maker positions.

Those responsibilities remain in Python research or provider bridges. A model
may cross into production only as a separately versioned, frozen inference
contract after forward evidence and explicit approval; it never gains authority
to bypass deterministic readiness and risk gates.
