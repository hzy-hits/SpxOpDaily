# Migration plan

Status: proposed staged migration. This document is not deployment authority.

## Principles

The Rust core is introduced by subtraction and comparison, not by replacing the
working Python system in one cutover. Each phase must have one writer, a defined
rollback, sanitized evidence, and an explicit acceptance decision.

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

Current repository status belongs to this phase. Documentation, example config
and example units are not evidence of deployment readiness.

## Phase 1: normalized bridge shadow

`spx-bridge` reads Python's sanitized normalized projection and emits versioned
provider-replacement envelopes to a local Unix socket while the existing
production strategy and notification writers remain authoritative. Rust writes
only its own isolated append log, projection and new SQLite ledger. Delivery
stays disabled.

Required evidence:

- Schwab and IBKR timestamps, quality and entitlement survive normalization;
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

Only after the prior gates pass may the Rust decision ledger become authoritative
for a bounded manual-advisory lane. Automatic ordering remains impossible.
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
