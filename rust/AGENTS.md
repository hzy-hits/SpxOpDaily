# SPX Spark Core collaboration guide

This repository is the clean-room Rust production core for SPX Spark. It does
not contain broker credentials, research notebooks, HMM training, DuckDB
queries, or real-order execution.

## Safety boundary

- The application is read-only with respect to brokerage accounts. It never
  places, changes, or cancels an order.
- `spx-core` may emit only `NO_TRADE` or `MANUAL_CANDIDATE` decisions.
- Unknown schema versions, enum values, provider states, or incomplete quotes
  fail closed.
- GTH actionable SPXW quotes require IBKR. Frozen Schwab quotes are audit-only.
- RTH is Schwab-first. IBKR validation/fallback must remain explicit.
- IBKR error 10197 means the external/mobile session owns the entitlement. The
  core must not try to evict it.
- Secrets are supplied at runtime through named environment variables. Never
  read, print, persist, or commit their values.

## Architecture boundary

- `spx-domain`: versioned wire and domain contracts; no I/O.
- `spx-bridge`: read-only adapter for the legacy normalized JSON projection;
  no broker SDK, candidate generation, notification delivery, or research.
- `spx-core`: quote book, snapshot, readiness, deterministic policy, health,
  raw append log, and Unix socket ingress.
- `spx-ledger`: the single SQLite/WAL operational ledger and legal transitions.
- `spx-delivery`: target claim, atomic `InFlight` transition, render, retry,
  receipt, and DLQ.
- Python research owns HMM training, replay, backtests, DuckDB, and Parquet.

No crate may import Python source code or read broker/notification secrets. The
bridge may read only the explicitly configured normalized projection and typed
provider-health files. Compatibility is proven with sanitized fixtures, live
read-only inspection, and differential tests.

## Validation

Run, in order:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
git diff --check
```

Do not deploy, connect to the production broker, or enable network delivery
without explicit user authorization.
