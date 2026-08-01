# SPX Spark Core

Clean-room Rust production runtime for SPX Spark.

> **Status:** the isolated core is deployed on Oracle; the normalized mirror
> bridge is implemented and in Phase-1 production-data validation. Rust does
> not connect to a broker and has no order-placement authority.

The project deliberately implements a small production boundary:

```text
Python normalized state --> spx-bridge --> spx-core --> SQLite decision/outbox ledger
                                                    |
                                                    v
                                              spx-delivery

append-only market frames --> Python research / Parquet / DuckDB / replay
```

`spx-core` normalizes one accepted snapshot, applies provider and exact-leg
readiness, produces only `NO_TRADE` or `MANUAL_CANDIDATE`, and atomically stores
the decision with any notification intent. `spx-delivery` is the only network
sender. Its worker owns claim, retry, receipts, uncertain outcomes, dead
letters, and explicit operator acknowledgement/replay. TTL, cancellation and
transport start are one atomic `Claimed -> InFlight` ledger transition.

This repository does **not**:

- connect directly to IB Gateway in its first migration phase;
- train or run unapproved HMM/research models;
- query DuckDB in a live path;
- place real or paper orders;
- treat OI/volume exposure proxies as actual dealer positions.

## Workspace

| Crate | Responsibility |
|---|---|
| `spx-domain` | Strict versioned contracts and invariants |
| `spx-bridge` | Fail-closed JSON mapping, durable cursor and typed ACK client |
| `spx-core` | Ingress, quote book, snapshot, readiness, policy, health |
| `spx-ledger` | SQLite/WAL decisions, intents, target state, receipts, DLQ |
| `spx-delivery` | Deterministic renderers and isolated HTTP delivery worker |

`spx-delivery run` and `once` refuse to open the ledger unless both the TOML
contains `network_enabled = true` and the command includes `--allow-network`.
The checked-in example keeps networking disabled.

The bridge consumes only Python's normalized current-state projection. It does
not open Schwab or IBKR sessions, and it cannot increase the IBKR ticker count.
Each provider update is a bounded, atomic `replace_provider_snapshot` frame;
missing, zero, crossed, stale or session-unknown quotes cannot leave an older
exact leg silently authoritative.

The current Python production strategies are not yet semantically replaceable
by Rust `EvaluationRequestV1`: RTH can produce a single-leg contract and the GTH
level lane uses dynamic 5–40 point verticals, while Rust v1 deliberately accepts
exactly two legs and a 10-point vertical. Quote mirroring may run in production
while Python remains the sole strategy and notification owner.

## Development

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo run -p spx-bridge -- check-config --config config/bridge.example.toml
```

CI runs the same locked workspace on native Ubuntu x86-64 and ARM64 runners;
the ARM lane matches the target Oracle host architecture.

Project documentation:

- [Architecture](docs/ARCHITECTURE.md)
- [Migration](docs/MIGRATION.md)
- [Operations](docs/OPERATIONS.md)
- [Research boundary](docs/RESEARCH_BOUNDARY.md)
- [State-machine contract](docs/STATE_MACHINES.md)
