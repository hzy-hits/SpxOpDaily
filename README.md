# SPX Spark Core

Clean-room Rust production runtime for SPX Spark.

> **Status:** implemented and locally verified migration candidate. It has not
> been deployed, is not connected to production brokers, and has no
> order-placement authority.

The project deliberately implements a small production boundary:

```text
Schwab bridge -----------+
                         +--> spx-core --> SQLite decision/outbox ledger
IBKR Python bridge ------+                     |
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
| `spx-core` | Ingress, quote book, snapshot, readiness, policy, health |
| `spx-ledger` | SQLite/WAL decisions, intents, target state, receipts, DLQ |
| `spx-delivery` | Deterministic renderers and isolated HTTP delivery worker |

`spx-delivery run` and `once` refuse to open the ledger unless both the TOML
contains `network_enabled = true` and the command includes `--allow-network`.
The checked-in example keeps networking disabled.

## Development

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

CI runs the same locked workspace on native Ubuntu x86-64 and ARM64 runners;
the ARM lane matches the target Oracle host architecture.

Project documentation:

- [Architecture](docs/ARCHITECTURE.md)
- [Migration](docs/MIGRATION.md)
- [Operations](docs/OPERATIONS.md)
- [Research boundary](docs/RESEARCH_BOUNDARY.md)
- [State-machine contract](docs/STATE_MACHINES.md)
