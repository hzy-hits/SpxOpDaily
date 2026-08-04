# SPX Spark Core

Clean-room Rust production runtime for SPX Spark.

This workspace now lives at `rust/` inside the SPX Spark monorepo. The monorepo
is the source of truth; the former standalone repository is retained only as a
read-only history source. The import preserved the original Rust commits
without squashing.

> **Status:** the isolated core and normalized mirror bridge run on Oracle.
> Half-hour report/delivery ownership is implemented as a separate cutover lane
> but is not changed merely by these checked-in files. Rust does not connect to
> a broker and has no order-placement authority.

The project deliberately implements a small production boundary:

```text
Python normalized/research/desk projections --> spx-bridge --> spx-core
                                                               |
                                                               +--> latest projections
                                                               +--> append-only frames
                                                               +--> SQLite/WAL ledger
                                                                          ^
GTH/RTH :00/:30 ET --> spx-report --> full DeepSeek desk report -----------+
                                                                          |
                                                                          v
                                                                    spx-delivery

append-only market frames --> Python research / Parquet / DuckDB / replay
```

`spx-core` normalizes one accepted snapshot, applies provider and exact-leg
readiness, produces only `NO_TRADE` or `MANUAL_CANDIDATE`, and stores durable
latest projections. `spx-report` owns GTH/RTH `:00`/`:30` ET scheduling and persists
a complete `scheduled_report` intent. `spx-delivery` is the only notification
sender. Its worker owns claim, retry, receipts, uncertain outcomes, dead
letters, and explicit operator acknowledgement/replay. TTL, cancellation and
transport start are one atomic `Claimed -> InFlight` ledger transition.

This repository does **not**:

- connect directly to IB Gateway in its first migration phase;
- fit or train HMM/research models inside the Rust live path;
- query DuckDB in a live path;
- place real or paper orders;
- treat OI/volume exposure proxies as actual dealer positions.

The advisory research lane accepts strict atomic `research_context.v2` and
`desk_map_projection.v1` files. Causal HMM/range context may appear in the
half-hour Desk Map, but it remains `action_authority=none` and cannot create a
trade-ready event, bypass readiness or place an order. The standalone research
projection never creates an intent; only the independently scheduled desk-map
lane may create an informational `scheduled_report` intent.

## Workspace

| Crate | Responsibility |
|---|---|
| `spx-domain` | Strict versioned contracts and invariants |
| `spx-bridge` | Fail-closed JSON mapping, durable cursor and typed ACK client |
| `spx-core` | Ingress, quote book, snapshot, readiness, policy, health |
| `spx-ledger` | SQLite/WAL decisions, intents, target state, receipts, DLQ |
| `spx-report` | Half-hour GTH/RTH schedule, DeepSeek writer, full report validation |
| `spx-delivery` | Deterministic renderers and isolated HTTP delivery worker |

`spx-report` and `spx-delivery` refuse outbound I/O unless both their TOML gate
is true and the command includes `--allow-network`. Checked-in examples keep
networking disabled. The report model is fixed to `deepseek-v4-flash` with
thinking enabled and `reasoning_effort=max`; `flash-max` is a mode, not another
model ID. A `finish_reason=length` response is rejected, and all eight report
sections are persisted and rendered without line or character truncation.

The bridge consumes only Python's bounded atomic normalized, research-context
and desk-map projections. It does not open Schwab or IBKR sessions, and it
cannot increase the IBKR ticker count.
Each provider update is a bounded, atomic `replace_provider_snapshot` frame;
missing, zero, crossed, stale or session-unknown quotes cannot leave an older
exact leg silently authoritative.

The current Python production strategies are not yet semantically replaceable
by Rust `EvaluationRequestV1`: RTH can produce a single-leg contract and the GTH
level lane uses dynamic 5–40 point verticals, while Rust v1 deliberately accepts
exactly two legs and a 10-point vertical. Python therefore remains strategy
owner. The half-hour informational lane is independent: the Python timer keeps
producing the atomic desk projection, while `SPX_RUST_REPORT_OWNER=true` fences
off Python enqueue so Rust alone owns schedule, writer, ledger/outbox and
delivery.

## Development

From the monorepo root, enter the Rust workspace first:

```bash
cd rust
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
cargo run --locked -p spx-bridge -- check-config --config config/bridge.example.toml
cargo run --locked -p spx-report -- check-config --config config/report.example.toml
```

CI runs the same locked workspace on native Ubuntu x86-64 and ARM64 runners;
the ARM lane matches the target Oracle host architecture.

Project documentation:

- [Architecture](docs/ARCHITECTURE.md)
- [Migration](docs/MIGRATION.md)
- [Operations](docs/OPERATIONS.md)
- [Research boundary](docs/RESEARCH_BOUNDARY.md)
- [State-machine contract](docs/STATE_MACHINES.md)
