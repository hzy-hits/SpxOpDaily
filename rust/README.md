# SPX Spark Core

<!-- documentation-status: 2026-10-05 -->
> **文档定位：现行运行与参考。** 现行说明；历史段落保留原适用日期。运行状态以实际服务和源字段时钟为准。
> [全仓文档、当前运行状态与合同优先级](../docs/README.md)（目录核对：2026-10-05）。

> **FROZEN：本 workspace 只接受生产故障与安全修复。Phase 6 退出已延期，见
> [当前执行基线](../docs/architecture-simplification-execution-plan-v1.md)。**

Clean-room Rust production runtime for SPX Spark.

This workspace now lives at `rust/` inside the SPX Spark monorepo. The monorepo
is the source of truth; the former standalone repository is retained only as a
read-only history source. The import preserved the original Rust commits
without squashing.

> **Status (2026-10-05):** core, bridge, report and delivery are deployed Oracle
> system services. The retained `shadow` path/name does not imply report delivery
> is disabled. Rust owns half-hour and data-recovery Desk Maps; Python remains
> the broker and final strategy owner. No order placement is implemented.

The project deliberately implements a small production boundary:

```text
Python normalized/research/desk projections --> spx-bridge --> spx-core
                                                               |
                                                               +--> latest projections
                                                               +--> append-only frames
                                                               +--> SQLite/WAL ledger
                                                                          ^
GTH/RTH slots + recovery --> spx-report --> validated desk report -------+
                                                                          |
                                                                          v
                                                                    spx-delivery

append-only market frames --> Python research / Parquet / DuckDB / replay
```

`spx-core` normalizes one accepted snapshot, applies provider and exact-leg
readiness, produces only `NO_TRADE` or `MANUAL_CANDIDATE`, and stores durable
latest projections. `spx-report` owns GTH/RTH `:00`/`:30` ET scheduling and persists
a complete `scheduled_report` intent. `spx-delivery` is the sole sender for
this Rust-owned lane; Python candidate notifications retain their own owner. Its worker owns claim, retry, receipts, uncertain outcomes, dead
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
| `spx-report` | Half-hour GTH/RTH reports, bounded model writer, deterministic recovery/fallback validation |
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
Versioned wire fixtures are shared at `../contracts/golden/`; their ownership
and producer/consumer roles are recorded in `../contracts/README.md`.
Each provider update is a bounded, atomic `replace_provider_snapshot` frame;
missing, zero, crossed, stale or session-unknown quotes cannot leave an older
exact leg silently authoritative.

Python production includes authorized directional spreads, butterflies and iron
condors, with their own versioned management contracts. These cannot be
replaced by Rust `EvaluationRequestV1`'s limited two-leg geometry. Full Python
strategy decisions remain outside Rust wire. With `SPX_RUST_REPORT_OWNER=true`,
Python publishes the bounded Desk projection while Rust owns its report lane.

A real data-capability recovery uses the same lane between half-hour slots,
with `recovery:<projection_id>` deduplication and no model writer. Partial data
recovery is not proof that all exact legs or Greeks are available. See
[current acceptance](../docs/desk-data-recovery-2026-10-05.md).

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
