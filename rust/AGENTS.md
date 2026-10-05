# SPX Spark Core collaboration guide

<!-- documentation-status: 2026-10-05 -->
> **文档定位：现行运行与参考。** 现行说明；历史段落保留原适用日期。运行状态以实际服务和源字段时钟为准。
> [全仓文档、当前运行状态与合同优先级](../docs/README.md)（目录核对：2026-10-05）。

> **FROZEN（2026-08-07）：Rust workspace 已冻结，只接受生产故障与安全修复。**
> 不新增 crate、wire version、golden fixture、策略、通知、报表和 lifecycle。
> Phase 6 退出已延期，见[当前执行基线](../docs/architecture-simplification-execution-plan-v1.md)。
> Python `build_strategy_decision` 仍是人读策略唯一授权出口；Rust typed decision 不替代它。

This directory is the clean-room Rust production core within the SPX Spark
monorepo. It does
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
cd rust  # when starting from the monorepo root
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
git diff --check
```

Do not deploy, connect to the production broker, or enable network delivery
without explicit user authorization.
