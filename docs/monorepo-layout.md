# SPX Spark monorepo contract

> **状态（2026-08-07）：本文档描述的 Python/Rust 双运行时所有权是当前部署现状，不再是目标架构。**
> Rust 控制面已冻结并按 `docs/architecture-simplification-execution-plan-v1.md` Phase 6 计划退出；
> 任何新增职责一律落在 Python 侧，不得扩展 Rust 所有权或跨语言 contract。

Status: repository integration contract. This change unifies source and CI; it
does not by itself authorize a production restart, report-owner switch, or
network-delivery change.

## One source of truth

`SpxOpDaily` is the canonical repository for both runtimes:

```text
SPX Spark
├── src/spx_spark/       Python provider, research, replay, and strategy runtime
├── tests/               Python contracts and application tests
├── rust/                Rust operational core workspace
│   ├── crates/          domain, bridge, core, ledger, report, delivery
│   ├── config/          non-secret examples and deployment overlays
│   └── systemd/         Rust system-service templates and Oracle overlays
├── contracts/golden/    versioned cross-runtime wire-contract fixtures
├── config/              Python tracked defaults and deployment examples
├── systemd/             Python user-service units
└── docs/                shared and Python-side architecture/operations evidence
```

The standalone Rust repository is frozen after the monorepo merge. New Rust,
Python, contract, CI, and deployment changes must be committed here so one Git
commit identifies the complete releasable system.

The integration commit must reach `master` through a normal merge commit or a
fast-forward. Squash or rebase merge would discard reachability of the original
Rust commit identities and is not acceptable for this import.

## Preserved history

The Rust workspace was imported at prefix `rust/` without squashing. The import
tip is:

```text
397d5410703e78c3b276e090a6e52e4a03fb8383
```

That commit contains the complete nine-commit linear Rust history through GTH
half-hour scheduling. It remains an ancestor of the monorepo branch, so blame,
audit, and rollback can refer to the original commit identity. Do not replace
the import with a copied directory or a squashed archive.

History acceptance:

```bash
git merge-base --is-ancestor 397d5410703e78c3b276e090a6e52e4a03fb8383 HEAD
git blame rust/crates/spx-report/src/main.rs
git ls-files rust/target  # must print nothing
```

## Runtime ownership

| Concern | Owner | Boundary |
|---|---|---|
| Schwab RTH and IBKR GTH/fallback sessions | Python | GTH SPXW stays IBKR-only; RTH stays Schwab-first; the 100-line IBKR budget remains collector-owned |
| Quote normalization and atomic mirror projections | Python | Rust consumes only bounded, typed files; it does not open broker sessions |
| HMM, range research, DuckDB, Parquet, notebooks, replay, backtests | Python | Research may iterate quickly but has `action_authority=none` until a versioned production contract is accepted |
| Provider/readiness/domain invariants | Rust | Unknown state, stale data, incomplete exact legs, and invalid transitions fail closed |
| Append-only frames and operational SQLite/WAL ledger | Rust | One writer per lane; no second outbox database |
| Half-hour report scheduling, full report validation, outbox and receipts | Rust | Network I/O still requires the existing config gate, CLI gate, and single-owner fence |
| Real or paper order placement | Neither | Automatic ordering remains unavailable |

Keeping both languages does not mean duplicating responsibilities. Python is
the adaptable data/research plane; Rust is the small typed operational plane.
Moving a responsibility requires a versioned contract, a single-writer switch,
observable lineage, and an executable rollback.

## CI and local validation

The only active GitHub Actions workflow is the root `.github/workflows/ci.yml`.
It runs Python tests plus the locked Rust workspace on native x86-64 and ARM64.
A workflow under `rust/.github/` would be ignored by GitHub and must not be
reintroduced.

Shared contract fixtures live under `contracts/golden/`, not inside either
language workspace. Python and Rust must both validate the advisory
`research_context.v2` and `desk_map_projection.v1` fixtures. Rust-only ingress,
decision, intent, and receipt fixtures remain in the same registry with their
producer/consumer ownership documented in `contracts/README.md`; location does
not imply that Python produces those wire shapes.

```bash
# Python
uv sync --locked --all-groups
uv run ruff check .
uv run pytest -q

# Rust
cd rust
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
cargo run --locked -p spx-core -- check-config --config config/core.example.toml
cargo run --locked -p spx-bridge -- check-config --config config/bridge.example.toml
cargo run --locked -p spx-report -- check-config --config config/report.example.toml
cargo run --locked -p spx-delivery -- check-config --config config/delivery.example.toml
```

## Production source migration

After the monorepo branch is reviewed and merged, a separately authorized
deployment may update Oracle's source checkout. The intended source locations
are:

```text
/home/ubuntu/spx-spark/          canonical monorepo checkout
/home/ubuntu/spx-spark/rust/     Rust Cargo workspace
```

Installed Rust binaries, protected config, secrets, ledger, frames, and health
paths do not move merely because the source repository moved. Build from the
locked `rust/` workspace, verify artifact checksums, then install through the
existing Rust operations procedure. Python services continue from the same
checkout.

Repository unification must not be combined with a report-owner cutover. The
first production deployment should be path-only and behavior-neutral:

1. verify the server is on the reviewed monorepo commit with a clean tree;
2. build and validate Rust from `rust/` without stopping collectors;
3. compare release binary checksums and runtime configuration;
4. install only changed artifacts and restart only affected Rust services;
5. verify provider timestamps, bridge ACK lineage, ledger health, report slot,
   outbox state, and an external delivery receipt;
6. leave Python/Rust lane ownership unchanged unless a separate cutover was
   explicitly approved.

Rollback restores the prior checkout and matching Rust release artifact as one
unit. Runtime databases and append-only evidence are preserved; no rollback
deletes or rewrites them.
