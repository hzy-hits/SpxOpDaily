# SPX Spark

<!-- documentation-status: 2026-10-18 -->

[English](README.md) · [简体中文](README.zh-CN.md)

**SPX/SPXW market intelligence, strategy research, and Desk Maps for human decisions.**

SPX Spark collects broker market data, evaluates strategy candidates, publishes
market summaries, and replays archived quotes for research. Schwab is the regular
trading hours (RTH) source; IBKR Paper serves global trading hours (GTH) and fallback.
All candidates require human decisions: `automatic_ordering=false`, with no live
or paper order execution.

[Architecture & diagrams](docs/architecture/README.md) ·
[Interactive HTML guide](docs/architecture/service-flow.html) ·
[Deployment](docs/headless-deployment.md#cloud-delivery-cutover-2026-10-17) ·
[Data capabilities](docs/market-data-capability-matrix.md) ·
[Document catalog](docs/README.md#document-catalog) · [Contributor rules](AGENTS.md)

## What the system does

- **Collect and verify data.** Preserve broker quotes, source identities, and
  separate availability clocks for prices, Greeks, and open interest.
- **Evaluate manual candidates.** Apply the current SPX strategy contracts for
  directional debit spreads, iron condors, and butterflies. Broader scanners and
  research views do not automatically authorize production strategies.
- **Publish Desk Maps.** Keep market context, candidate authorization, scheduled
  reports, and delivery receipts distinct.
- **Research from original quotes.** Replay broker observations with causal
  timestamps; preserve missing data and unsuccessful outcomes in the results.
- **Separate production from development.** Build in GitHub Actions, run live
  services on Oracle, and perform scheduled historical research at home.

## Current runtime overview

Architecture baseline: **2026-10-17**. The diagram describes the deployed delivery
and research paths; it is not a live health dashboard.

![System overview: cloud development and delivery, Oracle production, and home research](docs/architecture/diagrams/system-overview.en.svg)

| Location | Responsibility |
| --- | --- |
| Cloud development agents / developers | Change code and submit pull requests |
| GitHub Actions, Releases, and GHCR | Test, build AMD64/ARM64 artifacts, publish immutable versions, and deploy approved mainline code |
| Oracle VPS | Live collectors, strategy evaluation, reports, notifications, and operational state; prebuilt ARM64 packages under existing systemd services |
| Raspberry Pi | Prefect scheduling, job metadata, and lightweight daily archive transfer |
| Linux research laptop | Replay and backtests using a pinned AMD64 container and the dedicated `/srv/research` disk |
| NAS | Verified quote snapshots, manifests, research outputs, and backup receipts |
| Mac | Review, operations, and on-demand research; not a registered scheduled worker |

### Service and data flow

The collectors, Python Core, Python Worker, and Rust report lane have separate
owners. Core runs concurrent samplers, market-feature and shock tasks; the picture
below follows the handoffs, not a single synchronous loop.

![Service flow: independent collectors, concurrent Core tasks, decision commit, Python notifications and the separate Rust report lane](docs/architecture/diagrams/runtime.en.svg)

[Open full-size diagram](docs/architecture/diagrams/runtime.en.svg) ·
[Download interactive guide — English / 中文](https://raw.githubusercontent.com/hzy-hits/SpxOpDaily/master/docs/architecture/service-flow.html) ·
[Original HTML atlas — Chinese historical snapshot](docs/architecture/reference/README.md)

The interactive guide has separate views, zoom controls, searchable flow details,
and source references. GitHub displays HTML source; download the file and open it
in a browser. The original atlas is preserved at its `97fc378` source baseline;
current cloud delivery and home research are documented separately in the guide.

Python owns broker ingestion, analytics, research, and the final strategy decision.
`build_strategy_decision` is the sole manual-candidate authorization point. Rust
retains typed readiness, frames, and the scheduled Desk Map/report-delivery lane;
it does not connect to brokers or choose Python strategy legs. Full
`strategy_decision` objects do not cross the Rust wire.

See the [runtime diagram](docs/architecture/README.md#runtime-and-data-ownership),
[decision protocol](docs/architecture/decision-protocol.md),
[monorepo contract](docs/monorepo-layout.md), and
[notification contract](docs/notification-architecture.md) for exact ownership.
The existing Python/Rust split is retained; no language migration is assumed.

## Delivery and operations

Pull requests run Python and Rust checks plus native AMD64/ARM64 package validation.
After a change merges to **`master`**, eligible CI runs publish a
`runtime-<full-commit-SHA>` GitHub Release and GHCR images. A restricted deployment
receiver verifies the ARM64 package and updates Oracle's existing services, with
runtime checks and rollback to the previous binaries on failure.

Oracle does not pull source, resolve dependencies, or compile during normal
deployment. It uses the **native runtime bundle**, while the home research laptop
uses the **container image from the same commit**. Credentials, databases, and
broker Gateway remain under their existing host ownership. Migration changes
block automatic deployment; binary rollback does not undo database changes.

Documentation-only mainline changes do not match the deployment workflow's push
paths. They update this documentation without restarting production.

[Delivery diagram](docs/architecture/README.md#build-release-and-deployment) ·
[Deployment runbook](docs/headless-deployment.md#cloud-delivery-cutover-2026-10-17) ·
[Runtime schedule](docs/operations-schedule.md) ·
[Rust operations](rust/docs/OPERATIONS.md)

## Historical data and weekend research

- **Weekdays, 17:30 America/New_York:** the Pi pulls closed, verified quote
  partitions from Oracle over the existing private connection and streams them to
  NAS. NAS rereads and verifies the files before publishing a snapshot.
- **Saturday, 10:00 Asia/Shanghai:** the laptop pins an archived dataset, effective
  production-policy snapshot, and matching code image, then replays the current
  SPX production strategy with RTH/GTH results kept separate.
- **After each run:** reports, logs, input manifests, code/configuration identities,
  and checksums are backed up to NAS. Pending result backups can be retried.

The home orchestration deployment is managed separately from this application
repository. Cloning this repository does not provision Prefect, NAS, or a worker.
The [research architecture](docs/architecture/README.md#archive-and-research-lifecycle)
documents the integration and its current limits.

## Local development

Use a development machine with **Python 3.12+**, **uv**, and, for Rust work, the
toolchain pinned by [CI](.github/workflows/ci.yml). Broker access requires separate
local configuration; the commands below do not start production services.

```bash
git clone https://github.com/hzy-hits/SpxOpDaily.git
cd SpxOpDaily
uv sync --frozen
uv run spx --help

# Focused checks; choose additional tests for the change being made.
uv run pytest -q tests/test_iron_condor.py tests/test_desk_map_projection_export.py
uv run lint-imports
uv run ruff check src tests scripts
git diff --check
```

Read [AGENTS.md](AGENTS.md) before changing behavior and the
[module architecture](module-architecture.md) before moving code. Full release
validation and Rust commands are in the
[monorepo guide](docs/monorepo-layout.md#ci-and-local-validation).

## Repository map

| Path | Contents |
| --- | --- |
| `src/spx_spark/` | Python collectors, strategy, research, Core/Worker, and operations |
| `rust/` | Existing typed core, bridge, ledger, report, and delivery |
| `contracts/golden/` | Shared wire examples and acceptance fixtures |
| `tests/`, `rust/crates/*/tests/` | Python/Rust behavior, causality, economics, and boundary checks |
| `.github/workflows/ci.yml`, `Dockerfile`, `scripts/deploy-release.py` | Cloud build and guarded production delivery |
| `config/`, `systemd/` | Tracked defaults, deployment examples, and service definitions |
| `docs/architecture/` | English/Chinese guides, spaced SVG diagrams, interactive HTML, and the original source atlas |
| `docs/`, `site/` | Contracts, operations, historical evidence, notification-image entry, and historical review pages |

## Interpretation and limits

The global strategy baseline is `strategy_policy.bootstrap.v69`, with subsequent
scoped changes listed in the [strategy contract index](docs/README.md#strategy-contracts).
Production is primarily 0DTE. Multi-expiry queries or research do not establish
continuous quote coverage or authorize a 3–5 DTE production strategy.

A healthy process or advancing market stream does not prove that Greeks, open
interest, exact option legs, strategy permission, or notification delivery are
ready. Each has its own evidence and clock. Simulated historical path frequencies
are not calibrated win probabilities or evidence of a profitable strategy.

Weekend replay evaluates the **current code and captured policy on historical
quotes**. It is not an exact reconstruction of historical live decisions or fills:
initial feature state, historical model/macro/provider state, market impact, and
portfolio constraints remain incomplete. Historical strategy cards, `NO_TRADE`
records, and notification history cannot select backtest samples or prove edge.

The former live-surface/replay website is retired. The
[notification-image entry](site/spxw-surface/README.md) and
[historical strategy review](site/strategy-review/README.md) are not live portfolio
or complete market-data interfaces. Dated incident reports retain their original
observations; see the [document catalog](docs/README.md) for evidence and precedence.
