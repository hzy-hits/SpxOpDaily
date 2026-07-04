# SPX Spark

Near-real-time SPX/SPXW 0DTE dashboard and alert research system.

Current scope:

- Verify IBKR market data permissions.
- Record the boundary between live, delayed, and missing feeds.
- Keep the project isolated from the machine's default Codex setup.
- No automatic order placement.

## Quick Start

```bash
cd /home/ubuntu/spx-spark
cp .env.example .env
uv sync
scripts/run-ibkr-verifier.sh
```

IBKR requirements:

- TWS or IB Gateway must be running.
- API socket must be enabled.
- Use paper first: IB Gateway paper usually listens on `127.0.0.1:4002`.

## Isolated Codex Wrapper

```bash
scripts/run-codex-isolated.sh "summarize this project"
```

The wrapper uses project-local `.codex-home` and `.codex-log` directories. It does not modify `~/.codex`.

## Runtime Mode

```bash
uv run spx-spark-runtime-mode status
uv run spx-spark-runtime-mode ibkr-on --ttl-minutes 120 --reason "manual monitor request"
uv run spx-spark-runtime-mode protected --ttl-minutes 180 --reason "phone trading"
uv run spx-spark-runtime-mode clear
```

The runtime mode file is local state under `runtime/`. It lets an agent temporarily allow or block IBKR collection without changing permanent config.

## Schwab Verifier

```bash
scripts/run-schwab-verifier.sh --offline
scripts/run-schwab-verifier.sh --print-config
scripts/run-schwab-verifier.sh
```

The verifier reads `SCHWAB_ACCESS_TOKEN` or `SCHWAB_TOKEN_FILE`. It checks candidate index quotes, ETF/futures quotes, and option chains without placing orders.

## Maintenance Dry Run

```bash
scripts/run-maintenance-dry-run.sh
scripts/run-maintenance-dry-run.sh --json --no-write
```

The dry run scans disk usage and cleanup candidates only. It does not delete files.

## Notes

- Architecture plan: `docs/architecture-plan.md`
- Headless deployment: `docs/headless-deployment.md`
- Data source decision memo: `docs/data-source-decision.md`
- IBKR API research: `docs/ibkr-api-research.md`
- Storage plan: `docs/storage-plan.md`
- Sampling engine design: `docs/sampling-engine-design.md`
- Operations schedule: `docs/operations-schedule.md`
