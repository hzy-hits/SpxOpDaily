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

## IBKR Collector

```bash
scripts/run-ibkr-collector.sh --dry-run
scripts/run-ibkr-collector.sh --skip-options
scripts/run-ibkr-collector.sh --force --skip-options
scripts/run-ibkr-collector.sh --force
```

The collector writes normalized IBKR quotes into the same raw/latest-state path as the mock
collector. By default it respects runtime mode and will not connect if IBKR is protected or
outside the allowed schedule. Use `--force` only when you intentionally want this SSH host to
connect to TWS/IB Gateway.

## Schwab Verifier

```bash
scripts/create-schwab-token.sh
scripts/run-schwab-verifier.sh --offline
scripts/run-schwab-verifier.sh --print-config
scripts/run-schwab-verifier.sh
```

The token helper runs Schwab's manual OAuth flow for SSH/headless hosts. The verifier reads
`SCHWAB_ACCESS_TOKEN` or `SCHWAB_TOKEN_FILE`. It checks candidate index quotes, ETF/futures
quotes, and option chains without placing orders.

## Maintenance Dry Run

```bash
scripts/run-maintenance-dry-run.sh
scripts/run-maintenance-dry-run.sh --json --no-write
```

The dry run scans disk usage and cleanup candidates only. It does not delete files.

## Sampling Plan

```bash
scripts/run-sampling-plan.sh --underlier 7500 --expiry 20260706 --next-expiry 20260707
scripts/run-sampling-plan.sh --underlier 7500 --mode degraded --summary-json
```

The planner produces the SPXW hot lane and rolling quote groups for collectors. It does not request market data.

## Mock Data Loop

```bash
scripts/run-mock-collector.sh --underlier 7500 --expiry 20260706 --next-expiry 20260707
scripts/show-latest-state.sh --instrument index:SPX
scripts/show-latest-state.sh --all-providers
```

The mock collector generates normalized `Quote` rows, writes raw JSONL files under
`MARKET_DATA_DATA_ROOT/raw/`, and updates `MARKET_DATA_LATEST_STATE_PATH`. It is the
local no-broker test path for sampler, storage, latest-state, and fallback logic.

## Hyperliquid Collector

```bash
scripts/run-hyperliquid-collector.sh --print-config
scripts/run-hyperliquid-collector.sh --list-coins
scripts/run-hyperliquid-collector.sh --coin SPX --json
scripts/show-latest-state.sh --all-providers --instrument crypto_perp:SPX
```

The Hyperliquid collector uses public `POST /info` endpoints and does not need an API key.
It writes a normalized perp quote plus a Hyperliquid context row with funding, OI,
oracle premium, book imbalance, and recent-trade burst fields.

Important: live verification showed Hyperliquid `SPX` trading around `0.43`, so that symbol
is not official Cboe SPX / S&P 500 index data. Treat it as a Hyperliquid crypto/perp asset
unless a different verified S&P index symbol is found.

## Market Data Model

IBKR and Schwab payloads are normalized through `spx_spark.marketdata` before they reach
sampling, feature, greeks, alert, or dashboard code. Fallback should compare normalized
`Quote.quality` and provider priority instead of branching on provider-specific fields.

## Notes

- Architecture plan: `docs/architecture-plan.md`
- Headless deployment: `docs/headless-deployment.md`
- Data source decision memo: `docs/data-source-decision.md`
- IBKR API research: `docs/ibkr-api-research.md`
- Storage plan: `docs/storage-plan.md`
- Market data model: `docs/market-data-model.md`
- Sampling engine design: `docs/sampling-engine-design.md`
- Operations schedule: `docs/operations-schedule.md`
