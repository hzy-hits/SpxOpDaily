# Market Data Normalization Model

Date: 2026-07-04

## Purpose

IBKR, Schwab, Hyperliquid, and Polymarket will not return the same field names,
symbol formats, timestamps, or data-quality flags.

The collector boundary is therefore:

```text
provider raw payload -> provider adapter -> normalized market data model
```

Everything after that boundary must consume normalized objects only.

## Core Objects

Implemented in `src/spx_spark/marketdata.py`.

### `InstrumentId`

Provider-neutral instrument identity.

Examples:

```text
index:SPX
equity:SPY
future:ES
option:SPX:SPXW:20260706:7500:C
```

Keep provider symbols on the object for debugging, but do not use provider
symbols as the canonical join key.

### `Quote`

Provider-neutral quote row.

Important fields:

- `instrument`
- `provider`
- `bid`, `ask`, `last`, `mark`, `close`
- `bid_size`, `ask_size`, `volume`, `open_interest`
- `quote_time`, `trade_time`, `received_at`
- `source_latency_ms`
- `quality`
- `greeks`
- `sampling_mode`, `sampling_group`

Greek convention:

- `implied_vol` is decimal, not percent. For example, 18% IV is stored as `0.18`.

Computed fields:

- `mid`
- `spread`
- `spread_bps`
- `effective_price`
- `quote_age_ms`

### `MarketDataQuality`

Quality is first-class. Fallback must never blindly prefer a stale preferred
provider over a live fallback provider.

Quality order:

```text
live > frozen > delayed > delayed_frozen > synthetic > unknown > stale > missing/error
```

Provider priority only breaks ties inside the same quality level.

Default provider priority:

```text
IBKR > Schwab > Hyperliquid > Polymarket > internal
```

So:

- live IBKR beats live Schwab
- live Schwab beats stale IBKR
- delayed Schwab beats missing IBKR
- synthetic SPY-derived SPX must be labeled synthetic, never real SPX

## Provider Adapters

Current adapters:

- `quote_from_ibkr_row`
- `quote_from_schwab_payload`
- `quote_from_schwab_option_contract`
- mock quote generation in `spx_spark.mock_collector`

Future adapters should follow the same rule: raw payloads stay at the edge,
normalized `Quote` objects move through the system.

## Fallback Contract

Use `choose_best_quote(quotes)` for a single instrument.

The sampler, feature engine, greeks engine, alert engine, and dashboard should
not contain provider-specific checks like:

```text
if provider == "ibkr" ...
if provider == "schwab" ...
```

Those checks belong only inside adapters and provider health monitors.

## Snapshot Contract

Verifier snapshots may include raw provider summaries for debugging, but should
also include normalized quotes. That lets the verifier answer two questions:

1. Did the provider return something?
2. Can the system consume it without caring which provider returned it?

## Missing Or Degraded Data

Do not substitute silently:

- If SPXW Greeks are missing, mark Greeks missing or degraded.
- If VIX/VVIX/SKEW are missing, mark the vol-regime layer degraded.
- If SPX cash is missing and SPY*10 is used, mark it synthetic.
- If Hyperliquid SPX is used, treat it as sentiment/context, not official SPX.

This keeps alerts explainable and prevents false precision.

## Raw And Latest Storage

Raw normalized quotes are written as JSONL first. Parquet/DuckDB compaction can
come later after real daily footprint is measured.

Current raw path:

```text
data/raw/provider=<provider>/date=YYYY-MM-DD/hour=HH/quotes.jsonl
```

Latest-state path defaults to:

```text
data/latest/state.json
```

On the Oracle host, `.env` points these paths at:

```text
/srv/data/spx-spark/data/raw/...
/srv/data/spx-spark/data/latest/state.json
```

The latest-state file keeps provider-level latest quotes and selected best
quotes. It is not just a single best quote per instrument, because fallback
needs the current Schwab quote to remain available when IBKR becomes stale or
unavailable.

When latest state is read, live/frozen quotes older than
`MARKET_DATA_LATEST_STALE_AFTER_SECONDS` are marked stale and best quotes are
recomputed.

Useful commands:

```bash
scripts/run-mock-collector.sh --underlier 7500 --expiry 20260706 --next-expiry 20260707
scripts/run-ibkr-collector.sh --dry-run
scripts/run-ibkr-collector.sh --force --skip-options
scripts/run-hyperliquid-collector.sh --coin 'S&P500-USDC' --json
scripts/run-hyperliquid-collector.sh --dex xyz --coin xyz:SP500 --json
scripts/show-latest-state.sh --instrument index:SPX
scripts/show-latest-state.sh --all-providers
```

IBKR collector notes:

- default mode respects runtime policy and may only write `provider_state=unavailable`
- `--force` attempts a real TWS/IB Gateway socket connection
- `--skip-options` collects only base index/ETF/futures quotes
- without `--skip-options`, the collector estimates ATM and requests the configured SPXW option set

Hyperliquid collector notes:

- uses public `POST /info` endpoints, no API key
- writes normalized quote rows under `data/raw/provider=hyperliquid/...`
- writes chain-specific context under
  `data/context/provider=hyperliquid/dex=<dex>/coin=<coin>/date=YYYY-MM-DD/hour=HH/asset-context.jsonl`
- context includes mark, oracle, funding, open interest, day notional volume, book imbalance,
  recent trade stats, large trade count, and mark-oracle premium
- live smoke test on 2026-07-04 found HIP-3 dex `xyz` coin `xyz:SP500` around the 7,500
  index level. The CLI aliases `S&P500-USDC`, `S&P500/USDC`, `SP500-USDC`, and `SP500`
  resolve to `dex=xyz`, `coin=xyz:SP500`.
- default-dex `SPX` was near `0.43`, so it is a different Hyperliquid crypto/perp asset.
  Keep it separate from `index:SPX` and from `crypto_perp:xyz:SP500`.
