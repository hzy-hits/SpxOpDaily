# Runtime configuration

Operational runtime defaults live in `config/runtime.yaml`; Python code loads
them through `spx_spark.runtime_config`. Secrets remain in `.env`.

Each mutable value is represented as a `value` plus a human-readable
`description`. Numeric settings therefore carry their unit and purpose beside
the number instead of appearing as unexplained literals in collector code.

## Scope

The YAML file is the documented source for mutable, non-secret defaults across
market data and operations: Schwab, IBKR snapshot/stream/positions, runtime
policy, Hyperliquid, Polymarket, maintenance, storage, IV surface generation,
notification delivery policy, SPXW sampling, alert thresholds, intraday
shock/strategy windows, post-close review, scheduled push LLM writing and the
research data platform.

Secrets and operator-private endpoints stay out of YAML. API keys, app secrets,
device-specific Bark URLs, Feishu webhook URLs/secrets and other credentials
must be supplied through the environment or an ignored env file. Empty secret
URL fields intentionally default to `""` in code via single-argument env reads.

Algorithm constants and protocol identities stay in code. Examples include
basis-point conversions, schema versions, exchange/protocol multipliers and
un-overridden model thresholds used as mathematical identities or algorithm
definitions. Only mutable operational defaults belong in runtime YAML.

## Schwab symbol table

`schwab.instruments` is the provider mapping table. It separates:

- canonical instrument identity used by storage (`SPX`);
- Schwab quote identity (`$SPX`);
- Schwab option-chain identity (`$SPX`);
- returned option trading classes (`SPX`, `SPXW`).

Schwab exposes SPX and SPXW contracts through the `$SPX` chain. XSP uses
`$XSP`. Collectors and verifiers resolve these values through the table; they
do not carry their own provider aliases.

Configured ETF symbols retain the repository's stable `equity:*` namespace
even when Schwab labels the provider payload as subtype `ETF`. This keeps
Schwab `SPY`, `RSP`, and the eleven sector ETFs compatible with the existing
market-context and alert consumers.

`ES` and `MES` are logical roots. Before each quote batch, the resolver expands
them to a concrete quarterly Schwab symbol such as `/ESU26` or `/MESU26` using
the documented CME Monday-before-expiration roll boundary. The resolver changes
contracts at 18:00 New York time on the preceding Sunday, when that Monday's
Globex trading session begins. Storage preserves the concrete provider symbol
while publishing the stable canonical identities `future:ES` and `future:MES`,
so consumers do not change at rollover.

The hot SPX reference universe includes `SPY`, `RSP`, the VIX-family indexes,
and eleven sector ETFs. Sector rows feed one aggregate breadth feature; they do
not create individual human alerts. Redundant S&P 500 ETFs and leveraged or
inverse products are present as on-demand mappings with `collect_quote: false`
to avoid unnecessary 15-second raw-data growth. The current State Street ticker
is `SPYM`; obsolete `SPLG` is intentionally absent.

## Position-awareness boundary

Schwab market data and the SPX breadth/option analysis do not require IBKR
account polling. IBKR position polling remains disabled by default and isolated
from the market-data collectors. When it is disabled, the system explicitly
reports `disabled_no_account_visibility`: it must not infer that the account is
flat and cannot provide position-open/close, quantity-change, or book-PnL
alerts. Automated stops and time exits are not implemented even when polling is
enabled. This is safe for the current observation-only system, but the human or
broker UI remains responsible for live-position risk management.

Before re-enabling polling after a blind interval, reconcile the persisted
position-event state with the broker snapshot. Otherwise the first complete
snapshot can correctly report the net difference but make changes from the
whole blind interval look newly observed.

## Override order

1. Environment variables and `.env` values override a runtime default.
2. `SPX_SPARK_RUNTIME_CONFIG` may point to another YAML file.
3. Otherwise the repository `config/runtime.yaml` is loaded.

Production uses `schwab,ibkr,...` provider priority. Freshness and quality are
still evaluated before provider preference, so missing or stale Schwab data
falls back to usable IBKR data.

Automatic transitions are configured under `provider_failover`. Health
observation is enabled independently from the final IBKR stream-control switch,
which remains off during Schwab WebSocket shadow acceptance. See
[schwab-primary-ibkr-fallback.md](schwab-primary-ibkr-fallback.md).

`schwab.streaming.mode` controls the WebSocket owned by the OAuth/gateway
process. `shadow` writes a separate latest-state file for RTH comparison,
`live` feeds the production latest-state selector, and `off` creates no
WebSocket thread. Live-owned symbols are removed from the slower REST quote
batch, and `symbol_refresh_interval_seconds` controls active ES/MES contract
re-resolution for quarterly rollover. The default is `off`: the deployed
Schwab developer app currently has Market Data product access only, without
Trader API entitlement, so the streamer login (`/trader/v1/userPreference`)
returns `401` even though REST quotes succeed. See
[schwab-primary-ibkr-fallback.md](schwab-primary-ibkr-fallback.md) before
changing this to `shadow` or `live`.

Every setting consumed with `runtime_value("path.to.setting")` must have both
`value` and `description`. The architecture tests reject new literal defaults
passed directly to `env_bool`, `env_int`, `env_float`, `env_str`, `env_csv` or
`env_csv_preserve`; use `runtime_value` or `runtime_csv` instead.
