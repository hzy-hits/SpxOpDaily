# Runtime configuration

Operational runtime defaults live in `config/runtime.yaml`; Python code loads
them through `spx_spark.runtime_config` and, for new composition roots, through
typed `spx_spark.settings.load_settings()` (`AppSettings`). Secrets remain in
`.env` and are never loaded during unit tests (`SPX_SPARK_DISABLE_DOTENV=1` plus
`tests/fixtures/runtime.defaults.yaml`).

Each mutable value is represented as a `value` plus a human-readable
`description`. Numeric settings therefore carry their unit and purpose beside
the number instead of appearing as unexplained literals in collector code.

## Test isolation

Unit tests pin `SPX_SPARK_RUNTIME_CONFIG` to `tests/fixtures/runtime.defaults.yaml`
and disable `.env` loading plus `config/runtime.local.yaml` overlays
(`SPX_SPARK_DISABLE_DOTENV=1`, `SPX_SPARK_DISABLE_RUNTIME_OVERRIDES=1`). Deployment
edits to the workspace `.env`, local overrides, or live `config/runtime.yaml` must
not change unit-test outcomes. When product defaults change intentionally, update
both `config/runtime.yaml` and the frozen fixture.

Machine-local deployment values belong in `config/runtime.local.yaml` (gitignored;
see `config/runtime.local.yaml.example`), not in the tracked defaults file.

## Scope

The YAML file is the documented source for mutable, non-secret defaults across
market data and operations: Schwab, IBKR snapshot/stream/positions, runtime
policy, Hyperliquid, Polymarket, maintenance, storage, IV surface generation,
notification delivery policy, SPXW sampling, alert thresholds, intraday
shock/strategy windows, post-close review, scheduled push LLM writing, the
Steven observe-only guidance block (`steven.*`, default disabled), and the
research data platform.

The production GTH data budget gives IBKR `84` SPXW lines (`56` persistent hot
contracts plus `28` rotating contracts) and `0` SPY option lines; Schwab owns
the SPY option lane. This prevents a stale local environment override from
silently spending scarce IBKR lines on the wrong product. A two-second
flush cadence advances one 28-contract rotation slice while the 56-contract
hot lane remains continuously subscribed. The adaptive capacity tracker lowers
the plan after ticker-limit evidence. From 30 minutes before the actual RTH close
(15:30 ET on normal sessions, 12:30 ET on scheduled early closes) to 17:00 ET, acquisition
rolls its front contract to the next trading day's SPXW while analytics retains
the completed session until the normal 17:00 research rollover. The remaining
Phase 1 defaults include `sampling.hot_window_points=55`, Schwab REST
`collection.interval_seconds=5` with per-instrument chain tiers (A: `$SPX` at
5s/`strikeCount=40`; B: SPY/QQQ/IWM/XSP at 15s), and IBKR session-hardening
keys `ibkr_stream.freeze_quotes_on_connectivity_loss` plus
`provider_failover.ibkr_recovery_observations`. Environment variables still
override YAML (notably a local `IBKR_STREAM_MAX_OPTION_LINES` may pin the stream
below the YAML default until removed).

The Paper username receives shared Live subscriptions only while the sharing
Live username is not consuming them in TWS, Mobile, or Client Portal. IBKR
error `10197` is therefore an entitlement-owner conflict, not a line-capacity
or rotation failure. The collector preserves that reason through its cooldown
and probes every 15 seconds so it rebuilds the `54 + 24` SPXW plan promptly
after the Live session releases the data.

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
account polling. The normal Oracle deployment connects the IBKR Paper username
on port `4002` for GTH and fallback market data, so Paper account positions are
not evidence about the user's Live account. Account reads, client-172 position
shadowing, and the legacy client-174 poller remain disabled in this mode.

When position visibility is disabled, the system explicitly reports
`disabled_no_account_visibility`: it must not infer that the Live account is
flat and cannot provide position-open/close, quantity-change, or book-PnL
alerts. Automated stops and time exits are not implemented. IBKR Mobile or the
broker UI remains responsible for live-position risk management.

The position implementation remains in the repository for paper execution
testing and a future approved Live executor. Any reactivation must persist an
explicit `paper` or `live` broker-environment label; simulated positions must
never enter real-position alerts or risk gates.

Before re-enabling polling after a blind interval, reconcile the persisted
position-event state with the broker snapshot. Otherwise the first complete
snapshot can correctly report the net difference but make changes from the
whole blind interval look newly observed.

## Override order

1. The repository `config/runtime.yaml` supplies tracked defaults.
2. `SPX_SPARK_RUNTIME_CONFIG` may select another complete base YAML file.
3. An optional `config/runtime.local.yaml` overlays machine-specific values.
   The file is ignored by git. `SPX_SPARK_RUNTIME_OVERRIDES` may point to a
   different override file.
4. Environment variables and `.env` values override the merged YAML values.

Override files contain only existing paths and `value` leaves; descriptions
remain in the tracked base file. Unknown paths, missing explicit files, and
attempts to replace descriptions fail at startup. Example:

```yaml
steven:
  enabled:
    value: true
```
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
re-resolution for quarterly rollover. The deployed default is `live`: Trader
API streamer login is approved and ES/MES messages are production inputs.
SPXW option coverage remains an independent health dimension; a connected
WebSocket does not imply that GTH option quotes are available. See
[schwab-primary-ibkr-fallback.md](schwab-primary-ibkr-fallback.md).

Every setting consumed with `runtime_value("path.to.setting")` must have both
`value` and `description`. The architecture tests reject new literal defaults
passed directly to `env_bool`, `env_int`, `env_float`, `env_str`, `env_csv` or
`env_csv_preserve`; use `runtime_value` or `runtime_csv` instead.
