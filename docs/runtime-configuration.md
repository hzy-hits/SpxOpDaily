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

## Override order

1. Environment variables and `.env` values override a runtime default.
2. `SPX_SPARK_RUNTIME_CONFIG` may point to another YAML file.
3. Otherwise the repository `config/runtime.yaml` is loaded.

Production uses `schwab,ibkr,...` provider priority. Freshness and quality are
still evaluated before provider preference, so missing or stale Schwab data
falls back to usable IBKR data.

Every setting consumed with `runtime_value("path.to.setting")` must have both
`value` and `description`. The architecture tests reject new literal defaults
passed directly to `env_bool`, `env_int`, `env_float`, `env_str`, `env_csv` or
`env_csv_preserve`; use `runtime_value` or `runtime_csv` instead.
