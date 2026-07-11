# Runtime configuration

Operational market-data defaults live in `config/runtime.yaml`; Python code
loads them through `spx_spark.runtime_config`. Secrets remain in `.env`.

Each mutable value is represented as a `value` plus a human-readable
`description`. Numeric settings therefore carry their unit and purpose beside
the number instead of appearing as unexplained literals in collector code.

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
