# Runtime configuration

> **状态（2026-08-08）：旧 `runtime.yaml` 已迁为 `runtime.toml`，重复的 `runtime_config.py` loader 已删除；剩余 legacy settings loader 与 `config.py` env helper 已按执行方案第 0 节接受为按需技术债，P5-2 不再是施工队列。**
> 新配置只允许进入最小 pydantic-settings `AppSettings`；不得向过渡 TOML
> 增加键、env helper 或 loader 兼容分支。仅在实际修改对应 owner 时就地迁移。

Operational runtime defaults currently live in `config/runtime.toml`; Python loads
them through the typed `spx_spark.settings.load_settings()` (`AppSettings`) owner. Secrets remain in
`.env` and are never loaded during unit tests (`SPX_SPARK_DISABLE_DOTENV=1` plus
`tests/fixtures/runtime.defaults.toml`).

This file describes Python configuration only. Rust has strict TOML examples
and Oracle overlays under `rust/config/`; production Rust config and secrets
remain outside the checkout as documented in `rust/docs/OPERATIONS.md`. The
shared precedence `defaults < deployment < environment` does not permit Python
TOML to silently override Rust TOML or vice versa.

Each mutable value is represented as a `value` plus a human-readable
`description`. Numeric settings therefore carry their unit and purpose beside
the number instead of appearing as unexplained literals in collector code.

## Test isolation

Unit tests pin `SPX_SPARK_RUNTIME_CONFIG` to `tests/fixtures/runtime.defaults.toml`
and disable `.env` loading plus `config/runtime.local.toml` overlays
(`SPX_SPARK_DISABLE_DOTENV=1`, `SPX_SPARK_DISABLE_RUNTIME_OVERRIDES=1`). Deployment
edits to the workspace `.env`, local overrides, or live `config/runtime.toml` must
not change unit-test outcomes. When product defaults change intentionally, update
both `config/runtime.toml` and the frozen fixture.

Machine-local deployment values belong in `config/runtime.local.toml` (gitignored;
see `config/runtime.local.toml.example`), not in the tracked defaults file.

## Scope

The transitional TOML file is the documented source for legacy mutable, non-secret defaults across
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
override TOML (notably a local `IBKR_STREAM_MAX_OPTION_LINES` may pin the stream
below the tracked default until removed).

The Paper username receives shared Live subscriptions only while the sharing
Live username is not consuming them in TWS, Mobile, or Client Portal. IBKR
error `10197` is therefore an entitlement-owner conflict, not a line-capacity
or rotation failure. The collector preserves that reason through a dedicated
circuit breaker. Non-invasive probes start after
`IBKR_CONFLICT_PROBE_SECONDS` and back off exponentially to
`IBKR_CONFLICT_PROBE_MAX_SECONDS`; only continuous fresh usable flushes for
`IBKR_CONFLICT_RECOVERY_SECONDS` (boot default 8) close the circuit. Probe
cadence and recovery stability are independent. After startup the collector
hot-reloads `<MARKET_DATA_DATA_ROOT>/runtime/ibkr_conflict.toml` on each
flush, so operators can change `recovery_seconds` / `probe_seconds` /
`probe_max_seconds` without restarting `spx-spark-ibkr-stream`. Missing files
are seeded once from the boot defaults and never overwritten. A TCP reconnect
alone does not reset the circuit, and the collector never preempts or logs out
the external Live session.

The stream also writes
`<MARKET_DATA_DATA_ROOT>/latest/ibkr_stream_health.json`. This projection keeps
systemd process state separate from market-data readiness with explicit
`process_active`, `data_plane_healthy`, `policy_blocked`, `retry_at`,
`circuit_state`, and `reason` fields. Operators must not interpret an active
systemd unit as proof that the IBKR data plane is healthy.

Secrets and operator-private endpoints stay out of tracked TOML. API keys, app secrets,
device-specific Bark URLs, Feishu webhook URLs/secrets and other credentials
must be supplied through the environment or an ignored env file. Empty secret
URL fields intentionally default to `""` in code via single-argument env reads.

Algorithm constants and protocol identities stay in code. Examples include
basis-point conversions, schema versions, exchange/protocol multipliers and
un-overridden model thresholds used as mathematical identities or algorithm
definitions. Only mutable operational defaults belong in the transitional runtime
TOML. When an owning module is materially changed, its surviving keys must move
to the minimal `AppSettings` or be retired in the same change; no repository-wide
P5-2 cleanup remains scheduled.

## Session finalization and storage pressure

`spx-spark-session-finalize.timer` runs daily at 18:00 New York wall time. Its
top-level application resolves the latest completed exchange session, builds
one deterministic immutable Replay artifact, verifies it, and then reuses the
same payload for the optional LLM review and notification. Weekend and holiday
runs are idempotent repair opportunities; they do not manufacture empty
sessions. `Persistent=true` recovers a missed timer activation, while
`data_platform.replay_finalize_backlog_days` bounds how many already-published
artifact dates one pressure pass verifies. If the host misses multiple trading
days, backfill each missing date explicitly with `--date YYYY-MM-DD`; automatic
mode never invents or silently skips a historical review.

The hourly `spx-spark-storage-pressure.timer` calls that same application with
`--pressure-check`. Watermarks are typed configuration, not systemd literals:

- `data_platform.storage_pressure_action_free_bytes`: 28 GiB by default;
- `data_platform.storage_pressure_warning_free_bytes`: 24 GiB by default;
- `data_platform.storage_pressure_critical_free_bytes`: 20 GiB by default;
- `data_platform.replay_raw_delete_grace_hours`: minimum age after verified
  publication before an eligible raw source can be removed.

Free-space severity increases as available bytes cross those levels downward.
The critical default matches the Oracle Rust raw-log reserve, while the higher
levels leave room to finish compaction before ingress fails closed. Pressure
never grants deletion authority by itself: the completed-day artifact,
source/Parquet digests, row counts and grace gate must all pass. Any required
artifact or verification failure exits non-zero without deleting raw data.

The generic compaction CLI is permanently copy-only, and the hourly/weekend
units additionally force `DATA_PLATFORM_RAW_DELETE_ENABLED=false` even if a
legacy `.env` still enables the old 48-hour path. They continue producing
verified Parquet and manifests; only the session finalizer may remove an exact
raw partition.

Both timer paths use the same outer `flock`. The systemd units deliberately do
not pass watermark numbers, so `defaults < deployment < environment` remains
the sole precedence rule. Put machine-specific overrides in the ignored
`config/runtime.local.toml`.

## Schwab symbol table

`schwab.instruments` is the provider mapping table. It separates:

- canonical instrument identity used by storage (`SPX`);
- Schwab quote identity (`$SPX`);
- Schwab option-chain identity (`$SPX`);
- returned option trading classes (`SPX`, `SPXW`).

`schwab.collection.research_chain` controls a bounded raw-only RTH lane for
SPX 7D/30D ATM-IV and TLT/IEF call-put skew research. It reuses the Schwab
collector and raw quote writer, runs only in the normal RTH profile, and skips
active/burst, GTH, off-hours and quota-pressure modes. With the default two SPX
targets and two Treasury ETFs at a 60-second cadence, it adds four scheduled
requests per minute without consuming IBKR ticker lines. These rows are
compacted by the existing quote pipeline but are excluded from executable
latest-state pricing and strategy authority.

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

1. The repository `config/runtime.toml` supplies transitional tracked defaults.
2. `SPX_SPARK_RUNTIME_CONFIG` may select another complete base TOML file.
3. An optional `config/runtime.local.toml` overlays machine-specific values.
   The file is ignored by git. `SPX_SPARK_RUNTIME_OVERRIDES` may point to a
   different override file.
4. Environment variables and `.env` values override the merged TOML values.

Override files contain only existing paths and `value` leaves; descriptions
remain in the tracked base file. Unknown paths, missing explicit files, and
attempts to replace descriptions fail at startup. Example:

```toml
[steven.enabled]
value = true
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

Spring Gamma's tracked default
`spring_gamma_v3.min_paired_strikes=13` is the minimum number of complete C/P
pairs whose two legs independently pass the analytical gate. It is not the
density target: coverage still reports progress against the nearest 61
strikes, and labels 13–48 pairs only as `core_covered`. OI/volume from a
rejected analytical leg may remain visible for source auditing but cannot
satisfy the pair minimum. Local overrides must not lower this gate merely to
make a historical replay reach the 75% daily acceptance threshold.

Every legacy setting consumed with `runtime_value("path.to.setting")` must have both
`value` and `description`. The architecture tests reject new literal defaults
passed directly to `env_bool`, `env_int`, `env_float`, `env_str`, `env_csv` or
`env_csv_preserve`. New code must use the pydantic-settings `AppSettings`, not add
another `runtime_value` consumer.
