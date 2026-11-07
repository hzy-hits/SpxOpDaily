# Storage Plan

<!-- documentation-status: 2026-11-07 -->
> **文档定位：现行运行与参考。** 现行说明；历史段落保留原适用日期。运行状态以实际服务和源字段时钟为准。
> [全仓文档、当前运行状态与合同优先级](README.md)（目录核对：2026-10-05）。

## Current storage contract

The active system retains original IBKR/Schwab option and underlying history,
verified Parquet/manifests, bounded latest projections, Python operational
records, the Huey task store, and the separate frozen Rust ledger/frames.
Strategy replay uses raw-broker availability time; cards and NO_TRADE records
cannot select samples or provide PnL labels.

Hourly/weekend compaction is copy-only. Only existing manifest/digest/row-count
and grace checks authorize an exact source deletion. The verified redundant
research copy removed on September 23 was not the original broker lake.
Python warning/critical is below 10 GiB; Rust refuses an append leaving less
than 10 GiB. The 28 GiB action threshold is maintenance planning, not alert
suppression. Pressure work is now owned by Huey, not the retired pressure timer.

See [runtime configuration](runtime-configuration.md), [schedule](operations-schedule.md),
[redundancy audit](desk-condor-probabilities-and-storage-2026-09-23.md) and
[space threshold acceptance](condor-results-and-disk-threshold-2026-09-23.md).
The remaining sizing estimates and MVP proposals below are historical design;
they do not replace the current cleanup or retention implementation.

## Desk pipeline storage incident (2026-11-07)

Production inspected: `94c3de4e`. This is a Phase 7 maintenance-only repair and a
Phase 6 operational-monitoring repair under the execution plan's section 0;
no platform rewrite, new service, or automatic database rebuild is introduced.

The first observed Rust ingress reserve rejection on November 6 was at
16:48:11 UTC (11:48 ET): 10,737,184,768 bytes were available, below the required
10 GiB reserve plus the frame being appended. The bridge retried its pending
frame and later Desk Maps missed their useful delivery window. The last report
was saved at 16:32:36 UTC; the last mirrored projection was generated at 17:15:09
and acknowledged at 17:21:33 UTC. Fresh process heartbeats did not prove delivery.

The November 7 Rust retention run removed 11,187,518,939 bytes of raw frame
segments only after archive verification. Rejections continued until 00:30:29
UTC before that space recovery. At 03:47 UTC the data volume had about 20.2 GiB
free, so the current free-space reading cannot disprove the earlier exhaustion.
The 20:45 November 6 source projection expired at 21:05; it must not be renewed
or pushed as a current card during the closed weekend.

The 23:00 UTC session finalizer published its artifact, but marked cleanup
blocked because three still-young `hour=23` sources had no compaction manifest.
The repair retains missing-manifest hours within the existing retention grace
as `waiting_for_age`. Mature missing/invalid manifests still block deletion;
hashes, row counts, late-append protection, and the manifest age gate remain.
The existing monitor now distinguishes an expired, unforwarded historical map
from a new map waiting to be forwarded. It keeps the missed-map fault visible.

The NAS task failed with SSH/rsync exit 255. A read-only SSH check initially
confirmed the NAS export and 1.6 TiB free, but the subsequent existing-unit retry
failed during transfer; later SSH and Tailscale probes timed out. Peer “Online”
status therefore did not prove usable transfer. No local source was removed by
this failed retry. Its eligible cold backlog was only about 0.64 GiB.

The subsequent user-authorized repair adds online operational-history retention
in the existing `infrastructure/operational_db.py`, called by the existing hourly
Worker storage job. Phase 5/P5-1 and the Phase 7 maintenance exception apply.
`AppSettings.operational_retention_days=3` keeps recent UTC session dates locally;
older terminal outcomes are archived first, then unreferenced decisions and legs,
then unreferenced events. Unknown/unresolved outcomes and their dependencies,
feature parents, strategy versions and notification-correlated decisions remain.
All notification events/attempts, sessions, provider incidents and compaction
manifests remain in the live DB. Broker history is untouched.

Each gzip archive contains full SQL rows and lives under
`<SPX_DATA_ROOT>/backups/operational-history/<SHA256>.json.gz`. The filename hashes
the decompressed JSON. Publication and directories are fsynced, decompression must
match the original bytes, and a short SQLite write transaction rechecks rows,
legs and references before deleting anything. A crash before commit leaves the
source records available; an existing identical archive can be reused. Archive
failure never skips the independent raw-source cleanup. Neither scheduled path
stops services or replaces the database.

Maintenance scans at most 500 records / about 16 MiB per batch (one large record
can exceed that byte target), with a configurable 10-second SQL/time budget per
hour; lock contention yields to the next run. Archive filesystem I/O completes
its bounded batch before returning. The additive `0006_retention_indexes` migration indexes the two previously
unindexed FK references (`decisions.feature_snapshot_id`, `outcomes.decision_id`),
so archive eligibility and delete checks do not repeatedly scan payload tables.
Freed SQLite pages are reused rather than
requiring a recurring VACUUM or rebuild. This bounds routine hot history, not
unresolved observations or the permanent compressed archive. The existing manual
retention tool is used once for this incident's accumulated DB, retaining a full
compressed pre-cutover copy and all notification bookkeeping.

Offline restoration: decompress the gzip, verify its SHA-256 against the basename,
and read `table`, `rows`, and `decision_legs`. Insert complete rows into a separate
DB with the same schema, restoring events before decisions, then legs/outcomes;
resolve retained dependencies before enabling/checking foreign keys. Never restore
old rows over the active runtime. Historical operational reports must explicitly
include these archives; strategy replay still starts from original broker data.

Observed capacity drivers were 18.3 GiB of November 6 broker JSONL, a 13.2 GiB
operational DB, about 21 GiB of Parquet, and daily Rust frames. Sampling the largest
Schwab hour did not find consecutive identical records after excluding arrival
metadata; price equality alone cannot authorize quote deletion. The 24-hour
verified raw retention and 10 GiB Rust reserve remain in effect. NAS transport
is still an external dependency: its current job excludes the live DB and lake;
local archives are not falsely reported as off-host copies. Long-term cold-history
capacity still needs a reachable archive destination, even after online DB growth
is controlled. The release receiver now also keeps only current and immediate rollback code
bundles after successful cutover, so deployments do not accumulate another
unbounded copy of dependencies. Data, database backups and unknown paths are
excluded.

The first deployment of PR #26 (`465aef6c`) did not pass acceptance. On November 7,
the 5.51 GiB migration backup and checks held writers stopped for approximately
eight minutes; after restart, cold reads of the separate 1.1 GiB Rust operational
ledger exceeded the 105-second bridge readiness deadline. Automatic rollback
then exposed a second defect: old Worker startup could not resolve Alembic
`0006_retention_indexes`, and its failed restart prevented the grouped recovery
from reaching Rust. The operator downgraded only the added indexes/revision,
preserving all new rows, and restored all nine services. At 06:53 UTC the old
`94c3de4e` runtime had fresh decisions and accepted bridge acknowledgments again.
This failed attempt is not evidence of a successful release.

The receiver repair uses that data-preserving downgrade during application
rollback, attempts Rust restoration even when Python restart fails, and allows
up to 300 seconds for cold startup with unchanged process/health/socket checks.
Regression coverage includes the actual old Alembic startup graph and rows
written after migration. Scheduled hourly archive maintenance does not run a
whole-database backup or integrity scan and does not stop services.

The one-time operational rebuild reduced the live DB from 14,151,798,784 to
5,912,117,248 bytes at 06:33:57 UTC, with 4.3 seconds of Core/Worker suspension
for that separate cutover. All 11,056 notification events and 11,505 attempts
remained. Its full retired database was subsequently compressed and verified by
decompressed SHA-256 and size before the uncompressed copy was removed. The later
release outage above must not be confused with this 4.3-second data cutover.

Validation so far: focused cleanup, compactor, finalizer, monitor and operational
archive tests, Ruff and Import Linter pass. Archive tests cover row restoration,
foreign keys, incomplete outcomes, notification preservation, late references,
archive failure and repeated maintenance. Full release checks run in CI.

## Operational database archive and reset (2026-10-15, P5-1)

`/srv/data/spx-spark/spx.sqlite` is the Python operational database, not the
original broker quote lake. Its historical decision/feature JSON can be large:
the October 15 inspection found approximately 100–120 KB per recent decision,
with a new decision about every five seconds. The roughly 30 GiB file had no
free pages. `VACUUM` alone therefore would not reclaim that history.

The raw-data compaction/retention jobs and Rust frame retention do not prune
this database. A successful daily archive is not evidence that old operational
rows have been removed. This maintenance is a one-off historical reset; it does
not install automatic operational-database retention or reduce future writes.

Before resetting it, use the production market-data root explicitly when
checking the existing ledger fallback and manifest backfill:

```bash
MARKET_DATA_DATA_ROOT=/srv/data/spx-spark/data DATA_PLATFORM_LEDGER_PATH=/srv/data/spx-spark/spx.sqlite uv run spx data replay-spool
MARKET_DATA_DATA_ROOT=/srv/data/spx-spark/data DATA_PLATFORM_LEDGER_PATH=/srv/data/spx-spark/spx.sqlite uv run spx data sync-manifests
```

On October 15 the ordinary fallback queue was empty; all 7,681 compaction
manifests were already synchronized, with no sync failures. The 72 records
quarantined on July 11 remain preserved: 54 immutable-ledger conflicts and
18 missing-reference errors. They were also copied into the private archive;
no conflicting history was force-overwritten. This checks pending operational
persistence; it does not claim every historical strategy has complete outcome
labels. Strategy research still reconstructs raw broker quotes, never selects
its samples from old cards or notification records.

The reset uses the existing SQLite schema and zstd. Pause Core/Worker and the
existing DB-writing task timers; keep broker collectors running. Check that no
other process holds the DB, checkpoint WAL, and hold a write lock while building
the smaller runtime file. Keep the complete old file intact in a restricted
archive directory on the same filesystem. Rename preserves the original inode
without requiring another 30 GiB copy. The backup stays outside Git: it contains
operational history and notification payloads.

Build a replacement at a temporary sibling path. Preserve all notification
events/attempts, compaction manifests, schema/migration state and mutable
operational state. Keep the current GTH session from **2026-10-14 22:00 UTC**,
plus the older decisions/events referenced by retained outcomes and feature
snapshots. This is a storage cutoff, not a research sample filter. Validate the
schema, copied counts, SQLite integrity and foreign keys; only then move the
old file into the archive, install the replacement, restore WAL, and resume the
previously active services and timers. Compress the offline old file after
service recovery; verify decompressed byte count and SHA-256 against that file
before unlinking it. Compression does not prolong the decision-engine outage.
Old uncertain deliveries are not manually retried.

The one-off script and its manifest are retained with the archive, not installed
as a new production service or another active database. To inspect old records,
decompress the verified archive into an offline location with sufficient space.
Do not overwrite a running database to restore history; a rollback would also
discard all newer operational and notification state.

At 00:49:06 UTC the rebuilt runtime database was **807,989,248 bytes**
(770.56 MiB), down from **32,516,317,184 bytes** (30.28 GiB). It retained
322 decisions, 334 legs, 107 events, 104 outcomes, all 8,081 notification events,
all 8,853 notification attempts, all 7,681 manifests and the Alembic revision.
The unchanged schema, row counts, `quick_check`, and `foreign_key_check` passed.
A synthetic database using the production schema also passed full archive
restore, byte-hash, dependency-retention and notification-state checks.

The initial compression-first attempt paused writers for approximately
126 seconds, then was abandoned to avoid a long outage. The subsequent rebuild
and cutover paused them for approximately 71 seconds; old bytes remained intact
through both attempts. Core/Worker resumed with `NRestarts=0`, and a fresh
decision persisted at 00:49:15 UTC. Broker collectors remained active, IBKR
reported a healthy fresh flush, and Bridge ACKs continued. The incomplete
first compressed attempt was removed after the complete old DB was safely
retained offline.

At 01:11:41 UTC the archive's full decompressed byte count and SHA-256 matched
the frozen source, and the old uncompressed DB was unlinked. The archive is
**1,854,652,515 bytes** (1.727 GiB); the replacement plus archive reclaim
**29,853,675,421 bytes** (27.803 GiB) relative to the old file. The data volume
then had **51,396,165,632 bytes** (47.866 GiB) available. The live DB had already
grown to approximately 0.77 GiB from resumed writes, as expected.

The private archive is
`/srv/data/spx-spark/backups/20261015-operational-reset-v2/`:
`spx-before-reset.sqlite.zst`, `manifest.json`, the one-off scripts, synthetic
test evidence, quarantined July records and maintenance/health logs. The
manifest records both the compressed and original SHA-256. The final process
exited successfully; neither the uncompressed backup nor staging DB remains.

Complexity: production files added/deleted **0/0**, net production LOC **0**;
dependencies, config keys, services/timers and active databases/tables added or
removed **0**. Historical runtime rows moved to the verified archive; no new
production retention path, schema version or notification implementation.

End-to-end follow-up found a separate report-generation failure: the 01:00
Desk Map was generated and mirrored, but the report writer remained
`generating` from 01:00:20 for roughly its configured 180-second request window,
then recorded one generation failure without persisting a report. At 01:05 UTC
the existing monitor reported `scheduled_report_missing`; no successful target
receipt existed for this slot. Report/delivery code and configuration were not
changed by this database maintenance. Thus restored decisions and Bridge ACKs
must not be described as a successful 01:00 push.

## Host-wide cache and old-program cleanup (2026-10-15)

Following the P5-1 operational-DB maintenance, a separate whole-host inventory
removed unused build products and download caches. This was maintenance within
the existing ownership boundaries, not a restart of the deferred P7 rewrite.
No production code, configuration, package dependency, service, timer, database
or table was added or removed; no production service was restarted.

| Removed material | Scope and checks |
| --- | --- |
| Temporary and repository Rust build products, approximately 9.5 GiB allocated before cleanup | `cargo clean` for `/tmp/spx-bridge-repair-target` and `rust/target`; running binaries remain in the immutable `/opt/spx-spark-core-shadow/releases/` release |
| Old Cursor program versions and an old program-only backup, 3.29 GiB | Checked executable, working-directory, file-descriptor, memory-map and launcher references; retained current/active and previous versions; preserved all user snapshots and workspace state |
| Eight disabled Snap revisions and unused download-cache files | Removed exact disabled revisions with `snap remove --revision`; then removed only old regular cache files with one remaining hard link and no open file reference; current revisions and their linked cache remain |
| APT, pip, UV, Go and unused Docker build caches | Used the existing package/build tools; Docker prune was limited to unused build cache older than seven days; running images, containers and volumes retained |

Do not add apparent directory sizes to infer reclaimed space: Snap download
cache entries can hard-link installed revisions, and Cargo reports logical
bytes. Measure filesystem available bytes before and after. At **01:25:10 UTC**
the net available-space increase was **15.42 GiB**, separate from the earlier
27.80 GiB DB cleanup:

| Filesystem | Available before this pass | Available after this pass |
| --- | ---: | ---: |
| `/` | 10,811,461,632 bytes | 23,337,095,168 bytes |
| `/srv/data` | 51,343,904,768 bytes | 55,379,054,592 bytes |

The UV cleaner initially removed the home cache symlink. The physical cache
was then cleaned explicitly and `/home/ubuntu/.cache/uv` was restored to
`/srv/data/ubuntu-home/.cache/uv`; the installed Python environment still imports
successfully. Subsequent builds/downloads will repopulate caches. No new
automatic retention policy was installed by this pass.

Large retained items include about 13 GiB raw broker history, 14 GiB Parquet
lake, 5.4 GiB features, 2.8 GiB published data, 7.8 GiB Cursor snapshots, the
verified database archive and production rollback binaries. Raw and Parquet
directories are not interchangeable copies merely because both exist: deletion
still requires the existing per-partition manifest and content checks. The
8 GiB swap file had about 3.25 GiB in use and was retained. Active container
images, unrelated research projects and recent incident journals were retained.

Core/Worker, collectors and the four Rust units remained active with
`NRestarts=0`. At 01:24:18 UTC the newest decision was 3.9 seconds old, IBKR
reported a healthy data plane, and Bridge source/mirror and ACK were current.
The independent `scheduled_report_missing` fault remained; this cleanup does
not establish successful report generation or phone delivery. No deleted open
file larger than 10 MiB remained in the process-FD scan.

Private evidence is alongside the DB archive in
`backups/20261015-operational-reset-v2/host-storage-cleanup.json`,
`host-snap-cleanup.json` and `host-old-programs-cleanup.py`. It records exact
removed program paths, retained versions and filesystem measurements.
Complexity: production files **+0/-0**, net production LOC **0**; dependencies,
config keys, services/timers and databases/tables **+0/-0**. Removed paths are
rebuildable artifacts and unused program versions, not a production ownership
or legacy-code migration.

Date: 2026-07-04

## Historical MVP decision

Do not store full-market OPRA tick data in the MVP.

Store only:

- selected live quotes needed for the dashboard and alerts
- compressed feature bars
- alert events
- session summaries
- enough raw data to replay important decisions

This keeps storage manageable on the Oracle Linux host.

## Schwab And Cboe Index Data

Schwab may be able to quote some index symbols through the quote API, but VIX/VVIX/SKEW availability, symbol format, and real-time entitlement must be verified on the actual account.

Candidate Schwab symbols to test:

- `$SPX`
- `$VIX`
- `$VIX1D`
- `$VIX9D`
- `$VIX3M`
- `$VVIX`
- `$SKEW`

Fallback policy:

- If Schwab returns live Cboe index quotes, use them as broker data and tag `provider=schwab`.
- If IBKR returns Cboe Streaming Market Indexes, prefer IBKR during its available window and tag `provider=ibkr`.
- If neither source returns the official index, mark the official field `missing`.
- Do not rename synthetic estimates as official VIX, VVIX, or SKEW.

Synthetic fields are allowed, but must be clearly named:

- `vix_proxy`
- `near_vol_proxy`
- `tail_risk_proxy`
- `skew_proxy`

## What To Store

### Raw Ticks

Store raw ticks only for:

- selected SPXW or XSP contracts
- SPY/QQQ/IWM option contracts selected for fallback
- SPX, ES/MES, SPY, QQQ, IWM
- Cboe index values when available
- Hyperliquid SPX trades, BBO, mark, funding, and OI
- Polymarket markets being watched

Do not store:

- all OPRA options
- full SPX option chain as tick streams
- every strike for every expiry
- full L2 order books indefinitely

### Latest State

Keep an in-memory and on-disk latest state table:

- latest quote by instrument
- provider-level latest quote by instrument
- selected best quote by instrument after quality-aware fallback
- latest feature by feature name
- latest provider quality
- latest alert state and cooldown

This supports dashboard rendering without scanning raw tick files.

Current MVP storage:

```text
data/raw/provider=<provider>/date=YYYY-MM-DD/hour=HH/quotes.jsonl
data/latest/state.json
```

Use JSONL first. Move raw quotes to Parquet/DuckDB compaction after the real
provider volume and schema stability are measured.

### Feature Bars

Persist compact bars:

- 1 second bars for selected instruments during active windows
- 5 second bars for option microstructure features
- 1 minute bars for session analytics and replay

Example feature rows:

- `atm_straddle_mid`
- `atm_iv`
- `atm_delta`
- `atm_gamma`
- `atm_theta`
- `atm_vega`
- `quote_spread_bps`
- `quote_age_ms`
- `es_return_1m`
- `spy_return_1m`
- `hl_spx_return_1m`
- `vix1d_vix9d`
- `tail_risk_proxy`

### Alerts

Always store alert events. They are small and valuable.

Fields:

- timestamp
- trigger name
- severity
- provider state
- feature snapshot
- text sent to user
- whether user opened device or acknowledged, if available later
- forward returns after 5m, 15m, 60m, and close

## Storage Estimates

These are planning ranges, not hard promises.

## SPXW Contract Universe Sizing

For a selected strike window:

```text
contracts = expiries * option_types * strikes_per_expiry
strikes_per_expiry ~= floor((window_percent * 2 * spx_level) / strike_step) + 1
```

Where:

- `expiries`: 2 for 0DTE + 1DTE
- `option_types`: 2 for calls + puts
- `window_percent`: 0.02 for +/-2%
- `strike_step`: usually plan around 5 SPX points for SPXW near-the-money live streaming, unless the provider returns a denser chain

Examples with 5-point strikes:

```text
SPX 5000: +/-2% = 4900-5100, about 41 strikes per expiry
0DTE + 1DTE calls/puts ~= 2 * 2 * 41 = 164 contracts

SPX 6000: +/-2% = 5880-6120, about 49 strikes per expiry
0DTE + 1DTE calls/puts ~= 2 * 2 * 49 = 196 contracts

SPX 7000: +/-2% = 6860-7140, about 57 strikes per expiry
0DTE + 1DTE calls/puts ~= 2 * 2 * 57 = 228 contracts

SPX 7500: +/-2% = 7350-7650, about 61 strikes per expiry
0DTE + 1DTE calls/puts ~= 2 * 2 * 61 = 244 contracts
```

So a 150-300 contract budget is reasonable for 0DTE + 1DTE +/-2% if using 5-point strikes.

If a provider exposes 1-point strikes and the system subscribes to every strike, the same SPX 6000 example becomes about 964 contracts. Do not live-stream that in the MVP. Downselect to 5-point strikes, liquidity-filtered strikes, or a smaller window.

Provider line limits matter separately from server capacity:

- Oracle 4-core/24 GB can compute and store 150-300 selected contracts.
- IBKR may not allow 150-300 simultaneous live option lines unless the account has enough market-data lines.
- For IBKR, start with a smaller live set and use periodic chain snapshots for the wider +/-2% context.
- For Schwab, verify streaming stability and rate behavior before assuming all 150-300 contracts can stream continuously.

Default live-subscription ramp:

- Start: 20-40 contracts, 0DTE ATM +/- 25 to 50 points
- Step 2: 40-80 contracts, 0DTE ATM +/- 50 to 100 points
- Step 3: add selected 1DTE contracts around ATM
- Step 4: only after broker verification, expand toward the full 0DTE + 1DTE +/-2% universe

The full 150-300 contract universe is a sizing target for server capacity and storage planning, not the default broker subscription count.

## SPXW Subscription Tiling

Do not treat every strike inside +/-2% as equally urgent.

For 0DTE trading, the useful cadence depends on distance from ATM:

```text
Core ring:
  0DTE ATM +/- 25 to 50 SPX points
  5-point strikes
  calls + puts
  about 22-42 contracts
  target cadence: live stream or 1 second features

Near ring:
  0DTE ATM +/- 50 to 100 SPX points
  5-point strikes
  calls + puts
  about 40-82 contracts total including core
  target cadence: 5-15 second refresh/features

Outer ring:
  0DTE ATM +/- 1.0% to 2.0%
  5-point strikes, liquidity filtered
  target cadence: 30-60 second chain refresh or event-triggered refresh

Next-day ring:
  1DTE ATM +/- 50 to 100 SPX points first
  wider +/-2% only as snapshot/refresh context
  target cadence: 30-60 seconds unless it becomes the next 0DTE
```

SPX daily movement context:

- 1.0% at SPX 6000 is 60 points
- 1.5% at SPX 6000 is 90 points
- 2.0% at SPX 6000 is 120 points
- 1.0% at SPX 7500 is 75 points
- 1.5% at SPX 7500 is 112.5 points
- 2.0% at SPX 7500 is 150 points

So at SPX 7500, 0DTE ATM +/- 75 to 115 points is closer to a 1.0% to 1.5% normal-move window. A full +/-2% window is useful for tail context, gamma map, and event days, but it does not need tick-level updates by default.

If broker line limits are tight, rotate the outer ring:

```text
second 00: core ring
second 05: near-left slice
second 10: near-right slice
second 15: outer-left slice
second 20: outer-right slice
...
```

For a 16-slice rotation, the full outer window refreshes about once every 16 cycles. This is acceptable only for broad context, not for the ATM decision layer.

Implementation rule:

- Keep the core ring continuously subscribed when possible.
- Refresh the near ring frequently enough for alerts.
- Refresh the outer ring by slices.
- When SPX moves and ATM changes, rebalance rings rather than adding unlimited new contracts.
- Preserve old ATM strikes for a short grace period so alerts do not jump during fast moves.

## Rolling Slice Sampler

When broker line limits are tight, use a simple rotating sampler instead of trying to keep the full window subscribed.

Example at SPX 7500:

```text
window: 7500 +/- 200 points = 7300-7700
strike step: 5 points
strikes: about 81
contracts per expiry: 81 * calls/puts = 162
0DTE + 1DTE full window: about 324 contracts
```

Split the strike window into 20 slices:

```text
slices: 20
slice width: about 20 SPX points
strikes per slice: about 4-5
contracts per slice per expiry: about 8-10
contracts per slice for 0DTE + 1DTE: about 16-20
cadence: one slice every 3 seconds
full scan: about 60 seconds
```

This is a good fit for broad context:

- gamma map refresh
- skew/tail proxy refresh
- stale wide-strike quote detection
- event-day window expansion

It is not enough by itself for the immediate ATM decision layer, because the ATM slice could be stale for up to a minute. Use two lanes:

```text
hot lane:
  ATM +/- 25 to 50 points
  every 1-3 seconds
  used for straddle, ATM IV, Greeks, and alerts

rolling lane:
  full +/- 200 point window
  20 slices
  one slice every 3 seconds
  full scan every minute
  used for context and surface shape
```

If the provider supports a whole-chain endpoint efficiently, prefer one chain refresh per minute over 20 separate quote calls. If the provider only supports quote batches, use the slice sampler.

Avoid rapid streaming subscribe/unsubscribe churn if the broker penalizes it. The rolling sampler should use snapshot/batch quote calls when available. Streaming lines should be reserved for the hot lane.

## Human Alert Sampling Modes

The MVP is an alert and decision-support system, not an automatic execution engine. Therefore it does not need every wide-strike quote at tick speed.

Use sampling modes:

```text
human_alert:
  purpose: notify user to open the trading device
  ATM/near-ATM cadence: 4-15 seconds
  wide-window cadence: 30-60 seconds
  acceptable latency: human-scale, usually tens of seconds

execution_monitor:
  purpose: monitor a possible active order or very near-term 0DTE entry
  ATM/near-ATM cadence: 1-3 seconds
  wide-window cadence: 15-30 seconds
  acceptable latency: low single-digit seconds
```

Default to `human_alert`. Switch to `execution_monitor` only when:

- a high-severity alert is active
- a `device_required` alert is fired
- the system is inside a sensitive market window
- ATM straddle or underlier movement crosses an event threshold
- user manually requests it

`device_required` should automatically promote the relevant symbols to `execution_monitor`; do not require a separate confirmation.

Default promotion TTL:

- normal RTH: 30 minutes
- final RTH hour: 60 minutes
- pre-open watch: 20 minutes
- open 1h: 30 minutes
- post-close 1h: 20 minutes if an event is active; otherwise do not promote
- macro-event window: 60 minutes
- user manual override: user-specified TTL, default 60 minutes

The TTL can be shortened if:

- the alert invalidation level is hit
- quote quality becomes poor for several consecutive checks
- the triggering regime normalizes
- the market closes

Simple 4-group sampler:

```text
window: ATM +/- 200 points
groups: 4
strategy: interleaved by strike step
cadence: one group every 4 seconds
full scan: about 16 seconds
```

At SPX 7500 with 5-point strikes:

```text
full window strikes: about 81
strikes per group: about 20-21
contracts per group, 0DTE only: about 40-42
contracts per group, 0DTE + 1DTE: about 80-84
```

Example interleaved groups:

```text
group 0: 7300, 7320, 7340, ...
group 1: 7305, 7325, 7345, ...
group 2: 7310, 7330, 7350, ...
group 3: 7315, 7335, 7355, ...
```

This is simpler and more responsive than a 20-slice one-minute sampler, but each request batch is larger. It also avoids having one side of the market stale while the sampler is scanning the other side.

Recommended starting point:

- `human_alert`: 4 groups, 4 seconds per group, 16 second full scan
- `execution_monitor`: hot lane every 1-3 seconds plus 4-group scan every 16 seconds
- `degraded`: 20 groups, 3 seconds per group, 60 second full scan

Use official or broker-provided VIX/VVIX/SKEW values as separate low-rate context. They can update in real time, but they do not replace option quotes for ATM straddle, spread, and Greeks.

Sensitive market windows can automatically raise cadence:

```text
pre-open 1h:
  VIX/ETF/futures/HL/Polymarket context: 5-15 seconds
  options wide scan: 16-60 seconds if quotes are usable

open 1h:
  ATM/hot lane: 1-4 seconds
  +/-200 rolling scan: about 16 seconds if broker supports batch quotes

close 1h:
  ATM/hot lane: 1-3 seconds
  target spread legs: 1-3 seconds when alert is active
  wider scan: 16-30 seconds

post-close 1h:
  reduce or stop option sampling unless an event is active
  futures/ETF/chain context: 15-60 seconds
```

### Safe MVP

Universe:

- 40-80 live option contracts
- 20-40 ETF/index/futures symbols
- Hyperliquid SPX context
- Polymarket selected markets

Expected raw compressed storage:

- 100 MB to 1 GB per active trading day
- 3 GB to 25 GB per month

This is manageable.

### Aggressive MVP

Universe:

- 150-300 live option contracts
- more frequent quote updates
- wider raw replay retention
- more chain and smart-wallet context

Expected raw compressed storage:

- 1 GB to 5 GB per active trading day
- 25 GB to 125 GB per month

Still manageable with retention rules, but needs monitoring.

### Not MVP

Universe:

- full OPRA chain
- full SPX chain all expiries
- full depth/order-book data
- indefinite raw tick retention

Expected storage:

- can become tens or hundreds of GB per day depending on scope

Do not do this without a dedicated data vendor and storage budget.

## Retention Policy

Recommended default for the current 150 GB mounted disk, with about 80 GB reserved for project data:

- raw selected ticks: 7 trading days
- raw high-severity alert windows: 60 days
- 1 second feature bars: 30 days
- 5 second feature bars: 90 days
- 1 minute feature bars: keep indefinitely
- alert events: keep indefinitely
- daily session reports: keep indefinitely

If disk usage stays comfortably below 70% after two weeks, consider increasing raw selected ticks to 10 trading days. Do not expand to 14 trading days until the real daily footprint is known.

High-severity alert windows should preserve raw ticks from:

- 15 minutes before the alert
- 60 minutes after the alert
- through close if the alert was close-related

## File Layout

Use partitioned Parquet for append-heavy data:

```text
data/
  raw/
    provider=ibkr/date=YYYY-MM-DD/*.parquet
    provider=schwab/date=YYYY-MM-DD/*.parquet
    provider=hyperliquid/date=YYYY-MM-DD/*.parquet
    provider=polymarket/date=YYYY-MM-DD/*.parquet
  features/
    interval=1s/date=YYYY-MM-DD/*.parquet
    interval=5s/date=YYYY-MM-DD/*.parquet
    interval=1m/date=YYYY-MM-DD/*.parquet
  alerts/
    date=YYYY-MM-DD/*.jsonl
  reports/
    date=YYYY-MM-DD/session.md
```

Use DuckDB for local querying across Parquet files.

SQLite is acceptable for:

- runtime state
- subscriptions
- alert cooldowns
- small metadata tables

Do not use SQLite as the main raw tick store.

## Write Strategy

Use buffered writes:

- collect rows in memory for a short interval
- flush every 1-5 seconds or every N rows
- write Parquet chunks by provider/date/instrument class
- write alert JSONL immediately

Avoid:

- one file per tick
- one SQLite insert per tick without batching
- uncompressed raw JSON as the primary store

## Greeks And Calculation Storage

For SPXW Greeks:

- store raw quote
- store model inputs
- store calculated IV and Greeks
- store calculation version
- store quote quality flags

Do not recalculate historical alerts with a changed model without recording the model version.

Suggested fields:

- `model_name`
- `model_version`
- `underlier_source`
- `rate_source`
- `dividend_or_forward_source`
- `iv`
- `delta`
- `gamma`
- `theta`
- `vega`
- `calculation_status`

## Sources

- Cboe VIX methodology: https://cdn.cboe.com/resources/indices/Volatility_Index_Methodology_Cboe_Volatility_Index.pdf
- Cboe VIX mathematics methodology: https://cdn.cboe.com/resources/indices/Cboe_Volatility_Index_Mathematics_Methodology.pdf
- Cboe VIX historical data: https://www.cboe.com/tradable_products/vix/vix_historical_data/
- Cboe SKEW dashboard: https://www.cboe.com/us/indices/dashboard/skew/
- Schwab-py quote docs: https://schwab-py.readthedocs.io/en/latest/client.html

## Repeated historical alert and NAS recovery (2026-11-07 follow-up)

Phase 6 operational-monitoring and Phase 7/P7-Lite maintenance exceptions apply.
The first repair retained the Friday expired-map fault in the hourly alert set.
Consequently, a healthy weekend heartbeat still produced a fault notification.
The monitor now records the missed projection's source clock separately, notifies
that historical loss once, and keeps hourly reminders only for active faults.
It never re-dates or sends an expired map. A new expected trading slot without a
report, a halted/stale bridge, and a stopped delivery service still alert. The
status message explicitly distinguishes runtime checks from phone delivery.

The existing NAS owner is now versioned as `scripts/archive-nas.py`; the existing
`spx-nas-archive.service` and `.timer` retain their names and daily schedule.
The unit retries failures after 15 minutes with a 30-minute attempt deadline;
rsync keeps partial files and can reuse them. Gzip output is deterministic across
retries. Both NAS mount identity and remote streaming SHA-256/size verification
must pass before deletion. A same-sized corrupt destination is not accepted.
The job has a 256 MiB memory ceiling, 25% CPU budget, and idle I/O priority.
No additional daemon, queue or database is introduced.

The job also uploads finalized content-addressed operational gzip archives and
retired SQLite `.zst` files with `decompressed_sha256_and_size` receipts. Recent
compressed rollback/history files remain local for seven days after creation;
older payloads are removed only after a fresh remote verification. Receipts stay
local. Live databases and the raw broker lake are never traversed by this job.
Older backup formats lacking that receipt remain local for separate inspection.
A missing NAS mount fails closed; the Pi SD card is not a fallback destination.

Operational gzip archives use their original SHA-256 basename beneath
`/mnt/nas/spx-spark/oracle/backups/operational-history/`. Database copies preserve
their backup-directory names; release backups use `backups/releases/`.
Restore a requested payload to staging with rsync, verify its compressed hash
against the local `.nas-receipt.json`, then verify the decompressed hash against
the operational filename or SQLite compression receipt. Restore rows/databases
only into an offline research copy, never over the live runtime.

After the reviewed runtime bundle is deployed, install these two existing unit
files from the same commit, reload the user manager, and restart the timer/job.
The former unversioned `archive.py` and its superseded upload helper are retired;
all future executions use the immutable runtime bundle. This is adoption of the
existing owner, not a second archival path.

The independent NAS host check found approximately 3.9 GiB resident anonymous
memory in the `mihomo` container, almost exhausted swap and high memory/I/O
pressure. That shared household proxy is outside the SPX process boundary;
changing its resource limits/restarting it requires the user's explicit consent.
Transport resilience does not by itself establish that host-level cause is fixed.
