# SPX Spark Data Platform

Date: 2026-07-11

Storage-topology decision: [ADR-0001: Oracle-first market-data storage](adr/0001-oracle-first-market-data-storage.md).

## Decision

Keep the data platform in the `spx-spark` repository as an isolated package.
The market-data and strategy contracts still change together, and production
uses one checkout on the Oracle host.  Split the package into another
repository only after at least two independent producers need the contracts or
the compactor has an independent release and service lifecycle.

Use three storage roles instead of one lowest-common-denominator database:

- SQLite is the local operational ledger for low-volume transactional facts.
- ZSTD Parquet is the durable source of truth for high-volume historical data.
- DuckDB is a rebuildable read-only research catalog over SQLite and Parquet.

The existing `latest/*.json` and raw JSONL write path remain authoritative for
realtime behavior during rollout.

## Failure boundary

Research persistence must not delay or suppress a market alert. Realtime
ledger writes use short transactions and a bounded SQLite busy timeout. A
transient storage failure is appended to a mode-`0600` fallback spool for later
replay. Immutable-record conflicts, lookahead violations, and malformed
payloads are terminal: they are reported immediately instead of growing the
retry spool. A missing reference is retried after the rest of the spool so a
later parent record can heal ordering; it becomes terminal only when the parent
is still absent and no transient storage failure occurred in that batch. During
replay, legacy terminal records are preserved verbatim in a deduplicated,
fsynced mode-`0600` dead-letter JSONL before they are removed from the active
spool. The hourly batch automatically replays the spool, and a 64 MiB default
ceiling prevents an outage from consuming the filesystem indefinitely.
Compaction and DuckDB queries run out of process.

The operational ledger is event-driven: it records actual alert candidates,
delivery/reviewer outcomes and fixed-horizon results, not every neutral 5-second
evaluation. Continuous quote history belongs in compressed Parquet.

Raw deletion is wired into the compaction path but disabled by default. A source JSONL file can become eligible
for deletion only after all of the following are true:

1. the file is closed and older than the configured minimum age;
2. the source size, mtime and SHA-256 are stable;
3. a temporary Parquet file is written and verified;
4. row count and min/max timestamps match the source scan;
5. the final Parquet checksum is recorded in the SQLite manifest;
6. the configured 24-hour-or-longer rollback grace has elapsed.

The initial production rollout keeps deletion disabled. Operators may enable it
only after verified Parquet output and manifest checks pass, and the configured
rollback grace has elapsed.

## Storage ports

The abstraction exposes capabilities rather than generic CRUD:

- `DecisionLedger`: session, event, frozen feature, decision+legs, delivery,
  outcome and compaction-manifest writes.
- `QuoteLandingWriter`: append current provider quote batches.
- `HistoricalLake`: publish immutable verified partitions.
- `ResearchReader`: query stable versioned analytical datasets.

SQLite, JSONL, Parquet, DuckDB and in-memory test implementations are adapters
behind these ports. The realtime service never imports DuckDB.

## Time contract

Every replayable record distinguishes:

- `source_at`: provider or exchange event time;
- `received_at`: collector receipt time;
- `available_at`: earliest time the strategy could consume the record;
- `decision_at`: strategy evaluation time;
- `sent_at`: actual notification time;
- `target_at` and `sampled_at`: post-event outcome clocks.

Historical decision queries require `available_at <= decision_at`. Outcome
tables are joined only after replay and are never loaded into a replay input.

## SQLite ledger

The first migration creates:

- `sessions`
- `strategy_versions`
- `events`
- `feature_snapshots`
- `decisions`
- `decision_legs`
- `alert_deliveries`
- `outcomes`
- `compaction_manifests`
- `schema_migrations`

Decision and leg rows commit in one transaction. All identifiers are
deterministic and writes are retry-safe. The database lives on the Oracle local
filesystem, never on NFS, with WAL, foreign keys, owner-only permissions and
forward-only checksummed migrations.

Compaction lineage uses `(source_path, source_sha256)` as its natural key. A
replacement of a missing or corrupt Parquet output safely updates that current
lineage (including verification time and output checksum) instead of treating
the repair as an immutable-record conflict.

## Parquet lake

```text
data/lake/
  quotes/schema=v1/date=YYYY-MM-DD/provider=ibkr/hour=HH/*.parquet
  features/schema=v1/date=YYYY-MM-DD/feature=<name>/*.parquet
  bars/schema=v1/interval=1m/date=YYYY-MM-DD/*.parquet
  facts/schema=v1/date=YYYY-MM-DD/<dataset>/*.parquet
  preserved/schema=v1/date=YYYY-MM-DD/event_key=<opaque>/*.parquet
```

Do not partition by instrument; that creates too many small files. Quote files
are ordered by source/receipt time and use ZSTD compression. Each row and file
records schema/writer lineage. The Parquet payload does not embed the
compaction wall clock, so rebuilding the same source with the same writer is
byte-reproducible; the actual completion time remains in the manifest.
Schema-breaking changes write a new
`schema=vN` partition instead of rewriting old data.

## DuckDB research catalog

The catalog is disposable. Versioned views normalize schema evolution and
join the ledger to historical files:

- `research_strategy_outcome_v1`
- `put_call_bias_audit_v1`
- `session_data_quality_v1`

The first view has one row per decision and outcome horizon. It exposes the
strategy version, event, frozen feature snapshot, selected contract, delivery
result, SPX MFE/MAE and future option PnL fields. A `*_current` alias is moved
only after a new version passes compatibility tests.

## Production rollout

1. Deploy the package and dependency without enabling it.
2. Enable SQLite shadow writes and fallback spool; retain all existing files.
3. Run closed-hour compaction in dry-run mode against one day.
4. Enable Parquet writes with raw deletion still disabled.
5. Validate row counts, timestamps, replay events and research views over full
   trading sessions.
6. Only after explicit review enable retention for verified source files.
