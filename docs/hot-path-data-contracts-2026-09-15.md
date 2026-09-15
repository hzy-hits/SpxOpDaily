# Session queries, historical preparation and liquidation cash flows

Scope: Phase 5 P5-1 / S6 (existing operational table), Phase 3 P3-2 production latency repair, and S1/S3 valuation semantics. Baseline c8b736cb. No new strategy, process, queue, mutable store or notification owner.

## Changes

The decisions table receives five ordinary scalar columns and one partial covering index. The existing decision writer writes these fields and attributes_json in the same transaction. Alembic 0004 backfills existing selected rows. Session queries explicitly use the covering index and never fetch or JSON-extract the research payload. A decision-scoped ContextVar retains the authorization result only during one build; the next build reads again. Nothing is added to the persisted fact pack to hold that cache. Existing acceptance checks and Bark behavior remain unchanged.

Virtual generated columns were tested and rejected: SQLite still accessed the underlying JSON in the tested query bytecode. The regression checks actual Column/Function opcodes, not merely an index name in EXPLAIN QUERY PLAN. Migration runs before restarting the changed writer. The old writer must not continue producing selected records after the one-time backfill; Core is stopped for this cutover while broker collectors continue.

Physical spot-history decoding and historical joint-surface indexing share one bounded background executor within the existing Core process. Each owner permits one in-flight build and one ready snapshot; repeated decisions do not enqueue repeated jobs. Compact spot rows retain source/arrival timestamps and are filtered by the decision cutoff. File version changes invalidate spot preparation. Surface preparation retains its existing conservative 30-minute knowledge cutoff. Pin terminal-range estimation uses the same prepared spot history. Offline calls remain synchronous and deterministic.

Pending/failed required history cannot authorize its candidate; the selector continues its existing bounded next-candidate evaluation. Preparation returns historical data only: current quotes and candidate economics are evaluated again on the next decision. This removes synchronous history preparation from these live call sites, but does not establish CPU or memory isolation: one Python process still shares resources, and path valuation itself remains synchronous. Existing rolling butterfly preparation is unchanged. No heavy work is moved into the single notification worker.

Management marks now represent actual signed liquidation cash flow: closing a credit position normally costs cash and therefore has a negative value. Net PnL is entry cash flow plus liquidation cash flow minus fees. Inputs are named entry_price and contract_count; PolicyMark.liquidation_value and PolicyLabel.exit_liquidation_value replace the misleading bid names. The 2C-minus-buyback intermediate is deleted throughout the live path and connected research callers. Existing TP/SL, hard exits, gap/censoring behavior and quantity-based fees remain unchanged. This is a representation cleanup on top of prior economic fixes, not a new claim that old research has been recomputed. Existing serialized research artifacts are not rewritten; newly exported exit values use the explicit field name.

## Measured query acceptance

An isolated SQLite copy of Sep 14's 1,163 selected decision rows contains 104,925,152 bytes of JSON. These operational rows are used only to benchmark database access, never to select strategy research samples, construct PnL labels or infer edge. No notification/Bark records were read.

Five fresh SQLite connections per query, with normal OS page cache (not a forced cold disk benchmark): prior JSON projection median 277.977 ms; covering scalar projection median 2.299 ms, approximately 121x faster. Every projected row was identical. The old projection was explicitly constrained to the session index for a fair comparison; unconstrained SQLite planning had selected a strategy-wide index. Migration of this isolated selected-row subset took 3.86 seconds; this is not a production migration duration estimate.

## Acceptance and limitations

Tests cover covering-index bytecode, old-record backfill, unchanged public session results, per-decision cache lifetime, bounded pending jobs and failed preparation, no selection with pending required history, causal late-arrival handling, credit losses/3C stops, exact contract fees, and raw-broker research exits. Full Python/Rust release results and post-cutover runtime measurements are reported with delivery.

Bark, provider selection, exact-leg freshness and trading thresholds are unchanged. Faster valid decisions do not prove profitable signals, nor does a short GTH observation certify the next full RTH session. Research/backtests must continue rebuilding signals, legs and exits from original IBKR/Schwab data at their actual availability times.

Complexity: 10 existing src production files modified, zero src files added/deleted, net src LOC +55. One 39-line Alembic migration added (total production +94 including migration). Dependencies, config keys, services/timers, databases/tables: zero added/removed; existing table +5 columns/+1 index. Removed full-JSON session projection, duplicate per-build authorization reads, synchronous historical preparation at the modified live call sites, the old spot-session loader, and artificial credit policy marks. No compatibility wrapper preserves the deleted cash-flow API.
