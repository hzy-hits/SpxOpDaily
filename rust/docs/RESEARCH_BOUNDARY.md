# Research boundary

<!-- documentation-status: 2026-10-05 -->
> **文档定位：现行运行与参考。** 现行说明；历史段落保留原适用日期。运行状态以实际服务和源字段时钟为准。
> [全仓文档、当前运行状态与合同优先级](../../docs/README.md)（目录核对：2026-10-05）。

Production is intentionally simple; research may be complex behind a durable,
causal and versioned boundary.

## Ownership split

| Frozen Rust runtime | Python production | Python research |
|---|---|---|
| Typed ingress/readiness and bounded frames | Broker collection and field clocks | Feature exploration and raw-broker replay |
| Typed core decision, separate from Python strategy authority | `build_strategy_decision`, exact legs, policy gates | Training, calibration and policy comparison |
| Rust ledger and scheduled-report delivery | Existing operational DB and Huey candidate lane | Parquet/DuckDB analytical history |
| Scheduled reports and validated recovery summaries | Committed decision and Desk Map projection | Advisory model context and post-close artifacts |

The ordinary report writer and deterministic recovery/fallback paths are distinct.
A model response cannot create strategy authority. Full `strategy_decision` and
its per-leg diagnostics remain outside Rust wire. No new ownership transfer is
authorized by this document.

Rust does not query DuckDB in the live path. DuckDB is a query and computation
engine, not the operational source of truth. Parquet and versioned replay
artifacts are the rebuildable analytical record; SQLite is the small mutable
operational record.

## Intraday to post-close flow

```text
intraday
  normalized provider frames
      --> append-only bounded segments
      --> Rust decision snapshot + SQLite decision lineage

post-close
  segments + decision lineage
      --> one immutable Replay artifact
      --> Parquet partitions
      --> Python/DuckDB analysis, HMM and strategy comparison
```

The diagram describes Rust frame audit lineage, not the sampling source for
strategy profitability. Strategy replay must reconstruct signals and exact
option execution from original IBKR/Schwab data available at decision time;
cards, NO_TRADE rows and notification histories cannot select its samples.
Bark remains unchanged and is not an attribution input.

Replay artifacts are generated for bounded research runs and opened on demand. It is not a
permanent production service. A `.duckdb` file may be a disposable local cache,
but it is not shared mutable state and must be reconstructable from Parquet and
the replay manifest.

## Causality contract

Every research feature must carry:

- event/source timestamp;
- time it became available to the system;
- provider and quality;
- feature version and lineage ID;
- missing reason rather than a future-filled value.

At decision time `t`, a model may use only information available at or before
`t`. Future bars, final session high/low/close, post-close OI and later data
repairs are labels or audit evidence, never live features. Train/test splits are
by complete trading day and all normalization, imputation, state ordering and
calibration are fitted inside the training window.

## HMM boundary

HMM is an optional advisory regime/range feature. It does not own trigger
timing, direction, contract selection, provider readiness, risk authorization,
report scheduling, outbox state or notification delivery.

Allowed output is a versioned causal filtered posterior such as
`P(state_t | observations_1..t)`, posterior entropy and dwell information.
Full-sequence smoothing or Viterbi paths that use future observations cannot be
fed back into a historical decision. Latent states remain neutral identifiers
until stable out-of-sample evidence supports an interpretation.

HMM training, evaluation and replay stay in Python. A causal, fresh
`research_context.v2` may be embedded in `desk_map_projection.v1` and shown in
the scheduled Desk Map immediately as clearly labeled advisory context. This
fast shadow iteration does not grant trade authority and does not require the
model to wait for proof of edge. Rust validates the schema, lineage, timestamp,
nullability and document ID but does not fit or reinterpret the HMM. The
projection remains `action_authority=none` and `automatic_ordering=false`.

Promotion to a decision input is separate. It requires frozen inference
semantics, model version and explicit acceptance. Even then, deterministic
freshness, exact-leg, macro and session gates remain authoritative.

## Close range and dealer proxies

An option-implied close distribution is risk-neutral market pricing, not a
calibrated physical forecast. A physical close-range model must be evaluated as
a separate supervised research head, preferably against the risk-neutral
distribution baseline and with out-of-sample interval coverage.

OI/volume GEX, DEX, Vanna, Charm, walls, flip zones and pin candidates are
structural proxies. Without signed open/close flow and defensible participant
inventory, `dealer_position_sign` remains unknown. Research may estimate a
`dealer_pressure_proxy`; production messages may not claim actual market-maker
behavior or inventory.

## Promotion gate

A research artifact cannot gain trade-decision authority merely because one
replay or one month looks better. Promotion requires:

1. a frozen policy, feature manifest and cohort hash;
2. expanding or rolling walk-forward evaluation on complete prior days;
3. calibration, coverage, abstention and critical-session strata;
4. non-degradation against the deterministic baseline;
5. explicit handling of missing providers and `10197` periods;
6. reproducible artifacts and sanitized differential fixtures;
7. explicit approval and a rollback plan.

Until all gates pass, research output remains advisory-only and cannot create a
trade-ready event or change a production decision. It may be displayed inside
the scheduled informational Desk Map when its causal/freshness contract is
valid and its uncertainty and data quality remain visible.

## Live research and desk projection

`research_context.v2` is the current strict advisory boundary. Its atomic
document contains causal filtered regime posterior/entropy, freshness and
lineage, plus explicitly nullable range and close-location heads. Required-null
fields distinguish “not estimated” from a producer that silently omitted part
of the contract. A bootstrap HMM range remains labeled experimental rather than
a calibrated physical distribution.

Python may embed that exact v2 document and matching document ID in a complete
`desk_map_projection.v1`. `spx-bridge` validates and forwards both documents
without fitting or interpreting the model. `spx-core` durably appends each
ingress frame and atomically replaces separate latest research and desk-map
projections.

The research projection alone never calls readiness, candidate generation, the
decision ledger, notification intents or the outbox. The desk-map projection is
different: Rust `spx-report` may use it as the sole factual input to an
informational `scheduled_report` at RTH `:00`/`:30` ET. The resulting report has
a title and eight complete sections, no fake decision ID, and no character/line
compression. It still cannot create a trade-ready event or order. No statistical
promotion threshold is encoded in the advisory display contract.
