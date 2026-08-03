# Research boundary

Production is intentionally simple; research may be complex behind a durable,
causal and versioned boundary.

## Ownership split

| Production Rust | Python research |
|---|---|
| Provider/readiness enums and invariants | HMM fitting and state interpretation |
| One decision-time snapshot | Feature exploration and model comparison |
| Deterministic hard gates | Walk-forward calibration and backtests |
| `NO_TRADE` or `MANUAL_CANDIDATE` | Replay and counterfactual comparisons |
| SQLite operational ledger | Parquet analytical history and DuckDB queries |
| Notification intent and receipt lifecycle | Post-close reports and notebooks |

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

Replay is generated once after the session and opened on demand. It is not a
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

HMM is an optional shadow regime feature. It does not own trigger timing,
direction, contract selection, provider readiness, risk authorization or
notifications.

Allowed output is a versioned causal filtered posterior such as
`P(state_t | observations_1..t)`, posterior entropy and dwell information.
Full-sequence smoothing or Viterbi paths that use future observations cannot be
fed back into a historical decision. Latent states remain neutral identifiers
until stable out-of-sample evidence supports an interpretation.

HMM training, evaluation and replay stay in Python. Rust may consume a model
output only after the feature schema, inference semantics, model version and
promotion gate are frozen and explicitly approved. Even then, deterministic
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

A research artifact cannot enter production merely because one replay or one
month looks better. Promotion requires:

1. a frozen policy, feature manifest and cohort hash;
2. expanding or rolling walk-forward evaluation on complete prior days;
3. calibration, coverage, abstention and critical-session strata;
4. non-degradation against the deterministic baseline;
5. explicit handling of missing providers and `10197` periods;
6. reproducible artifacts and sanitized differential fixtures;
7. explicit approval and a rollback plan.

Until all gates pass, research output remains shadow-only and cannot create a
human notification or change a production decision.

## Experimental live projection

The optional `experimental_research_signals.v1` boundary is observational only.
Its atomic document contains a causal filtered regime posterior and zero or one
range for each typed head: `projected_open`, `risk_neutral_close`, and
`hmm_adjusted_close`. `projected_open` may be omitted after the opening target;
range levels must remain strictly ordered and may not be collapsed to a point.
An HMM-adjusted bootstrap range is labeled `experimental_heuristic`, not a
calibrated physical distribution.

`spx-bridge` validates and forwards the document without fitting or interpreting
the model. `spx-core` durably appends the ingress frame and atomically replaces a
separate latest research projection. This path does not call readiness,
candidate generation, the decision ledger, notification intents, or the outbox.
No statistical promotion threshold is encoded in this experimental contract.
