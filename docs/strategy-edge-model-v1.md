# Strategy Edge Model v1

Status: Draft implementation. No production deployment is included in this change.

## Change Brief

### User-visible goal

Stop treating rule matches or static payoff ratios as trading edge. Existing rules may enumerate legal SPXW structures and enforce quote/data/risk invariants, but only a promoted candidate-level model with a positive conservative expected-PnL bound may authorize a manual trading card.

### Existing owners and files

- Candidate generation and deterministic safety gates: `application/order_map/candidate_factory.py` and `strategy_ranker.py`.
- Sole strategy decision authority: `application/order_map/strategy_select.py`.
- Conservative combination pricing and management replay: `analytics/options/strategy_payoff.py`.
- Historical candidate/outcome persistence: `infrastructure/operational_db.py`.
- Historical option quote replay: `data_platform/research/odte_level_quotes.py` and `strategy_policy_backfill.py`.

### Existing dependencies reused

- `scikit-learn`, NumPy, SQLite, and the existing quote lake.
- Existing selected, no-trade nearest, and rank-2/3 shadow candidate rows.
- Existing conservative entry-at-ask, exit-at-bid combination replay.

### New dependency

None. Runtime inference uses a versioned JSON artifact and pure Python linear algebra. It does not load pickle or joblib files.

### Deleted or superseded authority

No files or services are deleted. Static `selection_score`, setup rules, and the unvalidated P/Q utility may still enumerate and pre-rank legal candidates, but they no longer grant production manual authority when `data_root` is present.

### Persistence, process, and notification impact

- No database migration, service, timer, queue, Rust contract, or notification channel is added.
- Training is a one-shot offline command over the existing SQLite database and quote lake.
- The model artifact is written under `data_root/research/strategy_edge_model.v1.json`.
- Production is fail-closed: missing, stale, unpromoted, malformed, or out-of-domain artifacts produce `NO_TRADE`.
- The system remains manual-only and never places an order.

### Minimal end-to-end acceptance path

1. Run the trainer against the existing two-month database and quote lake through the last completed market session.
2. Inspect the report and confirm at least one model is `promoted` by walk-forward and final-holdout gates.
3. Put the generated artifact at `data_root/research/strategy_edge_model.v1.json`.
4. Run strategy tests/replay and confirm rejected candidates surface model gate reasons.
5. Deploy only after the artifact and code commit are pinned together.

## Scope

Version 1 trains and promotes **debit verticals only**. Butterflies, iron condors, and event structures remain fail-closed unless a later independently validated model family is added. This avoids applying a 20-minute directional management label to a pin strategy with a different holding contract.

The model is candidate-level. A row represents one concrete quoted spread at one causal decision time, not a generic next-bar direction prediction.

## Training labels

The trainer uses a frozen 20-minute debit management contract:

- Entry: conservative synthetic combination ask.
- Valuation/exit: conservative synthetic combination bid.
- Profit arm: +50% return on debit.
- Premium stop: -50% of debit.
- Armed trail: existing 75% peak trail with entry-debit floor.
- Time stop: 20 minutes.
- Session hard close: 15:45 ET.
- Fees: existing per-leg round-trip fee assumption.

Training includes candidate-bearing rows with statuses `selected`, `no_trade`, and `shadow_candidate`, deduplicated at the candidate-opportunity level. This removes the old winner-only bias as far as the persisted candidate universe permits.

## Features

The feature contract is frozen as `strategy_edge_features.v1` and is shared by offline training and runtime inference. It includes:

- candidate economics, bid/ask friction, deltas, IV skew, breakeven/target/stop geometry;
- directional 1m/5m/15m/60m returns normalized by ATR;
- momentum acceleration, VWAP distance/slope/crosses, efficiency and breadth;
- expected move, ATM IV changes and VIX response;
- distances to Put Wall, Call Wall, Zero Gamma and Flip;
- session, time-to-close, regime, event, shock, pin and setup indicators.

All features are built only from the frozen decision payload and candidate quote available at the decision timestamp.

## Models

Each session/structure family is trained independently, initially:

- `rth|vertical`
- `gth|vertical`

The artifact contains three transparent linear models on a shared standardized feature vector:

1. Ridge regression for expected policy PnL.
2. Logistic regression for positive policy PnL probability.
3. Logistic regression for a premium stop occurring within five minutes.

The expected-PnL lower bound is:

```text
predicted expected PnL + 10th percentile walk-forward residual
```

The runtime artifact stores only means, scales, coefficients, intercepts, the residual bound, domain limit, thresholds, training window, and validation metrics.

## Walk-forward validation

Validation is by trading session, never by random minute split:

- expanding-window training;
- next-session out-of-fold prediction;
- final eight sessions held out by default;
- candidate-opportunity deduplication;
- no use of future quote marks in features.

A model is promoted only when all gates pass on both OOF and holdout selections:

- OOF trades >= 60;
- holdout trades >= 15;
- net PnL > 0;
- profit factor >= 1.25;
- average net PnL >= 0.15 SPX points;
- positive traded-session ratio >= 55%;
- maximum drawdown <= 6R;
- best session contributes <= 35% of total positive session PnL.

No manual version bump can override a failed report inside the artifact: `promoted` remains false.

## Runtime authority contract

A candidate can become a manual card only when its promoted model reports all of:

```text
expected_pnl_points >= 0.25
expected_pnl_lcb_points >= 0.10
p_profit >= 0.58
p_stop_first_5m <= 0.30
expected_pnl_lcb_points / max_loss_points >= 0.08
model_coverage == in_domain
```

Passed candidates are ranked by conservative return on risk, then expected PnL, then lower early-stop probability. Rule/static scores no longer select the production winner after this gate.

When `data_root` is intentionally omitted by isolated unit fixtures, the gate preserves legacy fixture behavior without loading an artifact. Every production call supplies `data_root`, so this is not a production bypass.

## Commands

```bash
uv run python -m spx_spark.data_platform.research.strategy_edge_train \
  --database /srv/data/spx.sqlite \
  --data-root /srv/data \
  --start-date 2026-06-19 \
  --end-date 2026-08-17 \
  --artifact /srv/data/research/strategy_edge_model.v1.json \
  --report /srv/data/research/strategy_edge_model.v1.report.json
```

Use the last completed session for `--end-date`; do not train on a partially completed current session and then use that artifact later in the same session. The exact production paths must be taken from deployment settings rather than copied blindly from this example.

## Cutover safeguards

- Do not deploy the code before generating and reviewing the artifact; otherwise all production manual candidates fail closed.
- Pin code commit, artifact version, training-through date, and report together in the deployment record.
- Retrain before the artifact exceeds its 14-day validity window.
- A promoted model authorizes only a manual candidate. It does not alter `automatic_ordering=false`.
