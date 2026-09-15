# GTH convergence visibility and gate diagnosis

Scope: S1/S3, existing GTH iron-condor evidence and Desk Map owners. Baseline 5659dc75. No new service, model, notification path, or schema migration.

## Production changes

Missing 15-minute displacement, missing ATR or nonpositive ATR means price balance is unknown. It no longer emits the factual assertion that the market is unbalanced. The existing missing-input gate remains closed. Old persisted transitions with both reasons are rendered as missing history too.

Smooth convergence receives a separate observation status: at least 30 existing observations, same-contract 15-minute straddle decay >=3%, 5/15-minute ATM IV changes <=0, and absolute 15-minute displacement <=1.25 ATR5m. Missing inputs yield unavailable. This observation does not require the 10% expansion / 8% peak-retracement sequence. It is explicitly observation_only: it does not grant entry authority or add a score. Existing qualified expansion-to-contraction entries remain available.

Desk Map now lists independently observed blockers together: price history/actual displacement, expansion, retracement, straddle decay, rising IV, peak age, insufficient credit ratio and excessive quote age/source skew. Displacement uses two decimals so 1.25/1.30 are not obscured by one-decimal rounding. A successful smooth observation is displayed separately from the original entry policy. Original thresholds are imported from their owner, not copied into new policy constants.

The GTH butterfly entry gap is not closed by moving the RTH clock. The existing rolling model is trained on matched RTH prefixes and outcomes; extending its clock alone would not create a GTH-trained model or reliable executable edge. No new GTH butterfly authorization is claimed in this release.

## Europe raw-quote diagnostic

Executable script and complete denominators: `/srv/data/spx-spark/research/gth-europe-convergence-2026-09-15/replay.py`, `results.json`.

Dates: 2026-09-09, 09-10, 09-11, 09-14, 09-15. Entry search UTC05:00–09:00; load a 15-minute prefix and endpoints through UTC10:00, capped by the available observation time. Read only original compacted IBKR SPXW records. Rank latest arrivals before filtering quality, so an invalid latest update cannot be hidden by an older live quote. Require received_at<=decision_at, source age<=30 seconds and package source skew<=10 seconds. One-minute snapshots select 199,762 quote rows across 1,330 available minutes. Missing minutes are not manufactured.

Freeze the first same-strike straddle-decay>=3% and nonrising 5/15-minute IV observation each date before checking entry/exit quote completeness. Compare 20Delta/10-wide IC and ATM-centered C/P butterflies of width10/15/20 at that same time; do not search for the best subsequent entry or center. Entry uses conservative ask/bid by signed quantity; liquidation reverses it. Fees are $1.32 per contract per side, counting +1/-2/+1 as four contracts. Report fixed15/30/60-minute endpoint marks, keeping missing entries and exits in the denominator. These are not TP/SL-managed realized results or assumed fills.

|Date|Volatility-screen minutes|First time UTC|IC entry/60-minute mark|
|---|---:|---|---|
|09-09|5|07:52|2.45 credit, below25% gate; net endpoint -$40.56|
|09-10|0|—|No observed qualifying volatility screen|
|09-11|0|—|No observed qualifying volatility screen|
|09-14|6|06:04|Entry four-leg quotes missing|
|09-15|17|05:12|2.75 credit; 60-minute exit quotes missing|

On09-15 the 15/30-minute IC endpoints were -$85.56/-$95.56. On09-09 and09-14, all six ATM butterfly variants passed the debit-price gate but their 60-minute endpoint marks ranged -$70.56 to -$130.56. These are two correlated sessions, not twelve independent validations. Missing endpoints are not zero-PnL trades and are not discarded to compute a success rate.

Limits: this is a volatility-screen/structure diagnostic, not a full production-policy replay. Macro, GCR, exact independent Greeks age, actual fills and price-balance filtering have not all been reconstructed. Spot uses a raw Greeks-underlier proxy. ATM butterflies are a simple baseline, not the existing fitted RTH rolling model. No conclusion about all GTH convergence strategies follows from these few observations. The data do not justify enabling a new entry policy in this change; more signals alone would not establish edge. No strategy-card, NO_TRADE, outbox or Bark history was used.

## Acceptance and complexity

Tests verify unknown versus observed imbalance, zero ATR, simultaneous independent blockers, smooth observation without implicit authorization, and the existing qualified GTH entry. Full release checks and live status are reported with delivery. Bark and broker session ownership are unchanged; a live IBKR10197 conflict still blocks executable GTH advice.

Production files added/deleted:0/0, modified2; net production LOC+10. Dependencies/config keys/services/timers/databases/tables:0 added/removed. Replaced the one-reason-only rendering branch and the missing-history-as-trend classification. No new live entry setup or RTH clock override.
