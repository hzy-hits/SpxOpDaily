# User-authorized GTH smooth-convergence iron condor

Scope: S1/S3 under the architecture simplification execution plan. Baseline 5ead639c. The user explicitly authorized the new iron-condor entry after reviewing the observation-only implementation and raw-quote diagnostic. This supersedes the observation-only restriction in AGENTS item38 and the entry-authorization conclusion of gth-convergence-diagnostics-2026-09-15.md. That report's empirical observations and limitations remain unchanged.

## Entry contract

GTH now accepts either the existing expansion/retracement evidence or smooth convergence. Smooth convergence requires the existing minimum30 source observations, positive current ATM straddle, comparable same-contract15-minute straddle decay>=3%, 5/15-minute ATM IV changes<=0, and absolute15-minute price displacement<=1.25ATR5m. Required observations or price history missing means unavailable. This route does not require an expansion low, a peak, a peak age, 10% prior expansion or 8% peak retracement.

When both routes qualify, the original expansion route retains attribution. Otherwise the smooth route records entry_kind=smooth_convergence and setup_state=GTH_SMOOTH_CONVERGENCE. The payoff/setup remains IRON_CONDOR_DELTA. Expansion diagnostics remain in expansion_reasons; a successful independent smooth route clears those as entry blockers. This feeds the existing human window, exact20Delta/10-wide leg reselection, candidate lock, ranker, explicit policy authority, and build_strategy_decision. No separate final-candidate path is introduced.

Unchanged execution terms: legal GTH; IBKR four-leg BBO/Greeks age<=30s and BBO skew<=10s; each short leg at the closest absolute Delta not above20%; fixed10-point wings; credit/width25%–55%; each side contributes at least25% of total credit; defined risk<=$1,000; GCR10<=20%; existing macro, session quota/lock and opportunity rules. Buyback<=0.5C takes profit, >=3C stops, and the existing session's12:30ET hard exit remains. 10197, unavailable data, or frozen Schwab quotes cannot authorize the candidate. The example2.40/10 quote remains below the unchanged credit gate.

The GTH evidence contract and schema advance to v4. Global bootstrap policy staysv69 because other setups are unchanged; the existing authority checker now imports the GTH hash from its sole owner instead of duplicating it. The v4 hash is SHA256 of GTH_ENTRY_CONTRACT_VERSION, a frozen version string encoding the authorized settings. New candidates are forward_unvalidated_user_override and manual-only, automatic_ordering=false. No promoted model, positive historical PnL or additional waiting period is imposed as an entry prerequisite for this explicitly authorized route.

The candidate formatter states the actual smooth-decay/IV/displacement basis, rather than claiming a preceding expansion. Only that explanatory text changes in delivery.py; Bark transport, credentials, queue, retry, cooldown and acceptance handling are untouched. No notification records are used for research and no test notification is sent.

## Acceptance

The synthetic same-clock decision fixture uses valid IBKR quotes and independently removes all expansion/peak history. It reaches a real unified IRON_CONDOR decision with manual authority, the new entry attribution, unchanged management policy, and correct candidate text. Counterexamples keep IV expansion, missing ATR,10197,24% credit and31-second quotes closed. The existing expansion route and its source-skew/Greeks/gamma checks remain covered. These tests establish executable business semantics, not profitability. Prior raw-broker diagnostic results are not relabeled or replaced by these fixtures.

Release test results, actual deployed SHA and live quote readiness are reported with delivery. No profitability or universal recovery claim is attached to the authorization.

Complexity: five existing production files modified, zero added/deleted; net production LOC+11. Dependencies, config keys, services/timers, databases/tables:0 added/removed. Removed the observation-only authority restriction and duplicate GTH evidence hash. No new model, process or mutable store.
