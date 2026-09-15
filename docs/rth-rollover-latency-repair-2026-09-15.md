# RTH rollover and latency recurrence — 2026-09-15

Scope: S1/S3 and Phase 6 production-fault exception under the architecture simplification execution plan. Baseline `7e95fa1c`. No strategy expansion or notification changes.

## Attribution

Sep 14 raw Schwab ES quotes identify `/ESZ26` (December); IBKR quotes identify expiry `2026-09-18` (September). Both can be fresh and valid. Rejecting a known selected contract because another provider carries a different maturity suppresses legitimate ES sampling. Simply deleting that rejection would also be wrong: globally freshest selection can alternate two maturities and fabricate price jumps.

The repaired normalized sample chooses Schwab during RTH and IBKR outside RTH when both fresh known maturities differ. Existing freshness filtering applies first. Actual selected maturity changes reset the old bar window; unknown selected identity with conflicting sources remains rejected. No splice or artificial backfill is introduced.

Raw five-second Sep 14 replay (received_at <= decision_at, original provider symbols and expiry retained) loaded 7,248 provider updates. Baseline: 2,124 accepted / 2,556 conflicting-contract rejections. Repair: 4,668 accepted / 12 duplicate-or-out-of-order rejections. December was selected 4,667 times; the final 16:00 ET boundary selected September once and reset the old window. This is an uninterrupted sampling counterfactual, not the production scheduler's exact rejection count and not a trade/PnL replay.

## Latency amplification fixed

The existing indexed session-card query still fetched and decoded full research payloads: Sep 14 has 1,163 selected snapshots, about 105 MB of JSON, but only 52 distinct opportunity identities. These counts were inspected solely to diagnose operational work, not to select research samples or infer trading outcomes. Query now projects five authorization fields inside SQLite. Warm measured public-call latency fell from 1.475–1.539 seconds to 0.305–0.584 seconds. Output semantics and exclusion/session fallback remain intact.

The authorization filter now checks each distinct opportunity once per invocation, including negative results. No result is cached between calls; a new acceptance remains visible immediately on the next call. Transport, notification storage, and Bark data/configuration were not changed or read.

The outcome observer's minute-file cache retained up to 64 versions of a growing session file, including unused provider diagnostics. One decoded Sep 14 version retained 26.26 MB in isolation. It now retains two compact versions containing only status and low/high/price. Repeated loading of 70 file versions with 4,303 rows retained 4.67 MB (peak 19.42 MB). Existing invalidation-boundary outcomes remain covered by tests.

## What this does not establish

Sep 14 15:53 ET total cycle was 398.1 seconds, including strategy build 192.6 seconds and persistence/research 195.8 seconds; shock reached 157.5 seconds. The confirmed redundant decoding and cache retention are amplification mechanisms, not proof that they explain all this latency. Service cgroup inspection before restart found MemoryCurrent 2,146,041,856 bytes against MemoryMax 2,147,483,648; MemorySwapCurrent 1,534,894,080 bytes. memory.events recorded 51,143 max-limit encounters, zero OOM kills; memory.pressure full total was 778,409,371 microseconds (cumulative, not attributed to a particular Sep 14 cycle). This establishes real shared memory pressure despite separate feature/shock threads. A process RSS/swap snapshot alone understated the cgroup pressure. A recovered GTH cycle cannot certify next RTH afternoon behavior. Post-deployment full-session latency and quote readiness still require observation; no claim that all missed moves would have been profitable.

## Validation and complexity

Targeted market, rollover, authorization, DB and outcome tests plus raw replay and repeated-version memory benchmark. Full Python/Rust release gates and deployment outcomes are recorded in the delivery response. Removed the obsolete operational_db module-size exemption because the module is now below 1,000 lines.

Five production files modified, zero added/deleted; net production LOC -4. Dependencies/config keys/services/timers/databases/tables: zero added/removed. Removed the redundant session-mode helper and repeated full-payload decode path. Artifacts: `/srv/data/spx-spark/research/rth-rollover-repair-2026-09-15/` (`replay.py`, `es-replay.json`, `cache-benchmark.json`).
