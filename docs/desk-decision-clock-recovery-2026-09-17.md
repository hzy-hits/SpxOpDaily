# Desk Map decision clock recovery — 2026-09-17

Scope: S1/S3 production repair under the architecture simplification execution plan.
Existing Python report owners only; no Rust, delivery, Bark, strategy threshold,
provider selection, dependency, config or persistence changes.

## Evidence

The 12:00 ET report rejected its strategy export while Core continued producing
NO_TRADE every five seconds. SPX front/next chain requests also returned HTTP 400;
the collector's last successful SPX chain clock was 10:30 ET. These are separate
faults: missing option facts cannot be repaired by making report validation pass.
The report did not persist the exact rejection branch, so the historical rejection
cannot be conclusively assigned to the known clock race.

During investigation the existing localhost Schwab gateway accepted unchanged
SPX chain requests at 120, 80 and 20 strikes. Production subsequently reported no
chain errors, 240 hot symbols and fresh SPX chain data. No request parameter or
freshness threshold was changed. The original HTTP 400 response cause remains
unproven; this repair does not claim to prevent recurrence of that upstream error.

## Change

Each report attempt reads one committed Core decision before report preparation,
then captures its evaluation clock. Report preparation cannot replace that object
with a newer export. The original manifest and future-data checks remain. A second
validation uses elapsed monotonic time to reject a decision that expired while
preparing the report. Retries acquire their own fresh decision and cutoff.

An unavailable decision reference takes priority over the old structure/intent
reason line, including a stale trade_ready intent. Observational structure no
longer explains away missing authorization.

## Acceptance

Regression cases publish a replacement decision during report work: the report
retains the original identity, rejects expiry during preparation, and rejects an
initially future decision even when it becomes temporally valid before completion.
Existing content-hash, decision age, and RTH/GTH fail-closed tests remain in force.
Live read-only report construction after recovery returned a committed decision
with market/options/quote quality READY; this is not an execution or edge claim.

Operational follow-up: monitor SPX front/next HTTP failures separately from
collector process liveness. Do not substitute frozen quotes or widen trading gates.

Release verification: full Python suite 3,479 passed (two existing dependency
warnings); the final additional future-decision case and its two companion cases
passed separately. Ruff, Import Linter, module/complexity budgets, Rust fmt,
workspace Clippy and workspace tests passed. The scheduled 12:30 ET projection
was READY with no quality reasons; report health recorded persistence at
16:30:38 UTC with no error. Phone delivery was not inspected.

Complexity: two existing production files modified, none added/deleted, net +7
production lines. No dependencies, config keys, services/timers, databases/tables
added or removed. Replaced the late latest-decision reread; no compatibility path.
