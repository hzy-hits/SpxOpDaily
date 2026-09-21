# Desk pipeline outage visibility and bounded restart recovery

Phase 6 production-fault exception / existing Phase 5 Worker owner. The Sep 18
bridge exhausted systemd's five-start/60-second limit and stayed down until a
manual restart on Sep 21. Live Python market data did not expose this report
pipeline failure to the operator.

## Changes

Existing `maintenance.py` owns an independent pipeline check, scheduled once per
minute at priority 20 by the existing Huey Worker. It checks bridge/report health
JSON timestamps (180-second bound), an explicitly halted bridge, upstream Desk
Map projections not forwarded after 180 seconds, and a half-hour report missing
five minutes after its market-calendar slot. GTH/RTH slots use the existing
exchange calendar; closed weekends do not demand reports. Lack of trading
candidates is never an excuse to omit a scheduled report.

Operational faults and recovery go directly through the existing Feishu sender,
without the Rust bridge/report/delivery ledger or Bark. Identical faults repeat
at most hourly after confirmed delivery; changed faults alert immediately.
Failed or unconfirmed delivery does not consume cooldown. The existing maintenance
disk-alert JSON retains a `desk_pipeline` section; disk and pipeline updates share
its existing file lock and preserve each other's data. No new mutable store.

The existing bridge unit retries after 60 seconds rather than five seconds. Its
five-start/60-second limiter therefore no longer permanently latches routine
runtime failures. This does not clear pending frames, bypass Core rejection,
change the 20 GiB disk reserve, alter manual stop semantics, or restart Gateway.

## Acceptance and limits

Tests cover live processes with missing reports, halted/future heartbeats,
weekend/grace behavior, projection lag, alert delivery failure followed by retry,
cooldown, recovery and preservation of disk state. A clearly labelled real Feishu
channel test was accepted. No Bark transport/config/data was changed or inspected.

This is independent of the Rust report pipeline, not of the entire host/network
or Worker process. A stalled Worker can delay the check; a host outage requires
external monitoring. Persisting a report is not proof of phone receipt. This
monitor does not inspect Bark receipts or claim end-to-end phone acknowledgement.

Deployment uses the existing Python installer, restarts Worker to load the added
periodic job, installs the tracked bridge unit, and daemon-reloads systemd. No
new permanent service, timer, queue, database, dependency or config key is added.

The check also queries only the delivery service's systemd active state; it does
not inspect notification credentials or Bark delivery records. Stopped or unknown
service state generates an independent operational alert. A running service alone
is deliberately not presented as delivery confirmation.

Validation: full Python suite 3,484 passed with two existing dependency warnings;
final delivery-state and monitor cases passed in the targeted 16-test run, and
final module-budget/monitor checks passed (6). Ruff, Import Linter, Rust fmt,
Clippy and workspace tests passed. systemd unit verification reported only existing
unrelated executable-bit warnings on Oracle monitoring units.

Complexity: two existing Python production files and one existing systemd unit
modified; no production files added/deleted; net +123 Python LOC, unit net zero.
No new dependency/config key/permanent service/timer/database/table/store. The
five-second restart storm is replaced by the existing unit's 60-second backoff.
