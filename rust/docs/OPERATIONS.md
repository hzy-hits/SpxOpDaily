# Operations guide

Status: operating contract. The isolated Oracle core has been installed without
network delivery; bridge activation is bounded to normalized quote mirror
ownership until live-session acceptance is complete.

## Local validation

Run from the Rust workspace. From the monorepo root, enter it first:

```bash
cd rust
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
cargo run --locked -p spx-core -- check-config --config config/core.example.toml
cargo run --locked -p spx-report -- check-config --config config/report.example.toml
cargo run --locked -p spx-delivery -- check-config --config config/delivery.example.toml
cargo run --locked -p spx-bridge -- check-config --config config/bridge.example.toml
```

The examples under `config/` and `systemd/` are templates. Keep the `.example`
suffix in development. Creating production files or enabling services requires
separate, explicit authorization.

`config/oracle-shadow.toml` and `systemd/spx-rust-core-shadow.service` are the
Phase-1 Oracle overlay. They deliberately use independent `*-shadow` paths,
run as the same non-root `ubuntu` identity as the future local mirror bridge,
and retain `PrivateNetwork=true`. The socket is mode `0600`, so this shared
identity is required until a separately tested group-access contract exists.
Oracle raw frames live under
`/srv/data/spx-spark/rust-core-shadow/frames`, not the nearly full root
filesystem. Before every guarded append, core verifies that the write would
leave the configured 20 GiB reserve; an unavailable or exhausted filesystem
stops ingress instead of consuming the host's last bytes.
`config/oracle-shadow-bridge.toml` and
`systemd/spx-rust-normalized-bridge.service` add the read-only normalized
sidecar. The sidecar and core may be enabled for unattended quote mirroring only
after bridge state is explicitly initialized, real source inspection fits the
frame bound, and raw-log retention/free-space monitoring is active.

`config/oracle-shadow-delivery.toml` exists only for the read-only `health`
command. Phase 1 does not install or start a delivery unit, does not create a
delivery secret environment, and keeps `network_enabled=false`.
`systemd/spx-rust-delivery.service` is the installable Oracle system-unit
overlay for the later cutover: it runs as `ubuntu`, shares only the shadow
ledger, reads `/etc/spx-spark-core-shadow/delivery.toml` and protected
`delivery.env`, and allows outbound HTTPS without granting broker access. The
unit existing in Git does not mean it is installed or enabled.

`config/oracle-shadow-report.toml` and
`systemd/spx-rust-report.service` describe the Oracle system-level report
runtime. The checked-in TOML keeps `writer.network_enabled=false` and targets
the non-human `shadow-audit` key; it cannot become the production sender by
accident. A production cutover installs a target-aligned runtime copy, supplies
only `DEEPSEEK_API_KEY` through the protected `report.env`, and enables the
config gate together with the explicit `--allow-network` CLI gate. The report
unit may use outbound HTTPS, can read only core's latest projection, and may
write only the shared ledger and its own health directory.

The target key/channel pairs allowed by `core.toml` must exactly match active
targets in `delivery.toml`. Core turns an unconfigured request target into
`NO_TRADE`; delivery also dead-letters a missing or channel-mismatched queued
target before transport, so a configuration change cannot silently reroute old
work or create a claim/restart loop.

## Proposed host layout

```text
/opt/spx-spark-core/bin/                 immutable release binaries
/etc/spx-spark-core/core.toml            non-secret core configuration
/etc/spx-spark-core/bridge.toml          non-secret normalized source configuration
/etc/spx-spark-core/report.toml          non-secret half-hour report configuration
/etc/spx-spark-core/delivery.toml        non-secret delivery configuration
/etc/spx-spark-core/report.env           root-owned DeepSeek environment, mode 0600
/etc/spx-spark-core/delivery.env         root-owned secret environment, mode 0600
/run/spx-spark-core/core.sock            local ingress socket
/var/lib/spx-spark-core/ledger/          single SQLite/WAL operational ledger
/var/lib/spx-spark-core/frames/          bounded *.NNNN.ndjson frame segments
/var/lib/spx-spark-core/latest/          replaceable health/projection files
/var/lib/spx-spark-bridge/state.json     durable cursor and exact pending frame
/var/lib/spx-spark-bridge/health.json    replaceable bridge health projection
/var/lib/spx-spark-report/health.json    replaceable report health projection
```

The Oracle overlay replaces the generic frame path with
`/srv/data/spx-spark/rust-core-shadow/frames`. Its
`spx-rust-frame-retention.timer` runs hourly with a seven-completed-day policy
and a 40 GiB cap. The retention command considers only strict
`YYYY-MM-DD.NNNN.ndjson` regular files in the configured directory, never
follows symlinks, and never deletes the current or a future UTC date. If those
protected files alone exceed the cap, the timer fails visibly and the append
reserve remains the final fail-closed guard.

Never put broker tokens, API credentials, notification endpoints or private keys
in TOML, systemd units, command lines, logs or replay artifacts. The delivery
example stores only environment-variable names. The corresponding values belong
in the protected environment file or an approved secret manager.

## Process ownership

- One bridge owner emits each provider stream.
- One `spx-core` owner writes decisions and decision-linked intents.
- One `spx-report` owner schedules each active GTH/RTH `:00`/`:30` ET Desk Map and writes
  `scheduled_report` intents.
- One `spx-delivery` owner claims targets from the same ledger.
- Python collectors/research write only bounded atomic source projections;
  research jobs are read-only with respect to the operational ledger.
- No component creates a second outbox database.

The core and bridge need only local filesystem and Unix-socket access.
`spx-report` needs outbound HTTPS only for DeepSeek, and `spx-delivery` needs
outbound HTTPS only for configured notification targets. The units isolate
those two network-capable roles from the broker/session boundary.

The optional strategy-distribution bridge lane strictly reads
`data/latest/strategy_distribution_forecast.json` and publishes the latest
accepted advisory document to `latest/strategy-distribution.json`. It is
independent of quote, research and desk-map forwarding. Its source, contract or
transport failures are visible in bridge health but cannot create an
evaluation, decision, intent or order.

`spx-report` provides `check-config`, `run`, `once`, and `health`. DeepSeek calls
require both `writer.network_enabled=true` and `--allow-network`. The endpoint,
model, thinking mode, reasoning effort and JSON output mode are fixed in the
binary as `https://api.deepseek.com/v1/chat/completions`,
`deepseek-v4-flash`, enabled, `max`, and `response_format=json_object`; a
runtime config cannot redirect the API key or silently choose another model.
The slot grace controls when a generation may start; an in-flight successful
generation is accepted until the source projection expires. The complete
response is accepted only when all eight sections validate and `finish_reason`
is not `length`.

`spx-delivery` provides `check-config`, `run`, `once`, `health`, `acknowledge`,
and `replay`.
`health` opens only an already initialized ledger in SQLite read-only mode; a
missing or wrong path fails instead of creating and migrating an empty database.
Network delivery still requires both `network_enabled = true` in TOML and
`--allow-network` on `run` or `once`. The example TOML intentionally leaves the
first gate false.

The long-running binaries handle `SIGINT`/`SIGTERM`, stop accepting new work,
and release their generation-fenced owner tombstone. A crash still recovers by
lease expiry. Core Unix ingress has a configured connection cap; excess clients
receive `server_busy` instead of creating unbounded threads.

`spx-bridge` provides `check-config`, `inspect`, `init-state`, and `run`.
`inspect` is read-only and prints only source counts, mapping/drop counters,
provider state and encoded frame sizes. `init-state` is explicit and refuses to
overwrite an existing cursor. `run` refuses missing or corrupt state and
persists an exact pending envelope before socket I/O. An adjacent advisory lock
fences a second bridge process, and the bridge's systemd sandbox can write only
its separate state directory, not the core ledger, projection or raw frames.

Treat the core binary, core TOML and systemd unit as one release during
rollback. Stop and disable the bridge first, restore the previous config/unit,
run `systemctl daemon-reload`, then switch the core release and restart it. Do
not point an older strict-config binary at a newer TOML: it will refuse unknown
fields instead of silently ignoring them.

Configuration validation caps owner leases at one hour, delivery claim leases
at five minutes, request timeouts at two minutes, and each of at most ten retry
delays at one day. These are typo guards; production values should remain much
shorter and are still subject to the cross-field lease relationships.

## Oracle half-hour report cutover

Oracle has two different service managers and they must not be confused:

- Rust core, bridge, report and delivery are host system services managed with
  `sudo systemctl` and installed under `/opt/spx-spark-core-shadow` plus
  `/etc/spx-spark-core-shadow`;
- Python collectors and `spx-spark-order-map-status.timer` are `ubuntu` user
  services managed with `systemctl --user` from `/home/ubuntu/spx-spark`.

The Python status timer remains enabled after cutover. It refreshes
`/srv/data/spx-spark/data/latest/desk_map_projection.json`; the owner flag only
removes its legacy report enqueue side effect. Rust bridge/core then publish the
accepted projection to
`/var/lib/spx-spark-core-shadow/latest/desk-map.json`, which is the only input
read by `spx-report`.

Before changing ownership:

1. validate the exact release binaries and all four runtime configs;
2. verify bridge health has independently accepted fresh quote, research,
   desk-map and strategy-distribution lanes, and compare each Python/core
   document or projection ID;
3. make report target keys/channels exactly match active delivery targets;
4. place `DEEPSEEK_API_KEY` only in root-protected `report.env`, and delivery
   endpoints only in root-protected `delivery.env`; never put values in TOML;
5. keep both checked-in network gates false until the coordinated switch;
6. choose a point between `:00` and `:30` ET and verify no unexpired Rust or
   Python report already represents the next slot;
7. verify
   `/etc/spx-spark-core-shadow/rust-report-owner.enabled` is absent. The marker
   is a root-owned startup fence for `spx-rust-report.service`; its absence
   prevents an accidental Rust report start while Python still owns report
   enqueueing.

With deployment authorization, fence the old writer before enabling the new
one:

```bash
systemctl --user stop spx-spark-order-map-status.timer
# A timer stop does not cancel an already-running oneshot. Wait until this
# reports inactive before changing report ownership.
systemctl --user is-active spx-spark-order-map-status.service
# Install SPX_RUST_REPORT_OWNER=true through the existing protected Python
# environment mechanism; do not put the value on a shared command line.
systemctl --user start spx-spark-order-map-status.service

# Only after the manual invocation has produced a fresh projection and no
# legacy scheduled-report row, arm the Rust writer with a root-owned marker.
sudo install -o root -g root -m 0644 /dev/null \
  /etc/spx-spark-core-shadow/rust-report-owner.enabled
sudo systemctl daemon-reload
sudo systemctl enable --now spx-rust-delivery.service
sudo systemctl enable --now spx-rust-report.service
systemctl --user start spx-spark-order-map-status.timer
```

The manual Python invocation must finish with a fresh atomic projection and no
new legacy scheduled-report outbox row. Do not create the owner marker or start
the Rust report service if that check fails. Before starting the unit, verify
the marker is a regular root-owned, non-writable-by-`ubuntu` file (for example,
`stat -c '%F %U:%G %a'` must report a regular file owned by `root:root` with
mode `644` or stricter). Both Rust network units use the marker as a startup
fence; removing it does not stop an already-running process. Enabling
production calls also requires
`writer.network_enabled=true` in `report.toml`, `network_enabled=true` in
`delivery.toml`, and the explicit `--allow-network` already present in the
system units. Both network-capable units retain `AF_UNIX` for resolver/system
services but make `/run/spx-spark-core-shadow` inaccessible, so they cannot
connect to the core ingress socket.

The initial production units intentionally keep the established single-user
`ubuntu` trust boundary. Root-owned `0600` environment files and the systemd
path sandbox prevent ordinary file access, but processes sharing one UID are
not a strict secret-isolation boundary. Dedicated service identities remain a
separate hardening phase, not a claim made by this cutover.

Accept the first live slot only when all of these agree:

- the Python source, bridge ACK and core latest file have the same projection
  ID and the projection is still within `valid_until`;
- report health records GTH/RTH `:00` or `:30` ET, the expected source projection,
  `deepseek-v4-flash`, a non-`length` finish reason, the full visible-body byte
  count and the provider-response hash;
- the ledger has one `scheduled_report` v2 event for that source/slot, no fake
  decision ID and the expected target keys;
- delivery rendered the title and every one of the eight sections and recorded
  a confirmed receipt matching the external sink;
- no Python scheduled-report event was created after the owner switch.

Rollback is also a fenced ownership change. Disable and stop
`spx-rust-report.service` first, immediately remove
`/etc/spx-spark-core-shadow/rust-report-owner.enabled` so an automatic or manual
restart cannot re-arm it, and then disable and stop `spx-rust-delivery.service`.
Use `systemctl disable --now` for both units, not a transient `stop`. Inspect and
resolve all unexpired or uncertain scheduled-report targets without deleting
the ledger. Keep the marker absent. At the next clean slot boundary, stop the
Python status timer, restore
`SPX_RUST_REPORT_OWNER=false`, run one status invocation, and start the timer.
Never allow an old Rust intent and a new Python intent for the same slot to be
deliverable at the same time.

## Start and health acceptance

After a separately authorized installation, verify more than process liveness:

1. exactly one owner holds each core, report and delivery lease;
2. the Unix socket exists with restrictive permissions;
3. the ledger opens in WAL mode and passes SQLite `quick_check`;
4. provider state has fresh source timestamps and current entitlement;
5. an RTH snapshot selects Schwab first;
6. a GTH snapshot refuses Schwab and requires fresh IBKR exact legs;
7. decisions are only `NO_TRADE` or `MANUAL_CANDIDATE`;
8. scheduled reports occur only in active GTH/RTH `:00`/`:30` ET slots, bind to the accepted
   source projection/slot and retain all eight message sections;
9. notification receipt state agrees with the external target;
10. bridge health reports quote, research, desk-map and strategy-distribution
    lanes independently, accepted ACK IDs match, and no frame is pending;
11. mapping counters explain every dropped stale, unsupported or session-unknown quote.

Read-only diagnostics after authorization may use:

```bash
sudo systemctl status spx-rust-core-shadow.service --no-pager
sudo systemctl status spx-rust-normalized-bridge.service --no-pager
sudo systemctl status spx-rust-report.service spx-rust-delivery.service --no-pager
sudo journalctl -u spx-rust-report.service -u spx-rust-delivery.service -n 100 --no-pager
test -S /run/spx-spark-core-shadow/core.sock
/opt/spx-spark-core-shadow/current/bin/spx-report health \
  --config /etc/spx-spark-core-shadow/report.toml
/opt/spx-spark-core-shadow/current/bin/spx-delivery health \
  --config /etc/spx-spark-core-shadow/delivery.toml
/opt/spx-spark-core-shadow/current/bin/spx-bridge inspect \
  --config /etc/spx-spark-core-shadow/bridge.toml
systemctl --user status spx-spark-order-map-status.timer --no-pager
/opt/spx-spark-core-shadow/current/bin/spx-core prune-frames \
  --config /etc/spx-spark-core-shadow/core.toml --keep-completed-days 7 \
  --max-total-bytes 42949672960 --dry-run
sudo systemctl status spx-rust-frame-retention.timer --no-pager
sudo journalctl -u spx-rust-frame-retention.service -n 20 --no-pager
```

Do not log full ingress payloads if they can contain notification endpoints or
other sensitive metadata. Structured logs should use IDs, enum states, ages and
reason codes.

## IBKR `10197` runbook

`10197` means an external/mobile/TWS session owns live-data entitlement.

1. Do not restart Gateway to fight for ownership and do not evict the user.
2. The bridge publishes `operational=external_session_owns`,
   `entitlement=missing` and `reason=competing_session_10197`.
3. The bridge enters bounded backoff and only probes when due.
4. GTH produces `NO_TRADE`; it must not use frozen Schwab SPXW quotes.
5. RTH continues through Schwab if Schwab and both exact legs are fresh.
6. TCP reconnect, process restart or `active` service status does not clear the
   incident. Only a fresh usable IBKR flush may restore live readiness.

## Unknown delivery outcome

When transport has started but the process loses the response, the target is
`uncertain`. This is a terminal operator-review state:

- do not automatically retry;
- inspect the external sink and the attempt/receipt IDs;
- record an explicit operator acknowledgement after review;
- replay only after confirming the sink did not deliver and while TTL is valid;
- replay uses a new replay generation and must retain the original evidence;
- acknowledgement means reviewed, not delivered.

HTTP `429` is the only automatic HTTP retry class because it is an explicit
throttling response. A `5xx` after transport start is `uncertain`: Bark,
Feishu and generic webhooks do not provide a repository-controlled guarantee
that the idempotency header prevents duplicates after a server-side failure.

This rule favors one missed advisory over an unbounded duplicate advisory.

Operator commands are explicit ledger mutations and do not perform network I/O:

```bash
spx-delivery acknowledge --config /etc/spx-spark-core/delivery.toml \
  --target-id '<target-id>' --actor '<operator>' --reason '<review-code>'

spx-delivery replay --config /etc/spx-spark-core/delivery.toml \
  --target-id '<target-id>' --actor '<operator>' --reason '<verification-code>'
```

Acknowledgement never changes a target to delivered. Replay is limited to an
unexpired `dead_letter` or `uncertain` target, increments its replay generation,
and preserves earlier attempts and receipts.

## External acknowledgement contracts

HTTP 2xx alone is sufficient only for a generic webhook. Channel adapters also
validate the documented provider body:

- Bark requires JSON `code = 200`, following the official
  [Bark server API](https://github.com/Finb/bark-server/blob/master/docs/API_V2.md).
- Feishu requires JSON `code = 0`; legacy `StatusCode = 0` remains accepted for
  compatibility with the official
  [custom bot contract](https://open.feishu.cn/document/ukTMukTMukTM/ucTM5YjL3ETO24yNxkjN?lang=zh-CN).

A 2xx response with a missing or unreadable acknowledgement code is `uncertain`,
because the external side effect may have happened. A present, readable
non-success code is a confirmed rejection. Any provider contract change
requires a fixture and transport test before promotion.

## Failure and recovery rules

- Unknown config keys, schema versions and enum values stop startup or reject the
  message; they are not coerced.
- A stale quote or missing exact leg yields `NO_TRADE`, not a cached candidate.
- Preserve the ledger, WAL files and append log during incident analysis.
- Never start a second writer as a quick workaround.
- Backup and restore procedures must preserve the ledger and its WAL-consistent
  state; rehearse them on a copy before production use.
- Deployment, restart, replay and operator acknowledgement are state-changing
  actions and require appropriate authorization.
