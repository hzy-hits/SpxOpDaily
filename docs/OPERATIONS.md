# Operations guide

Status: pre-deployment operating contract. The services described below have
not been installed, enabled, started or connected to production by this work.

## Local validation

Run from the repository root:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo run -p spx-core -- check-config --config config/core.example.toml
cargo run -p spx-delivery -- check-config --config config/delivery.example.toml
```

The examples under `config/` and `systemd/` are templates. Keep the `.example`
suffix in development. Creating production files or enabling services requires
separate, explicit authorization.

The target key/channel pairs allowed by `core.toml` must exactly match active
targets in `delivery.toml`. Core turns an unconfigured request target into
`NO_TRADE`; delivery also dead-letters a missing or channel-mismatched queued
target before transport, so a configuration change cannot silently reroute old
work or create a claim/restart loop.

## Proposed host layout

```text
/opt/spx-spark-core/bin/                 immutable release binaries
/etc/spx-spark-core/core.toml            non-secret core configuration
/etc/spx-spark-core/delivery.toml        non-secret delivery configuration
/etc/spx-spark-core/delivery.env         root-owned secret environment, mode 0600
/run/spx-spark-core/core.sock            local ingress socket
/var/lib/spx-spark-core/ledger/          single SQLite/WAL operational ledger
/var/lib/spx-spark-core/frames/          bounded *.NNNN.ndjson frame segments
/var/lib/spx-spark-core/latest/          replaceable health/projection files
```

Never put broker tokens, API credentials, notification endpoints or private keys
in TOML, systemd units, command lines, logs or replay artifacts. The delivery
example stores only environment-variable names. The corresponding values belong
in the protected environment file or an approved secret manager.

## Process ownership

- One bridge owner emits each provider stream.
- One `spx-core` owner writes decisions and notification intents.
- One `spx-delivery` owner claims targets from the same ledger.
- Research jobs are read-only with respect to the operational ledger.
- No component creates a second outbox database.

The core service needs only local filesystem and Unix-socket access. Delivery is
the only Rust component expected to need outbound network access. The example
units reflect that separation.

`spx-delivery` provides `check-config`, `run`, `once`, `health`, `acknowledge`,
and `replay`.
`health` opens only an already initialized ledger in SQLite read-only mode; a
missing or wrong path fails instead of creating and migrating an empty database.
Network delivery still requires both `network_enabled = true` in TOML and
`--allow-network` on `run` or `once`. The example TOML intentionally leaves the
first gate false.

Both long-running binaries handle `SIGINT`/`SIGTERM`, stop accepting new work,
and release their generation-fenced owner tombstone. A crash still recovers by
lease expiry. Core Unix ingress has a configured connection cap; excess clients
receive `server_busy` instead of creating unbounded threads.

Configuration validation caps owner leases at one hour, delivery claim leases
at five minutes, request timeouts at two minutes, and each of at most ten retry
delays at one day. These are typo guards; production values should remain much
shorter and are still subject to the cross-field lease relationships.

## Start and health acceptance

After a separately authorized installation, verify more than process liveness:

1. exactly one owner holds each core and delivery lease;
2. the Unix socket exists with restrictive permissions;
3. the ledger opens in WAL mode and passes SQLite `quick_check`;
4. provider state has fresh source timestamps and current entitlement;
5. an RTH snapshot selects Schwab first;
6. a GTH snapshot refuses Schwab and requires fresh IBKR exact legs;
7. decisions are only `NO_TRADE` or `MANUAL_CANDIDATE`;
8. notification receipt state agrees with the external target.

Read-only diagnostics after authorization may use:

```bash
systemctl status spx-core.service --no-pager
journalctl -u spx-core.service -n 100 --no-pager
test -S /run/spx-spark-core/core.sock
/opt/spx-spark-core/bin/spx-delivery health --config /etc/spx-spark-core/delivery.toml
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
