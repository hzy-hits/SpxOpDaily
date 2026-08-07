# Headless Deployment Notes

This project is designed to run on a Linux headless host.

The Oracle checkout is now a monorepo: Python commands run from
`/home/ubuntu/spx-spark`, while Cargo commands run from
`/home/ubuntu/spx-spark/rust`. Rust system-service installation and report-owner
cutover remain separate procedures in `rust/docs/OPERATIONS.md`; pulling one
repository must not implicitly restart or transfer ownership between them.

## IBKR Runtime

Use IB Gateway for unattended collection.

Typical ports:

- Paper: `4002`
- Live: `4001`

Keep the API socket bound to localhost. Do not expose `4001` or `4002` to the network.

This machine currently uses the ARM64 IB Gateway installer and runs Gateway under Xvfb:

```bash
/home/ubuntu/apps/ibgateway/ibgateway
```

Headless startup:

```bash
cd /home/ubuntu/spx-spark
scripts/start-ibgateway-xvfb.sh
tail -f logs/ibgateway.log
```

The startup script uses `setsid` instead of plain `nohup` because this SSH
execution environment can clean up ordinary background children after the shell
returns. It also removes stale X lock/socket files and verifies that the virtual
display is connectable before launching Gateway.

Stop it:

```bash
scripts/stop-ibgateway.sh
```

Important: Xvfb only supplies a virtual display. To actually interact with the login window on a headless server, use one of:

- SSH X11 forwarding from a machine with an X server.
- VNC/x11vnc attached to the virtual display.
- IBC automation after credentials are deliberately configured under `/srv/data`.

Current manual login path:

```bash
cd /home/ubuntu/spx-spark
scripts/start-ibgateway-xvfb.sh
scripts/start-ibgateway-vnc.sh
```

From your local machine:

```bash
ssh -L 5909:127.0.0.1:5909 ubuntu@YOUR_SERVER
```

Then open a local VNC viewer at `127.0.0.1:5909`. The x11vnc bridge is started
with `-localhost -nopw`, so it is reachable only through SSH tunnel or local
processes on the server. Stop it with:

```bash
scripts/stop-ibgateway-vnc.sh
```

## IBC Automation

IBC is installed outside the repository:

```bash
/home/ubuntu/apps/ibc
```

The project wrapper runs IBC against a compatibility tree at
`/home/ubuntu/apps/ibc-ibgateway-compat` so IBC does not rename or mutate the
normal `/home/ubuntu/apps/ibgateway/ibgateway` launcher used by the manual
Xvfb scripts.

Credentials are not stored in git or `.env`. Configure them interactively:

```bash
cd /home/ubuntu/spx-spark
scripts/install-ibc.sh
scripts/configure-ibc-secrets.sh
scripts/install-ibc-service.sh
systemctl --user start ibc-gateway.service
```

The generated file is:

```text
/srv/data/spx-spark/runtime/ibc/config.ini
```

It is written with `600` permissions under a `700` runtime directory.

Default security posture from `scripts/configure-ibc-secrets.sh`:

- `ReadOnlyLogin=no` because IB Gateway does not support read-only login
- `ReadOnlyApi=yes`
- `TrustedTwsApiClientIPs=127.0.0.1`
- `AcceptIncomingConnectionAction=accept`
- `ExistingSessionDetectedAction=secondary`
- `OverrideTwsApiPort=4001` for live or `4002` for paper
- `ReloginAfterSecondFactorAuthenticationTimeout=yes`
- `AutoRestartTime=03:55 AM` (server time) so the daily Gateway restart keeps
  the authenticated session instead of forcing a full relogin and a new 2FA
  round; one 2FA approval then lasts roughly a week

`ExistingSessionDetectedAction=secondary` is intentional. If the phone or
desktop trading session is active, the IBC Gateway session should yield rather
than fight for the broker session. The user service has `Restart=always` with a
60 second delay and `StartLimitIntervalSec=0`, so it keeps retrying forever and
logs back in automatically once the manual session is gone. Do not reintroduce
a finite start limit: with 60-second retries a limit of 60 starts per hour is
exhausted by any manual session longer than an hour, after which systemd marks
the service failed and never logs back in.

## Session Recovery Chain

### Exact-leg quote-demand contract rollout

`ibkr_exact_leg_quote_demand` v2 adds an explicit `right` discriminator so the
same typed wire contract can carry Call or Put debit spreads. Deploy this as a
consumer-first rolling change: restart `spx-spark-ibkr-stream` with the v1/v2
reader before restarting the shock/market-feature writer that emits v2. The new
reader keeps existing v1 Call-only leases valid; an old reader intentionally
rejects v2 and releases or refuses the pin rather than guessing the option
right. Therefore reversing the order is fail-closed, but creates a temporary
loss of exact-leg coverage and is not an accepted production rollout.

What happens when a phone/desktop login preempts the automated session:

1. Gateway detects the existing session and yields (`secondary`); IBC exits.
2. systemd restarts the service every 60 seconds, indefinitely. Each attempt
   yields again while the manual session is still active, so nothing fights
   the human.
3. When the manual session ends, the next attempt logs in and the API port
   comes back.
4. The collector recovery attempt (`IBKR_CONFLICT_PROBE_SECONDS=60`) rebuilds
   the read-only market-data subscriptions. Repeated 10197 responses back off
   exponentially to five minutes.

A brief usable flush starts recovery but does not erase the conflict history.
The circuit closes only after five continuous minutes of fresh live SPXW data.
While it is open or recovering, farm-health automation must not restart IB
Gateway: 10197 is an entitlement-owner conflict, and a second Gateway restart
during the broker-session handoff can make the overlap worse.

Farm-health restart automation is also disabled while the regular ES Globex
session is closed. The daily IBC autorestart intentionally runs in that quiet
window; stale farm flags there are not evidence that a second Gateway restart
would restore market data.

For the failure mode where the Gateway process stays alive but the API is dead
(stuck login dialog, silent session loss), a watchdog timer checks the API port
every 2 minutes and restarts `ibc-gateway.service` after 3 consecutive failures
(`IBC_WATCHDOG_FAILURE_THRESHOLD`). It skips while runtime mode is `protected`
and while the service is disabled, and a healthy port always resets its
counter, so it can never kick a working or manual session:

```bash
scripts/install-ibc-service.sh        # links and enables ibc-watchdog.timer too
systemctl --user list-timers ibc-watchdog.timer
journalctl --user -u ibc-watchdog.service -n 20 --no-pager
```

All units are user services. Enable lingering once, or nothing survives logout
or reboot:

```bash
sudo loginctl enable-linger ubuntu
```

`ReadOnlyApi=yes` is the key API safety gate. Gateway still performs a normal
broker login, so this is not equivalent to TWS read-only login.

Inspect:

```bash
systemctl --user status ibc-gateway.service --no-pager
journalctl --user -u ibc-gateway.service -n 100 --no-pager
scripts/start-ibc-gateway.sh --check
```

Stop:

```bash
systemctl --user stop ibc-gateway.service
# or
scripts/stop-ibc-gateway.sh
```

If IBKR changes a login or security-token dialog and IBC cannot handle it,
use the VNC path above to take over the same Xvfb display manually.

After logging in, keep IB Gateway's Read-Only API setting enabled. This project
does not need API trading permission for data verification or collection.

IB Gateway may still listen on all interfaces even when the UI says only
localhost clients are allowed. Add OS-level loopback-only rules for the common
TWS/Gateway API ports:

```bash
scripts/harden-ibkr-api-localhost.sh
```

These iptables rules are runtime hardening. Re-run the script after reboot unless
you later persist firewall rules through the host firewall manager.

Development flow:

1. Run TWS locally when debugging contracts and symbols.
2. Run IB Gateway on the headless host for continuous collectors.
3. Start with paper trading mode.
4. Confirm market data status with the verifier before writing collectors.

## First Verifier Run

```bash
cd /home/ubuntu/spx-spark
cp .env.example .env
uv sync
uv run spx verify ibkr
```

The verifier writes JSON snapshots to `logs/`.

For a trading-hours entitlement report in the normal Oracle Paper deployment:

```bash
IBKR_PORT=4002 uv run spx report ibkr-hours --skip-options
IBKR_PORT=4002 IBKR_MAX_OPTION_LINES=40 uv run spx report ibkr-hours --strict
```

The report uses the same market-data-only connection path as the verifier:
Read-Only API, no startup account fetches, no orders, no positions, and no
executions. Use `--skip-options` for a fast index/ETF/futures check, then remove
it during regular trading hours to validate SPXW bid/ask and model greeks. GTH
SPXW validation uses the persistent stream: the stateless report deliberately
does not turn raw ES into an SPX strike without the stream's qualified basis.

## systemd User Timer

Install the verifier timer for the current user:

```bash
mkdir -p ~/.config/systemd/user
ln -sfn /home/ubuntu/spx-spark/systemd/spx-ibkr-verifier.service ~/.config/systemd/user/spx-ibkr-verifier.service
ln -sfn /home/ubuntu/spx-spark/systemd/spx-ibkr-verifier.timer ~/.config/systemd/user/spx-ibkr-verifier.timer
systemctl --user daemon-reload
systemctl --user enable --now spx-ibkr-verifier.timer
```

Inspect logs:

```bash
journalctl --user -u spx-ibkr-verifier.service -n 100 --no-pager
```

## 24h Service Loop

The 24h loop is modular. By default it runs public Hyperliquid collection, IV
surface snapshots, and alert evaluation. IBKR collection is disabled unless
`.env` explicitly sets `SPX_SERVICE_ENABLE_IBKR=true`, so the service will not
take the broker session by accident.

Dry check:

```bash
scripts/run-24h-service.sh --print-config
SPX_SERVICE_ENABLE_HYPERLIQUID=false scripts/run-24h-service.sh --once
```

Install the user service:

```bash
mkdir -p ~/.config/systemd/user
ln -sfn /home/ubuntu/spx-spark/systemd/spx-spark-24h.service ~/.config/systemd/user/spx-spark-24h.service
systemctl --user daemon-reload
systemctl --user enable --now spx-spark-24h.service
```

Inspect logs:

```bash
journalctl --user -u spx-spark-24h.service -n 100 --no-pager
journalctl --user -u spx-spark-24h.service -f
```

## OpenClaw Weixin Alerts

OpenClaw is used only for agent analysis and message delivery. It is separate
from IBKR and never has broker credentials.

Gateway should run on loopback:

```bash
openclaw config set gateway.mode local
openclaw config set gateway.bind loopback
openclaw config set gateway.auth.mode none
openclaw gateway install --port 18789 --force
openclaw gateway start
openclaw gateway status
openclaw channels status
```

Install/login Weixin:

```bash
npx -y @tencent-weixin/openclaw-weixin-cli@latest install
```

Test delivery:

```bash
scripts/send-openclaw-test-alert.sh
ALERT_NOTIFY_OPENCLAW_DRY_RUN=false scripts/send-openclaw-test-alert.sh
```

The first command is a dry-run unless `ALERT_NOTIFY_OPENCLAW_DRY_RUN=false` is
set. Real Weixin sends require a valid conversation context token. If the raw
account `userId` returns `sendMessage ret=-2`, send one message from Weixin to
the OpenClaw bot first so the gateway can cache context.

Fast agent-confirmed alert pushes should use the local Codex CLI, then deliver
the resulting confirmation through OpenClaw Weixin:

```env
ALERT_NOTIFY_ENABLED=true
ALERT_NOTIFY_OPENCLAW_ENABLED=false
ALERT_NOTIFY_CODEX_ENABLED=true
ALERT_NOTIFY_CODEX_DELIVER=true
ALERT_NOTIFY_CODEX_MODEL=gpt-5.3-codex-spark
ALERT_NOTIFY_CODEX_REASONING_EFFORT=high
ALERT_NOTIFY_CODEX_REQUIRE_DELIVERY_CUE=true
```

Keep `ALERT_NOTIFY_OPENCLAW_ENABLED=false` for this mode so the user receives
the Codex-confirmed explanation rather than both the raw deterministic alert and
the Codex follow-up. Require an explicit Codex delivery cue in production so
`不需要推送:` smoke-test or degraded-data conclusions do not reach Weixin.

Human-facing Weixin messages are limited to SPX, SPXW option structure, and ES.
Other feeds remain hidden scoring context and are kept out of the Codex prompt's
human-visible explanation.

## Post-Close SPX Review

The production post-close owner is `spx-spark-session-finalize.timer`. It runs
at `18:00 America/New_York` every calendar day, so systemd follows EDT/EST
without duplicate UTC schedules. The application resolves the latest completed
trading day: weekend and exchange-holiday invocations either repair an
unfinished session or verify the existing immutable artifact and exit
idempotently. An `already_published` artifact skips the human LLM/push phase,
so weekend, holiday and `Persistent=true` replays cannot duplicate a report.

The top-level finalizer builds and verifies the deterministic Replay artifact
first, then reuses that same payload for the LLM review and human push. It must
not invoke the older review command for a second full scan. Artifact failure is
terminal for the run and authorizes no raw deletion; an LLM or notification
failure is recorded separately and cannot invalidate an already verified
artifact.

Install the user units through the repository installer:

```bash
scripts/install-spx-spark-services.sh
systemctl --user list-timers \
  spx-spark-session-finalize.timer \
  spx-spark-storage-pressure.timer
```

The installer disables the legacy independent
`spx-spark-post-close-review.timer`. Its service and runner remain available
only for explicit manual compatibility:

```bash
uv run spx job post-close-review --date 2026-08-06
```

An hourly `spx-spark-storage-pressure.timer` runs at `:20` and calls the same finalizer with
`--pressure-check`. It never invokes a separate review and never bypasses the
complete-artifact, source-digest, completed-day, or grace-period gates. Both
paths share `spx-spark-session-finalize.lock`, so a pressure pass cannot race
the 18:00 finalization; the offset also gives the daily owner first access to
the lock.

Inspect a run with:

```bash
systemctl --user status spx-spark-session-finalize.service --no-pager
journalctl --user -u spx-spark-session-finalize.service -n 100 --no-pager
systemctl --user status spx-spark-storage-pressure.service --no-pager
```

Hermes can append this file to the local daily report:

```text
/home/ubuntu/research/finance/daily/spx-options-review/latest-spx-options-review.md
```

## Security

- Do not commit `.env`.
- Do not store IBKR credentials in this repository.
- Store IBKR credentials only in `/srv/data/spx-spark/runtime/ibc/config.ini` after explicit approval.
- Keep IB Gateway API access on localhost.
- Keep IB Gateway Read-Only API enabled for this project.
- Keep the normal Paper market-data deployment free of account and position
  polling. Paper positions are simulation data, not Live-account exposure.
- Keep live order placement disabled. Paper execution tests require an
  explicitly labelled simulation mode.
- Use SSH tunnels for remote dashboard access.
- Keep automatic order placement out of the MVP.

## Data Disk

This host has a separate mounted data disk at `/srv/data`.

Project runtime data should use:

```bash
/srv/data/spx-spark/data
/srv/data/spx-spark/logs
/srv/data/spx-spark/runtime
```

The local ignored `.env` should point maintenance and runtime state there:

```bash
MAINTENANCE_DATA_ROOT=/srv/data/spx-spark/data
MAINTENANCE_LOGS_ROOT=/srv/data/spx-spark/logs
MAINTENANCE_OUTPUT_ROOT=/srv/data/spx-spark/logs
RUNTIME_MODE_PATH=/srv/data/spx-spark/runtime/mode.json
SCHWAB_TOKEN_FILE=/srv/data/spx-spark/runtime/schwab-token.json
```

Keep the repository, virtual environment, and source files under `/home/ubuntu/spx-spark`; keep raw data and runtime tokens under `/srv/data/spx-spark`.

Storage pressure must be evaluated using absolute free bytes as well as a used
percentage. The Rust frame writer reserves 20 GiB, so waiting for a 90% disk
threshold can stop ingress first on the Oracle volume. The typed finalizer
defaults begin artifact-gated action at 28 GiB free, escalate warning at 24 GiB
and become critical at 20 GiB. The systemd units intentionally contain none of
these numbers; deployment overrides belong in `config/runtime.local.toml`.

For the production Schwab callback, single-owner refresh client, Cloudflare Tunnel route,
and localhost gateway deployment, follow
[schwab-cloudflare-oauth.md](schwab-cloudflare-oauth.md).

The REST market-data scheduler is a separate long-running process. It uses the loopback OAuth
gateway and never reads or refreshes the token itself:

```bash
ln -sfn /home/ubuntu/spx-spark/systemd/spx-spark-schwab-marketdata.service \
  ~/.config/systemd/user/spx-spark-schwab-marketdata.service
systemctl --user daemon-reload
systemctl --user enable --now spx-spark-schwab-marketdata.service
```

Keep `schwab.collection.service_loop_enabled=false`; otherwise the 24h loop and the dedicated
service would both schedule the same REST lanes.
