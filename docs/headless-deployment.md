# Headless Deployment Notes

This project is designed to run on a Linux headless host.

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
- IBC automation after you have reviewed how credentials will be stored.

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
scripts/run-ibkr-verifier.sh
```

The verifier writes JSON snapshots to `logs/`.

For a trading-hours entitlement report, run Gateway in live mode and then:

```bash
IBKR_PORT=4001 scripts/run-ibkr-trading-hours-report.sh --skip-options
IBKR_PORT=4001 IBKR_MAX_OPTION_LINES=40 scripts/run-ibkr-trading-hours-report.sh --strict
```

The report uses the same market-data-only connection path as the verifier:
Read-Only API, no startup account fetches, no orders, no positions, and no
executions. Use `--skip-options` for a fast index/ETF/futures check, then remove
it during regular trading hours to validate SPXW bid/ask and model greeks.

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
```

Keep `ALERT_NOTIFY_OPENCLAW_ENABLED=false` for this mode so the user receives
the Codex-confirmed explanation rather than both the raw deterministic alert and
the Codex follow-up.

## Security

- Do not commit `.env`.
- Do not store IBKR credentials in this repository.
- Do not store IBKR credentials in IBC or any other automation until explicitly approved.
- Keep IB Gateway API access on localhost.
- Keep IB Gateway Read-Only API enabled for this project.
- Keep IBKR code market-data only: no orders, account polling, position polling, or execution polling.
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
