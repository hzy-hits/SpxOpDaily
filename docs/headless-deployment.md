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

Stop it:

```bash
scripts/stop-ibgateway.sh
```

Important: Xvfb only supplies a virtual display. To actually interact with the login window on a headless server, use one of:

- SSH X11 forwarding from a machine with an X server.
- VNC/x11vnc attached to the virtual display.
- IBC automation after you have reviewed how credentials will be stored.

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

## Security

- Do not commit `.env`.
- Do not store IBKR credentials in this repository.
- Keep IB Gateway API access on localhost.
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
