# SPXW Notification Image Entry

The Live Surface and Session Replay websites are retired. Production no longer
runs the `spxw-surface` dashboard container, the Core surface projection loop,
the Live/Replay HTTP API, or the replay warmer timer.

The remaining `spxw-surface-entry` container exists only for two fixed,
account-free notification images:

- `https://spx.zh3nyu.com/oi/latest.png`
- `https://spx.zh3nyu.com/strategy-risk/latest.png`

Each exact-match route exposes one atomically replaced PNG. Parent directories,
JSON projections, account data, order state and directory listings remain
unavailable. The strategy-risk sheet is advisory-only and cannot change
strategy authority or enable automatic ordering.

Dashboard aliases `/`, `/live`, `/live/`, `/replay`, `/replay/`, `/sessions`
and `/friday` return HTTP 410. All other unknown routes return 404.

The managed-tunnel ingress remains path-free:

```yaml
- hostname: spx.zh3nyu.com
  service: http://127.0.0.1:18084
```

## Deployment

```bash
docker compose -f /home/ubuntu/spx-spark/site/spxw-surface/compose.yaml \
  up -d --remove-orphans spxw-surface-entry
docker compose -f /home/ubuntu/spx-spark/site/spxw-surface/compose.yaml ps
```

Verify the two images return 200, retired dashboard paths return 410, and the
entry container is healthy. Historical surface and replay data are deliberately
retained under `/srv/data/spx-spark/data/published/spxw-surface`; retirement does
not authorize deleting those artifacts. Replay modules remain available as
explicit internal research tooling but have no production HTTP owner or warmer.
