# SPXW Exposure Surface Site

This directory serves the live, read-only SPXW exposure-surface projection and
explicitly frozen historical replays. It does not mount the repository,
account data, order state, or the general `latest/` directory.

## Access

The nginx sidecar shares the existing code-server network namespace and listens
on port `18082`:

`https://code.zh3nyu.com/proxy/18082/`

The memorable shortcut redirects into that authenticated code-server route:

- live: `https://spx.zh3nyu.com/`
- Friday replay: `https://spx.zh3nyu.com/friday`

The shortcut service exposes no dashboard data. It listens only on host
loopback port `18084` and returns redirects; code-server still owns the login
gate for the destination.

The corresponding managed-tunnel ingress is intentionally path-free:

```yaml
- hostname: spx.zh3nyu.com
  service: http://127.0.0.1:18084
```

Keep this rule before the final catch-all ingress and validate both the tunnel
rule and the unauthenticated 302/401 chain after recovery on a new host.

The browser polls the relative endpoint `api/v1/snapshot` every five seconds.
The endpoint maps only to the dedicated publisher output:

`/srv/data/spx-spark/data/published/spxw-surface/snapshot.json`

The checked Friday replay is served once from the exact endpoint
`api/v1/replays/2026-07-17T183500Z`. It maps to:

`/srv/data/spx-spark/data/published/spxw-surface/replays/2026-07-17T183500Z.json`

Unknown replay IDs and every other `/api/` or `/data/` path return 404. Replay
mode stops the live polling loop and cannot be mistaken for a valid live lease.

The host directory, rather than the JSON inode, is mounted because the publisher
uses atomic rename. The file remains owner-only (`0600`); nginx runs as the same
configurable UID/GID and does not require broader permissions.

## Run

The producer must publish at least one snapshot before the endpoint can return
200. Then start the sidecar:

```bash
docker compose -f /home/ubuntu/spx-spark/site/spxw-surface/compose.yaml up -d
docker compose -f /home/ubuntu/spx-spark/site/spxw-surface/compose.yaml ps
```

For a non-production manual QA directory, set `SPXW_SURFACE_PUBLISH_DIR` before
starting Compose. Test fixtures must never be copied into `public/`; when the
snapshot is missing or unavailable, the UI intentionally shows an empty state.

On a new host, generate the immutable Friday replay from the normalized
Parquet lake into an empty target with:

```bash
.venv/bin/python -m spx_spark.surface_dashboard_replay \
  --as-of 2026-07-17T18:35:00Z \
  --data-root /srv/data/spx-spark/data \
  --output-path /srv/data/spx-spark/data/published/spxw-surface/replays/2026-07-17T183500Z.json
```

The generator refuses to overwrite an existing replay. `--force` is reserved
for an explicitly audited replacement; normal historical runs use a new replay
ID and path. A same-directory exclusive `.lock` also prevents concurrent first
writes; inspect the owning process before removing a stale lock after a crash.
