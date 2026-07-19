# SPXW Exposure Surface Site

This directory serves the live, read-only SPXW exposure-surface projection and
the Session Replay player. It does not expose account data, order state, or the
general `latest/` directory.

## Access

The nginx sidecar shares the existing code-server network namespace and listens
on port `18082`:

`https://code.zh3nyu.com/proxy/18082/live`

The memorable shortcut redirects into that authenticated code-server route:

- live: `https://spx.zh3nyu.com/`
- session replay: `https://spx.zh3nyu.com/replay`
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

Replay uses four read-only API resources:

- `api/v1/replay/sessions`
- `api/v1/replay/sessions/YYYY-MM-DD/timeline?step_minutes=5`
- `api/v1/replay/sessions/YYYY-MM-DD/trend?role=front&weighting=oi_weighted&metric=signed_gamma`
- `api/v1/replay/sessions/YYYY-MM-DD/frame?at=...`

The compact trend artifact combines the observed intraday SPX path with the
zero-forward-minute Gamma slice from each validated keyframe. The browser draws
a smooth, approximately 30-fps playhead over those real observations; 30 fps is
rendering frequency, not market-data frequency. SPX is held from the latest
known observation and Gamma is held only through its declared validity window,
with gaps shown explicitly. The separate spot-by-forward-time surface is a
collapsed scenario diagnostic. The Y-axis uses a first-observation fixed window
instead of future session extrema. Gamma intensity is normalized per keyframe,
so its sign and location are comparable over time but color depth is not an
absolute cross-time magnitude.

The host service discovers sessions from Parquet, stores an atomic five-minute
bucket catalog, and generates policy-v3 frames on demand. Actual
frame cutoffs are the last complete observed chain in each bucket, so their
seconds need not be `00`. Derived state is stored under:

```text
/srv/data/spx-spark/data/published/spxw-surface/replay-catalog/
/srv/data/spx-spark/data/published/spxw-surface/replay-cache/policy=v3/lookback=*/projection=*/source=*/
/srv/data/spx-spark/data/published/spxw-surface/trend-cache/
```

The original checked Friday v2 replay remains at the exact archival endpoint
`api/v1/replays/2026-07-17T183500Z` and maps to:

`/srv/data/spx-spark/data/published/spxw-surface/replays/2026-07-17T183500Z.json`

Unknown routes and every other `/api/` or `/data/` path return 404. Replay mode
stops the live polling loop and cannot be mistaken for a valid live lease.

Historical Schwab data lacks a real response-finished/availability clock.
Policy v3 therefore labels every frame `bounded_not_proven`, reports the
availability clock as unavailable, still filters all five known clocks, and
requires zero selected lookahead rows. A session appears only after a two-hour
post-close grace period, but that is not proof that compaction finished; the
catalog reports `data_finalization_proven=false`. Do not relabel these artifacts
as strict/proven point-in-time data or finalized source data.

The host directory, rather than the JSON inode, is mounted because the publisher
uses atomic rename. The file remains owner-only (`0600`); nginx runs as the same
configurable UID/GID and does not require broader permissions.

## Run

The live producer must publish at least one snapshot before its endpoint can
return 200. Install and start the Unix-socket replay service first:

```bash
install -m 0644 \
  /home/ubuntu/spx-spark/systemd/spx-spark-surface-replay.service \
  /home/ubuntu/.config/systemd/user/spx-spark-surface-replay.service
install -m 0644 \
  /home/ubuntu/spx-spark/systemd/spx-spark-surface-replay-warm.service \
  /home/ubuntu/spx-spark/systemd/spx-spark-surface-replay-warm.timer \
  /home/ubuntu/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now spx-spark-surface-replay.service
systemctl --user enable --now spx-spark-surface-replay-warm.timer
curl --unix-socket \
  /srv/data/spx-spark/data/published/spxw-surface/runtime/replay-api.sock \
  http://localhost/healthz
```

Then start or recreate the read-only sidecars so nginx receives the runtime
socket-directory mount:

```bash
docker compose -f /home/ubuntu/spx-spark/site/spxw-surface/compose.yaml up -d
docker compose -f /home/ubuntu/spx-spark/site/spxw-surface/compose.yaml ps
```

The replay process runs with low CPU/IO weight and a single advisory generation
lock. The persistent coordination inode is never removed as a stale-lock fix;
kernel `flock` ownership is released when the process exits. It
does not expose TCP; nginx connects through
`published/spxw-surface/runtime/replay-api.sock`. Nginx mounts both the publish
and runtime directories read-only. The timer checks the newest post-close-grace
session catalog at 21:20, 22:20, and 23:20 UTC on weekdays (covering New York
DST), then materializes the default compact trend artifact. It does not touch
live strategy state. Frame and trend URLs use private revalidation.

For a non-production manual QA directory, set `SPXW_SURFACE_PUBLISH_DIR` and
`SPXW_SURFACE_REPLAY_RUNTIME_DIR` before starting Compose. Test fixtures must
never be copied into `public/`; unavailable data produces an empty state.

The archival one-frame generator remains available for an explicitly named
cutoff:

```bash
.venv/bin/python -m spx_spark.surface_dashboard_replay \
  --as-of 2026-07-17T18:35:00Z \
  --data-root /srv/data/spx-spark/data \
  --output-path /srv/data/spx-spark/data/published/spxw-surface/replays/2026-07-17T183500Z.json
```

The generator refuses to overwrite an existing replay. `--force` is reserved
for an explicitly audited replacement. Session Replay never uses `--force`;
lookback, projection-policy, or source-version changes write to a new cache
namespace.
